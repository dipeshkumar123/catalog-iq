"""REST API route controllers for CatalogIQ."""

import json
import secrets
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from app.core.config import settings
from app.core.exceptions import (
    InvalidProductDataError,
    JobNotFoundError,
    ProductNotFoundError,
)
from app.core.metrics import metrics
from app.core.utils import now_iso
from app.db.session import db_manager
from app.llm.validator import ALLOWED_CATEGORIES
from app.pipeline.dedup import content_hash
from app.pipeline.queue import enqueue_job

router = APIRouter(prefix="/api")


@router.get("/health")
async def get_health():
    """Health check endpoint exposing active configuration."""
    return {
        "status": "ok",
        "llm_provider": settings.LLM_PROVIDER,
        "llm_concurrency": settings.LLM_CONCURRENCY,
    }


@router.get("/metrics")
async def get_metrics():
    """Metrics endpoint reporting LLM call counters and high-water mark concurrency."""
    return {
        "llm_calls_total": metrics.llm_calls_total,
        "llm_errors_total": metrics.llm_errors_total,
        "max_concurrent_llm_calls": metrics.max_concurrent_llm_calls,
    }


@router.post("/jobs", status_code=202)
async def create_job(request: Request):
    """Enqueues a new catalogue ingestion job and returns within 1 second.

    Returns:
        HTTP 202 with newly created job object.
    """
    try:
        body = await request.json()
    except Exception:
        raise InvalidProductDataError("Invalid JSON request body.")

    if not isinstance(body, dict) or "products" not in body:
        raise InvalidProductDataError("Request body must contain 'products' list.")

    products_data = body["products"]
    if not isinstance(products_data, list) or len(products_data) == 0:
        raise InvalidProductDataError("products list is empty")

    # Validate all product listings before any DB operations
    prepared_rows: List[tuple] = []
    now_ts = now_iso()
    job_id = "j_" + secrets.token_hex(4)

    for idx, item in enumerate(products_data):
        if not isinstance(item, dict):
            raise InvalidProductDataError(f"Product item at index {idx} must be an object.")

        sku = item.get("sku")
        if not sku or not str(sku).strip():
            raise InvalidProductDataError(f"Product item at index {idx} is missing required 'sku'.")

        raw_title = item.get("raw_title")
        if not raw_title or not str(raw_title).strip():
            raise InvalidProductDataError(f"Product '{sku}' is missing required 'raw_title'.")

        clean_sku = str(sku).strip()
        clean_raw_title = str(raw_title).strip()
        raw_desc = item.get("raw_description")
        clean_raw_desc = str(raw_desc).strip() if raw_desc is not None and str(raw_desc).strip() else None

        h = content_hash(clean_raw_title, clean_raw_desc)

        prepared_rows.append((
            clean_sku,
            job_id,
            clean_raw_title,
            clean_raw_desc,
            "pending",
            h,
            now_ts,
        ))

    # 1. Insert parent job record
    await db_manager.execute_write(
        """
        INSERT INTO jobs (id, status, total, done, failed, cache_hits, created_at)
        VALUES (:id, 'queued', :total, 0, 0, 0, :created_at)
        """,
        {
            "id": job_id,
            "total": len(prepared_rows),
            "created_at": now_ts,
        },
    )

    # 2. Batch-upsert products using execute_write_many for sub-second ingestion
    await db_manager.execute_write_many(
        """
        INSERT INTO products (sku, job_id, raw_title, raw_description, status, content_hash, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(sku) DO UPDATE SET
            job_id = excluded.job_id,
            raw_title = excluded.raw_title,
            raw_description = excluded.raw_description,
            status = 'pending',
            clean_title = NULL,
            category = NULL,
            brand = NULL,
            tags = NULL,
            error = NULL,
            attempts = 0,
            content_hash = excluded.content_hash,
            updated_at = excluded.updated_at
        """,
        prepared_rows,
    )

    # 3. Non-blocking enqueue to background worker
    await enqueue_job(job_id)

    job_response = {
        "id": job_id,
        "status": "queued",
        "total": len(prepared_rows),
        "done": 0,
        "failed": 0,
        "cache_hits": 0,
        "created_at": now_ts,
        "started_at": None,
        "finished_at": None,
    }
    return JSONResponse(status_code=202, content=job_response)


@router.get("/jobs/{job_id}")
async def get_job(job_id: str):
    """Retrieves live status and progress counts for a specific job."""
    row = await db_manager.fetch_one(
        """
        SELECT id, status, total, done, failed, cache_hits, created_at, started_at, finished_at
        FROM jobs
        WHERE id = :id
        """,
        {"id": job_id},
    )
    if not row:
        raise JobNotFoundError(job_id)
    return row


@router.get("/products")
async def list_products(
    page: str = Query("1"),
    page_size: str = Query("20"),
    category: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
):
    """Lists enriched, approved, and failed products with pagination and search filters."""
    # Validate page
    try:
        page_num = int(page)
    except ValueError:
        raise HTTPException(status_code=400, detail="page must be an integer.")
    if page_num < 1:
        raise HTTPException(status_code=400, detail="page must be >= 1.")

    # Validate page_size
    try:
        page_size_num = int(page_size)
    except ValueError:
        raise HTTPException(status_code=400, detail="page_size must be an integer.")
    if page_size_num < 1:
        raise HTTPException(status_code=400, detail="page_size must be >= 1.")
    if page_size_num > 100:
        page_size_num = 100  # Clamp to max 100

    offset = (page_num - 1) * page_size_num
    q_param = f"%{q.strip()}%" if q and q.strip() else None

    # Only return products in terminal statuses (enriched, failed, approved)
    where_sql = """
        status IN ('enriched', 'failed', 'approved')
        AND (:category IS NULL OR category = :category)
        AND (:q IS NULL OR clean_title LIKE :q OR raw_title LIKE :q)
    """
    params = {
        "category": category,
        "q": q_param,
        "limit": page_size_num,
        "offset": offset,
    }

    # Fetch total count
    total_row = await db_manager.fetch_one(
        f"SELECT COUNT(*) as total FROM products WHERE {where_sql}",
        {"category": category, "q": q_param},
    )
    total = total_row["total"] if total_row else 0

    # Fetch paginated rows sorted by SKU
    rows = await db_manager.fetch_all(
        f"""
        SELECT sku, raw_title, raw_description, clean_title, category, brand, tags, status, error
        FROM products
        WHERE {where_sql}
        ORDER BY sku ASC
        LIMIT :limit OFFSET :offset
        """,
        params,
    )

    items = []
    for r in rows:
        tags_raw = r.get("tags")
        if isinstance(tags_raw, str):
            try:
                r["tags"] = json.loads(tags_raw)
            except Exception:
                r["tags"] = []
        elif tags_raw is None:
            r["tags"] = []
        items.append(r)

    return {
        "items": items,
        "page": page_num,
        "page_size": page_size_num,
        "total": total,
    }


@router.get("/products/{sku}")
async def get_product(sku: str):
    """Retrieves a single product listing by SKU."""
    row = await db_manager.fetch_one(
        """
        SELECT sku, raw_title, raw_description, clean_title, category, brand, tags, status, error
        FROM products
        WHERE sku = :sku AND status IN ('enriched', 'failed', 'approved')
        """,
        {"sku": sku},
    )
    if not row:
        raise ProductNotFoundError(sku)

    tags_raw = row.get("tags")
    if isinstance(tags_raw, str):
        try:
            row["tags"] = json.loads(tags_raw)
        except Exception:
            row["tags"] = []
    elif tags_raw is None:
        row["tags"] = []

    return row


@router.patch("/products/{sku}")
async def patch_product(sku: str, request: Request):
    """Updates product review fields (clean_title, category, tags) and sets status to 'approved'."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body.")

    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object.")

    # Product must exist in terminal state
    existing = await db_manager.fetch_one(
        "SELECT * FROM products WHERE sku = :sku AND status IN ('enriched', 'failed', 'approved')",
        {"sku": sku},
    )
    if not existing:
        raise ProductNotFoundError(sku)

    update_fields: Dict[str, Any] = {}

    if "clean_title" in body:
        clean_title = body["clean_title"]
        if not isinstance(clean_title, str) or not clean_title.strip():
            raise HTTPException(status_code=400, detail="clean_title must be a non-empty string.")
        update_fields["clean_title"] = clean_title.strip()

    if "category" in body:
        category = body["category"]
        if category not in ALLOWED_CATEGORIES:
            raise HTTPException(
                status_code=400,
                detail=f"category must be one of {sorted(ALLOWED_CATEGORIES)}.",
            )
        update_fields["category"] = category

    if "tags" in body:
        tags = body["tags"]
        if not isinstance(tags, list):
            raise HTTPException(status_code=400, detail="tags must be a list of strings.")
        if len(tags) > 5:
            raise HTTPException(status_code=400, detail="tags cannot exceed 5 items.")
        for t in tags:
            if not isinstance(t, str) or t != t.lower():
                raise HTTPException(status_code=400, detail=f"Tag '{t}' must be a lowercase string.")
        update_fields["tags"] = json.dumps([t.strip() for t in tags])

    # Build update SQL
    set_clauses = ["status = 'approved'", "updated_at = :updated_at"]
    params: Dict[str, Any] = {"sku": sku, "updated_at": now_iso()}

    for col, val in update_fields.items():
        set_clauses.append(f"{col} = :{col}")
        params[col] = val

    sql = f"UPDATE products SET {', '.join(set_clauses)} WHERE sku = :sku"
    await db_manager.execute_write(sql, params)

    # Return updated product
    return await get_product(sku)
