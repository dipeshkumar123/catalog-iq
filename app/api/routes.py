"""REST API route controllers for CatalogIQ."""

import json
import re
import secrets
import time
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

_product_search_cache: Dict[tuple, tuple[float, Any]] = {}


def invalidate_product_search_cache() -> None:
    _product_search_cache.clear()

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
    seen_skus = set()
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
        if clean_sku in seen_skus:
            raise InvalidProductDataError(f"Duplicate SKU '{clean_sku}' in products list.")
        seen_skus.add(clean_sku)
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
            confidence = NULL,
            review_reasons = '[]',
            attempts = 0,
            content_hash = excluded.content_hash,
            updated_at = excluded.updated_at
        """,
        prepared_rows,
    )
    invalidate_product_search_cache()

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
    status: Optional[str] = Query(None),
    cursor: Optional[str] = Query(None),
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
    terms = re.findall(r"[\w]+", q or "", flags=re.UNICODE)
    fts_query = " AND ".join('"' + token.replace('"', '""') + '"' for token in terms) or None
    if status and status not in {"enriched", "pending_review", "failed", "approved"}:
        raise HTTPException(status_code=400, detail="Invalid product status filter.")
    cache_key = (q or "", category or "", status or "", cursor or "", page_num, page_size_num)
    cached = _product_search_cache.get(cache_key)
    if cached and cached[0] > time.monotonic():
        return cached[1]
    allowed_statuses = [status] if status else ["enriched", "pending_review", "failed", "approved"]
    status_placeholders = ",".join(f":status{i}" for i in range(len(allowed_statuses)))
    cursor_sql = " AND p.sku > :cursor" if cursor else ""

    # Only return products in terminal statuses (enriched, failed, approved)
    base_where_sql = f"p.status IN ({status_placeholders}) AND (:category IS NULL OR p.category = :category) AND (:fts IS NULL OR p.rowid IN (SELECT rowid FROM products_fts WHERE products_fts MATCH :fts))"
    where_sql = base_where_sql + cursor_sql
    params = {"category": category, "fts": fts_query, "limit": page_size_num + 1, "offset": offset, "cursor": cursor}

    # Fetch total count
    total_row = await db_manager.fetch_one(
        f"SELECT COUNT(*) as total FROM products p WHERE {base_where_sql}",
        {"category": category, "fts": fts_query, **{f"status{i}": v for i,v in enumerate(allowed_statuses)}},
    )
    total = total_row["total"] if total_row else 0

    # Fetch paginated rows sorted by SKU
    rows = await db_manager.fetch_all(
        f"""
        SELECT p.sku, p.raw_title, p.raw_description, p.clean_title, p.category, p.brand, p.tags, p.status, p.error, p.confidence, p.review_reasons
        FROM products p
        WHERE {where_sql}
        ORDER BY p.sku ASC
        LIMIT :limit {"" if cursor else "OFFSET :offset"}
        """,
        {**params, **{f"status{i}": v for i,v in enumerate(allowed_statuses)}},
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
        try:
            r["review_reasons"] = json.loads(r.get("review_reasons") or "[]")
        except (TypeError, ValueError):
            r["review_reasons"] = []
        items.append(r)

    has_more = len(items) > page_size_num
    items = items[:page_size_num]
    response = {
        "items": items,
        "page": page_num,
        "page_size": page_size_num,
        "total": total,
        "next_cursor": items[-1]["sku"] if has_more and items else None,
    }
    # Bound memory usage and keep result freshness within the documented short TTL.
    if len(_product_search_cache) >= 256:
        _product_search_cache.pop(next(iter(_product_search_cache)))
    _product_search_cache[cache_key] = (time.monotonic() + 60, response)
    return response


@router.get("/products/{sku}")
async def get_product(sku: str):
    """Retrieves a single product listing by SKU."""
    row = await db_manager.fetch_one(
        """
        SELECT sku, raw_title, raw_description, clean_title, category, brand, tags, status, error, confidence, review_reasons
        FROM products
        WHERE sku = :sku AND status IN ('enriched', 'pending_review', 'failed', 'approved')
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
    try:
        row["review_reasons"] = json.loads(row.get("review_reasons") or "[]")
    except (TypeError, ValueError):
        row["review_reasons"] = []

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
        "SELECT * FROM products WHERE sku = :sku AND status IN ('enriched', 'pending_review', 'failed', 'approved')",
        {"sku": sku},
    )
    if not existing:
        raise ProductNotFoundError(sku)

    update_fields: Dict[str, Any] = {}

    if "clean_title" in body:
        clean_title = body["clean_title"]
        if not isinstance(clean_title, str) or not clean_title.strip():
            raise HTTPException(status_code=400, detail="clean_title must be a non-empty string.")
        if not 5 <= len(clean_title.strip()) <= 100:
            raise HTTPException(status_code=400, detail="clean_title must be between 5 and 100 characters.")
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
            if not isinstance(t, str) or t.strip() != t.strip().lower() or (t.strip() and not re.fullmatch(r"[\w]+(?:[-'][\w]+)*", t.strip(), flags=re.UNICODE)):
                raise HTTPException(status_code=400, detail=f"Tag '{t}' must be a lowercase single word.")
        update_fields["tags"] = json.dumps([t.strip() for t in tags if t.strip()])

    if "brand" in body:
        brand = body["brand"]
        if brand is not None and (not isinstance(brand, str) or len(brand.strip()) > 120):
            raise HTTPException(status_code=400, detail="brand must be null or a string up to 120 characters.")
        update_fields["brand"] = brand.strip() if isinstance(brand, str) and brand.strip() else None

    # Build update SQL
    set_clauses = ["status = 'approved'", "review_reasons = '[]'", "error = NULL", "updated_at = :updated_at"]
    params: Dict[str, Any] = {"sku": sku, "updated_at": now_iso()}

    for col, val in update_fields.items():
        set_clauses.append(f"{col} = :{col}")
        params[col] = val

    sql = f"UPDATE products SET {', '.join(set_clauses)} WHERE sku = :sku"
    await db_manager.execute_write(sql, params)
    invalidate_product_search_cache()

    # Return updated product
    return await get_product(sku)
