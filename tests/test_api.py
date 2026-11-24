"""API integration tests covering all CatalogIQ REST endpoints and error contracts."""

import asyncio
import json
import os
import time
import httpx
from httpx import ASGITransport
import pytest
import pytest_asyncio

from app.core.config import settings
from app.core.metrics import metrics
from app.core.singletons import init_singletons
from app.db.init_db import init_db
from app.db.session import db_manager
from app.main import app

TEST_API_DB = "test_api_catalog_iq.db"


@pytest_asyncio.fixture
async def client():
    """Provides hermetic httpx AsyncClient with an isolated test SQLite database."""
    original_db_path = db_manager.db_path
    db_manager.db_path = TEST_API_DB
    await init_db()
    init_singletons()
    metrics.reset()

    # Clear tables for absolute test isolation
    await db_manager.execute_write("DELETE FROM products;")
    await db_manager.execute_write("DELETE FROM jobs;")
    await db_manager.execute_write("DELETE FROM dedup_cache;")

    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    await db_manager.close()
    db_manager.db_path = original_db_path
    for ext in ["", "-wal", "-shm"]:
        p = f"{TEST_API_DB}{ext}"
        if os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass


@pytest.mark.asyncio
async def test_health(client: httpx.AsyncClient):
    """GET /api/health -> 200 with exact keys and configured values."""
    res = await client.get("/api/health")
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "ok"
    assert data["llm_provider"] == settings.LLM_PROVIDER
    assert data["llm_concurrency"] == settings.LLM_CONCURRENCY


@pytest.mark.asyncio
async def test_metrics(client: httpx.AsyncClient):
    """GET /api/metrics -> 200 with integer metric counters."""
    res = await client.get("/api/metrics")
    assert res.status_code == 200
    data = res.json()
    assert isinstance(data["llm_calls_total"], int)
    assert isinstance(data["llm_errors_total"], int)
    assert isinstance(data["max_concurrent_llm_calls"], int)


@pytest.mark.asyncio
async def test_post_job_empty_list(client: httpx.AsyncClient):
    """POST /api/jobs with empty products list -> 400 {"error": ...}."""
    res = await client.post("/api/jobs", json={"products": []})
    assert res.status_code == 400
    assert "error" in res.json()
    assert "products list is empty" in res.json()["error"]


@pytest.mark.asyncio
async def test_post_job_duplicate_sku_is_rejected_atomically(client: httpx.AsyncClient):
    res = await client.post("/api/jobs", json={"products": [
        {"sku": "DUP-1", "raw_title": "First item"},
        {"sku": "DUP-1", "raw_title": "Second item"},
    ]})
    assert res.status_code == 400
    assert "Duplicate SKU" in res.json()["error"]
    assert await db_manager.fetch_one("SELECT id FROM jobs") is None


@pytest.mark.asyncio
async def test_post_job_missing_sku(client: httpx.AsyncClient):
    """POST /api/jobs with product missing SKU -> 400 {"error": ...}."""
    res = await client.post(
        "/api/jobs",
        json={"products": [{"raw_title": "Milk without SKU"}]},
    )
    assert res.status_code == 400
    assert "error" in res.json()
    assert "missing required 'sku'" in res.json()["error"]


@pytest.mark.asyncio
async def test_post_job_missing_title(client: httpx.AsyncClient):
    """POST /api/jobs with product missing title -> 400 {"error": ...}."""
    res = await client.post(
        "/api/jobs",
        json={"products": [{"sku": "SKU-NO-TITLE"}]},
    )
    assert res.status_code == 400
    assert "error" in res.json()
    assert "missing required 'raw_title'" in res.json()["error"]


@pytest.mark.asyncio
async def test_post_job_returns_202_fast(client: httpx.AsyncClient):
    """POST /api/jobs with 200 products returns 202 in well under 1 second."""
    products = [
        {"sku": f"PERF-{i:03d}", "raw_title": f"Product Listing {i}", "raw_description": "Sample desc"}
        for i in range(200)
    ]
    start = time.perf_counter()
    res = await client.post("/api/jobs", json={"products": products})
    elapsed = time.perf_counter() - start

    assert res.status_code == 202
    assert elapsed < 1.0, f"Endpoint took {elapsed:.3f}s (must be < 1.0s)"

    job = res.json()
    assert job["id"].startswith("j_")
    assert job["status"] == "queued"
    assert job["total"] == 200


@pytest.mark.asyncio
async def test_get_job_unknown(client: httpx.AsyncClient):
    """GET /api/jobs/<unknown> -> 404 {"error": ...}."""
    res = await client.get("/api/jobs/j_nonexistent")
    assert res.status_code == 404
    assert "error" in res.json()


@pytest.mark.asyncio
async def test_get_job_after_create(client: httpx.AsyncClient):
    """GET /api/jobs/<job_id> returns job status and progress counts."""
    post_res = await client.post(
        "/api/jobs",
        json={"products": [{"sku": "SKU-JOB-1", "raw_title": "Coffee 100g"}]},
    )
    job_id = post_res.json()["id"]

    res = await client.get(f"/api/jobs/{job_id}")
    assert res.status_code == 200
    data = res.json()
    assert data["id"] == job_id
    assert data["total"] == 1
    assert data["status"] in ("queued", "running", "completed")


@pytest.mark.asyncio
async def test_get_products_empty_when_pending(client: httpx.AsyncClient):
    """GET /api/products returns empty items when products are still pending."""
    await client.post(
        "/api/jobs",
        json={"products": [{"sku": "SKU-PENDING-1", "raw_title": "Pending Item"}]},
    )
    res = await client.get("/api/products")
    assert res.status_code == 200
    data = res.json()
    # Pending items must NOT appear in the catalogue
    assert data["items"] == []
    assert data["total"] == 0


@pytest.mark.asyncio
async def test_get_products_after_enrichment(client: httpx.AsyncClient):
    """GET /api/products returns items once they are enriched."""
    await db_manager.execute_write(
        """
        INSERT INTO jobs (id, status, total) VALUES ('j_manual', 'completed', 1);
        """
    )
    await db_manager.execute_write(
        """
        INSERT INTO products (sku, job_id, raw_title, clean_title, category, brand, tags, status, content_hash)
        VALUES ('SKU-MANUAL-1', 'j_manual', 'AMUL BUTTER', 'Amul Butter', 'Groceries', 'Amul', '["dairy"]', 'enriched', 'h_man');
        """
    )

    res = await client.get("/api/products")
    assert res.status_code == 200
    data = res.json()
    assert len(data["items"]) == 1
    item = data["items"][0]
    assert item["sku"] == "SKU-MANUAL-1"
    assert item["clean_title"] == "Amul Butter"
    assert item["tags"] == ["dairy"]


@pytest.mark.asyncio
async def test_product_search_includes_brand_tags_and_cursor(client: httpx.AsyncClient):
    await db_manager.execute_write("INSERT INTO jobs (id,status,total) VALUES ('j_search','completed',3)")
    rows = [
        ("A-01", "j_search", "Amul butter", "Clean Butter", "Groceries", "Amul", '["dairy"]', "enriched", "h1"),
        ("A-02", "j_search", "Milk item", "Clean Milk", "Groceries", "OtherBrand", '["amul"]', "pending_review", "h2"),
        ("A-03", "j_search", "Other listing", "Other Clean", "Other", None, "[]", "enriched", "h3"),
    ]
    await db_manager.execute_write_many(
        "INSERT INTO products(sku,job_id,raw_title,clean_title,category,brand,tags,status,content_hash) VALUES(?,?,?,?,?,?,?,?,?)", rows
    )
    res = await client.get("/api/products?q=amul")
    assert res.status_code == 200
    assert {item["sku"] for item in res.json()["items"]} == {"A-01", "A-02"}
    review = await client.get("/api/products?status=pending_review")
    assert [item["sku"] for item in review.json()["items"]] == ["A-02"]
    first = await client.get("/api/products?page_size=1")
    assert first.json()["next_cursor"] == "A-01"
    second = await client.get("/api/products?page_size=1&cursor=A-01")
    assert second.json()["items"][0]["sku"] == "A-02"
    last = await client.get("/api/products?page_size=1&cursor=A-02")
    assert last.json()["items"][0]["sku"] == "A-03"
    assert last.json()["next_cursor"] is None


@pytest.mark.asyncio
async def test_invalid_status_filter_rejected(client: httpx.AsyncClient):
    res = await client.get("/api/products?status=processing")
    assert res.status_code == 400


@pytest.mark.asyncio
async def test_get_products_pagination(client: httpx.AsyncClient):
    """GET /api/products pagination: 45 items, page=2 & page_size=20 -> 20 items, total=45."""
    await db_manager.execute_write("INSERT INTO jobs (id, status, total) VALUES ('j_page', 'completed', 45);")
    rows = [
        (f"SKU-PAGE-{i:03d}", "j_page", f"Title {i}", f"Clean Title {i}", "Groceries", "enriched", f"h_{i}")
        for i in range(45)
    ]
    await db_manager.execute_write_many(
        """
        INSERT INTO products (sku, job_id, raw_title, clean_title, category, status, content_hash)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

    res = await client.get("/api/products?page=2&page_size=20")
    assert res.status_code == 200
    data = res.json()
    assert data["page"] == 2
    assert data["page_size"] == 20
    assert data["total"] == 45
    assert len(data["items"]) == 20
    assert data["items"][0]["sku"] == "SKU-PAGE-020"


@pytest.mark.asyncio
async def test_get_products_non_numeric_page(client: httpx.AsyncClient):
    """GET /api/products?page=abc -> 400 {"error": ...}."""
    res = await client.get("/api/products?page=abc")
    assert res.status_code == 400
    assert "error" in res.json()


@pytest.mark.asyncio
async def test_get_products_category_filter(client: httpx.AsyncClient):
    """GET /api/products?category=Groceries filters exclusively for that category."""
    await db_manager.execute_write("INSERT INTO jobs (id, status, total) VALUES ('j_cat', 'completed', 2);")
    await db_manager.execute_write(
        """
        INSERT INTO products (sku, job_id, raw_title, clean_title, category, status, content_hash)
        VALUES ('SKU-GROC', 'j_cat', 'Milk', 'Fresh Milk', 'Groceries', 'enriched', 'h1'),
               ('SKU-ELEC', 'j_cat', 'TV', 'Smart TV', 'Electronics', 'enriched', 'h2');
        """
    )

    res = await client.get("/api/products?category=Groceries")
    assert res.status_code == 200
    data = res.json()
    assert data["total"] == 1
    assert len(data["items"]) == 1
    assert data["items"][0]["category"] == "Groceries"


@pytest.mark.asyncio
async def test_get_products_search(client: httpx.AsyncClient):
    """GET /api/products?q=amul performs case-insensitive search across raw and clean titles."""
    await db_manager.execute_write("INSERT INTO jobs (id, status, total) VALUES ('j_srch', 'completed', 2);")
    await db_manager.execute_write(
        """
        INSERT INTO products (sku, job_id, raw_title, clean_title, category, status, content_hash)
        VALUES ('SKU-1', 'j_srch', 'AMUL Butter 500g', 'Amul Butter', 'Groceries', 'enriched', 'h1'),
               ('SKU-2', 'j_srch', 'Tata Tea Gold', 'Tata Tea', 'Beverages', 'enriched', 'h2');
        """
    )

    res = await client.get("/api/products?q=amul")
    assert res.status_code == 200
    data = res.json()
    assert data["total"] == 1
    assert data["items"][0]["sku"] == "SKU-1"


@pytest.mark.asyncio
async def test_get_product_by_sku(client: httpx.AsyncClient):
    """GET /api/products/<sku> -> 200 if enriched/failed/approved, 404 if unknown."""
    await db_manager.execute_write("INSERT INTO jobs (id, status, total) VALUES ('j_sku', 'completed', 1);")
    await db_manager.execute_write(
        """
        INSERT INTO products (sku, job_id, raw_title, clean_title, category, brand, tags, status, content_hash)
        VALUES ('SKU-EXISTS', 'j_sku', 'Raw', 'Clean', 'Groceries', 'Brand', '["tag1"]', 'enriched', 'h1');
        """
    )

    # Exists
    res = await client.get("/api/products/SKU-EXISTS")
    assert res.status_code == 200
    assert res.json()["sku"] == "SKU-EXISTS"

    # Unknown
    res_404 = await client.get("/api/products/SKU-UNKNOWN")
    assert res_404.status_code == 404
    assert "error" in res_404.json()


@pytest.mark.asyncio
async def test_patch_product_valid(client: httpx.AsyncClient):
    """PATCH /api/products/<sku> updates clean_title, sets status='approved', keeps other fields."""
    await db_manager.execute_write("INSERT INTO jobs (id, status, total) VALUES ('j_patch', 'completed', 1);")
    await db_manager.execute_write(
        """
        INSERT INTO products (sku, job_id, raw_title, clean_title, category, brand, tags, status, content_hash)
        VALUES ('SKU-PATCH-1', 'j_patch', 'Raw Title', 'Old Title', 'Groceries', 'BrandX', '["tag1"]', 'enriched', 'h1');
        """
    )

    res = await client.patch("/api/products/SKU-PATCH-1", json={"clean_title": "Newly Approved Title"})
    assert res.status_code == 200
    data = res.json()
    assert data["sku"] == "SKU-PATCH-1"
    assert data["clean_title"] == "Newly Approved Title"
    assert data["status"] == "approved"
    assert data["category"] == "Groceries"
    assert data["brand"] == "BrandX"


@pytest.mark.asyncio
async def test_patch_invalid_category(client: httpx.AsyncClient):
    """PATCH /api/products/<sku> with category outside allowed 8 -> 400 {"error": ...}."""
    await db_manager.execute_write("INSERT INTO jobs (id, status, total) VALUES ('j_patch_cat', 'completed', 1);")
    await db_manager.execute_write(
        """
        INSERT INTO products (sku, job_id, raw_title, clean_title, category, status, content_hash)
        VALUES ('SKU-PATCH-CAT', 'j_patch_cat', 'Raw', 'Clean', 'Groceries', 'enriched', 'h1');
        """
    )

    res = await client.patch("/api/products/SKU-PATCH-CAT", json={"category": "Toys"})
    assert res.status_code == 400
    assert "error" in res.json()


@pytest.mark.asyncio
async def test_patch_empty_title(client: httpx.AsyncClient):
    """PATCH /api/products/<sku> with empty clean_title -> 400 {"error": ...}."""
    await db_manager.execute_write("INSERT INTO jobs (id, status, total) VALUES ('j_patch_title', 'completed', 1);")
    await db_manager.execute_write(
        """
        INSERT INTO products (sku, job_id, raw_title, clean_title, category, status, content_hash)
        VALUES ('SKU-PATCH-EMPTY', 'j_patch_title', 'Raw', 'Clean', 'Groceries', 'enriched', 'h1');
        """
    )

    res = await client.patch("/api/products/SKU-PATCH-EMPTY", json={"clean_title": "  "})
    assert res.status_code == 400
    assert "error" in res.json()


@pytest.mark.asyncio
async def test_patch_unknown_sku(client: httpx.AsyncClient):
    """PATCH /api/products/<sku> for non-existent SKU -> 404 {"error": ...}."""
    res = await client.patch("/api/products/SKU-NONEXISTENT", json={"clean_title": "Test"})
    assert res.status_code == 404
    assert "error" in res.json()


@pytest.mark.asyncio
async def test_job_timestamp_format(client: httpx.AsyncClient):
    """Verify job timestamp format matches ISO-8601 format (YYYY-MM-DDTHH:MM:SS)."""
    import re
    ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$")

    res = await client.post(
        "/api/jobs",
        json={"products": [{"sku": "SKU-TS-1", "raw_title": "Timestamp Test"}]},
    )
    assert res.status_code == 202
    job = res.json()

    assert ISO_RE.match(job["created_at"]), f"created_at '{job['created_at']}' does not match ISO_RE"
    assert job["started_at"] is None or ISO_RE.match(job["started_at"])
