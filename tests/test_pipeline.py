"""Integration tests for the CatalogIQ pipeline (workers, concurrency, retries, dedup, and metrics)."""

import asyncio
import json
import os
from typing import Any, Dict, List, Optional
import pytest
import pytest_asyncio

from app.core.config import settings
from app.core.exceptions import LLMError
from app.core.metrics import metrics
from app.core.singletons import (
    set_dedup_manager,
    set_llm_provider_instance,
    set_llm_semaphore,
)
from app.db.init_db import init_db
from app.db.session import DatabaseManager, db_manager
from app.llm.base import BaseLLMProvider
from app.llm.mock import MockLLMProvider
from app.pipeline.dedup import DedupManager, content_hash
from app.pipeline.worker import process_job

TEST_PIPELINE_DB = "test_pipeline_catalog_iq.db"


@pytest_asyncio.fixture
async def pipeline_env():
    """Sets up an isolated database, singletons, and resets metrics for each test."""
    original_db_path = db_manager.db_path
    for ext in ["", "-wal", "-shm"]:
        p = f"{TEST_PIPELINE_DB}{ext}"
        if os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass

    db_manager.db_path = TEST_PIPELINE_DB
    manager = DatabaseManager(db_path=TEST_PIPELINE_DB)
    await init_db(manager)

    # Initialize singletons bound to this test database and reset metrics
    metrics.reset()
    dedup = DedupManager(manager=manager, metrics=metrics)
    set_dedup_manager(dedup)
    set_llm_semaphore(asyncio.Semaphore(settings.LLM_CONCURRENCY))

    yield manager, dedup

    await manager.close()
    # process_job uses db_manager when tests omit the db argument, so close
    # that connection while it still points at the isolated pipeline file.
    await db_manager.close()
    db_manager.db_path = original_db_path
    for ext in ["", "-wal", "-shm"]:
        p = f"{TEST_PIPELINE_DB}{ext}"
        if os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass


async def create_job(
    manager: DatabaseManager,
    job_id: str,
    products: List[Dict[str, Any]],
) -> None:
    """Helper to insert a job and its pending products into SQLite."""
    await manager.execute_write(
        "INSERT INTO jobs (id, status, total, done, failed, cache_hits) VALUES (:id, 'queued', :total, 0, 0, 0)",
        {"id": job_id, "total": len(products)},
    )
    for p in products:
        h = content_hash(p["raw_title"], p.get("raw_description"))
        await manager.execute_write(
            """
            INSERT INTO products (sku, job_id, raw_title, raw_description, status, content_hash)
            VALUES (:sku, :job_id, :raw_title, :raw_description, 'pending', :content_hash)
            """,
            {
                "sku": p["sku"],
                "job_id": job_id,
                "raw_title": p["raw_title"],
                "raw_description": p.get("raw_description"),
                "content_hash": h,
            },
        )


@pytest.mark.asyncio
async def test_a_concurrency_limit(pipeline_env):
    """Test A: Concurrency limit.

    Submit job with 50 products, MOCK_LATENCY_MS=50, LLM_CONCURRENCY=5.
    Assert metrics.max_concurrent_llm_calls == 5 (never exceeds).
    Assert all 50 products end up enriched.
    """
    manager, _ = pipeline_env
    set_llm_semaphore(asyncio.Semaphore(5))
    set_llm_provider_instance(MockLLMProvider(latency_ms=50, failure_rate=0.0))

    products = [
        {"sku": f"CONC-{i:03d}", "raw_title": f"Unique Title {i} 500g", "raw_description": f"Desc {i}"}
        for i in range(50)
    ]
    job_id = "job_test_concurrency_50"
    await create_job(manager, job_id, products)

    await process_job(job_id)

    # Concurrency limit verification
    assert metrics.max_concurrent_llm_calls == 5
    assert metrics.llm_calls_total == 50

    # Job status verification
    job = await manager.fetch_one("SELECT * FROM jobs WHERE id = :id", {"id": job_id})
    assert job["status"] == "completed"
    assert job["done"] == 50
    assert job["failed"] == 0

    # All products enriched
    rows = await manager.fetch_all("SELECT status FROM products WHERE job_id = :id", {"id": job_id})
    assert len(rows) == 50
    assert all(r["status"] == "enriched" for r in rows)


@pytest.mark.asyncio
async def test_b_retry_counting(pipeline_env):
    """Test B: Retry counting.

    Submit a job with 1 product. Fail 3 times with LLMError then succeed.
    Assert metrics.llm_calls_total == 4, product status is enriched.
    """
    manager, _ = pipeline_env

    class FlakyProvider(BaseLLMProvider):
        def __init__(self):
            self.attempts = 0

        async def enrich(self, raw_title: str, raw_description: Optional[str] = None) -> Dict[str, Any]:
            self.attempts += 1
            if self.attempts < 4:
                raise LLMError(f"Transient error on attempt {self.attempts}")
            return {
                "clean_title": "Flaky Product Recovered",
                "category": "Groceries",
                "brand": "FlakyBrand",
                "tags": ["flaky", "groceries"],
            }

    provider = FlakyProvider()
    set_llm_provider_instance(provider)

    job_id = "job_retry_success"
    await create_job(manager, job_id, [{"sku": "SKU-FLAKY-1", "raw_title": "Flaky Item"}])

    await process_job(job_id)

    # 4 calls made (1 initial + 3 retries)
    assert metrics.llm_calls_total == 4
    assert metrics.llm_errors_total == 3

    # Product successfully enriched
    prod = await manager.fetch_one("SELECT * FROM products WHERE sku = 'SKU-FLAKY-1'")
    assert prod["status"] == "pending_review"  # confidence is missing from this test provider
    assert prod["clean_title"] == "Flaky Product Recovered"

    job = await manager.fetch_one("SELECT * FROM jobs WHERE id = :id", {"id": job_id})
    assert job["done"] == 1
    assert job["failed"] == 0


@pytest.mark.asyncio
async def test_c_retry_exhaustion(pipeline_env):
    """Test C: Retry exhaustion.

    Provider always fails. Assert product status is failed, metrics.llm_calls_total == 4,
    metrics.llm_errors_total == 4, job's failed counter == 1.
    """
    manager, _ = pipeline_env

    class AlwaysFailingProvider(BaseLLMProvider):
        async def enrich(self, raw_title: str, raw_description: Optional[str] = None) -> Dict[str, Any]:
            raise LLMError("Permanent upstream error")

    set_llm_provider_instance(AlwaysFailingProvider())

    job_id = "job_retry_fail"
    await create_job(manager, job_id, [{"sku": "SKU-FAIL-1", "raw_title": "Failing Item"}])

    await process_job(job_id)

    # 4 calls made, 4 errors
    assert metrics.llm_calls_total == 4
    assert metrics.llm_errors_total == 4

    # Product marked failed
    prod = await manager.fetch_one("SELECT * FROM products WHERE sku = 'SKU-FAIL-1'")
    assert prod["status"] == "failed"
    assert "Permanent upstream error" in prod["error"]

    job = await manager.fetch_one("SELECT * FROM jobs WHERE id = :id", {"id": job_id})
    assert job["done"] == 0
    assert job["failed"] == 1


@pytest.mark.asyncio
async def test_d_dedup_within_a_job(pipeline_env):
    """Test D: Dedup within a job.

    Submit job with 5 products having identical raw_title + raw_description.
    Assert exactly 1 LLM call in metrics, 4 cache hits, all 5 products enriched.
    """
    manager, _ = pipeline_env
    set_llm_provider_instance(MockLLMProvider(latency_ms=60, failure_rate=0.0))

    products = [
        {"sku": f"SKU-DUP-{i}", "raw_title": "Amul Butter 500g", "raw_description": "pck of 2"}
        for i in range(1, 6)
    ]
    job_id = "job_dedup_intra"
    await create_job(manager, job_id, products)

    await process_job(job_id)

    # Exactly 1 LLM call occurred across all 5 duplicates
    assert metrics.llm_calls_total == 1

    job = await manager.fetch_one("SELECT * FROM jobs WHERE id = :id", {"id": job_id})
    assert job["done"] == 5
    assert job["failed"] == 0
    assert job["cache_hits"] == 4

    # All 5 products enriched
    prods = await manager.fetch_all("SELECT * FROM products WHERE job_id = :id", {"id": job_id})
    assert len(prods) == 5
    for p in prods:
        assert p["status"] == "enriched"
        assert p["clean_title"] == "Amul Butter 500g"


@pytest.mark.asyncio
async def test_e_cache_hit_counter_across_jobs(pipeline_env):
    """Test E: Cache hit counter across jobs.

    Submit job1 with product X, wait for complete, then submit job2 with identical content under different SKU.
    Assert job2 has cache_hits >= 1 and llm_calls_total did NOT increase.
    """
    manager, _ = pipeline_env
    set_llm_provider_instance(MockLLMProvider(latency_ms=20, failure_rate=0.0))

    # Job 1
    job1_id = "job_cross_1"
    await create_job(
        manager,
        job1_id,
        [{"sku": "SKU-CROSS-1", "raw_title": "Tata Tea Gold 1kg", "raw_description": "Leaf tea"}],
    )
    await process_job(job1_id)

    calls_after_job1 = metrics.llm_calls_total
    assert calls_after_job1 == 1

    job1 = await manager.fetch_one("SELECT * FROM jobs WHERE id = :id", {"id": job1_id})
    assert job1["done"] == 1
    assert job1["cache_hits"] == 0

    # Job 2 with identical content under new SKU
    job2_id = "job_cross_2"
    await create_job(
        manager,
        job2_id,
        [{"sku": "SKU-CROSS-2", "raw_title": "Tata Tea Gold 1kg", "raw_description": "Leaf tea"}],
    )
    await process_job(job2_id)

    # LLM calls must NOT increase for job2 (persistent cache hit!)
    assert metrics.llm_calls_total == calls_after_job1

    job2 = await manager.fetch_one("SELECT * FROM jobs WHERE id = :id", {"id": job2_id})
    assert job2["done"] == 1
    assert job2["cache_hits"] == 1

    prod2 = await manager.fetch_one("SELECT * FROM products WHERE sku = 'SKU-CROSS-2'")
    assert prod2["status"] == "enriched"
    assert prod2["clean_title"] == "Tata Tea Gold 1kg"
