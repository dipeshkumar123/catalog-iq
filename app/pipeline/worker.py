"""Job and product processing pipeline workers."""

import asyncio
import json
import logging
from typing import Any, Dict, Optional

from app.core.config import settings
from app.core.exceptions import RetryExhaustedError
from app.core.metrics import metrics
from app.core.singletons import (
    get_dedup_manager,
    get_llm_provider_instance,
    get_llm_semaphore,
)
from app.core.utils import now_iso
from app.db.session import DatabaseManager, db_manager
from app.pipeline.dedup import content_hash
from app.pipeline.retry import with_retry

logger = logging.getLogger(__name__)

# Module-level in-memory job counters tracking {done, failed, cache_hits} per active job_id
job_counters: Dict[str, Dict[str, int]] = {}


async def _flush_job_counters(job_id: str, db: Optional[DatabaseManager] = None) -> None:
    """Flushes current in-memory job counters to the database."""
    target_db = db or db_manager
    counts = job_counters.get(job_id)
    if not counts:
        return
    await target_db.execute_write(
        """
        UPDATE jobs
        SET done = :done, failed = :failed, cache_hits = :cache_hits
        WHERE id = :id
        """,
        {
            "id": job_id,
            "done": counts["done"],
            "failed": counts["failed"],
            "cache_hits": counts["cache_hits"],
        },
    )


async def _finalize_product(
    job_id: str,
    sku: str,
    result: Optional[Dict[str, Any]],
    failed: bool = False,
    error: Optional[str] = None,
    was_cache_hit: bool = False,
    db: Optional[DatabaseManager] = None,
) -> None:
    """Updates product record in DB and increments job progress counters."""
    target_db = db or db_manager

    if not failed and result is not None:
        tags_json = json.dumps(result.get("tags", []))
        await target_db.execute_write(
            """
            UPDATE products
            SET status = 'enriched',
                clean_title = :clean_title,
                category = :category,
                brand = :brand,
                tags = :tags,
                error = NULL,
                updated_at = :updated_at
            WHERE sku = :sku
            """,
            {
                "sku": sku,
                "clean_title": result["clean_title"],
                "category": result["category"],
                "brand": result.get("brand"),
                "tags": tags_json,
                "updated_at": now_iso(),
            },
        )

        if job_id in job_counters:
            job_counters[job_id]["done"] += 1
            if was_cache_hit:
                job_counters[job_id]["cache_hits"] += 1
    else:
        # Failure path
        await target_db.execute_write(
            """
            UPDATE products
            SET status = 'failed',
                error = :error,
                updated_at = :updated_at
            WHERE sku = :sku
            """,
            {
                "sku": sku,
                "error": error or "Unknown processing error",
                "updated_at": now_iso(),
            },
        )

        if job_id in job_counters:
            job_counters[job_id]["failed"] += 1

    # Batch flush every 25 products to minimize database write frequency
    if job_id in job_counters:
        processed = job_counters[job_id]["done"] + job_counters[job_id]["failed"]
        if processed % 25 == 0:
            await _flush_job_counters(job_id, db=target_db)


async def process_one_product(
    job_id: str,
    product: Dict[str, Any],
    db: Optional[DatabaseManager] = None,
) -> None:
    """Executes enrichment pipeline for a single product listing."""
    target_db = db or db_manager
    sku = product["sku"]
    raw_title = product["raw_title"]
    raw_desc = product.get("raw_description")
    h = content_hash(raw_title, raw_desc)

    # 1. Mark in_progress for crash recovery tracking
    await target_db.execute_write(
        "UPDATE products SET status = 'in_progress', updated_at = :updated_at WHERE sku = :sku",
        {"sku": sku, "updated_at": now_iso()},
    )

    # 2. Define compute_fn: acquire semaphore and track metrics INSIDE each retry attempt
    async def compute_fn() -> Dict[str, Any]:
        async def attempt() -> Dict[str, Any]:
            sem = get_llm_semaphore()
            async with sem:
                async with metrics.track_llm_call():
                    provider = get_llm_provider_instance()
                    return await provider.enrich(raw_title, raw_desc)

        return await with_retry(attempt, max_retries=3, base_delay=0.2, sku=sku)

    # 3. Deduplication: Persistent check -> In-flight -> Leader computation
    dedup = get_dedup_manager()
    try:
        result, was_cache_hit = await dedup.get_or_compute(h, compute_fn)
        await _finalize_product(job_id, sku, result, was_cache_hit=was_cache_hit, db=target_db)
    except RetryExhaustedError as e:
        logger.warning("Product '%s' retry attempts exhausted: %s", sku, e)
        await _finalize_product(job_id, sku, None, failed=True, error=str(e), db=target_db)
    except Exception as e:
        logger.exception("Unexpected error processing product '%s': %s", sku, e)
        await _finalize_product(job_id, sku, None, failed=True, error=f"Unexpected error: {e}", db=target_db)


async def process_job(job_id: str, db: Optional[DatabaseManager] = None) -> None:
    """Orchestrates parallel background execution of all pending products in a job."""
    target_db = db or db_manager
    logger.info("Starting processing for job '%s'.", job_id)

    # Mark job as running
    await target_db.execute_write(
        """
        UPDATE jobs
        SET status = 'running', started_at = :started_at
        WHERE id = :id AND started_at IS NULL
        """,
        {"id": job_id, "started_at": now_iso()},
    )

    # Initialize or load in-memory counters
    job_row = await target_db.fetch_one(
        "SELECT done, failed, cache_hits, total FROM jobs WHERE id = :id",
        {"id": job_id},
    )
    if not job_row:
        logger.error("Job '%s' not found in database.", job_id)
        return

    job_counters[job_id] = {
        "done": job_row.get("done") or 0,
        "failed": job_row.get("failed") or 0,
        "cache_hits": job_row.get("cache_hits") or 0,
    }

    # Fetch all pending products for this job
    pending_products = await target_db.fetch_all(
        "SELECT * FROM products WHERE job_id = :job_id AND status = 'pending' ORDER BY sku ASC",
        {"job_id": job_id},
    )

    if not pending_products:
        logger.info("Job '%s' has no pending products. Marking completed.", job_id)
        await _flush_job_counters(job_id, db=target_db)
        await target_db.execute_write(
            """
            UPDATE jobs
            SET status = 'completed', finished_at = :finished_at
            WHERE id = :id
            """,
            {"id": job_id, "finished_at": now_iso()},
        )
        job_counters.pop(job_id, None)
        return

    # Enqueue pending products into in-memory queue
    product_queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
    for product in pending_products:
        product_queue.put_nowait(product)

    # Worker pool sizing: limited by number of products and concurrency factor
    num_workers = min(len(pending_products), max(settings.LLM_CONCURRENCY * 4, 20))

    async def worker():
        while not product_queue.empty():
            try:
                product = product_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                await process_one_product(job_id, product, db=target_db)
            finally:
                product_queue.task_done()

    # Launch worker tasks and await completion
    workers = [asyncio.create_task(worker()) for _ in range(num_workers)]
    await asyncio.gather(*workers)

    # Finalize job status and flush counters
    final_counts = job_counters.get(job_id, {"done": 0, "failed": 0, "cache_hits": 0})
    await target_db.execute_write(
        """
        UPDATE jobs
        SET status = 'completed',
            finished_at = :finished_at,
            done = :done,
            failed = :failed,
            cache_hits = :cache_hits
        WHERE id = :id
        """,
        {
            "id": job_id,
            "done": final_counts["done"],
            "failed": final_counts["failed"],
            "cache_hits": final_counts["cache_hits"],
            "finished_at": now_iso(),
        },
    )
    job_counters.pop(job_id, None)
    logger.info("Job '%s' completed successfully: %s", job_id, final_counts)
