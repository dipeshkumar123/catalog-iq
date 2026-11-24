"""Database schema creation and startup crash recovery procedures."""

import logging
from typing import Optional
from app.db.session import DatabaseManager, db_manager

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
-- 1. JOBS TABLE
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK(status IN ('queued', 'running', 'completed', 'failed')),
    total INTEGER NOT NULL DEFAULT 0,
    done INTEGER NOT NULL DEFAULT 0,
    failed INTEGER NOT NULL DEFAULT 0,
    cache_hits INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    started_at TEXT,
    finished_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);

-- 2. PRODUCTS TABLE
CREATE TABLE IF NOT EXISTS products (
    sku TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    raw_title TEXT NOT NULL,
    raw_description TEXT,
    clean_title TEXT,
    category TEXT CHECK(category IS NULL OR category IN (
        'Groceries', 'Beverages', 'Personal Care', 'Household',
        'Electronics', 'Fashion', 'Home & Kitchen', 'Other'
    )),
    brand TEXT,
    tags TEXT, -- JSON array of strings, e.g. '["tag1", "tag2"]'
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending', 'in_progress', 'enriched', 'failed', 'approved')),
    error TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    content_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_products_job_status ON products(job_id, status);
CREATE INDEX IF NOT EXISTS idx_products_content_hash ON products(content_hash);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_products_clean_title ON products(clean_title);
CREATE INDEX IF NOT EXISTS idx_products_raw_title ON products(raw_title);

-- 3. DEDUP CACHE TABLE
CREATE TABLE IF NOT EXISTS dedup_cache (
    content_hash TEXT PRIMARY KEY,
    clean_title TEXT NOT NULL,
    category TEXT NOT NULL,
    brand TEXT,
    tags TEXT NOT NULL, -- JSON array of strings
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


async def init_db(manager: Optional[DatabaseManager] = None) -> None:
    """Initializes tables and indexes in SQLite."""
    target_manager = manager or db_manager
    logger.info("Initializing database schema at: %s", target_manager.db_path)
    await target_manager.execute_script(SCHEMA_SQL)
    logger.info("Database schema initialized successfully.")


async def run_crash_recovery(manager: Optional[DatabaseManager] = None) -> int:
    """Scans and recovers interrupted jobs after a crash or restart.

    Procedure:
    1. Query all jobs left in 'queued' or 'running' states.
    2. Reset abandoned 'in_progress' products back to 'pending'.
    3. Recompute ground-truth counts (done, failed) from the products table.
       Assumption: cache_hits cannot be retroactively reconstructed from product
       row status, so the last persisted cache_hits counter is preserved.
    4. Reset job status to 'queued' and re-enqueue to the processing pipeline.

    Returns:
        Number of recovered jobs.
    """
    # Circular import guard
    from app.pipeline.queue import enqueue_job

    target_manager = manager or db_manager
    logger.info("Starting crash recovery scan for interrupted jobs...")

    interrupted_jobs = await target_manager.fetch_all(
        "SELECT id, total, cache_hits FROM jobs WHERE status IN ('queued', 'running')"
    )

    if not interrupted_jobs:
        logger.info("Crash recovery: no interrupted jobs found.")
        return 0

    recovered_count = 0
    for job in interrupted_jobs:
        job_id = job["id"]

        # 1. Reset in_progress products back to pending
        await target_manager.execute_write(
            "UPDATE products SET status = 'pending' WHERE job_id = :job_id AND status = 'in_progress'",
            {"job_id": job_id},
        )

        # 2. Recompute ground-truth counts from products table
        counts = await target_manager.fetch_one(
            """
            SELECT 
                COUNT(CASE WHEN status IN ('enriched', 'approved') THEN 1 END) as count_done,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as count_failed
            FROM products
            WHERE job_id = :job_id
            """,
            {"job_id": job_id},
        )

        done = counts["count_done"] if counts else 0
        failed = counts["count_failed"] if counts else 0

        # 3. Update job row with recomputed progress and reset status to queued
        await target_manager.execute_write(
            """
            UPDATE jobs
            SET status = 'queued',
                done = :done,
                failed = :failed
            WHERE id = :job_id
            """,
            {"job_id": job_id, "done": done, "failed": failed},
        )

        # 4. Re-enqueue job to background pipeline
        await enqueue_job(job_id)
        recovered_count += 1
        logger.info(
            "Recovered interrupted job '%s' (done=%d, failed=%d, re-enqueued).",
            job_id,
            done,
            failed,
        )

    logger.info("Crash recovery: resuming %d interrupted jobs.", recovered_count)
    return recovered_count
