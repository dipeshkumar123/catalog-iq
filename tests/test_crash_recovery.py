"""Crash recovery tests verifying recovery of interrupted jobs and orphaned in-progress products."""

import os
import pytest
import pytest_asyncio

from app.db.init_db import init_db, run_crash_recovery
from app.db.session import DatabaseManager
from app.pipeline.queue import get_job_queue

TEST_CRASH_DB = "test_crash_recovery_catalog_iq.db"


@pytest_asyncio.fixture
async def crash_env():
    """Sets up an isolated test database for crash recovery simulation."""
    for ext in ["", "-wal", "-shm"]:
        p = f"{TEST_CRASH_DB}{ext}"
        if os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass

    manager = DatabaseManager(db_path=TEST_CRASH_DB)
    await init_db(manager)

    # Empty job queue before test
    queue = get_job_queue()
    while not queue.empty():
        queue.get_nowait()

    yield manager

    await manager.close()
    for ext in ["", "-wal", "-shm"]:
        p = f"{TEST_CRASH_DB}{ext}"
        if os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass


@pytest.mark.asyncio
async def test_crash_recovery_resets_in_progress_and_reenqueues(crash_env: DatabaseManager):
    """Verify that interrupted jobs with mid-flight products are recovered without repeating work.

    Scenario:
    - 1 job in 'running' status (total=10)
    - 3 products are 'enriched'
    - 2 products were 'in_progress' when the server died
    - 5 products were 'pending'
    """
    job_id = "j_interrupted_100"

    # 1. Insert interrupted job
    await crash_env.execute_write(
        """
        INSERT INTO jobs (id, status, total, done, failed, cache_hits)
        VALUES (:id, 'running', 10, 0, 0, 1)
        """,
        {"id": job_id},
    )

    # 2. Insert products across various statuses
    # 3 Enriched
    for i in range(3):
        await crash_env.execute_write(
            """
            INSERT INTO products (sku, job_id, raw_title, clean_title, category, status, content_hash)
            VALUES (:sku, :job_id, :raw, 'Clean', 'Groceries', 'enriched', :h)
            """,
            {"sku": f"SKU-DONE-{i}", "job_id": job_id, "raw": f"Enriched {i}", "h": f"h_done_{i}"},
        )

    # 2 In-Progress (orphaned mid-call)
    for i in range(2):
        await crash_env.execute_write(
            """
            INSERT INTO products (sku, job_id, raw_title, status, content_hash)
            VALUES (:sku, :job_id, :raw, 'in_progress', :h)
            """,
            {"sku": f"SKU-INPROG-{i}", "job_id": job_id, "raw": f"In-Progress {i}", "h": f"h_inp_{i}"},
        )

    # 5 Pending
    for i in range(5):
        await crash_env.execute_write(
            """
            INSERT INTO products (sku, job_id, raw_title, status, content_hash)
            VALUES (:sku, :job_id, :raw, 'pending', :h)
            """,
            {"sku": f"SKU-PEND-{i}", "job_id": job_id, "raw": f"Pending {i}", "h": f"h_pend_{i}"},
        )

    # 3. Execute crash recovery
    recovered_count = await run_crash_recovery(crash_env)
    assert recovered_count == 1

    # 4. Verify in_progress products were reset back to pending (2 + 5 = 7 total pending)
    pending_rows = await crash_env.fetch_all(
        "SELECT sku FROM products WHERE job_id = :job_id AND status = 'pending'",
        {"job_id": job_id},
    )
    assert len(pending_rows) == 7

    # 5. Verify no products remain in 'in_progress'
    in_prog_rows = await crash_env.fetch_all(
        "SELECT sku FROM products WHERE job_id = :job_id AND status = 'in_progress'",
        {"job_id": job_id},
    )
    assert len(in_prog_rows) == 0

    # 6. Verify job status updated to 'queued', done=3, failed=0
    job = await crash_env.fetch_one("SELECT * FROM jobs WHERE id = :id", {"id": job_id})
    assert job["status"] == "queued"
    assert job["done"] == 3
    assert job["failed"] == 0
    assert job["cache_hits"] == 1  # Last persisted cache_hits preserved

    # 7. Verify job was re-enqueued onto the background job queue
    queue = get_job_queue()
    assert not queue.empty()
    enqueued_job_id = queue.get_nowait()
    assert enqueued_job_id == job_id
