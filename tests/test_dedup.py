"""Comprehensive test suite for in-flight and persistent deduplication logic."""

import asyncio
import json
import os
import pytest
import pytest_asyncio

from app.core.exceptions import LLMError
from app.db.init_db import init_db
from app.db.session import DatabaseManager
from app.pipeline.dedup import DedupManager, content_hash

TEST_DEDUP_DB = "test_dedup_catalog_iq.db"


@pytest_asyncio.fixture
async def dedup_env():
    """Provides an isolated DatabaseManager and DedupManager instance."""
    for ext in ["", "-wal", "-shm"]:
        p = f"{TEST_DEDUP_DB}{ext}"
        if os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass

    manager = DatabaseManager(db_path=TEST_DEDUP_DB)
    await init_db(manager)
    dedup = DedupManager(manager=manager)

    yield dedup, manager

    await manager.close()
    for ext in ["", "-wal", "-shm"]:
        p = f"{TEST_DEDUP_DB}{ext}"
        if os.path.exists(p):
            try:
                os.remove(p)
            except OSError:
                pass


@pytest.mark.asyncio
async def test_1_persistent_cache_hit(dedup_env):
    """Test 1: Persistent cache hit — pre-populate dedup_cache, assert was_cache_hit=True and compute_fn not called."""
    dedup, manager = dedup_env
    h = content_hash("Amul Butter 500g", "pack of 2")

    # Pre-populate dedup_cache in SQLite
    await manager.execute_write(
        """
        INSERT INTO dedup_cache (content_hash, clean_title, category, brand, tags)
        VALUES (:content_hash, :clean_title, :category, :brand, :tags)
        """,
        {
            "content_hash": h,
            "clean_title": "Amul Butter 500g (Pack of 2)",
            "category": "Groceries",
            "brand": "Amul",
            "tags": json.dumps(["butter", "dairy"]),
        },
    )

    compute_called = False

    async def compute_fn():
        nonlocal compute_called
        compute_called = True
        return {"clean_title": "Never Called"}

    cache_hit_called = False

    def on_cache_hit():
        nonlocal cache_hit_called
        cache_hit_called = True

    result, was_cache_hit = await dedup.get_or_compute(
        content_hash_key=h,
        compute_fn=compute_fn,
        on_cache_hit=on_cache_hit,
    )

    assert was_cache_hit is True
    assert compute_called is False
    assert cache_hit_called is True
    assert result["clean_title"] == "Amul Butter 500g (Pack of 2)"
    assert result["category"] == "Groceries"
    assert result["tags"] == ["butter", "dairy"]


@pytest.mark.asyncio
async def test_2_concurrent_identical_requests(dedup_env):
    """Test 2: Concurrent identical requests — 10 simultaneous tasks, compute_fn takes 100ms.

    Assert compute_fn called exactly once, all 10 get the same result, exactly 1 has was_cache_hit=False.
    """
    dedup, _ = dedup_env
    h = content_hash("Coca Cola Can 330ml", "Chilled soda")

    compute_call_count = 0

    async def compute_fn():
        nonlocal compute_call_count
        compute_call_count += 1
        await asyncio.sleep(0.1)  # 100ms simulated LLM latency
        return {
            "clean_title": "Coca-Cola 330ml Can",
            "category": "Beverages",
            "brand": "Coca-Cola",
            "tags": ["soda", "beverage"],
        }

    # Spawn 10 simultaneous tasks
    tasks = [dedup.get_or_compute(h, compute_fn) for _ in range(10)]
    results = await asyncio.gather(*tasks)

    # 1 leader (was_cache_hit=False) and 9 followers (was_cache_hit=True)
    assert compute_call_count == 1
    assert len(results) == 10

    cache_hits = [was_hit for _, was_hit in results if was_hit]
    cache_misses = [was_hit for _, was_hit in results if not was_hit]

    assert len(cache_misses) == 1
    assert len(cache_hits) == 9

    # All 10 tasks received identical enrichment data
    for res_dict, _ in results:
        assert res_dict["clean_title"] == "Coca-Cola 330ml Can"
        assert res_dict["category"] == "Beverages"

    # In-flight dictionary should be completely cleaned up
    assert len(dedup.in_flight) == 0


@pytest.mark.asyncio
async def test_3_leader_failure_propagation(dedup_env):
    """Test 3: Leader failure propagation — compute_fn raises LLMError.

    Assert all followers receive the exception and in_flight is empty afterward.
    """
    dedup, _ = dedup_env
    h = content_hash("Failing Item", "Test Desc")

    async def failing_compute_fn():
        await asyncio.sleep(0.05)
        raise LLMError("LLM Provider Timeout")

    tasks = [dedup.get_or_compute(h, failing_compute_fn) for _ in range(5)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    assert len(results) == 5
    for res in results:
        assert isinstance(res, LLMError)
        assert "LLM Provider Timeout" in str(res)

    assert len(dedup.in_flight) == 0


@pytest.mark.asyncio
async def test_4_staggered_arrival(dedup_env):
    """Test 4: Staggered arrival — start leader, 50ms later start 5 followers (leader takes 100ms).

    Assert all followers hit the in-flight future, not the persistent cache.
    """
    dedup, _ = dedup_env
    h = content_hash("Staggered Item 100", "Description")

    async def compute_fn():
        await asyncio.sleep(0.1)  # 100ms
        return {
            "clean_title": "Staggered Clean Title",
            "category": "Household",
            "brand": "StaggeredBrand",
            "tags": ["clean"],
        }

    # Start leader
    leader_task = asyncio.create_task(dedup.get_or_compute(h, compute_fn))

    # Wait 40ms so leader is definitely in-flight
    await asyncio.sleep(0.04)
    assert h in dedup.in_flight

    follower_hit_count = 0

    def on_follower_hit():
        nonlocal follower_hit_count
        follower_hit_count += 1

    # Spawn 5 followers while leader is still running
    follower_tasks = [
        asyncio.create_task(dedup.get_or_compute(h, compute_fn, on_cache_hit=on_follower_hit))
        for _ in range(5)
    ]

    all_results = await asyncio.gather(leader_task, *follower_tasks)

    leader_res, leader_hit = all_results[0]
    assert leader_hit is False
    assert leader_res["clean_title"] == "Staggered Clean Title"

    for follower_res, follower_hit in all_results[1:]:
        assert follower_hit is True
        assert follower_res["clean_title"] == "Staggered Clean Title"

    assert follower_hit_count == 5
    assert len(dedup.in_flight) == 0


@pytest.mark.asyncio
async def test_5_sequential_dedup(dedup_env):
    """Test 5: Sequential dedup — call get_or_compute, wait for completion, call again.

    Assert second call hits persistent cache.
    """
    dedup, _ = dedup_env
    h = content_hash("Sequential Product", "Desc")

    compute_count = 0

    async def compute_fn():
        nonlocal compute_count
        compute_count += 1
        return {
            "clean_title": "Sequential Clean",
            "category": "Electronics",
            "brand": "SeqBrand",
            "tags": ["electronics"],
        }

    # First call: leader computes and persists
    res1, hit1 = await dedup.get_or_compute(h, compute_fn)
    assert hit1 is False
    assert compute_count == 1
    assert res1["clean_title"] == "Sequential Clean"

    # Second call: sequential, should hit persistent SQLite dedup_cache
    res2, hit2 = await dedup.get_or_compute(h, compute_fn)
    assert hit2 is True
    assert compute_count == 1  # Not incremented!
    assert res2["clean_title"] == "Sequential Clean"


@pytest.mark.asyncio
async def test_6_leader_cancellation(dedup_env):
    """Test 6: Leader cancellation — leader task cancelled mid-computation.

    Assert followers get an exception (not hang forever) and in_flight is clean.
    """
    dedup, _ = dedup_env
    h = content_hash("Cancelled Item", "Desc")

    leader_started = asyncio.Event()

    async def long_compute_fn():
        leader_started.set()
        await asyncio.sleep(2.0)  # Long running
        return {
            "clean_title": "Never Reached",
            "category": "Other",
            "brand": None,
            "tags": [],
        }

    # Start leader task
    leader_task = asyncio.create_task(dedup.get_or_compute(h, long_compute_fn))

    # Wait until leader starts computation
    await leader_started.wait()
    assert h in dedup.in_flight

    # Start follower task
    follower_task = asyncio.create_task(
        dedup.get_or_compute(h, long_compute_fn)
    )

    # Yield control to let follower task execute and begin awaiting leader's future
    await asyncio.sleep(0.02)

    # Cancel leader task mid-flight
    leader_task.cancel()

    # Verify leader raises CancelledError
    with pytest.raises(asyncio.CancelledError):
        await leader_task

    # Verify follower raises CancelledError or exception, unblocked immediately
    with pytest.raises(asyncio.CancelledError):
        await follower_task

    # Ensure in_flight dictionary is cleaned up
    assert len(dedup.in_flight) == 0

