"""Opt-in Redis Streams, shared cache, and rate limiter smoke tests."""
import os
import pytest
from redis.asyncio import Redis
from app.core.config import settings
from app.db.init_db import init_db
from app.db.session import DatabaseManager
from app.pipeline import queue, rate_limit
from app.pipeline.dedup import DedupManager


@pytest.mark.integration
@pytest.mark.skipif(not os.getenv("REDIS_URL"), reason="REDIS_URL not set")
@pytest.mark.asyncio
async def test_live_redis_stream_cache_and_distributed_limiter(monkeypatch, tmp_path):
    monkeypatch.setattr(settings, "REDIS_URL", os.environ["REDIS_URL"])
    monkeypatch.setattr(settings, "LLM_RATE_LIMIT_RPM", 30)
    monkeypatch.setattr(settings, "LLM_RATE_LIMIT_TPM", 1000)
    await queue.initialize_redis_queue()
    stream_id = await queue._redis_client.xadd(settings.REDIS_QUEUE_STREAM, {"job_id": "redis-smoke"})
    assert stream_id

    redis = Redis.from_url(settings.REDIS_URL, decode_responses=True)
    try:
        manager = DatabaseManager(str(tmp_path / "redis-cache.db"))
        await init_db(manager)
        first = DedupManager(manager=manager)
        result = {"clean_title":"Amul Butter 500g", "category":"Groceries", "brand":"Amul", "tags":["butter"], "confidence":.9, "review_reasons":[]}
        await first._redis_write("redis-smoke-hash", result)
        second = DedupManager(manager=manager)
        cached, was_hit = await second.get_or_compute("redis-smoke-hash", lambda: pytest.fail("should hit Redis"))
        assert was_hit and cached == result
        await rate_limit.acquire_llm_capacity(100)
        assert await redis.exists("catalogiq:llm:rpm") == 1
        await manager.close()
        await first.aclose()
        await second.aclose()
    finally:
        await redis.delete(f"catalogiq:cache:redis-smoke-hash", "catalogiq:llm:rpm", "catalogiq:llm:tpm")
        await queue.close_redis_queue()
        await rate_limit.close_rate_limiter()
        await redis.aclose()
