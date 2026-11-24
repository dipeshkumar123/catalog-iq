import pytest
from app.core.config import settings
from app.pipeline import queue, rate_limit
from app.pipeline.dedup import DedupManager


class FakeRedis:
    def __init__(self): self.calls = []; self.values = {}
    async def xadd(self, stream, fields): self.calls.append(("xadd", stream, fields)); return "1-0"
    async def ping(self): self.calls.append(("ping",)); return True
    async def xgroup_create(self, *args, **kwargs): self.calls.append(("xgroup_create", args, kwargs))
    async def get(self, key): self.calls.append(("get", key)); return self.values.get(key)
    async def setex(self, key, ttl, value): self.calls.append(("setex", key, ttl, value)); self.values[key] = value
    async def eval(self, *args): self.calls.append(("eval", args)); return 0
    async def aclose(self): self.calls.append(("close",))


@pytest.mark.asyncio
async def test_redis_queue_initializes_stream_and_enqueues(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(settings, "REDIS_URL", "redis://test")
    monkeypatch.setattr(queue, "_redis_client", fake)
    await queue.initialize_redis_queue()
    await queue.enqueue_job("job-1")
    assert fake.calls[0][0] == "ping"
    assert any(call[0] == "xgroup_create" for call in fake.calls)
    assert ("xadd", settings.REDIS_QUEUE_STREAM, {"job_id": "job-1"}) in fake.calls
    await queue.close_redis_queue()


@pytest.mark.asyncio
async def test_dedup_redis_cache_roundtrip_and_sqlite_fallback(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(settings, "REDIS_URL", "redis://test")
    monkeypatch.setattr(settings, "REDIS_CACHE_TTL_SECONDS", 60)
    manager = DedupManager()
    manager._redis = fake
    cached = {"clean_title":"Example product title", "category":"Other", "brand":None, "tags":[], "confidence":.9, "review_reasons":[]}
    await manager._redis_write("hash", cached)
    assert any(call[0] == "setex" and call[2] == 60 for call in fake.calls)
    assert await manager._redis_read("hash") == cached
    await manager.aclose()


@pytest.mark.asyncio
async def test_distributed_rate_limiter_uses_atomic_script(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(settings, "REDIS_URL", "redis://test")
    monkeypatch.setattr(settings, "LLM_RATE_LIMIT_RPM", 60)
    monkeypatch.setattr(settings, "LLM_RATE_LIMIT_TPM", 5000)
    monkeypatch.setattr(rate_limit, "_client", fake)
    await rate_limit.acquire_llm_capacity(125)
    call = next(call for call in fake.calls if call[0] == "eval")
    assert call[1][1:4] == (2, "catalogiq:llm:rpm", "catalogiq:llm:tpm")
    assert call[1][4:8] == (60, 5000, 1, 125)
    await rate_limit.close_rate_limiter()
