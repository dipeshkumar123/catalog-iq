import asyncio
import pytest
from app.core.config import settings
from app.llm.batching import BatchingProvider
from app.llm.base import BaseLLMProvider


class BatchSpy(BaseLLMProvider):
    def __init__(self): self.batches = []
    async def enrich(self, raw_title, raw_description=None):
        return {"clean_title": raw_title}
    async def enrich_batch(self, products):
        self.batches.append(list(products))
        await asyncio.sleep(0)
        return [{"clean_title": title} for title, _ in products]


@pytest.mark.asyncio
async def test_batcher_coalesces_requests_and_preserves_order(monkeypatch):
    monkeypatch.setattr(settings, "LLM_BATCH_SIZE", 3)
    monkeypatch.setattr(settings, "LLM_BATCH_WINDOW_MS", 200)
    provider = BatchSpy()
    batched = BatchingProvider(provider)
    results = await asyncio.gather(*(batched.enrich(f"Product {i}") for i in range(3)))
    assert [r["clean_title"] for r in results] == ["Product 0", "Product 1", "Product 2"]
    assert provider.batches == [[("Product 0", None), ("Product 1", None), ("Product 2", None)]]


@pytest.mark.asyncio
async def test_batcher_flushes_after_window_and_propagates_count_mismatch(monkeypatch):
    monkeypatch.setattr(settings, "LLM_BATCH_SIZE", 10)
    monkeypatch.setattr(settings, "LLM_BATCH_WINDOW_MS", 1)
    class Broken(BatchSpy):
        async def enrich_batch(self, products): return []
    with pytest.raises(ValueError, match="different number"):
        await BatchingProvider(Broken()).enrich("A listing")
