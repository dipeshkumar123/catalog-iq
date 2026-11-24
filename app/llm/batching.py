"""Small time-window batcher that coalesces concurrent per-product requests."""
import asyncio
from typing import Any, Dict, List, Optional, Tuple
from app.core.config import settings
from app.llm.base import BaseLLMProvider
from app.pipeline.rate_limit import acquire_llm_capacity


class BatchingProvider(BaseLLMProvider):
    def __init__(self, provider: BaseLLMProvider) -> None:
        self.provider = provider
        self.pending: List[Tuple[str, Optional[str], asyncio.Future]] = []
        self.lock = asyncio.Lock()
        self.flush_task: Optional[asyncio.Task] = None

    async def enrich(self, raw_title: str, raw_description: Optional[str] = None) -> Dict[str, Any]:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        async with self.lock:
            self.pending.append((raw_title, raw_description, future))
            if len(self.pending) >= max(1, settings.LLM_BATCH_SIZE):
                await self._flush_locked()
            elif self.flush_task is None or self.flush_task.done():
                self.flush_task = asyncio.create_task(self._flush_after_window())
        return await future

    async def _flush_after_window(self) -> None:
        await asyncio.sleep(max(0, settings.LLM_BATCH_WINDOW_MS) / 1000)
        async with self.lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        if not self.pending:
            return
        batch = self.pending[:max(1, settings.LLM_BATCH_SIZE)]
        del self.pending[:len(batch)]
        products = [(title, description) for title, description, _ in batch]
        try:
            token_estimate = sum(max(1, (len(title) + len(description or "")) // 4) for title, description in products)
            await acquire_llm_capacity(token_estimate)
            results = await self.provider.enrich_batch(products)
            if len(results) != len(batch):
                raise ValueError("Batch provider returned a different number of results")
            for (_, _, future), result in zip(batch, results):
                if not future.done():
                    future.set_result(result)
        except BaseException as exc:
            for _, _, future in batch:
                if not future.done():
                    future.set_exception(exc)
        if self.pending:
            self.flush_task = asyncio.create_task(self._flush_after_window())

    async def aclose(self) -> None:
        async with self.lock:
            await self._flush_locked()
        close = getattr(self.provider, "aclose", None)
        if close:
            await close()
