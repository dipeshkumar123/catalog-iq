"""Metrics tracking system for LLM calls, errors, and concurrency high-water mark."""

from contextlib import asynccontextmanager


class Metrics:
    """Thread-safe and race-free metrics collector for CatalogIQ.

    Single-Threaded Atomicity Guarantee:
    Because Python's asyncio event loop runs cooperatively on a single thread and
    no `await` suspension points occur between checking and incrementing counters
    inside `track_llm_call()`, updates to `_active_llm_calls`, `max_concurrent_llm_calls`,
    `llm_calls_total`, and `llm_errors_total` are executed atomically. No mutex or
    locking primitives are needed.
    """

    def __init__(self):
        self.llm_calls_total: int = 0
        self.llm_errors_total: int = 0
        self._active_llm_calls: int = 0
        self.max_concurrent_llm_calls: int = 0

    @asynccontextmanager
    async def track_llm_call(self):
        """Context manager: increments active gauge, updates high-water mark, increments total."""
        self._active_llm_calls += 1
        self.llm_calls_total += 1
        if self._active_llm_calls > self.max_concurrent_llm_calls:
            self.max_concurrent_llm_calls = self._active_llm_calls
        try:
            yield
        except Exception:
            self.llm_errors_total += 1
            raise
        finally:
            self._active_llm_calls -= 1

    def reset(self) -> None:
        """Resets all metrics counters back to zero (useful for test isolation)."""
        self.llm_calls_total = 0
        self.llm_errors_total = 0
        self._active_llm_calls = 0
        self.max_concurrent_llm_calls = 0


# Global metrics singleton
metrics = Metrics()
