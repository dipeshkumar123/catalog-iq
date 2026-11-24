"""Exponential backoff retry controller for LLM invocations and validations."""

import asyncio
import logging
import random
from typing import Any, Callable, Coroutine, Optional, TypeVar

from app.core.exceptions import LLMError, LLMValidationError, RetryExhaustedError

logger = logging.getLogger(__name__)

T = TypeVar("T")


async def with_retry(
    coro_func: Callable[[], Coroutine[Any, Any, T]],
    max_retries: int = 3,
    base_delay: float = 0.2,
    sku: str = "unknown",
    on_retry: Optional[Callable[[int, Exception], Any]] = None,
) -> T:
    """Executes an asynchronous function with exponential backoff on LLM and validation errors.

    Total attempts = 1 initial attempt + max_retries (default 4 total attempts).

    Args:
        coro_func: A zero-argument callable returning a coroutine to execute.
        max_retries: Number of additional retry attempts after initial failure (default: 3).
        base_delay: Starting delay in seconds (default: 0.2s = 200ms).
        sku: Product SKU identifier for contextual error reporting.
        on_retry: Optional callback invoked on each retry with (attempt_number, exception).

    Returns:
        The successful result of coro_func().

    Raises:
        RetryExhaustedError: When all attempts fail.
    """
    total_attempts = 1 + max_retries
    last_error: Optional[Exception] = None

    for attempt in range(1, total_attempts + 1):
        try:
            return await coro_func()
        except (LLMError, LLMValidationError) as exc:
            last_error = exc
            logger.warning(
                "Attempt %d/%d for SKU '%s' failed: %s",
                attempt,
                total_attempts,
                sku,
                exc,
            )

            if on_retry:
                try:
                    res = on_retry(attempt, exc)
                    if asyncio.iscoroutine(res):
                        await res
                except Exception as cb_err:
                    logger.debug("on_retry callback error: %s", cb_err)

            if attempt < total_attempts:
                # Exponential backoff with small random jitter
                backoff = base_delay * (2 ** (attempt - 1))
                jitter = random.uniform(0.01, 0.05)
                delay = backoff + jitter
                await asyncio.sleep(delay)
            else:
                raise RetryExhaustedError(
                    sku=sku,
                    attempts=total_attempts,
                    last_error=str(last_error),
                ) from exc

    # Fallback guard
    raise RetryExhaustedError(
        sku=sku,
        attempts=total_attempts,
        last_error=str(last_error or "Unknown failure"),
    )
