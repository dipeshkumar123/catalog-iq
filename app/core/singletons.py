"""Global module-level singletons shared across API routes and pipeline workers."""

import asyncio
from typing import Optional

from app.core.config import settings
from app.core.metrics import metrics
from app.db.session import db_manager
from app.llm.base import BaseLLMProvider
from app.llm.provider import get_llm_provider
from app.pipeline.dedup import DedupManager

_global_llm_semaphore: Optional[asyncio.Semaphore] = None
_llm_provider: Optional[BaseLLMProvider] = None
_dedup_manager: Optional[DedupManager] = None


def get_llm_semaphore() -> asyncio.Semaphore:
    """Returns the shared global LLM concurrency semaphore, lazily initialized in the active loop."""
    global _global_llm_semaphore
    if _global_llm_semaphore is None:
        _global_llm_semaphore = asyncio.Semaphore(settings.LLM_CONCURRENCY)
    return _global_llm_semaphore


def set_llm_semaphore(sem: Optional[asyncio.Semaphore]) -> None:
    """Sets or resets the global LLM semaphore (used in tests to vary concurrency)."""
    global _global_llm_semaphore
    _global_llm_semaphore = sem


def get_llm_provider_instance() -> BaseLLMProvider:
    """Returns the configured LLM provider singleton."""
    global _llm_provider
    if _llm_provider is None:
        _llm_provider = get_llm_provider()
    return _llm_provider


def set_llm_provider_instance(provider: Optional[BaseLLMProvider]) -> None:
    """Overrides the global LLM provider (used in tests for mocking)."""
    global _llm_provider
    _llm_provider = provider


def get_dedup_manager() -> DedupManager:
    """Returns the shared DedupManager instance."""
    global _dedup_manager
    if _dedup_manager is None:
        _dedup_manager = DedupManager(manager=db_manager, metrics=metrics)
    return _dedup_manager


def set_dedup_manager(dm: Optional[DedupManager]) -> None:
    """Sets or resets the DedupManager instance."""
    global _dedup_manager
    _dedup_manager = dm


def init_singletons() -> None:
    """Explicitly initializes all singletons within an active event loop."""
    global _global_llm_semaphore, _llm_provider, _dedup_manager
    _global_llm_semaphore = asyncio.Semaphore(settings.LLM_CONCURRENCY)
    _llm_provider = get_llm_provider()
    _dedup_manager = DedupManager(manager=db_manager, metrics=metrics)
