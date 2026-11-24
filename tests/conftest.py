"""Shared cleanup that keeps async database and singleton state test-local."""

import pytest_asyncio
from app.core.config import settings

from app.core.metrics import metrics
from app.core.singletons import (
    set_dedup_manager,
    set_llm_provider_instance,
    set_llm_semaphore,
)
from app.db.session import db_manager


@pytest_asyncio.fixture(autouse=True)
async def reset_shared_runtime_state(monkeypatch):
    """Close persistent connections and reset globals after every test."""
    # Keep ordinary tests hermetic even when the developer has optional services configured.
    monkeypatch.setattr(settings, "REDIS_URL", None)
    monkeypatch.setattr(settings, "LLM_FALLBACK_PROVIDER", "none")
    monkeypatch.setattr(settings, "LLM_COMPLEX_PROVIDER", None)
    yield
    await db_manager.close()
    set_dedup_manager(None)
    set_llm_provider_instance(None)
    set_llm_semaphore(None)
    metrics.reset()
