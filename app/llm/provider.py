"""LLM provider factory."""

from app.core.config import settings
from app.llm.base import BaseLLMProvider
from app.llm.groq import GroqLLMProvider
from app.llm.mock import MockLLMProvider
from app.llm.gemini import GeminiLLMProvider
from app.llm.resilient import ResilientLLMProvider
from app.llm.batching import BatchingProvider


def _provider_for(name: str):
    normalized = name.lower().strip()
    if normalized == "mock":
        return MockLLMProvider()
    if normalized == "groq":
        return GroqLLMProvider(model=settings.GROQ_MODEL)
    if normalized == "gemini":
        return GeminiLLMProvider()
    raise ValueError(f"Unknown provider '{name}'. Supported providers: mock, groq, gemini.")


def get_llm_provider() -> BaseLLMProvider:
    """Factory creating configured LLM provider instance."""
    provider_name = settings.LLM_PROVIDER.lower().strip()
    if provider_name == "mock":
        return BatchingProvider(MockLLMProvider())
    if provider_name == "groq":
        primary = _provider_for("groq")
        fallback_name = settings.LLM_FALLBACK_PROVIDER
        if fallback_name is None:
            fallback = GeminiLLMProvider() if settings.GEMINI_API_KEY else None
        elif fallback_name.lower().strip() in {"", "none", "off"}:
            fallback = None
        else:
            fallback = _provider_for(fallback_name)
        complex_provider = _provider_for(settings.LLM_COMPLEX_PROVIDER) if settings.LLM_COMPLEX_PROVIDER else None
        return BatchingProvider(ResilientLLMProvider(primary, fallback, complex_provider))
    if provider_name == "gemini":
        return BatchingProvider(GeminiLLMProvider())
    raise ValueError(f"Unknown LLM_PROVIDER '{settings.LLM_PROVIDER}'. Supported providers: mock, groq, gemini.")
