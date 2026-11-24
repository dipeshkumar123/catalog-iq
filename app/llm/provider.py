"""LLM provider factory."""

from app.core.config import settings
from app.llm.base import BaseLLMProvider
from app.llm.groq import GroqLLMProvider
from app.llm.mock import MockLLMProvider


def get_llm_provider() -> BaseLLMProvider:
    """Factory creating configured LLM provider instance."""
    provider_name = settings.LLM_PROVIDER.lower().strip()
    if provider_name == "mock":
        return MockLLMProvider()
    if provider_name == "groq":
        return GroqLLMProvider()
    raise ValueError(f"Unknown LLM_PROVIDER '{settings.LLM_PROVIDER}'. Supported providers: mock, groq.")
