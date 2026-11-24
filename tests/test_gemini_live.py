"""Opt-in live Gemini contract and fallback checks; never run in the default suite."""
import os
import pytest
from app.core.exceptions import LLMError
from app.llm.base import BaseLLMProvider
from app.llm.gemini import GeminiLLMProvider
from app.llm.resilient import ResilientLLMProvider
from app.llm.validator import ALLOWED_CATEGORIES
from app.pipeline.retry import with_retry


class RateLimitedProvider(BaseLLMProvider):
    async def enrich(self, raw_title, raw_description=None):
        raise LLMError("Groq rate limit")


@pytest.mark.integration
@pytest.mark.skipif(not os.getenv("GEMINI_API_KEY"), reason="GEMINI_API_KEY not set")
@pytest.mark.asyncio
async def test_live_gemini_enrichment_and_transient_fallback():
    gemini = GeminiLLMProvider()
    try:
        result = await with_retry(lambda: gemini.enrich("Amul Butter 500g", "Dairy product, 500 gram pack"), max_retries=3, base_delay=.5)
        assert result["clean_title"] and result["category"] in ALLOWED_CATEGORIES
        assert isinstance(result["tags"], list) and len(result["tags"]) <= 5
        fallback_provider = ResilientLLMProvider(RateLimitedProvider(), gemini)
        fallback = await with_retry(lambda: fallback_provider.enrich(
            "Sony wireless headphones", "Over-ear noise cancelling headphones"
        ), max_retries=3, base_delay=.5)
        assert fallback["clean_title"] and fallback["category"] in ALLOWED_CATEGORIES
    finally:
        await gemini.aclose()
