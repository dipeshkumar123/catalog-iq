import json
import httpx
import pytest
from app.core.exceptions import LLMError
from app.llm.gemini import GeminiLLMProvider
from app.llm.resilient import ResilientLLMProvider
from app.llm.base import BaseLLMProvider


class StubProvider(BaseLLMProvider):
    def __init__(self, error=None): self.error, self.calls = error, 0
    async def enrich(self, raw_title, raw_description=None):
        self.calls += 1
        if self.error: raise self.error
        return {"clean_title": "Example product title", "category": "Other", "brand": None, "tags": [], "confidence": .9}


@pytest.mark.asyncio
async def test_gemini_uses_key_header_and_validates_structured_response():
    body = {"candidates": [{"content": {"parts": [{"text": json.dumps({"clean_title":"Example product title","category":"Other","brand":None,"tags":[],"confidence":.9})}]}}]}
    def handler(request):
        assert request.headers["x-goog-api-key"] == "secret-test-key"
        assert request.url.path.endswith(":generateContent")
        return httpx.Response(200, json=body)
    provider = GeminiLLMProvider(api_key="secret-test-key", transport=httpx.MockTransport(handler))
    assert (await provider.enrich("Example title"))["confidence"] == .9
    await provider.aclose()


@pytest.mark.asyncio
async def test_provider_fallback_only_for_transient_errors():
    primary = StubProvider(LLMError("Groq rate limit"))
    fallback = StubProvider()
    result = await ResilientLLMProvider(primary, fallback).enrich("short item")
    assert result["confidence"] == .9 and fallback.calls == 1
    bad_primary = StubProvider(LLMError("Groq client error: 401"))
    unused = StubProvider()
    with pytest.raises(LLMError, match="401"):
        await ResilientLLMProvider(bad_primary, unused).enrich("short item")
    assert unused.calls == 0
