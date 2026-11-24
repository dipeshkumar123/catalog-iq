"""Groq provider tests using HTTPX's in-memory transport (no network required)."""

import json

import httpx
import pytest

from app.core.config import settings
from app.core.exceptions import LLMError, LLMValidationError
from app.llm.groq import GROQ_CHAT_COMPLETIONS_URL, SYSTEM_PROMPT, GroqLLMProvider
from app.llm.mock import MockLLMProvider
from app.llm.provider import get_llm_provider


VALID_CONTENT = json.dumps(
    {
        "clean_title": "Amul Butter 500g (Pack of 2)",
        "category": "Groceries",
        "brand": "Amul",
        "tags": ["butter", "dairy"],
    }
)


def response_transport(status_code: int, body: dict | str) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json=body if isinstance(body, dict) else None,
                              text=body if isinstance(body, str) else None)

    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_groq_success_sends_expected_request_and_validates_response():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": VALID_CONTENT}}]},
        )

    provider = GroqLLMProvider(api_key="test-key", transport=httpx.MockTransport(handler))
    try:
        result = await provider.enrich("Amul butter 500g", "pck of 2")
        assert result == {
            "clean_title": "Amul Butter 500g (Pack of 2)",
            "category": "Groceries",
            "brand": "Amul",
            "tags": ["butter", "dairy"],
            "confidence": None,
        }
        assert len(seen) == 1
        request = seen[0]
        assert request.method == "POST"
        assert str(request.url) == GROQ_CHAT_COMPLETIONS_URL
        assert request.headers["authorization"] == "Bearer test-key"
        payload = json.loads(request.content)
        assert payload["model"] == "openai/gpt-oss-120b"
        assert payload["messages"] == [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": "Title: Amul butter 500g\nDescription: pck of 2"},
        ]
        assert payload["temperature"] == 0
        assert payload["response_format"] == {"type": "json_object"}
        first_client = provider._client
        await provider.enrich("Amul butter 500g", None)
        assert provider._client is first_client
        assert len(seen) == 2
    finally:
        await provider.aclose()


@pytest.mark.asyncio
async def test_groq_batch_maps_out_of_order_ids_and_rejects_duplicates():
    entry = {"clean_title": "Amul Butter 500g", "category": "Groceries", "brand": "Amul", "tags": ["butter"], "confidence": .9}
    def handler(request):
        body = {"results": [{"id": 1, **{**entry, "clean_title": "Amul Butter 500g Pack"}}, {"id": 0, **entry}]}
        return httpx.Response(200, json={"choices": [{"message": {"content": json.dumps(body)}}]})
    provider = GroqLLMProvider(api_key="test", transport=httpx.MockTransport(handler))
    results = await provider.enrich_batch([("Amul butter 500g", None), ("Amul butter pack 500g", None)])
    assert results[0]["clean_title"] == "Amul Butter 500g"
    assert results[1]["clean_title"] == "Amul Butter 500g Pack"
    await provider.aclose()


@pytest.mark.asyncio
async def test_groq_malformed_model_json_raises_validation_error():
    provider = GroqLLMProvider(
        api_key="test-key",
        transport=response_transport(
            200, {"choices": [{"message": {"content": "{malformed"}}]}
        ),
    )
    try:
        with pytest.raises(LLMValidationError, match="not valid JSON"):
            await provider.enrich("Amul butter")
    finally:
        await provider.aclose()


@pytest.mark.asyncio
async def test_groq_rate_limit_is_transient_llm_error():
    provider = GroqLLMProvider(
        api_key="test-key", transport=response_transport(429, {"error": "slow down"})
    )
    try:
        with pytest.raises(LLMError, match="rate limit"):
            await provider.enrich("Amul butter")
    finally:
        await provider.aclose()


def test_groq_requires_api_key(monkeypatch):
    monkeypatch.setattr(settings, "GROQ_API_KEY", None)
    with pytest.raises(LLMError, match="GROQ_API_KEY is not set"):
        GroqLLMProvider()


def test_groq_system_prompt_matches_contract_exactly():
    assert SYSTEM_PROMPT == (
        "You are a product catalogue enricher. Given a raw product listing, return STRICT JSON with exactly these keys:\n"
        "- clean_title: a clean human-readable product title (string, 5-100 chars)\n"
        "- category: exactly one of: Groceries, Beverages, Personal Care, Household, Electronics, Fashion, Home & Kitchen, Other\n"
        "- brand: the brand name as a string, or null if you cannot determine it\n"
        "- tags: up to 5 lowercase single-word strings describing the product\n"
            "- confidence: a number from 0 to 1 estimating confidence (uncertain outputs should be below 0.85)\n"
        "Return ONLY the JSON object. No markdown fences. No explanation."
    )


def test_factory_selects_groq_and_mock(monkeypatch):
    monkeypatch.setattr(settings, "GROQ_API_KEY", "test-key")
    monkeypatch.setattr(settings, "LLM_PROVIDER", "groq")
    from app.llm.batching import BatchingProvider
    assert isinstance(get_llm_provider(), BatchingProvider)
    assert isinstance(get_llm_provider().provider.primary, GroqLLMProvider)

    monkeypatch.setattr(settings, "LLM_PROVIDER", "mock")
    assert isinstance(get_llm_provider(), BatchingProvider)
    assert isinstance(get_llm_provider().provider, MockLLMProvider)


def test_factory_rejects_unknown_provider(monkeypatch):
    monkeypatch.setattr(settings, "LLM_PROVIDER", "unknown")
    with pytest.raises(ValueError, match="Unknown LLM_PROVIDER"):
        get_llm_provider()


@pytest.mark.asyncio
async def test_groq_timeout_is_transient_llm_error():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    provider = GroqLLMProvider(api_key="test-key", transport=httpx.MockTransport(handler))
    try:
        with pytest.raises(LLMError, match="Groq timeout"):
            await provider.enrich("Amul butter")
    finally:
        await provider.aclose()
