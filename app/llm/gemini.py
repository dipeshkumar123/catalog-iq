"""Gemini generateContent provider used as an optional fallback."""

from typing import Any, Dict, Optional
import httpx
from app.core.config import settings
from app.core.exceptions import LLMError
from app.llm.base import BaseLLMProvider
from app.llm.validator import validate_enrichment


class GeminiLLMProvider(BaseLLMProvider):
    def __init__(self, model: Optional[str] = None, *, api_key: Optional[str] = None,
                 transport: Optional[httpx.AsyncBaseTransport] = None) -> None:
        key = settings.GEMINI_API_KEY if api_key is None else api_key
        if not key or not key.strip():
            raise LLMError("GEMINI_API_KEY is not set")
        self.api_key = key.strip()
        self.model = model or settings.GEMINI_MODEL
        self.transport = transport
        self.client: Optional[httpx.AsyncClient] = None

    async def aclose(self) -> None:
        if self.client:
            await self.client.aclose()
            self.client = None

    async def enrich(self, raw_title: str, raw_description: Optional[str] = None) -> Dict[str, Any]:
        if self.client is None:
            self.client = httpx.AsyncClient(timeout=30, transport=self.transport)
        prompt = (
            "Return only a JSON object with exactly clean_title, category, brand, tags, confidence. "
            "clean_title must be 5-100 chars; category must be one of Groceries, Beverages, Personal Care, "
            "Household, Electronics, Fashion, Home & Kitchen, Other; brand string or null; tags up to 5 "
            "lowercase single words; confidence a number 0..1.\n"
            f"Seller title: {raw_title}\nSeller description: {raw_description or '(none)'}"
        )
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model}:generateContent"
        try:
            response = await self.client.post(url, headers={"x-goog-api-key": self.api_key}, json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"temperature": 0, "responseMimeType": "application/json"},
            })
        except httpx.TimeoutException as exc:
            raise LLMError("Gemini timeout") from exc
        except httpx.RequestError as exc:
            raise LLMError(f"Gemini request error: {exc}") from exc
        if response.status_code in (429, 500, 503):
            detail = response.text.replace(self.api_key, "[REDACTED]")[:500]
            raise LLMError(f"Gemini transient provider error: {response.status_code} {detail}")
        if response.status_code != 200:
            detail = response.text.replace(self.api_key, "[REDACTED]")[:500]
            raise LLMError(f"Gemini provider error: {response.status_code} {detail}")
        try:
            text = response.json()["candidates"][0]["content"]["parts"][0]["text"]
        except (ValueError, KeyError, IndexError, TypeError) as exc:
            raise LLMError(f"Gemini response was malformed: {exc}") from exc
        return validate_enrichment(text)
