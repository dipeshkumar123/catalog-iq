"""Groq Chat Completions provider using the shared enrichment contract."""

from typing import Any, Dict, Optional

import httpx

from app.core.config import settings
from app.core.exceptions import LLMError
from app.llm.base import BaseLLMProvider
from app.llm.validator import validate_enrichment

GROQ_CHAT_COMPLETIONS_URL = "https://api.groq.com/openai/v1/chat/completions"
SYSTEM_PROMPT = (
    "You are a product catalogue enricher. Given a raw product listing, return STRICT JSON with exactly these keys:\n"
    "- clean_title: a clean human-readable product title (string, 5-100 chars)\n"
    "- category: exactly one of: Groceries, Beverages, Personal Care, Household, Electronics, Fashion, Home & Kitchen, Other\n"
    "- brand: the brand name as a string, or null if you cannot determine it\n"
    "- tags: up to 5 lowercase single-word strings describing the product\n"
    "Return ONLY the JSON object. No markdown fences. No explanation."
)


class GroqLLMProvider(BaseLLMProvider):
    """Enrich products through Groq's OpenAI-compatible chat completions API."""

    def __init__(
        self,
        model: str = "openai/gpt-oss-120b",
        *,
        api_key: Optional[str] = None,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> None:
        key = settings.GROQ_API_KEY if api_key is None else api_key
        if not key or not key.strip():
            raise LLMError("GROQ_API_KEY is not set")
        self.api_key = key.strip()
        self.model = model
        self._transport = transport
        self._client: Optional[httpx.AsyncClient] = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=30.0, transport=self._transport)
        return self._client

    async def aclose(self) -> None:
        """Closes the shared HTTP client on application shutdown."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def enrich(
        self,
        raw_title: str,
        raw_description: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": f"Title: {raw_title}\nDescription: {raw_description or '(none)'}",
                },
            ],
            "temperature": 0,
            "response_format": {"type": "json_object"},
        }

        try:
            response = await self._get_client().post(
                GROQ_CHAT_COMPLETIONS_URL,
                headers={"Authorization": f"Bearer {self.api_key}"},
                json=payload,
            )
        except httpx.TimeoutException as exc:
            raise LLMError("Groq timeout") from exc
        except httpx.RequestError as exc:
            raise LLMError(f"Groq request error: {exc}") from exc

        status = response.status_code
        if status == 429:
            raise LLMError("Groq rate limit")
        if status in (500, 503):
            raise LLMError(f"Groq server error: {status}")
        if status in (400, 401):
            raise LLMError(f"Groq client error: {status} {response.text}")
        if status != 200:
            raise LLMError(f"Groq unexpected response: {status} {response.text}")

        try:
            data = response.json()
            content = data["choices"][0]["message"]["content"]
        except (ValueError, KeyError, IndexError, TypeError) as exc:
            raise LLMError(f"Groq response was malformed: {exc}") from exc

        return validate_enrichment(content)
