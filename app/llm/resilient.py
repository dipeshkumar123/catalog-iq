"""Provider routing with conservative transient-error fallback and title complexity tiers."""

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple
import asyncio
from app.core.config import settings
from app.core.exceptions import LLMError
from app.llm.base import BaseLLMProvider
from app.llm.gemini import GeminiLLMProvider
from app.llm.groq import GroqLLMProvider


class ResilientLLMProvider(BaseLLMProvider):
    def __init__(self, primary: BaseLLMProvider, fallback: Optional[BaseLLMProvider] = None,
                 complex_provider: Optional[BaseLLMProvider] = None) -> None:
        self.primary, self.fallback, self.complex_provider = primary, fallback, complex_provider

    async def enrich(self, raw_title: str, raw_description: Optional[str] = None) -> Dict[str, Any]:
        # Long or description-heavy listings can be routed to a separately configured model.
        complex_item = len(re.findall(r"\w+", raw_title or "")) > settings.LLM_SIMPLE_TITLE_WORD_LIMIT or bool(raw_description and len(raw_description) > 240)
        preferred = self.complex_provider if complex_item and self.complex_provider else self.primary
        backup = self.primary if preferred is self.complex_provider and self.primary else self.fallback
        try:
            return await preferred.enrich(raw_title, raw_description)
        except LLMError as exc:
            transient = any(marker in str(exc).lower() for marker in ("rate limit", "429", "503", "server error", "timeout", "request error"))
            if transient and backup and backup is not preferred:
                return await backup.enrich(raw_title, raw_description)
            raise

    async def aclose(self) -> None:
        for provider in (self.primary, self.fallback, self.complex_provider):
            if provider and hasattr(provider, "aclose"):
                await provider.aclose()

    async def enrich_batch(self, products: Sequence[Tuple[str, Optional[str]]]) -> List[Dict[str, Any]]:
        simple, complex_items = [], []
        for index, (title, description) in enumerate(products):
            target = complex_items if (len(re.findall(r"\w+", title or "")) > settings.LLM_SIMPLE_TITLE_WORD_LIMIT or bool(description and len(description) > 240)) and self.complex_provider else simple
            target.append((index, (title, description)))
        results: List[Optional[Dict[str, Any]]] = [None] * len(products)
        async def run_group(group, provider):
            if not group:
                return
            items = [item for _, item in group]
            try:
                values = await provider.enrich_batch(items)
            except LLMError as exc:
                transient = any(marker in str(exc).lower() for marker in ("rate limit", "429", "503", "server error", "timeout", "request error"))
                fallback = self.primary if provider is self.complex_provider else self.fallback
                if not transient or not fallback or fallback is provider:
                    raise
                values = await fallback.enrich_batch(items)
            if len(values) != len(group):
                raise LLMError("Provider returned a different number of batch results")
            for (index, _), value in zip(group, values):
                results[index] = value
        await asyncio.gather(run_group(simple, self.primary), run_group(complex_items, self.complex_provider or self.primary))
        return [value for value in results if value is not None]
