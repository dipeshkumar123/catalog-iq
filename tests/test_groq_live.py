"""Opt-in Groq integration check; never contacts Groq in the default suite."""

import os

import pytest

from app.llm.groq import GroqLLMProvider
from app.llm.validator import ALLOWED_CATEGORIES


@pytest.mark.integration
@pytest.mark.skipif(not os.getenv("GROQ_API_KEY"), reason="GROQ_API_KEY not set")
@pytest.mark.asyncio
async def test_live_groq_enrichment():
    provider = GroqLLMProvider()
    try:
        result = await provider.enrich("Amul Butter 500g pck of 2", None)
        assert isinstance(result["clean_title"], str) and result["clean_title"].strip()
        assert result["category"] in ALLOWED_CATEGORIES
        assert isinstance(result["tags"], list) and len(result["tags"]) <= 5
    finally:
        await provider.aclose()


@pytest.mark.integration
@pytest.mark.skipif(not os.getenv("GROQ_API_KEY"), reason="GROQ_API_KEY not set")
@pytest.mark.asyncio
async def test_live_groq_batch_enrichment():
    provider = GroqLLMProvider()
    try:
        results = await provider.enrich_batch([
            ("Amul Butter 500g pack", "Dairy product"),
            ("Sony wireless headphones", "Over-ear noise cancelling headphones"),
        ])
        assert len(results) == 2
        assert all(result["category"] in ALLOWED_CATEGORIES for result in results)
        assert all(0 <= result["confidence"] <= 1 for result in results)
    finally:
        await provider.aclose()
