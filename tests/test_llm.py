"""Unit tests for LLM provider, mock behavior, output validator, and retry backoff."""

import asyncio
import pytest

from app.core.config import settings
from app.core.exceptions import LLMError, LLMValidationError, RetryExhaustedError
from app.llm.mock import MockLLMProvider
from app.llm.validator import validate_enrichment
from app.pipeline.retry import with_retry


@pytest.mark.asyncio
async def test_mock_returns_valid_data_for_standard_title():
    """Verify mock LLM returns a valid dictionary conforming to schema for typical titles."""
    mock = MockLLMProvider(latency_ms=10, failure_rate=0.0)
    result = await mock.enrich("AMUL butter 500G", "pck of 2")

    assert isinstance(result, dict)
    assert result["clean_title"] == "Amul Butter 500G"
    assert result["category"] == "Groceries"
    assert result["brand"] == "Amul"
    assert isinstance(result["tags"], list)
    assert len(result["tags"]) <= 5
    assert all(t == t.lower() for t in result["tags"])


@pytest.mark.asyncio
async def test_mock_raises_llm_error_when_forced(monkeypatch):
    """Verify mock LLM raises LLMError when failure rate is forced to 1.0."""
    mock = MockLLMProvider(latency_ms=0, failure_rate=1.0)
    with pytest.raises(LLMError) as exc_info:
        await mock.enrich("Coca Cola Can 330ml")
    assert "Simulated LLM failure" in str(exc_info.value)


@pytest.mark.asyncio
async def test_mock_deterministic_test_hooks():
    """Verify __FAIL_JSON__ and __FAIL_CATEGORY__ trigger validation errors."""
    mock = MockLLMProvider(latency_ms=0, failure_rate=0.0)

    # __FAIL_JSON__ should trigger LLMValidationError
    with pytest.raises(LLMValidationError) as json_err:
        await mock.enrich("Product with __FAIL_JSON__ in title")
    assert "not valid JSON" in str(json_err.value)

    # __FAIL_CATEGORY__ should trigger LLMValidationError for invalid category
    with pytest.raises(LLMValidationError) as cat_err:
        await mock.enrich("Product with __FAIL_CATEGORY__ in title")
    assert "Invalid category 'Toys'" in str(cat_err.value)


def test_validator_rejects_bad_json():
    """Validator must reject non-JSON strings or improperly formatted JSON."""
    with pytest.raises(LLMValidationError) as exc:
        validate_enrichment("{'invalid': json}")
    assert "not valid JSON" in str(exc.value)


def test_validator_rejects_bad_category():
    """Validator must reject categories outside the 8 allowed enum values."""
    bad_data = {
        "clean_title": "Clean Toy Car",
        "category": "Automotive",
        "brand": "HotWheels",
        "tags": ["toy", "car"],
    }
    with pytest.raises(LLMValidationError) as exc:
        validate_enrichment(bad_data)
    assert "Invalid category 'Automotive'" in str(exc.value)


def test_validator_rejects_empty_title():
    """Validator must reject missing or whitespace-only titles."""
    with pytest.raises(LLMValidationError) as exc1:
        validate_enrichment({
            "clean_title": "   ",
            "category": "Groceries",
            "brand": None,
            "tags": [],
        })
    assert "clean_title must be a non-empty string" in str(exc1.value)

    with pytest.raises(LLMValidationError) as exc2:
        validate_enrichment({
            "category": "Groceries",
            "brand": None,
            "tags": [],
        })
    assert "clean_title must be a non-empty string" in str(exc2.value)


def test_validator_rejects_invalid_tags():
    """Validator must reject uppercase tags or more than 5 tags."""
    # Uppercase tag
    with pytest.raises(LLMValidationError) as exc_upper:
        validate_enrichment({
            "clean_title": "Valid Title",
            "category": "Groceries",
            "brand": "Brand",
            "tags": ["milk", "DAIRY"],
        })
    assert "must be lowercase" in str(exc_upper.value)

    # More than 5 tags
    with pytest.raises(LLMValidationError) as exc_len:
        validate_enrichment({
            "clean_title": "Valid Title",
            "category": "Groceries",
            "brand": "Brand",
            "tags": ["one", "two", "three", "four", "five", "six"],
        })
    assert "tags exceeds limit of 5 items" in str(exc_len.value)


@pytest.mark.asyncio
async def test_with_retry_succeeds_after_transient_failures():
    """with_retry should succeed if a failure resolves before exhausting retries."""
    attempts = 0

    async def flaky_call():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise LLMError("Temporary provider glitch")
        return {"status": "ok", "attempt": attempts}

    result = await with_retry(flaky_call, max_retries=3, base_delay=0.01, sku="SKU-FLAKY")
    assert result == {"status": "ok", "attempt": 3}
    assert attempts == 3


@pytest.mark.asyncio
async def test_with_retry_exhausts_and_raises_retry_exhausted_error():
    """with_retry must execute 4 total attempts (1 initial + 3 retries) before raising RetryExhaustedError."""
    attempts = 0

    async def always_failing_call():
        nonlocal attempts
        attempts += 1
        raise LLMError("Permanent failure")

    with pytest.raises(RetryExhaustedError) as exc_info:
        await with_retry(
            always_failing_call,
            max_retries=3,
            base_delay=0.01,
            sku="SKU-FAIL",
        )

    # 1 initial + 3 retries = 4 total attempts
    assert attempts == 4
    assert exc_info.value.attempts == 4
    assert exc_info.value.sku == "SKU-FAIL"
    assert "Permanent failure" in exc_info.value.last_error
