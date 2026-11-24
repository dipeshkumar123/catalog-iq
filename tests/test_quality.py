import pytest
from app.core.exceptions import LLMValidationError
from app.llm.validator import validate_enrichment
from app.llm.quality import assess_enrichment_quality


@pytest.mark.parametrize("confidence", [-0.01, 1.01, float("nan"), True, "0.9"])
def test_validator_rejects_invalid_confidence(confidence):
    with pytest.raises(LLMValidationError, match="confidence"):
        validate_enrichment({"clean_title": "Valid product title", "category": "Other", "brand": None, "tags": [], "confidence": confidence})


@pytest.mark.parametrize("data", [
    {"clean_title": "Tiny", "category": "Other", "brand": None, "tags": []},
    {"clean_title": "A sufficiently long product title", "category": "Other", "brand": None, "tags": ["two words"]},
    {"clean_title": "A sufficiently long product title", "category": "Other", "brand": None, "tags": [], "unexpected": 1},
])
def test_validator_rejects_malformed_contract_edges(data):
    with pytest.raises(LLMValidationError):
        validate_enrichment(data)


def test_quality_flags_ungrounded_brand_conflict_and_low_confidence():
    result = {"brand": "Sony", "category": "Beverages", "confidence": 0.2}
    reasons = assess_enrichment_quality("Conditioner 300ml", None, result)
    assert reasons == ["brand_not_grounded", "category_conflict:conditioner", "confidence_below_threshold"]


def test_quality_brand_grounding_is_case_insensitive_and_token_based():
    assert assess_enrichment_quality("SONY headphones", "Wireless model", {"brand": "Sony", "category": "Electronics", "confidence": .9}) == []
