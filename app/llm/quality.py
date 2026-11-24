"""Deterministic quality gates that route uncertain enrichments to human review."""

import re
from typing import Any, Dict, List, Optional


_TOKEN_RE = re.compile(r"[\w]+", re.UNICODE)

# Strong product-type signals. Commonly ambiguous words such as "milk" are
# deliberately omitted so descriptions do not create noisy false positives.
_CATEGORY_SIGNALS = {
    "shampoo": "Personal Care",
    "conditioner": "Personal Care",
    "toothpaste": "Personal Care",
    "toothbrush": "Personal Care",
    "lotion": "Personal Care",
    "lipstick": "Personal Care",
    "deodorant": "Personal Care",
    "detergent": "Household",
    "dishwash": "Household",
    "cleaner": "Household",
    "bleach": "Household",
    "charger": "Electronics",
    "headphones": "Electronics",
    "laptop": "Electronics",
    "smartphone": "Electronics",
    "television": "Electronics",
    "jeans": "Fashion",
    "sneakers": "Fashion",
    "dress": "Fashion",
    "rice": "Groceries",
    "butter": "Groceries",
    "chocolate": "Groceries",
    "noodles": "Groceries",
    "juice": "Beverages",
    "coffee": "Beverages",
    "soda": "Beverages",
    "cola": "Beverages",
}


def _tokens(value: Optional[str]) -> List[str]:
    return [token.casefold() for token in _TOKEN_RE.findall(value or "")]


def assess_enrichment_quality(
    raw_title: str,
    raw_description: Optional[str],
    result: Dict[str, Any],
    *,
    confidence_threshold: float = 0.85,
) -> List[str]:
    """Return stable review reason codes for ungrounded or uncertain metadata."""
    source_tokens = _tokens(f"{raw_title or ''} {raw_description or ''}")
    source = set(source_tokens)
    reasons: List[str] = []

    brand = result.get("brand")
    if isinstance(brand, str) and brand.strip():
        brand_tokens = _tokens(brand)
        if not brand_tokens or not all(token in source for token in brand_tokens):
            reasons.append("brand_not_grounded")

    category = result.get("category")
    matched_signals = sorted({token for token in source_tokens if token in _CATEGORY_SIGNALS})
    conflicting = [token for token in matched_signals if _CATEGORY_SIGNALS[token] != category]
    if conflicting:
        reasons.append("category_conflict:" + ",".join(conflicting))

    confidence = result.get("confidence")
    if confidence is None:
        reasons.append("confidence_missing")
    elif confidence < confidence_threshold:
        reasons.append("confidence_below_threshold")

    return reasons
