"""Output validation logic for LLM enrichment responses."""

import json
import math
import re
from typing import Any, Dict, Set
from app.core.exceptions import LLMValidationError

ALLOWED_CATEGORIES: Set[str] = {
    "Groceries",
    "Beverages",
    "Personal Care",
    "Household",
    "Electronics",
    "Fashion",
    "Home & Kitchen",
    "Other",
}


def validate_enrichment(data: Any) -> Dict[str, Any]:
    """Validates raw LLM enrichment payload against catalogue schema constraints.

    Rules:
    - Must be a dictionary (or JSON string deserializable into a dict).
    - clean_title: non-empty string.
    - category: exactly one of the 8 allowed categories.
    - brand: string or None.
    - tags: list of up to 5 lowercase strings.

    Args:
        data: The output from the LLM provider (dict or string).

    Returns:
        Standardized enrichment dictionary.

    Raises:
        LLMValidationError: If any validation rule is violated.
    """
    # 1. Parse JSON if string
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception as e:
            raise LLMValidationError(f"LLM output is not valid JSON: {e}")

    # 2. Must be dictionary
    if not isinstance(data, dict):
        raise LLMValidationError(f"Expected dictionary output, got {type(data).__name__}")

    allowed_keys = {"clean_title", "category", "brand", "tags", "confidence"}
    extras = set(data) - allowed_keys
    if extras:
        raise LLMValidationError(f"Unexpected fields: {sorted(extras)}")

    # 3. Validate clean_title
    clean_title = data.get("clean_title")
    if not isinstance(clean_title, str) or not clean_title.strip():
        raise LLMValidationError("clean_title must be a non-empty string.")
    if len(clean_title.strip()) < 5 or len(clean_title.strip()) > 100:
        raise LLMValidationError("clean_title must be between 5 and 100 characters.")

    # 4. Validate category
    category = data.get("category")
    if category not in ALLOWED_CATEGORIES:
        raise LLMValidationError(
            f"Invalid category '{category}'. Must be one of {sorted(ALLOWED_CATEGORIES)}."
        )

    # 5. Validate brand
    brand = data.get("brand")
    if brand is not None and not isinstance(brand, str):
        raise LLMValidationError("brand must be a string or null.")

    clean_brand = brand.strip() if isinstance(brand, str) and brand.strip() else None

    # 6. Validate tags
    tags = data.get("tags")
    if tags is None:
        tags = []
    elif not isinstance(tags, list):
        raise LLMValidationError("tags must be a list of strings.")

    if len(tags) > 5:
        raise LLMValidationError(f"tags exceeds limit of 5 items (got {len(tags)}).")

    sanitized_tags = []
    for item in tags:
        if not isinstance(item, str):
            raise LLMValidationError(f"Tag item '{item}' must be a string.")
        trimmed = item.strip()
        if trimmed != trimmed.lower():
            raise LLMValidationError(f"Tag '{trimmed}' must be lowercase.")
        if trimmed:
            if not re.fullmatch(r"[\w]+(?:[-'][\w]+)*", trimmed, flags=re.UNICODE):
                raise LLMValidationError(f"Tag '{trimmed}' must be a single word.")
            sanitized_tags.append(trimmed)

    confidence = data.get("confidence")
    if confidence is not None and (isinstance(confidence, bool) or not isinstance(confidence, (int, float)) or not math.isfinite(confidence) or not 0 <= confidence <= 1):
        raise LLMValidationError("confidence must be a finite number between 0 and 1.")

    return {
        "clean_title": clean_title.strip(),
        "category": category,
        "brand": clean_brand,
        "tags": sanitized_tags,
        "confidence": float(confidence) if confidence is not None else None,
    }
