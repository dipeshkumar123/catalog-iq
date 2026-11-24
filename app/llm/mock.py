"""Mock LLM Provider simulating latency, random failures, and realistic enrichment."""

import asyncio
import random
import re
from typing import Any, Dict, List, Optional

from app.core.config import settings
from app.core.exceptions import LLMError
from app.llm.base import BaseLLMProvider
from app.llm.validator import validate_enrichment

# Keyword to category mappings
CATEGORY_KEYWORDS = {
    "Beverages": [
        "coffee", "tea", "drink", "juice", "soda", "water", "cola", "beverage",
        "energy", "syrup", "cold brew", "latte", "espresso", "frappe"
    ],
    "Groceries": [
        "milk", "butter", "bread", "oil", "flour", "rice", "dal", "chips",
        "biscuit", "cookie", "noodle", "chocolate", "paneer", "sauce",
        "ketchup", "oats", "cereal", "atta", "sugar", "salt", "snack",
        "namkeen", "corn flakes", "yogurt", "masala", "spice"
    ],
    "Personal Care": [
        "soap", "shampoo", "cream", "lotion", "wash", "toothpaste", "sanitary",
        "serum", "deodorant", "perfume", "razor", "pad", "diaper", "brush",
        "mouthwash", "conditioner", "sunscreen", "gel", "fragrance"
    ],
    "Household": [
        "detergent", "cleaner", "air freshener", "dishwash", "scrub",
        "insect", "repellent", "spray", "bleach", "wipe", "mop", "refill"
    ],
    "Electronics": [
        "tv", "phone", "headphone", "earbud", "charger", "mouse", "keyboard",
        "battery", "laptop", "speaker", "router", "watch", "camera", "cable",
        "adapter", "microsd", "flash drive", "power bank", "smartwatch"
    ],
    "Fashion": [
        "jeans", "shirt", "t-shirt", "shoe", "clog", "jacket", "blazer",
        "kurta", "underwear", "brief", "trouser", "sunglasses", "polo", "dress"
    ],
    "Home & Kitchen": [
        "cooker", "kettle", "iron", "grinder", "pan", "bed", "sheet",
        "pillow", "mattress", "flask", "toaster", "lunch box", "fan",
        "blender", "sponge", "bottle", "safe", "locker"
    ],
}


class MockLLMProvider(BaseLLMProvider):
    """Simulates an LLM with configurable latency, failure rate, and deterministic test hooks."""

    def __init__(
        self,
        latency_ms: Optional[int] = None,
        failure_rate: Optional[float] = None,
    ):
        self.latency_ms = latency_ms if latency_ms is not None else settings.MOCK_LATENCY_MS
        self.failure_rate = failure_rate if failure_rate is not None else settings.MOCK_FAILURE_RATE

    async def enrich(
        self,
        raw_title: str,
        raw_description: Optional[str] = None,
        validate: bool = True,
    ) -> Dict[str, Any]:
        """Simulates LLM enrichment with artificial delay and randomized/deterministic failures."""
        # 1. Simulate network / compute latency
        if self.latency_ms > 0:
            await asyncio.sleep(self.latency_ms / 1000.0)

        # 2. Deterministic Testing Hooks
        if "__FAIL_JSON__" in raw_title:
            raw_output = "{'bad': 'json'"
            if validate:
                return validate_enrichment(raw_output)
            return raw_output  # type: ignore

        if "__FAIL_CATEGORY__" in raw_title:
            raw_output = {
                "clean_title": self._generate_clean_title(raw_title),
                "category": "Toys",  # Not among allowed 8
                "brand": self._extract_brand(raw_title),
                "tags": self._extract_tags(raw_title),
            }
            if validate:
                return validate_enrichment(raw_output)
            return raw_output

        # 3. Randomized Failure Rate
        if self.failure_rate > 0 and random.random() < self.failure_rate:
            raise LLMError("Simulated LLM failure (random failure rate triggered).")

        # 4. Realistic Mock Output Generation
        clean_title = self._generate_clean_title(raw_title)
        category = self._classify_category(raw_title, raw_description)
        brand = self._extract_brand(raw_title)
        tags = self._extract_tags(raw_title)

        data = {
            "clean_title": clean_title,
            "category": category,
            "brand": brand,
            "tags": tags,
            "confidence": 0.92,
        }

        if validate:
            return validate_enrichment(data)
        return data

    def _generate_clean_title(self, raw_title: str) -> str:
        """Tidies raw title by collapsing whitespace and normalizing capitalization."""
        collapsed = re.sub(r"\s+", " ", raw_title).strip()
        # Title case while keeping numbers and units clean
        words = collapsed.split(" ")
        clean_words = []
        for word in words:
            # If word is an abbreviation or has mixed digits (e.g. 500G, 750W, USB-C), preserve
            if re.search(r"\d", word) or word.isupper() and len(word) <= 3:
                clean_words.append(word)
            else:
                clean_words.append(word.capitalize())
        return " ".join(clean_words)

    def _classify_category(self, raw_title: str, raw_description: Optional[str]) -> str:
        """Classifies listing using keyword heuristics into one of 8 allowed categories."""
        combined_text = f"{raw_title} {raw_description or ''}".lower()
        for category, keywords in CATEGORY_KEYWORDS.items():
            for kw in keywords:
                if re.search(r"\b" + re.escape(kw) + r"\b", combined_text):
                    return category
        return "Other"

    def _extract_brand(self, raw_title: str) -> Optional[str]:
        """Extracts brand if the first word appears to be a brand identifier."""
        words = raw_title.strip().split()
        if not words:
            return None
        first_word = words[0]
        # Clean punctuation from first word
        first_clean = re.sub(r"[^\w]", "", first_word)
        if first_clean and (first_clean[0].isupper() or first_clean.isupper()):
            # Return proper title case
            return first_clean.capitalize()
        return None

    def _extract_tags(self, raw_title: str) -> List[str]:
        """Extracts up to 5 lowercase tags from listing tokens."""
        # Split into alphanumeric tokens
        tokens = re.findall(r"[a-zA-Z]{3,}", raw_title.lower())
        stopwords = {"pack", "with", "from", "for", "and", "the", "pck"}
        filtered = [t for t in tokens if t not in stopwords]
        # De-duplicate while preserving order
        unique_tags = list(dict.fromkeys(filtered))
        return unique_tags[:5]
