"""Base abstract interface for LLM providers."""

from abc import ABC, abstractmethod
import asyncio
from typing import Any, Dict, List, Optional, Sequence, Tuple


class BaseLLMProvider(ABC):
    """Abstract interface shared by both Mock and Real LLM providers."""

    @abstractmethod
    async def enrich(
        self,
        raw_title: str,
        raw_description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Enriches raw product listing into structured catalogue metadata.

        Args:
            raw_title: The original raw title string from seller.
            raw_description: Optional seller description.

        Returns:
            Dictionary containing clean_title, category, brand, and tags.

        Raises:
            LLMError: On simulated or upstream provider errors.
            LLMValidationError: If the enrichment violates validation rules.
        """
        pass

    async def enrich_batch(self, products: Sequence[Tuple[str, Optional[str]]]) -> List[Dict[str, Any]]:
        """Default compatibility path; batch-aware providers override this method."""
        return await asyncio.gather(*(self.enrich(title, description) for title, description in products))
