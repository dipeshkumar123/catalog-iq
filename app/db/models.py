"""Pydantic schemas and data models for CatalogIQ.

Includes models matching the exact API contract as well as internal DB models.
"""

import json
from typing import Any, List, Literal, Optional, Union
from pydantic import BaseModel, Field, field_validator


# Allowed 8 Categories per requirement specification
AllowedCategory = Literal[
    "Groceries",
    "Beverages",
    "Personal Care",
    "Household",
    "Electronics",
    "Fashion",
    "Home & Kitchen",
    "Other",
]

# API Contract Job Statuses
JobStatus = Literal["queued", "running", "completed", "failed"]

# API Contract Product Statuses (Only enriched, failed, or approved are exposed)
ProductStatus = Literal["enriched", "pending_review", "failed", "approved"]

# Internal Database Product Statuses (includes pending & in_progress)
DBProductStatus = Literal["pending", "in_progress", "enriched", "pending_review", "failed", "approved"]


class Job(BaseModel):
    """Job object representation matching API contract."""

    id: str
    status: JobStatus
    total: int = 0
    done: int = 0
    failed: int = 0
    cache_hits: int = 0
    created_at: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


class Product(BaseModel):
    """Public product model conforming strictly to the API contract.

    Permitted statuses: 'enriched', 'failed', 'approved'.
    """

    sku: str
    raw_title: str
    raw_description: Optional[str] = None
    clean_title: Optional[str] = None
    category: Optional[AllowedCategory] = None
    brand: Optional[str] = None
    tags: Optional[List[str]] = Field(default_factory=list)
    confidence: Optional[float] = None
    review_reasons: List[str] = Field(default_factory=list)
    status: ProductStatus
    error: Optional[str] = None

    @field_validator("tags", mode="before")
    @classmethod
    def parse_tags_json(cls, v: Any) -> Optional[List[str]]:
        """Parses JSON string stored in SQLite into a list of strings."""
        if v is None:
            return []
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                if isinstance(parsed, list):
                    return [str(item) for item in parsed]
            except Exception:
                return [v]
        if isinstance(v, list):
            return [str(item) for item in v]
        return []

    @field_validator("review_reasons", mode="before")
    @classmethod
    def parse_review_reasons_json(cls, v: Any) -> List[str]:
        if v is None:
            return []
        if isinstance(v, str):
            try:
                parsed = json.loads(v)
                return [str(item) for item in parsed] if isinstance(parsed, list) else []
            except Exception:
                return []
        if isinstance(v, list):
            return [str(item) for item in v]
        return []


class ProductInDB(BaseModel):
    """Internal product model representing the full SQLite database row."""

    sku: str
    job_id: str
    raw_title: str
    raw_description: Optional[str] = None
    clean_title: Optional[str] = None
    category: Optional[AllowedCategory] = None
    brand: Optional[str] = None
    tags: Optional[Union[str, List[str]]] = None
    status: DBProductStatus
    error: Optional[str] = None
    confidence: Optional[float] = None
    review_reasons: Optional[Union[str, List[str]]] = None
    attempts: int = 0
    content_hash: str
    updated_at: str


class ProductInput(BaseModel):
    """Input listing item within a job creation payload."""

    sku: str
    raw_title: str
    raw_description: Optional[str] = None


class JobCreateRequest(BaseModel):
    """Payload for POST /api/jobs."""

    products: List[ProductInput]


class ProductPatchRequest(BaseModel):
    """Payload for PATCH /api/products/<sku> review approvals."""

    clean_title: Optional[str] = None
    category: Optional[AllowedCategory] = None
    tags: Optional[List[str]] = None
    brand: Optional[str] = None


class ProductListResponse(BaseModel):
    """Response payload for GET /api/products."""

    items: List[Product]
    page: int
    page_size: int
    total: int


class HealthResponse(BaseModel):
    """Response payload for GET /api/health."""

    status: str
    llm_provider: str
    llm_concurrency: int


class MetricsResponse(BaseModel):
    """Response payload for GET /api/metrics."""

    llm_calls_total: int
    llm_errors_total: int
    max_concurrent_llm_calls: int
