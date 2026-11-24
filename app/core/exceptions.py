"""Domain and pipeline exceptions for CatalogIQ."""


class CatalogIQError(Exception):
    """Base exception for all CatalogIQ domain and pipeline errors."""

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class JobNotFoundError(CatalogIQError):
    """Raised when querying a job that does not exist in the database."""

    def __init__(self, job_id: str):
        super().__init__(f"Job '{job_id}' not found.")
        self.job_id = job_id


class ProductNotFoundError(CatalogIQError):
    """Raised when querying a product SKU that does not exist in the database."""

    def __init__(self, sku: str):
        super().__init__(f"Product with SKU '{sku}' not found.")
        self.sku = sku


class InvalidProductDataError(CatalogIQError):
    """Raised when input product data fails validation (e.g., missing title or SKU)."""
    pass


class LLMError(CatalogIQError):
    """Base exception for LLM provider failures (network, timeout, API error)."""
    pass


class LLMValidationError(LLMError):
    """Raised when LLM output violates schema or produces an unapproved category."""
    pass


class RetryExhaustedError(LLMError):
    """Raised when a product fails all retry attempts (e.g., 4 attempts exhausted)."""

    def __init__(self, sku: str, attempts: int, last_error: str):
        super().__init__(f"Product '{sku}' failed after {attempts} attempts. Last error: {last_error}")
        self.sku = sku
        self.attempts = attempts
        self.last_error = last_error
