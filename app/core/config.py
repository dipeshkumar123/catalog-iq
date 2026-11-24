"""Application configuration management using Pydantic Settings."""

from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """CatalogIQ runtime settings loaded from environment or .env file."""

    # LLM Settings
    LLM_PROVIDER: str = "mock"
    LLM_CONCURRENCY: int = 5
    LLM_BATCH_SIZE: int = 10
    LLM_BATCH_WINDOW_MS: int = 20
    LLM_REVIEW_CONFIDENCE_THRESHOLD: float = 0.85
    LLM_SIMPLE_TITLE_WORD_LIMIT: int = 8
    MOCK_LATENCY_MS: int = 200
    MOCK_FAILURE_RATE: float = 0.1
    GROQ_MODEL: str = "openai/gpt-oss-120b"
    GEMINI_MODEL: str = "gemini-3.5-flash-lite"
    LLM_FALLBACK_PROVIDER: Optional[str] = None
    LLM_COMPLEX_PROVIDER: Optional[str] = None

    # Optional Real LLM Provider Keys
    GROQ_API_KEY: Optional[str] = None
    GEMINI_API_KEY: Optional[str] = None

    # Optional Redis adapters for distributed jobs, cache, and rate limiting.
    REDIS_URL: Optional[str] = None
    REDIS_QUEUE_STREAM: str = "catalogiq:jobs"
    REDIS_QUEUE_GROUP: str = "catalogiq-workers"
    REDIS_QUEUE_CLAIM_IDLE_MS: int = 300000
    REDIS_CACHE_TTL_SECONDS: int = 86400
    LLM_RATE_LIMIT_RPM: int = 0
    LLM_RATE_LIMIT_TPM: int = 0

    # Server & Storage Settings
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    DATABASE_PATH: str = "catalog_iq.db"
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
