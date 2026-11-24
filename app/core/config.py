"""Application configuration management using Pydantic Settings."""

from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """CatalogIQ runtime settings loaded from environment or .env file."""

    # LLM Settings
    LLM_PROVIDER: str = "mock"
    LLM_CONCURRENCY: int = 5
    MOCK_LATENCY_MS: int = 200
    MOCK_FAILURE_RATE: float = 0.1

    # Optional Real LLM Provider Keys
    GROQ_API_KEY: Optional[str] = None
    GEMINI_API_KEY: Optional[str] = None

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
