"""Configuration management using Pydantic Settings."""
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # LLM Configuration
    llm_provider: Literal["claude", "openai", "azure"] = "claude"
    llm_model: str = "claude-sonnet-4-5-20250929"
    llm_api_key: str = ""

    # Azure-specific (only needed if llm_provider == "azure")
    azure_openai_endpoint: str = ""
    azure_openai_deployment: str = ""
    azure_openai_api_version: str = "2024-02-01"

    # Snowflake Configuration
    snowflake_account: str = ""
    snowflake_user: str = ""
    snowflake_password: str = ""
    snowflake_warehouse: str = "COMPUTE_WH"
    snowflake_database: str = "ENT_HACKATHON_DATA_SHARE"
    snowflake_schema: str = "EA_HACKATHON"
    snowflake_role: str = ""

    # Monitor Configuration
    monitor_interval_minutes: int = 5
    monitor_enabled: bool = True

    # Application Configuration
    app_name: str = "AZ Data Agent"
    debug: bool = False
    log_level: str = "INFO"


# Global settings instance
_settings: Settings | None = None


def get_settings() -> Settings:
    """Get or create the global Settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reset_settings() -> None:
    """Reset the global settings instance (useful for testing)."""
    global _settings
    _settings = None