"""Tests for configuration management."""
import os
import pytest
from pathlib import Path


def test_settings_loads_from_env(monkeypatch):
    """Test that Settings loads values from environment variables."""
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_MODEL", "gpt-4")
    monkeypatch.setenv("LLM_API_KEY", "test-key")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test-account")
    monkeypatch.setenv("MONITOR_INTERVAL_MINUTES", "10")

    from src.core.config import Settings

    settings = Settings()

    assert settings.llm_provider == "openai"
    assert settings.llm_model == "gpt-4"
    assert settings.llm_api_key == "test-key"
    assert settings.snowflake_account == "test-account"
    assert settings.monitor_interval_minutes == 10


def test_settings_has_defaults():
    """Test that Settings has sensible defaults."""
    from src.core.config import Settings

    # Create with minimal env (clear any existing)
    settings = Settings()

    assert settings.llm_provider == "claude"
    assert settings.snowflake_database == "ENT_HACKATHON_DATA_SHARE"
    assert settings.snowflake_schema == "EA_HACKATHON"
    assert settings.monitor_interval_minutes == 5


def test_settings_validates_llm_provider():
    """Test that Settings validates LLM provider."""
    import pydantic
    from src.core.config import Settings

    with pytest.raises(pydantic.ValidationError):
        Settings(llm_provider="invalid_provider")