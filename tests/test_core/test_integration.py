"""Integration tests for core module."""
import pytest


def test_core_module_exports():
    """Test that core module exports all expected components."""
    from src.core import Settings, get_settings, reset_settings
    from src.core.config import Settings as ConfigSettings
    from src.core.database import get_snowflake_connection, execute_query
    from src.core.llm_provider import get_llm, UnsupportedProviderError

    # Verify exports
    assert Settings is ConfigSettings
    assert callable(get_settings)
    assert callable(reset_settings)
    assert callable(get_snowflake_connection)
    assert callable(execute_query)
    assert callable(get_llm)
    assert issubclass(UnsupportedProviderError, Exception)


def test_settings_singleton():
    """Test that get_settings returns a singleton."""
    from src.core.config import get_settings, reset_settings

    reset_settings()
    settings1 = get_settings()
    settings2 = get_settings()

    assert settings1 is settings2

    reset_settings()