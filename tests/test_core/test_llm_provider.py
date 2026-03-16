"""Tests for LLM provider module."""
import pytest
from unittest.mock import patch, MagicMock


def test_get_llm_returns_claude_model():
    """Test that get_llm returns Claude model when provider is claude."""
    with patch("src.core.llm_provider.ChatAnthropic") as mock_claude:
        mock_model = MagicMock()
        mock_claude.return_value = mock_model

        from src.core.llm_provider import get_llm
        from src.core.config import Settings

        settings = Settings(
            llm_provider="claude",
            llm_model="claude-sonnet-4-5-20250929",
            llm_api_key="test-key",
        )
        llm = get_llm(settings)

        assert llm == mock_model
        mock_claude.assert_called_once_with(
            model="claude-sonnet-4-5-20250929",
            api_key="test-key",
        )


def test_get_llm_returns_openai_model():
    """Test that get_llm returns OpenAI model when provider is openai."""
    with patch("src.core.llm_provider.ChatOpenAI") as mock_openai:
        mock_model = MagicMock()
        mock_openai.return_value = mock_model

        from src.core.llm_provider import get_llm
        from src.core.config import Settings

        settings = Settings(
            llm_provider="openai",
            llm_model="gpt-4",
            llm_api_key="test-key",
        )
        llm = get_llm(settings)

        assert llm == mock_model
        mock_openai.assert_called_once()


def test_get_llm_raises_for_unsupported_provider():
    """Test that get_llm raises error for unsupported provider."""
    from src.core.llm_provider import get_llm, UnsupportedProviderError
    from src.core.config import Settings

    # Create settings with invalid provider by bypassing validation
    settings = Settings()

    with pytest.raises(UnsupportedProviderError):
        get_llm(settings, provider="unsupported")