"""LLM provider abstraction for multiple model backends."""
from typing import Literal

from langchain_anthropic import ChatAnthropic
from langchain_openai import AzureChatOpenAI, ChatOpenAI

from src.core.config import Settings


class UnsupportedProviderError(Exception):
    """Raised when an unsupported LLM provider is requested."""

    pass


def get_llm(
    settings: Settings,
    provider: Literal["claude", "openai", "azure"] | None = None,
):
    """
    Get an LLM instance based on settings.

    Args:
        settings: Application settings containing LLM configuration
        provider: Optional provider override (defaults to settings.llm_provider)

    Returns:
        LangChain chat model instance

    Raises:
        UnsupportedProviderError: If provider is not supported
    """
    provider = provider or settings.llm_provider

    if provider == "claude":
        return ChatAnthropic(
            model=settings.llm_model,
            api_key=settings.llm_api_key,
        )

    elif provider == "openai":
        return ChatOpenAI(
            model=settings.llm_model,
            api_key=settings.llm_api_key,
        )

    elif provider == "azure":
        if not settings.azure_openai_endpoint:
            raise ValueError(
                "Azure OpenAI requires azure_openai_endpoint to be set"
            )

        return AzureChatOpenAI(
            azure_deployment=settings.azure_openai_deployment,
            azure_endpoint=settings.azure_openai_endpoint,
            api_version=settings.azure_openai_api_version,
            api_key=settings.llm_api_key,
        )

    else:
        raise UnsupportedProviderError(
            f"Unsupported LLM provider: {provider}. "
            f"Supported providers: claude, openai, azure"
        )