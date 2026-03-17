"""Agent tools package."""
from typing import Callable

from src.agent.tools.snowflake_tool import snowflake_query
from src.agent.tools.chart_tool import create_chart


__all__ = [
    "snowflake_query",
    "create_chart",
    "get_default_tools",
]


def get_default_tools() -> list[Callable]:
    """Get the list of default tools for the agent."""
    return [
        snowflake_query,
        create_chart,
    ]