"""Agent tools package."""
from typing import Callable

from src.agent.tools.snowflake_tool import snowflake_query
from src.agent.tools.chart_tool import create_chart
from src.agent.tools.db_explorer_tool import (
    list_tables,
    describe_table,
    preview_table,
    get_table_stats,
)


__all__ = [
    # Query tools
    "snowflake_query",
    # Visualization tools
    "create_chart",
    # Database exploration tools
    "list_tables",
    "describe_table",
    "preview_table",
    "get_table_stats",
    # Helper functions
    "get_default_tools",
]


def get_default_tools() -> list[Callable]:
    """Get the list of default tools for the agent."""
    return [
        # Database exploration (should be used first to understand schema)
        list_tables,
        describe_table,
        preview_table,
        get_table_stats,
        # Query execution
        snowflake_query,
        # Visualization
        create_chart,
    ]