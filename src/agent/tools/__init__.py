"""Agent tools package.

This module provides tools for the AZ Data Agent:
- Snowflake SQL tools (via SQLDatabaseToolkit)
- Chart visualization tool
"""
from typing import Callable

from langchain_core.language_models import BaseLanguageModel
from langchain_core.tools import BaseTool

from src.agent.tools.snowflake import get_snowflake_tools, get_snowflake_db
from src.agent.tools.chart_tool import create_chart


__all__ = [
    # Snowflake tools (SQLDatabaseToolkit: query, schema, list_tables, query_checker)
    "get_snowflake_tools",
    "get_snowflake_db",
    # Visualization tools
    "create_chart",
    # Helper functions
    "get_default_tools",
]


def get_default_tools(llm: BaseLanguageModel) -> list[BaseTool]:
    """Get the list of default tools for the agent.

    Args:
        llm: Language model required for query checking tool.

    Returns:
        List of tools including Snowflake SQL tools and visualization.
    """
    tools = get_snowflake_tools(llm)
    tools.append(create_chart)
    return tools