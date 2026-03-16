"""Snowflake tool for executing SQL queries."""
from typing import Optional, Type
from pydantic import BaseModel, Field
from langchain.tools import BaseTool

from src.core.config import get_settings
from src.core.database import execute_query_with_columns


class SnowflakeToolInput(BaseModel):
    """Input for Snowflake tool."""
    sql: str = Field(description="SQL query to execute against Snowflake")


class SnowflakeTool(BaseTool):
    """Tool for executing SQL queries against Snowflake."""

    name: str = "snowflake_query"
    description: str = """
    Execute SQL queries against the Snowflake data warehouse.
    Use this tool to query business data including P&L metrics, market data,
    product information, and time-based analysis.

    Available tables:
    - FACT_PNL_BASE_BRAND: Financial P&L metrics
    - FACT_COM_BASE_BRAND: Commercial/market metrics
    - DIM_ACCOUNT: P&L accounts
    - DIM_PRODUCT: Products/brands
    - DIM_MARKET: Therapeutic markets
    - DIM_TIME: Calendar dimension
    - DIM_SCENARIO: Planning scenarios
    """
    args_schema: Type[BaseModel] = SnowflakeToolInput

    def _run(self, sql: str) -> str:
        """Execute the SQL query and return results."""
        settings = get_settings()
        try:
            columns, rows = execute_query_with_columns(sql, settings)
            if not rows:
                return "Query returned no results."

            # Format as readable text
            result = f"Columns: {', '.join(columns)}\n\n"
            result += f"Found {len(rows)} rows:\n"
            for i, row in enumerate(rows[:10]):  # Limit to 10 rows
                result += f"{i+1}. {row}\n"

            if len(rows) > 10:
                result += f"\n... and {len(rows) - 10} more rows"

            return result
        except Exception as e:
            return f"Error executing query: {str(e)}"

    async def _arun(self, sql: str) -> str:
        """Async execution."""
        return self._run(sql)