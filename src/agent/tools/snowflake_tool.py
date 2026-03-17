"""Snowflake tool for executing SQL queries."""
from langchain.tools import tool

from src.core.config import get_settings
from src.core.database import execute_query_with_columns


@tool
def snowflake_query(sql: str) -> str:
    """
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

    Args:
        sql: SQL query to execute against Snowflake

    Returns:
        Query results formatted as readable text
    """
    settings = get_settings()
    try:
        columns, rows = execute_query_with_columns(sql, settings)
        if not rows:
            return "Query returned no results."

        # Format as readable text
        result = f"Columns: {', '.join(columns)}\n\n"
        result += f"Found {len(rows)} rows:\n"
        for i, row in enumerate(rows[:20]):  # Limit to 20 rows
            row_str = " | ".join(str(v) if v is not None else "NULL" for v in row)
            result += f"{i+1}. {row_str}\n"

        if len(rows) > 20:
            result += f"\n... and {len(rows) - 20} more rows"

        return result
    except Exception as e:
        return f"Error executing query: {str(e)}"