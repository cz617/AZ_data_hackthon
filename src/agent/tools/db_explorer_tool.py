"""Database exploration tool for Snowflake schema discovery."""
from typing import Literal, Optional

from langchain.tools import tool

from src.core.config import get_settings
from src.core.database import execute_query_with_columns


@tool
def list_tables(
    database: str = "ENT_HACKATHON_DATA_SHARE",
    schema: str = "EA_HACKATHON",
) -> str:
    """
    List all tables available in the Snowflake database schema.

    Use this tool to discover what tables exist before writing queries.

    Args:
        database: Database name (default: ENT_HACKATHON_DATA_SHARE)
        schema: Schema name (default: EA_HACKATHON)

    Returns:
        List of table names with descriptions
    """
    settings = get_settings()
    try:
        sql = f"""
        SELECT
            TABLE_NAME,
            TABLE_TYPE,
            COMMENT
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}'
        ORDER BY TABLE_NAME
        """
        columns, rows = execute_query_with_columns(sql, settings)

        if not rows:
            return f"No tables found in {database}.{schema}"

        result = f"## Tables in {database}.{schema}\n\n"
        result += "| Table Name | Type | Comment |\n"
        result += "|------------|------|--------|\n"

        for row in rows:
            table_name = row[0] or ""
            table_type = row[1] or ""
            comment = row[2] or ""
            result += f"| {table_name} | {table_type} | {comment} |\n"

        return result
    except Exception as e:
        return f"Error listing tables: {str(e)}"


@tool
def describe_table(
    table_name: str,
    database: str = "ENT_HACKATHON_DATA_SHARE",
    schema: str = "EA_HACKATHON",
) -> str:
    """
    Get the structure of a specific table including columns, types, and descriptions.

    Use this tool to understand table schema before writing queries.

    Args:
        table_name: Name of the table to describe
        database: Database name (default: ENT_HACKATHON_DATA_SHARE)
        schema: Schema name (default: EA_HACKATHON)

    Returns:
        Table structure with column names, types, and descriptions
    """
    settings = get_settings()
    try:
        sql = f"""
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            COMMENT
        FROM {database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME = '{table_name.upper()}'
        ORDER BY ORDINAL_POSITION
        """
        columns, rows = execute_query_with_columns(sql, settings)

        if not rows:
            return f"Table '{table_name}' not found in {database}.{schema}"

        result = f"## Table: {table_name}\n\n"
        result += "| Column | Type | Nullable | Default | Comment |\n"
        result += "|--------|------|----------|---------|--------|\n"

        for row in rows:
            col_name = row[0] or ""
            data_type = row[1] or ""
            nullable = row[2] or ""
            default = row[3] or ""
            comment = row[4] or ""
            result += f"| {col_name} | {data_type} | {nullable} | {default} | {comment} |\n"

        return result
    except Exception as e:
        return f"Error describing table: {str(e)}"


@tool
def preview_table(
    table_name: str,
    limit: int = 5,
    database: str = "ENT_HACKATHON_DATA_SHARE",
    schema: str = "EA_HACKATHON",
) -> str:
    """
    Preview sample data from a table to understand its content.

    Use this tool to see actual data examples before writing complex queries.

    Args:
        table_name: Name of the table to preview
        limit: Number of rows to return (default: 5, max: 20)
        database: Database name (default: ENT_HACKATHON_DATA_SHARE)
        schema: Schema name (default: EA_HACKATHON)

    Returns:
        Sample rows from the table
    """
    settings = get_settings()
    limit = min(limit, 20)  # Cap at 20 rows

    try:
        full_table_path = f"{database}.{schema}.{table_name.upper()}"
        sql = f"SELECT * FROM {full_table_path} LIMIT {limit}"

        columns, rows = execute_query_with_columns(sql, settings)

        if not rows:
            return f"Table '{table_name}' is empty or not found."

        result = f"## Preview: {table_name} ({len(rows)} rows)\n\n"
        result += "| " + " | ".join(columns) + " |\n"
        result += "| " + " | ".join(["---"] * len(columns)) + " |\n"

        for row in rows:
            row_values = [str(v) if v is not None else "NULL" for v in row]
            # Truncate long values
            row_values = [v[:50] + "..." if len(v) > 50 else v for v in row_values]
            result += "| " + " | ".join(row_values) + " |\n"

        return result
    except Exception as e:
        return f"Error previewing table: {str(e)}"


@tool
def get_table_stats(
    table_name: str,
    database: str = "ENT_HACKATHON_DATA_SHARE",
    schema: str = "EA_HACKATHON",
) -> str:
    """
    Get statistics about a table including row count and size.

    Use this tool to understand data volume before running expensive queries.

    Args:
        table_name: Name of the table
        database: Database name (default: ENT_HACKATHON_DATA_SHARE)
        schema: Schema name (default: EA_HACKATHON)

    Returns:
        Table statistics including row count and bytes
    """
    settings = get_settings()
    try:
        sql = f"""
        SELECT
            TABLE_NAME,
            ROW_COUNT,
            BYTES,
            CREATED,
            LAST_ALTERED
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME = '{table_name.upper()}'
        """
        columns, rows = execute_query_with_columns(sql, settings)

        if not rows:
            return f"Table '{table_name}' not found."

        row = rows[0]
        result = f"## Table Statistics: {table_name}\n\n"
        result += f"- **Row Count**: {row[1]:,}\n" if row[1] else "- **Row Count**: Unknown\n"
        result += f"- **Size**: {row[2]:,} bytes ({row[2]/1024/1024:.2f} MB)\n" if row[2] else "- **Size**: Unknown\n"
        result += f"- **Created**: {row[3]}\n" if row[3] else ""
        result += f"- **Last Altered**: {row[4]}\n" if row[4] else ""

        return result
    except Exception as e:
        return f"Error getting table stats: {str(e)}"