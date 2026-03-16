"""Snowflake database connection and query execution."""
from typing import Any

import snowflake.connector
from snowflake.connector import SnowflakeConnection

from src.core.config import Settings


def get_snowflake_connection(settings: Settings) -> SnowflakeConnection:
    """
    Create and return a Snowflake connection.

    Args:
        settings: Application settings containing connection parameters

    Returns:
        SnowflakeConnection: Active Snowflake connection
    """
    connection_params = {
        "account": settings.snowflake_account,
        "user": settings.snowflake_user,
        "password": settings.snowflake_password,
        "warehouse": settings.snowflake_warehouse,
        "database": settings.snowflake_database,
        "schema": settings.snowflake_schema,
    }

    # Add role if specified
    if settings.snowflake_role:
        connection_params["role"] = settings.snowflake_role

    return snowflake.connector.connect(**connection_params)


def execute_query(
    sql: str,
    settings: Settings,
    params: dict[str, Any] | None = None,
) -> list[tuple]:
    """
    Execute a SQL query and return results.

    Args:
        sql: SQL query to execute
        settings: Application settings for database connection
        params: Optional query parameters

    Returns:
        List of result tuples

    Raises:
        Exception: If query execution fails
    """
    conn = get_snowflake_connection(settings)

    try:
        with conn.cursor() as cursor:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            return cursor.fetchall()
    finally:
        conn.close()


def execute_query_with_columns(
    sql: str,
    settings: Settings,
    params: dict[str, Any] | None = None,
) -> tuple[list[str], list[tuple]]:
    """
    Execute a SQL query and return column names and results.

    Args:
        sql: SQL query to execute
        settings: Application settings for database connection
        params: Optional query parameters

    Returns:
        Tuple of (column_names, result_rows)
    """
    conn = get_snowflake_connection(settings)

    try:
        with conn.cursor() as cursor:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            return columns, results
    finally:
        conn.close()