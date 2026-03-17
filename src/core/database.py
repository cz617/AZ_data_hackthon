"""Database connection and query execution (SQLite implementation)."""
from typing import Any

import sqlite3

# SQLite database path
DB_PATH = "data/detection.db"


def get_connection():
    """
    Create and return a SQLite connection.

    Returns:
        SQLite connection object
    """
    return sqlite3.connect(DB_PATH)


def execute_query(
    sql: str,
    settings=None,
    params: tuple[Any, ...] | list[Any] | None = None,
) -> list[tuple]:
    """
    Execute a SQL query and return results.

    Args:
        sql: SQL query to execute
        settings: Application settings (unused, for compatibility)
        params: Optional query parameters (SQLite uses ?)

    Returns:
        List of result tuples

    Raises:
        Exception: If query execution fails
    """
    conn = get_connection()

    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        results = cursor.fetchall()
        conn.commit()
        return results
    finally:
        conn.close()


def execute_query_with_columns(
    sql: str,
    settings=None,
    params: tuple[Any, ...] | list[Any] | None = None,
) -> tuple[list[str], list[tuple]]:
    """
    Execute a SQL query and return column names and results.

    Args:
        sql: SQL query to execute
        settings: Application settings (unused, for compatibility)
        params: Optional query parameters (SQLite uses ?)

    Returns:
        Tuple of (column_names, result_rows)
    """
    conn = get_connection()

    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        conn.commit()
        return columns, results
    finally:
        conn.close()


# Legacy functions for backward compatibility
def get_snowflake_connection(settings=None):
    """Deprecated: Use get_connection() instead."""
    return get_connection()