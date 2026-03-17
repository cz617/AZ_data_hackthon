"""Snowflake database tools using LangChain SQLDatabaseToolkit.

This module provides a clean interface to Snowflake using LangChain's
standard SQLDatabaseToolkit, which includes:
- sql_db_query: Execute SQL queries
- sql_db_schema: Get table schema and sample data
- sql_db_list_tables: List all tables
- sql_db_query_checker: Validate SQL before execution
"""
import urllib.parse
from typing import Optional

from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_community.utilities import SQLDatabase
from langchain_core.language_models import BaseLanguageModel
from langchain_core.tools import BaseTool
from snowflake.sqlalchemy import URL

from src.core.config import Settings, get_settings

# Global database instance (connection pool is managed by SQLAlchemy)
_db: Optional[SQLDatabase] = None


def get_snowflake_db(settings: Optional[Settings] = None) -> SQLDatabase:
    """Create or return the Snowflake database connection.

    Uses SQLAlchemy's built-in connection pooling for efficient connection management.

    Args:
        settings: Application settings. Uses global settings if not provided.

    Returns:
        SQLDatabase instance with connection pooling enabled.
    """
    global _db
    if _db is not None:
        return _db

    settings = settings or get_settings()

    # URL encode password to handle special characters
    password_encoded = urllib.parse.quote(settings.snowflake_password)

    # Build Snowflake SQLAlchemy URL using the helper
    url_params = {
        "account": settings.snowflake_account,
        "user": settings.snowflake_user,
        "password": password_encoded,
        "database": settings.snowflake_database,
        "schema": settings.snowflake_schema,
        "warehouse": settings.snowflake_warehouse,
    }

    # Add role if specified
    if settings.snowflake_role:
        url_params["role"] = settings.snowflake_role

    # Create database connection with connection pool
    _db = SQLDatabase.from_uri(
        URL(**url_params),
        engine_args={
            "pool_size": 5,          # Number of connections to keep in pool
            "max_overflow": 10,      # Additional connections allowed beyond pool_size
            "pool_pre_ping": True,   # Check connection health before use
            "pool_recycle": 3600,    # Recycle connections after 1 hour
        },
    )

    return _db


def get_snowflake_tools(llm: BaseLanguageModel, settings: Optional[Settings] = None) -> list[BaseTool]:
    """Get Snowflake tools using LangChain's SQLDatabaseToolkit.

    Returns 4 standard tools:
    - sql_db_query: Execute SQL queries against Snowflake
    - sql_db_schema: Get table schema and sample rows
    - sql_db_list_tables: List all available tables
    - sql_db_query_checker: Validate SQL syntax before execution

    Args:
        llm: Language model for query checking
        settings: Application settings. Uses global settings if not provided.

    Returns:
        List of 4 SQL tools ready for agent use.
    """
    db = get_snowflake_db(settings)
    toolkit = SQLDatabaseToolkit(db=db, llm=llm)
    return toolkit.get_tools()


def reset_db() -> None:
    """Reset the database connection pool.

    Useful for testing or when connection settings change.
    """
    global _db
    _db = None


__all__ = [
    "get_snowflake_db",
    "get_snowflake_tools",
    "reset_db",
]