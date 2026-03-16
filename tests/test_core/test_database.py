"""Tests for database connection module."""
import pytest
from unittest.mock import patch, MagicMock


def test_get_snowflake_connection_returns_connection():
    """Test that get_snowflake_connection returns a connection object."""
    with patch("src.core.database.snowflake.connector.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        from src.core.database import get_snowflake_connection
        from src.core.config import Settings

        settings = Settings(
            snowflake_account="test",
            snowflake_user="user",
            snowflake_password="pass",
        )
        conn = get_snowflake_connection(settings)

        assert conn == mock_conn
        mock_connect.assert_called_once()


def test_execute_query_returns_results():
    """Test that execute_query returns query results."""
    with patch("src.core.database.get_snowflake_connection") as mock_get_conn:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(1, "test"), (2, "test2")]
        mock_cursor.description = [("id",), ("name",)]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        from src.core.database import execute_query
        from src.core.config import Settings

        settings = Settings()
        results = execute_query("SELECT * FROM test", settings)

        assert len(results) == 2
        assert results[0] == (1, "test")


def test_execute_query_handles_errors():
    """Test that execute_query properly handles and re-raises errors."""
    with patch("src.core.database.get_snowflake_connection") as mock_get_conn:
        mock_get_conn.side_effect = Exception("Connection failed")

        from src.core.database import execute_query
        from src.core.config import Settings

        settings = Settings()

        with pytest.raises(Exception, match="Connection failed"):
            execute_query("SELECT 1", settings)