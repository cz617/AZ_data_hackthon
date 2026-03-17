#!/usr/bin/env python3
"""
Search Objects - Unified Database Schema Explorer with Progressive Disclosure
==============================================================================

A dbhub MCP-inspired schema exploration tool that follows progressive disclosure
patterns for efficient database navigation.

Usage:
    # Progressive disclosure pattern - explore tables
    python .amandax/skills/db-toolkit/scripts/search.py --database bird_testing --type table --detail names
    python .amandax/skills/db-toolkit/scripts/search.py --database bird_testing --type table --pattern "%user%" --detail summary

    # Explore columns
    python .amandax/skills/db-toolkit/scripts/search.py --database bird_testing --type column --table users --detail full

    # Explore indexes
    python .amandax/skills/db-toolkit/scripts/search.py --database bird_testing --type index --table users --detail full

    # SQLite
    python .amandax/skills/db-toolkit/scripts/search.py --sqlite db.sqlite --type table --detail full

Detail Levels:
    | Level   | What you get                          | When to use                        |
    |---------|---------------------------------------|------------------------------------|
    | names   | Just object names                     | Browsing, finding the right table  |
    | summary | Names + metadata (row count, etc.)    | Choosing between similar tables    |
    | full    | Complete structure (columns, indexes) | Before writing queries             |

Object Types:
    - schema: Database schemas (PostgreSQL) or database-level info (SQLite/MySQL)
    - table: Table definitions
    - column: Column definitions
    - index: Index definitions
"""

import argparse
import json
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


def _validate_identifier(name: str) -> str:
    """Validate SQL identifier to prevent injection.

    Args:
        name: Table, column, or index name

    Returns:
        The validated name

    Raises:
        ValueError: If name contains invalid characters
    """
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", name):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return name


# Load environment variables if available
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


# =============================================================================
# Connection Management
# =============================================================================


def find_settings_file() -> Path | None:
    """Find the settings.json file starting from current directory."""
    current = Path.cwd()

    # Check current directory first
    settings = current / ".amandax" / "settings.json"
    if settings.exists():
        return settings

    # Check parent directories
    for _ in range(5):
        parent = current.parent
        if parent == current:
            break
        settings = parent / ".amandax" / "settings.json"
        if settings.exists():
            return settings
        current = parent

    return None


def load_db_config_from_settings() -> dict | None:
    """Load database configuration from .amandax/settings.json."""
    settings_file = find_settings_file()
    if not settings_file:
        return None

    try:
        with open(settings_file, encoding="utf-8") as f:
            settings = json.load(f)

        db_config = settings.get("database", {})

        # Check for named database configurations
        # Format: { "database": { "prod_dw": { "type": "postgresql", ... } } }
        return db_config
    except (OSError, json.JSONDecodeError):
        return None


def get_connection_config(database_name: str | None = None) -> dict | None:
    """Get database connection configuration from settings."""
    db_config = load_db_config_from_settings()
    if not db_config:
        return None

    # If database_name provided, look for that specific connection
    if database_name and database_name in db_config:
        return db_config[database_name]

    # Otherwise return first available connection (for backwards compatibility)
    # Or look for default connection
    for _name, config in db_config.items():
        if isinstance(config, dict) and "type" in config:
            return config

    return None


def create_connection(
    sqlite_path: str | None = None,
    database_name: str | None = None,
) -> Any:
    """Create a database connection.

    Args:
        sqlite_path: Path to SQLite database file
        database_name: Name of database connection from settings.json

    Returns:
        Database connection object
    """
    if sqlite_path:
        conn = sqlite3.connect(sqlite_path)
        conn.row_factory = sqlite3.Row
        return conn

    if database_name:
        config = get_connection_config(database_name)
        if not config:
            raise ValueError(f"Database connection '{database_name}' not found in settings.json")

        db_type = config.get("type", "sqlite")
        conn_str = config.get("connectionString", "")

        if db_type == "sqlite":
            # Parse sqlite:///path format
            path = conn_str.replace("sqlite://", "").replace("sqlite:///", "")
            conn = sqlite3.connect(path)
            conn.row_factory = sqlite3.Row
            return conn

        elif db_type == "postgresql":
            try:
                import psycopg2
                import psycopg2.extras
            except ImportError:
                raise ImportError("psycopg2 not installed. Run: pip install psycopg2-binary") from None

            conn = psycopg2.connect(conn_str)
            return conn

        elif db_type == "mysql":
            try:
                import mysql.connector
            except ImportError:
                raise ImportError(
                    "mysql-connector-python not installed. Run: pip install mysql-connector-python"
                ) from None

            # Parse MySQL connection string
            mysql_params = _parse_mysql_conn_str(conn_str)
            conn = mysql.connector.connect(**mysql_params)
            return conn

        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    raise ValueError("Either --sqlite or --database must be specified")


def _parse_mysql_conn_str(conn_str: str) -> dict:
    """Parse MySQL connection string into keyword arguments."""
    conn_str = conn_str.replace("mysql://", "")
    match = re.match(r'([^:]+):([^@]*)@([^:/]+)(?::(\d+))?/(.+)', conn_str)
    if not match:
        raise ValueError("Invalid MySQL connection string format")

    user, password, host, port, database = match.groups()
    result = {"user": user, "password": password, "host": host, "database": database}
    if port:
        result["port"] = int(port)
    return result


# =============================================================================
# Pattern Matching
# =============================================================================


def filter_by_pattern(items: list[str], pattern: str | None) -> list[str]:
    """Filter items by SQL LIKE pattern.

    Args:
        items: List of strings to filter
        pattern: SQL LIKE pattern (% is wildcard)

    Returns:
        Filtered list of items
    """
    if not pattern:
        return items

    # Convert SQL LIKE pattern to regex
    # Escape special regex characters except %
    regex_pattern = ""
    i = 0
    while i < len(pattern):
        char = pattern[i]
        if char == "%":
            regex_pattern += ".*"
        elif char in r"\.^$*+?{}[]|()":
            regex_pattern += "\\" + char
        else:
            regex_pattern += char
        i += 1

    # Case-insensitive matching
    regex = re.compile(f"^{regex_pattern}$", re.IGNORECASE)

    return [item for item in items if regex.match(item)]


# =============================================================================
# Schema Search Functions
# =============================================================================


def search_schemas(conn: Any, detail: str = "names") -> dict:
    """Search database schemas.

    Args:
        conn: Database connection
        detail: Detail level (names, summary, full)

    Returns:
        Dictionary with schema information
    """
    # Detect database type
    db_type = _detect_db_type(conn)

    if db_type == "sqlite":
        # SQLite has single 'main' schema
        if detail == "names":
            return {"schemas": ["main"]}
        else:
            # Get table count for summary/full
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            table_count = cursor.fetchone()[0]

            return {
                "schemas": [
                    {
                        "name": "main",
                        "table_count": table_count,
                    }
                ]
            }

    elif db_type == "postgresql":
        cursor = conn.cursor()
        if detail == "names":
            cursor.execute("""
                SELECT schema_name
                FROM information_schema.schemata
                WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                ORDER BY schema_name
            """)
            schemas = [row[0] for row in cursor.fetchall()]
            return {"schemas": schemas}
        else:
            cursor.execute("""
                SELECT
                    s.schema_name,
                    COUNT(t.table_name) as table_count
                FROM information_schema.schemata s
                LEFT JOIN information_schema.tables t
                    ON s.schema_name = t.table_schema
                    AND t.table_type = 'BASE TABLE'
                WHERE s.schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                GROUP BY s.schema_name
                ORDER BY s.schema_name
            """)
            schemas = [{"name": row[0], "table_count": row[1]} for row in cursor.fetchall()]
            return {"schemas": schemas}

    elif db_type == "mysql":
        cursor = conn.cursor()
        if detail == "names":
            cursor.execute("SELECT DATABASE()")
            db_name = cursor.fetchone()[0]
            return {"schemas": [db_name]}
        else:
            cursor.execute("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
            """)
            table_count = cursor.fetchone()[0]
            cursor.execute("SELECT DATABASE()")
            db_name = cursor.fetchone()[0]
            return {"schemas": [{"name": db_name, "table_count": table_count}]}

    return {"schemas": []}


def search_tables(
    conn: Any,
    detail: str = "names",
    pattern: str | None = None,
    schema: str | None = None,
) -> dict:
    """Search database tables.

    Args:
        conn: Database connection
        detail: Detail level (names, summary, full)
        pattern: SQL LIKE pattern to filter tables
        schema: Schema name (PostgreSQL only)

    Returns:
        Dictionary with table information
    """
    db_type = _detect_db_type(conn)
    cursor = conn.cursor()

    if db_type == "sqlite":
        cursor.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        table_names = [row[0] for row in cursor.fetchall()]

        # Apply pattern filter
        table_names = filter_by_pattern(table_names, pattern)

        if detail == "names":
            return {"tables": table_names}

        # Get summary/full details
        tables = []
        for name in table_names:
            table_info = {"name": name}

            # Get row count
            try:
                cursor.execute(f'SELECT COUNT(*) FROM "{_validate_identifier(name)}"')
                table_info["row_count"] = cursor.fetchone()[0]
            except sqlite3.OperationalError:
                table_info["row_count"] = None

            if detail == "summary":
                tables.append(table_info)
            else:  # full
                # Get column count
                cursor.execute(f'PRAGMA table_info("{_validate_identifier(name)}")')
                table_info["column_count"] = len(cursor.fetchall())

                # Get columns with full detail
                table_info["columns"] = _get_sqlite_columns(cursor, name)

                tables.append(table_info)

        return {"tables": tables}

    elif db_type == "postgresql":
        schema_filter = schema or "public"

        if detail == "names":
            cursor.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """,
                (schema_filter,),
            )
            table_names = [row[0] for row in cursor.fetchall()]
            table_names = filter_by_pattern(table_names, pattern)
            return {"tables": table_names}

        # Summary/full
        cursor.execute(
            """
            SELECT
                t.table_name,
                COALESCE(s.n_live_tup, 0) as row_count
            FROM information_schema.tables t
            LEFT JOIN pg_stat_user_tables s ON t.table_name = s.relname
            WHERE t.table_schema = %s AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name
        """,
            (schema_filter,),
        )

        tables = []
        for row in cursor.fetchall():
            name = row[0]
            if pattern and not filter_by_pattern([name], pattern):
                continue

            table_info = {
                "name": name,
                "row_count": row[1],
            }

            if detail == "full":
                table_info["columns"] = _get_postgresql_columns(cursor, name, schema_filter)

            tables.append(table_info)

        return {"tables": tables}

    elif db_type == "mysql":
        if detail == "names":
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            table_names = [row[0] for row in cursor.fetchall()]
            table_names = filter_by_pattern(table_names, pattern)
            return {"tables": table_names}

        # Summary/full
        cursor.execute("""
            SELECT
                table_name,
                table_rows
            FROM information_schema.tables
            WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)

        tables = []
        for row in cursor.fetchall():
            name = row[0]
            if pattern and not filter_by_pattern([name], pattern):
                continue

            table_info = {
                "name": name,
                "row_count": row[1],
            }

            if detail == "full":
                table_info["columns"] = _get_mysql_columns(cursor, name)

            tables.append(table_info)

        return {"tables": tables}

    return {"tables": []}


def search_columns(
    conn: Any,
    detail: str = "names",
    pattern: str | None = None,
    table: str | None = None,
    schema: str | None = None,
) -> dict:
    """Search database columns.

    Args:
        conn: Database connection
        detail: Detail level (names, summary, full)
        pattern: SQL LIKE pattern to filter columns
        table: Table name to filter columns
        schema: Schema name (PostgreSQL only)

    Returns:
        Dictionary with column information
    """
    db_type = _detect_db_type(conn)
    cursor = conn.cursor()

    if db_type == "sqlite":
        # Get tables to search
        if table:
            tables_to_search = [table]
        else:
            cursor.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """)
            tables_to_search = [row[0] for row in cursor.fetchall()]

        columns = []
        for tbl_name in tables_to_search:
            cursor.execute(f'PRAGMA table_info("{_validate_identifier(tbl_name)}")')
            for row in cursor.fetchall():
                col_name = row[1]

                # Apply pattern filter
                if pattern and not filter_by_pattern([col_name], pattern):
                    continue

                if detail == "names":
                    columns.append(col_name)
                else:
                    col_info = {
                        "name": col_name,
                        "table": tbl_name,
                        "type": row[2],
                        "nullable": row[3] == 0,  # notnull flag: 0 = nullable
                    }

                    if detail == "full":
                        col_info["primary_key"] = row[5] == 1
                        col_info["default"] = row[4]

                    columns.append(col_info)

        return {"columns": columns}

    elif db_type == "postgresql":
        schema_filter = schema or "public"

        query = """
            SELECT
                column_name,
                table_name,
                data_type,
                is_nullable,
                column_default,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s
        """
        params = [schema_filter]

        if table:
            query += " AND table_name = %s"
            params.append(table)

        query += " ORDER BY table_name, ordinal_position"

        cursor.execute(query, params)

        columns = []
        for row in cursor.fetchall():
            col_name = row[0]

            if pattern and not filter_by_pattern([col_name], pattern):
                continue

            if detail == "names":
                columns.append(col_name)
            else:
                col_info = {
                    "name": col_name,
                    "table": row[1],
                    "type": row[2],
                    "nullable": row[3] == "YES",
                }

                if detail == "full":
                    col_info["default"] = row[4]
                    col_info["position"] = row[5]

                columns.append(col_info)

        return {"columns": columns}

    elif db_type == "mysql":
        query = """
            SELECT
                column_name,
                table_name,
                data_type,
                is_nullable,
                column_default,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
        """
        params = []

        if table:
            query += " AND table_name = %s"
            params.append(table)

        query += " ORDER BY table_name, ordinal_position"

        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        columns = []
        for row in cursor.fetchall():
            col_name = row[0]

            if pattern and not filter_by_pattern([col_name], pattern):
                continue

            if detail == "names":
                columns.append(col_name)
            else:
                col_info = {
                    "name": col_name,
                    "table": row[1],
                    "type": row[2],
                    "nullable": row[3] == "YES",
                }

                if detail == "full":
                    col_info["default"] = row[4]
                    col_info["position"] = row[5]

                columns.append(col_info)

        return {"columns": columns}

    return {"columns": []}


def search_indexes(
    conn: Any,
    detail: str = "names",
    pattern: str | None = None,
    table: str | None = None,
    schema: str | None = None,
) -> dict:
    """Search database indexes.

    Args:
        conn: Database connection
        detail: Detail level (names, summary, full)
        pattern: SQL LIKE pattern to filter indexes
        table: Table name to filter indexes
        schema: Schema name (PostgreSQL only)

    Returns:
        Dictionary with index information
    """
    db_type = _detect_db_type(conn)
    cursor = conn.cursor()

    if db_type == "sqlite":
        # Get tables to search
        if table:
            tables_to_search = [table]
        else:
            cursor.execute("""
                SELECT name FROM sqlite_master
                WHERE type='table' AND name NOT LIKE 'sqlite_%'
            """)
            tables_to_search = [row[0] for row in cursor.fetchall()]

        indexes = []
        for tbl_name in tables_to_search:
            cursor.execute(f'PRAGMA index_list("{_validate_identifier(tbl_name)}")')
            for idx_row in cursor.fetchall():
                idx_name = idx_row[1]

                if pattern and not filter_by_pattern([idx_name], pattern):
                    continue

                if detail == "names":
                    indexes.append(idx_name)
                else:
                    idx_info = {
                        "name": idx_name,
                        "table": tbl_name,
                        "unique": idx_row[2] == 1,
                    }

                    if detail == "full":
                        # Get columns in index
                        cursor.execute(f'PRAGMA index_info("{_validate_identifier(idx_name)}")')
                        columns = [row[2] for row in cursor.fetchall()]
                        idx_info["columns"] = columns

                    indexes.append(idx_info)

        return {"indexes": indexes}

    elif db_type == "postgresql":
        schema_filter = schema or "public"

        query = """
            SELECT
                i.relname as index_name,
                t.relname as table_name,
                ix.indisunique as is_unique
            FROM pg_class t
            JOIN pg_index ix ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            WHERE t.relkind = 'r'
                AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = %s)
        """
        params = [schema_filter]

        if table:
            query += " AND t.relname = %s"
            params.append(table)

        query += " ORDER BY t.relname, i.relname"

        cursor.execute(query, params)

        indexes = []
        for row in cursor.fetchall():
            idx_name = row[0]

            if pattern and not filter_by_pattern([idx_name], pattern):
                continue

            if detail == "names":
                indexes.append(idx_name)
            else:
                idx_info = {
                    "name": idx_name,
                    "table": row[1],
                    "unique": row[2],
                }

                if detail == "full":
                    # Get columns in index
                    cursor.execute(
                        """
                        SELECT a.attname
                        FROM pg_class t
                        JOIN pg_index ix ON t.oid = ix.indrelid
                        JOIN pg_class i ON i.oid = ix.indexrelid
                        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                        WHERE i.relname = %s AND t.relname = %s
                        ORDER BY a.attnum
                    """,
                        (idx_name, row[1]),
                    )
                    idx_info["columns"] = [r[0] for r in cursor.fetchall()]

                indexes.append(idx_info)

        return {"indexes": indexes}

    elif db_type == "mysql":
        query = """
            SELECT DISTINCT
                index_name,
                table_name,
                non_unique
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
        """
        params = []

        if table:
            query += " AND table_name = %s"
            params.append(table)

        query += " ORDER BY table_name, index_name"

        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        indexes = []
        for row in cursor.fetchall():
            idx_name = row[0]

            if pattern and not filter_by_pattern([idx_name], pattern):
                continue

            if detail == "names":
                indexes.append(idx_name)
            else:
                idx_info = {
                    "name": idx_name,
                    "table": row[1],
                    "unique": row[2] == 0,
                }

                if detail == "full":
                    # Get columns in index
                    cursor.execute(
                        """
                        SELECT column_name
                        FROM information_schema.statistics
                        WHERE table_schema = DATABASE()
                            AND index_name = %s
                            AND table_name = %s
                        ORDER BY seq_in_index
                    """,
                        (idx_name, row[1]),
                    )
                    idx_info["columns"] = [r[0] for r in cursor.fetchall()]

                indexes.append(idx_info)

        return {"indexes": indexes}

    return {"indexes": []}


# =============================================================================
# Helper Functions
# =============================================================================


def _detect_db_type(conn: Any) -> str:
    """Detect database type from connection."""
    conn_module = conn.__class__.__module__

    if "sqlite3" in conn_module:
        return "sqlite"
    elif "psycopg2" in conn_module:
        return "postgresql"
    elif "mysql" in conn_module:
        return "mysql"

    return "unknown"


def _get_sqlite_columns(cursor: Any, table_name: str) -> list[dict]:
    """Get column information for SQLite table."""
    cursor.execute(f'PRAGMA table_info("{_validate_identifier(table_name)}")')
    columns = []
    for row in cursor.fetchall():
        columns.append(
            {
                "name": row[1],
                "type": row[2],
                "nullable": row[3] == 0,
                "default": row[4],
                "primary_key": row[5] == 1,
            }
        )
    return columns


def _get_postgresql_columns(cursor: Any, table_name: str, schema: str) -> list[dict]:
    """Get column information for PostgreSQL table."""
    cursor.execute(
        """
        SELECT
            column_name,
            data_type,
            is_nullable,
            column_default,
            ordinal_position
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """,
        (schema, table_name),
    )

    columns = []
    for row in cursor.fetchall():
        columns.append(
            {
                "name": row[0],
                "type": row[1],
                "nullable": row[2] == "YES",
                "default": row[3],
                "position": row[4],
            }
        )
    return columns


def _get_mysql_columns(cursor: Any, table_name: str) -> list[dict]:
    """Get column information for MySQL table."""
    cursor.execute(
        """
        SELECT
            column_name,
            data_type,
            is_nullable,
            column_default,
            ordinal_position
        FROM information_schema.columns
        WHERE table_schema = DATABASE() AND table_name = %s
        ORDER BY ordinal_position
    """,
        (table_name,),
    )

    columns = []
    for row in cursor.fetchall():
        columns.append(
            {
                "name": row[0],
                "type": row[1],
                "nullable": row[2] == "YES",
                "default": row[3],
                "position": row[4],
            }
        )
    return columns


# =============================================================================
# Output Functions
# =============================================================================


def generate_markdown(data: dict, object_type: str) -> str:
    """Generate Markdown documentation from search results."""
    lines = [f"# {object_type.title()} Search Results"]
    lines.append(f"\n*Generated: {datetime.now().isoformat()}*\n")

    if object_type == "table":
        tables = data.get("tables", [])
        if isinstance(tables[0] if tables else None, str):
            # Names only
            lines.append("## Tables\n")
            for name in tables:
                lines.append(f"- `{name}`")
        else:
            # Summary or full
            lines.append("## Tables\n")
            lines.append("| Name | Row Count |")
            lines.append("|------|-----------|")
            for table in tables:
                row_count = table.get("row_count", "N/A")
                lines.append(f"| `{table['name']}` | {row_count} |")

            if tables and "columns" in tables[0]:
                lines.append("\n## Column Details\n")
                for table in tables:
                    if "columns" in table:
                        lines.append(f"\n### {table['name']}\n")
                        lines.append("| Column | Type | Nullable |")
                        lines.append("|--------|------|----------|")
                        for col in table["columns"]:
                            nullable = "YES" if col.get("nullable", True) else "NO"
                            lines.append(f"| `{col['name']}` | {col.get('type', 'N/A')} | {nullable} |")

    elif object_type == "column":
        columns = data.get("columns", [])
        if columns and isinstance(columns[0], dict):
            lines.append("## Columns\n")
            lines.append("| Table | Column | Type | Nullable |")
            lines.append("|-------|--------|------|----------|")
            for col in columns:
                nullable = "YES" if col.get("nullable", True) else "NO"
                lines.append(
                    f"| `{col.get('table', 'N/A')}` | `{col['name']}` | {col.get('type', 'N/A')} | {nullable} |"
                )
        else:
            lines.append("## Columns\n")
            for name in columns:
                lines.append(f"- `{name}`")

    elif object_type == "index":
        indexes = data.get("indexes", [])
        if indexes and isinstance(indexes[0], dict):
            lines.append("## Indexes\n")
            lines.append("| Name | Table | Unique |")
            lines.append("|------|-------|--------|")
            for idx in indexes:
                unique = "YES" if idx.get("unique", False) else "NO"
                lines.append(f"| `{idx['name']}` | `{idx.get('table', 'N/A')}` | {unique} |")

            if "columns" in indexes[0]:
                lines.append("\n## Index Details\n")
                for idx in indexes:
                    if "columns" in idx:
                        cols = ", ".join(f"`{c}`" for c in idx["columns"])
                        lines.append(f"- **{idx['name']}**: {cols}")
        else:
            lines.append("## Indexes\n")
            for name in indexes:
                lines.append(f"- `{name}`")

    elif object_type == "schema":
        schemas = data.get("schemas", [])
        if schemas and isinstance(schemas[0], dict):
            lines.append("## Schemas\n")
            lines.append("| Name | Table Count |")
            lines.append("|------|-------------|")
            for schema in schemas:
                lines.append(f"| `{schema['name']}` | {schema.get('table_count', 'N/A')} |")
        else:
            lines.append("## Schemas\n")
            for name in schemas:
                lines.append(f"- `{name}`")

    return "\n".join(lines)


def write_output(data: dict, output_dir: Path, prefix: str = "search") -> None:
    """Write search results to JSON and Markdown files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # Write JSON
    json_path = output_dir / f"{prefix}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)

    # Write Markdown
    object_type = prefix.split("_")[0] if "_" in prefix else prefix
    md_content = generate_markdown(data, object_type)
    md_path = output_dir / f"{prefix}.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md_content)


# =============================================================================
# Main Entry Point
# =============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Search Objects - Unified Database Schema Explorer with Progressive Disclosure",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Detail Levels:
  names    - Just object names (browsing, finding the right table)
  summary  - Names + metadata like row count (choosing between similar tables)
  full     - Complete structure with columns, types, indexes (before writing queries)

Examples:
  # Progressive disclosure pattern
  python search_objects.py --database prod_dw --type schema --detail names
  python search_objects.py --database prod_dw --type table --detail names
  python search_objects.py --database prod_dw --type table --pattern "%user%" --detail summary
  python search_objects.py --database prod_dw --type column --table users --detail full

  # SQLite
  python search_objects.py --sqlite db.sqlite --type table --detail full --output artifacts/schema/
        """,
    )

    # Connection options
    conn_group = parser.add_mutually_exclusive_group(required=True)
    conn_group.add_argument("--sqlite", "-s", metavar="PATH", help="Path to SQLite database file")
    conn_group.add_argument("--database", "-d", metavar="NAME", help="Database connection name from settings.json")

    # Search options
    parser.add_argument(
        "--type", "-t", choices=["schema", "table", "column", "index"], required=True, help="Object type to search"
    )
    parser.add_argument(
        "--detail", choices=["names", "summary", "full"], default="names", help="Detail level (default: names)"
    )
    parser.add_argument(
        "--pattern", "-p", metavar="PATTERN", help="SQL LIKE pattern to filter results (e.g., '%%user%%')"
    )
    parser.add_argument("--table", metavar="NAME", help="Filter by table name (for column/index search)")
    parser.add_argument("--schema", metavar="NAME", help="Filter by schema name (PostgreSQL only)")
    parser.add_argument("--output", "-o", metavar="DIR", help="Output directory for JSON and Markdown files")

    args = parser.parse_args()

    # Create connection
    try:
        conn = create_connection(
            sqlite_path=args.sqlite,
            database_name=args.database,
        )
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

    try:
        # Execute search based on type
        if args.type == "schema":
            result = search_schemas(conn, detail=args.detail)
        elif args.type == "table":
            result = search_tables(
                conn,
                detail=args.detail,
                pattern=args.pattern,
                schema=args.schema,
            )
        elif args.type == "column":
            result = search_columns(
                conn,
                detail=args.detail,
                pattern=args.pattern,
                table=args.table,
                schema=args.schema,
            )
        elif args.type == "index":
            result = search_indexes(
                conn,
                detail=args.detail,
                pattern=args.pattern,
                table=args.table,
                schema=args.schema,
            )
        else:
            print(f"Unknown object type: {args.type}")
            sys.exit(1)

        # Add metadata
        result["object_type"] = args.type
        result["detail_level"] = args.detail
        result["generated_at"] = datetime.now().isoformat()

        # Output results
        if args.output:
            output_dir = Path(args.output)
            write_output(result, output_dir, prefix=f"{args.type}s")
            print(f"Results written to {output_dir}")
        else:
            # Print JSON to stdout
            print(json.dumps(result, indent=2, ensure_ascii=False, default=str))

    finally:
        conn.close()


if __name__ == "__main__":
    main()
