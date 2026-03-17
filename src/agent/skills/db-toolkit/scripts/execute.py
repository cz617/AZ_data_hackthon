#!/usr/bin/env python3
"""
Execute SQL - SQL Query Executor
=================================

Execute SQL queries and output results to stdout (CSV format).
Use --output to save results to file.

**Security Warning:** This tool executes arbitrary SQL queries. Be cautious when
accepting user input as queries. In production, consider implementing query
whitelisting or parameterized query templates.

Usage:
    # Execute query and output to stdout (default, max 100 rows)
    python .amandax/skills/db-toolkit/scripts/execute.py --database bird_testing "SELECT * FROM users LIMIT 10"

    # Execute from file
    python .amandax/skills/db-toolkit/scripts/execute.py --database bird_testing --file query.sql

    # Save full results to file (no row limit)
    python .amandax/skills/db-toolkit/scripts/execute.py --database bird_testing "SELECT * FROM users" --output artifacts/query_result/users.csv

    # Pagination (requires --output)
    python .amandax/skills/db-toolkit/scripts/execute.py --database bird_testing "SELECT * FROM trans" --paginate 10000 --output artifacts/batches/trans_

    # SQLite
    python .amandax/skills/db-toolkit/scripts/execute.py --sqlite db.sqlite "SELECT * FROM users LIMIT 10"

Output Behavior:
    - Default: Output to stdout in CSV format (max 100 rows, with truncation message)
    - With --output: Save ALL results to file (no truncation)
"""

import argparse
import csv
import json
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# Constants
DEFAULT_DISPLAY_ROWS = 100
MAX_CELL_LENGTH = 100

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
    for _name, config in db_config.items():
        if isinstance(config, dict) and "type" in config:
            return config

    return None


def create_connection(
    sqlite_path: str | None = None,
    database_name: str | None = None,
) -> tuple[Any, str | None]:
    """Create a database connection.

    Args:
        sqlite_path: Path to SQLite database file
        database_name: Name of database connection from settings.json

    Returns:
        Tuple of (connection, db_type) or (None, None) on failure
    """
    if sqlite_path:
        if not Path(sqlite_path).exists():
            print(f"Error: SQLite file not found: {sqlite_path}")
            return None, None
        conn = sqlite3.connect(sqlite_path)
        conn.row_factory = sqlite3.Row
        return conn, "sqlite"

    if database_name:
        config = get_connection_config(database_name)
        if not config:
            print(f"Error: Database connection '{database_name}' not found in settings.json")
            return None, None

        db_type = config.get("type", "sqlite")
        conn_str = config.get("connectionString", "")

        if db_type == "sqlite":
            # Parse sqlite:///path format
            path = conn_str.replace("sqlite://", "").replace("sqlite:///", "")
            conn = sqlite3.connect(path)
            conn.row_factory = sqlite3.Row
            return conn, "sqlite"

        elif db_type == "postgresql":
            try:
                import psycopg2
                import psycopg2.extras
            except ImportError:
                print("Error: psycopg2 not installed. Run: pip install psycopg2-binary")
                return None, None

            conn = psycopg2.connect(conn_str)
            return conn, "postgresql"

        elif db_type == "mysql":
            try:
                import mysql.connector
            except ImportError:
                print("Error: mysql-connector-python not installed. Run: pip install mysql-connector-python")
                return None, None

            # Parse MySQL connection string
            mysql_params = _parse_mysql_conn_str(conn_str)
            conn = mysql.connector.connect(**mysql_params)
            return conn, "mysql"

        else:
            print(f"Error: Unsupported database type: {db_type}")
            return None, None

    print("Error: Either --sqlite or --database must be specified")
    return None, None


def _parse_mysql_conn_str(conn_str: str) -> dict:
    """Parse MySQL connection string into keyword arguments."""
    conn_str = conn_str.replace("mysql://", "")
    match = re.match(r"([^:]+):([^@]*)@([^:/]+)(?::(\d+))?/(.+)", conn_str)
    if not match:
        raise ValueError("Invalid MySQL connection string format")

    user, password, host, port, database = match.groups()
    result = {"user": user, "password": password, "host": host, "database": database}
    if port:
        result["port"] = int(port)
    return result


# =============================================================================
# Query Execution
# =============================================================================


def read_query(query: str | None = None, file_path: str | None = None) -> str:
    """Read SQL query from string or file.

    Args:
        query: SQL query string
        file_path: Path to SQL file

    Returns:
        SQL query string
    """
    if file_path:
        with open(file_path, encoding="utf-8") as f:
            return f.read().strip()
    elif query:
        return query.strip()
    else:
        raise ValueError("Either query or file_path must be provided")


def execute_query(
    conn: Any,
    query: str,
    db_type: str,
    limit: int | None = None,
    offset: int | None = None,
) -> tuple[list[str], list[dict]]:
    """Execute SQL query and return results.

    Args:
        conn: Database connection
        query: SQL query string
        db_type: Database type (sqlite, postgresql, mysql)
        limit: Maximum rows to return (applied in Python after fetch)
        offset: Number of rows to skip (applied in Python after fetch)

    Returns:
        Tuple of (column_names, list_of_row_dicts)
    """
    cursor = conn.cursor()

    try:
        if db_type == "postgresql":
            import psycopg2.extras

            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            results = [dict(row) for row in rows]
        elif db_type == "mysql":
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
        else:  # sqlite
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            results = [dict(row) for row in rows]

        # Apply offset and limit in Python
        if offset:
            results = results[offset:]
        if limit:
            results = results[:limit]

        return columns, results

    except Exception as e:
        # Re-raise with context for proper error handling
        raise RuntimeError(f"Query execution failed: {e}") from e


# =============================================================================
# Output Functions
# =============================================================================


def save_csv(columns: list[str], results: list[dict], filepath: Path) -> None:
    """Save results to CSV file."""
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(results)


def output_to_stdout(columns: list[str], results: list[dict], max_rows: int = DEFAULT_DISPLAY_ROWS) -> None:
    """Output results directly to stdout in CSV format.

    Args:
        columns: Column names
        results: Query results
        max_rows: Maximum rows to display (default 100)
    """
    if not results:
        return

    # Determine rows to display
    total_rows = len(results)
    display_rows = results[:max_rows]

    # Output CSV to stdout
    writer = csv.DictWriter(sys.stdout, fieldnames=columns)
    writer.writeheader()
    writer.writerows(display_rows)

    # Flush stdout before printing to stderr
    sys.stdout.flush()

    # Print truncation message to stderr if needed
    if total_rows > max_rows:
        print(f"[已显示 {max_rows} 行，共 {total_rows} 行]", file=sys.stderr)


def generate_filename() -> str:
    """Generate unique filename with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"query_result_{timestamp}.csv"


# =============================================================================
# Pagination Support
# =============================================================================


def save_with_pagination(
    columns: list[str],
    results: list[dict],
    output_path: Path,
    batch_size: int,
) -> list[Path]:
    """Save results in paginated CSV files.

    Args:
        columns: Column names
        results: Query results
        output_path: Base output path (directory or file prefix)
        batch_size: Number of rows per file

    Returns:
        List of created file paths
    """
    created_files = []
    total_rows = len(results)
    total_batches = (total_rows + batch_size - 1) // batch_size

    # Determine output directory
    if output_path.suffix:
        # Has extension, use parent as directory
        output_dir = output_path.parent
        base_name = output_path.stem
    else:
        # No extension, treat as directory
        output_dir = output_path
        base_name = "batch"

    output_dir.mkdir(parents=True, exist_ok=True)

    for batch_num in range(total_batches):
        start = batch_num * batch_size
        end = min(start + batch_size, total_rows)
        batch_results = results[start:end]

        # Generate filename with batch number
        batch_filename = f"{base_name}_{batch_num + 1:03d}.csv"
        batch_path = output_dir / batch_filename

        save_csv(columns, batch_results, batch_path)
        created_files.append(batch_path)

        print(f"Batch {batch_num + 1}/{total_batches}: {len(batch_results)} rows -> {batch_path}", file=sys.stderr)

    return created_files


# =============================================================================
# Main Entry Point
# =============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Execute SQL - SQL Query Executor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Output Behavior:
  - Default: Output to stdout in CSV format (max 100 rows)
  - With --output: Save ALL results to file (no row limit)

Examples:
  # Execute query and output to stdout (max 100 rows)
  python execute.py --sqlite db.sqlite "SELECT * FROM users LIMIT 10"

  # Save full results to file
  python execute.py --sqlite db.sqlite "SELECT * FROM users" --output results.csv

  # Pagination (requires --output)
  python execute.py --sqlite db.sqlite "SELECT * FROM users" --paginate 100 --output batches/

  # Using database from settings.json
  python execute.py --database prod_dw "SELECT * FROM orders LIMIT 10"
        """,
    )

    # Connection options (mutually exclusive)
    conn_group = parser.add_mutually_exclusive_group(required=True)
    conn_group.add_argument("--sqlite", "-s", metavar="PATH", help="Path to SQLite database file")
    conn_group.add_argument("--database", "-d", metavar="NAME", help="Database connection name from settings.json")

    # Query options
    parser.add_argument("query", nargs="?", help="SQL query to execute")
    parser.add_argument("--file", "-f", metavar="PATH", help="SQL file to execute")

    # Output options
    parser.add_argument(
        "--output",
        "-o",
        metavar="PATH",
        default=None,
        help="Output path (file or directory). If not specified, output to stdout (max 100 rows)",
    )

    # Pagination options
    parser.add_argument("--limit", "-l", type=int, metavar="N", help="Maximum rows to return")
    parser.add_argument("--offset", type=int, metavar="N", help="Number of rows to skip")
    parser.add_argument("--paginate", type=int, metavar="BATCH_SIZE", help="Auto-paginate results into multiple files")

    args = parser.parse_args()

    # Get query
    try:
        if args.file:
            query = read_query(file_path=args.file)
        elif args.query:
            query = read_query(query=args.query)
        else:
            parser.print_help()
            print("\nError: Either query string or --file must be provided")
            sys.exit(1)
    except Exception as e:
        print(f"Error reading query: {e}")
        sys.exit(1)

    # Create connection
    conn, db_type = create_connection(
        sqlite_path=args.sqlite,
        database_name=args.database,
    )

    if not conn:
        sys.exit(1)

    try:
        # Execute query
        db_name = Path(args.sqlite).stem if args.sqlite else args.database

        columns, results = execute_query(
            conn,
            query,
            db_type,
            limit=args.limit,
            offset=args.offset,
        )

        if not results:
            print("Query executed successfully but returned no data.", file=sys.stderr)
            return

        # Print metadata to stderr
        print(f"Executing query on '{db_name}' ({db_type.upper() if db_type else 'unknown'})...", file=sys.stderr)
        print(f"Retrieved {len(results)} rows", file=sys.stderr)

        # Handle output
        if args.output:
            # Save to file (ALL rows, no truncation)
            output_path = Path(args.output)

            if args.paginate:
                # Paginated output
                created_files = save_with_pagination(
                    columns,
                    results,
                    output_path,
                    args.paginate,
                )
                print(f"Created {len(created_files)} files", file=sys.stderr)
            else:
                # Single file output
                if output_path.suffix:
                    # Has extension, treat as file
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    filepath = output_path
                else:
                    # No extension, treat as directory
                    output_path.mkdir(parents=True, exist_ok=True)
                    filepath = output_path / generate_filename()

                save_csv(columns, results, filepath)
                print(f"Results saved to: {filepath}", file=sys.stderr)
        else:
            # Output to stdout (max 100 rows by default)
            output_to_stdout(columns, results)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
