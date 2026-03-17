#!/usr/bin/env python3
"""
Connect - Manage database connections in .amandax/settings.json

Usage:
    python .amandax/skills/db-toolkit/scripts/connect.py --list
    python .amandax/skills/db-toolkit/scripts/connect.py --add postgresql --name bird_testing --connection-string "postgresql://user:pass@host:5432/db"
    python .amandax/skills/db-toolkit/scripts/connect.py --add mysql --name local_mysql --connection-string "mysql://root:password@localhost:3306/mydb"
    python .amandax/skills/db-toolkit/scripts/connect.py --add sqlite --name local_sqlite --connection-string "sqlite:///./data.db"
    python .amandax/skills/db-toolkit/scripts/connect.py --test bird_testing
    python .amandax/skills/db-toolkit/scripts/connect.py --remove bird_testing

**Security Note:** Passwords are stored in plaintext in settings.json.
Consider using environment variables or a secrets manager for production use.
"""

import argparse
import json
import re
import sys
from pathlib import Path

# Load environment variables if available
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


def find_settings_file():
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


def load_settings():
    """Load settings.json or return empty dict."""
    settings_file = find_settings_file()
    if not settings_file:
        return {}

    try:
        with open(settings_file) as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        print(f"Warning: Could not parse settings.json: {e}")
        return {"database": {}}
    except OSError as e:
        print(f"Warning: Could not read settings.json: {e}")
        return {"database": {}}


def save_settings(settings):
    """Save settings back to settings.json."""
    settings_file = find_settings_file()
    if not settings_file:
        # Create .amandax directory if needed
        settings_file = Path.cwd() / ".amandax" / "settings.json"
        settings_file.parent.mkdir(parents=True, exist_ok=True)

    with open(settings_file, "w", encoding="utf-8") as f:
        json.dump(settings, f, indent=2, ensure_ascii=False)


def mask_password(conn_str: str) -> str:
    """Mask password in connection string for display."""
    return re.sub(r'://([^:]+):([^@]+)@', r'://\1:***@', conn_str)


def list_connections():
    """List all database connections."""
    settings = load_settings()
    db_config = settings.get("database", {})

    if not db_config:
        print("No database connections configured.")
        return

    print("Database Connections:")
    print("=" * 80)
    for name, config in db_config.items():
        db_type = config.get("type", "unknown")
        conn_str = config.get("connectionString", "")

        print(f"  {name}:")
        print(f"    type: {db_type}")
        if conn_str:
            print(f"    connectionString: {mask_password(conn_str)}")
        print()


def add_connection(db_type: str, name: str, connection_string: str):
    """Add a new database connection.

    Args:
        db_type: Database type (postgresql, mysql, sqlite)
        name: Connection name
        connection_string: Full connection string
    """
    settings = load_settings()

    if "database" not in settings:
        settings["database"] = {}

    if name in settings["database"]:
        print(f"Error: Connection '{name}' already exists. Use --remove first.")
        sys.exit(1)

    connection = {
        "type": db_type,
        "connectionString": connection_string,
    }

    settings["database"][name] = connection
    save_settings(settings)
    print(f"Connection '{name}' added successfully.")
    print(f"  Connection string: {mask_password(connection_string)}")


def test_connection(name: str):
    """Test a database connection.

    Args:
        name: Connection name from settings.json
    """
    settings = load_settings()
    db_config = settings.get("database", {})

    if name not in db_config:
        print(f"Error: Connection '{name}' not found.")
        sys.exit(1)

    config = db_config[name]
    db_type = config.get("type", "unknown")
    conn_str = config.get("connectionString", "")

    if not conn_str:
        print(f"Error: No connectionString configured for '{name}'.")
        sys.exit(1)

    print(f"Testing connection '{name}' ({db_type})...")

    if db_type == "sqlite":
        import sqlite3

        # Extract path from sqlite://path format
        path = conn_str.replace("sqlite://", "")
        if not Path(path).exists():
            print(f"Error: SQLite file not found: {path}")
            sys.exit(1)
        try:
            conn = sqlite3.connect(path)
            conn.close()
            print(f"Success: Connected to SQLite database at {path}")
        except sqlite3.Error as e:
            print(f"Error: SQLite connection failed - {e}")
            sys.exit(1)

    elif db_type == "postgresql":
        try:
            import psycopg2
        except ImportError:
            print("Error: Required driver not installed for postgresql")
            print("Install with: pip install psycopg2-binary")
            sys.exit(1)

        try:
            conn = psycopg2.connect(conn_str)
            conn.close()
            # Extract host from connection string for display
            host_match = re.search(r'@([^:/]+)', conn_str)
            host = host_match.group(1) if host_match else "unknown"
            print(f"Success: Connected to PostgreSQL at {host}")
        except Exception as e:
            print(f"Error: PostgreSQL connection failed - {e}")
            sys.exit(1)

    elif db_type == "mysql":
        try:
            import mysql.connector
        except ImportError:
            print("Error: Required driver not installed for mysql")
            print("Install with: pip install mysql-connector-python")
            sys.exit(1)

        try:
            # mysql.connector uses different format, need to parse connection string
            # mysql://user:pass@host:port/database
            conn = mysql.connector.connect(option_files=None, **_parse_mysql_conn_str(conn_str))
            conn.close()
            host_match = re.search(r'@([^:/]+)', conn_str)
            host = host_match.group(1) if host_match else "unknown"
            print(f"Success: Connected to MySQL at {host}")
        except Exception as e:
            print(f"Error: MySQL connection failed - {e}")
            sys.exit(1)

    else:
        print(f"Error: Unsupported database type: {db_type}")
        sys.exit(1)


def _parse_mysql_conn_str(conn_str: str) -> dict:
    """Parse MySQL connection string into keyword arguments.

    Args:
        conn_str: Connection string like mysql://user:pass@host:port/database

    Returns:
        Dict of connection parameters for mysql.connector
    """
    # Remove mysql:// prefix
    conn_str = conn_str.replace("mysql://", "")

    # Parse user:pass@host:port/database
    match = re.match(r'([^:]+):([^@]*)@([^:/]+)(?::(\d+))?/(.+)', conn_str)
    if not match:
        raise ValueError(f"Invalid MySQL connection string format: {conn_str}")

    user, password, host, port, database = match.groups()

    result = {
        "user": user,
        "password": password,
        "host": host,
        "database": database,
    }
    if port:
        result["port"] = int(port)

    return result


def remove_connection(name: str):
    """Remove a database connection.

    Args:
        name: Connection name to remove
    """
    settings = load_settings()

    if "database" not in settings or name not in settings["database"]:
        print(f"Error: Connection '{name}' not found.")
        sys.exit(1)

    del settings["database"][name]
    save_settings(settings)
    print(f"Connection '{name}' removed successfully.")


def main():
    parser = argparse.ArgumentParser(
        description="Connect - Manage database connections in .amandax/settings.json",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Connection String Formats:
  PostgreSQL: postgresql://user:password@host:5432/database
  MySQL:      mysql://user:password@host:3306/database
  SQLite:     sqlite:///path/to/database.db

Examples:
  # List all connections
  python connect.py --list

  # Add PostgreSQL connection
  python connect.py --add postgresql --name bird_testing \\
      --connection-string "postgresql://amandax:password@pgm-xxx.rds.aliyuncs.com:5432/bird"

  # Add MySQL connection
  python connect.py --add mysql --name local_mysql \\
      --connection-string "mysql://root:password@localhost:3306/mydb"

  # Add SQLite connection
  python connect.py --add sqlite --name local_sqlite \\
      --connection-string "sqlite:///./data.db"

  # Test connection
  python connect.py --test bird_testing

  # Remove connection
  python connect.py --remove bird_testing
        """,
    )

    # Mutually exclusive actions
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument("--list", action="store_true", help="List all connections")
    action_group.add_argument("--add", choices=["postgresql", "mysql", "sqlite"], help="Add a new connection")
    action_group.add_argument("--test", metavar="NAME", help="Test a connection")
    action_group.add_argument("--remove", metavar="NAME", help="Remove a connection")

    # Connection parameters for --add
    parser.add_argument("--name", help="Connection name (for --add)")
    parser.add_argument("--connection-string", help="Full connection string (for --add)")

    args = parser.parse_args()

    if args.list:
        list_connections()
    elif args.add:
        if not args.name:
            print("Error: --name is required when adding a connection.")
            sys.exit(1)
        if not args.connection_string:
            print("Error: --connection-string is required when adding a connection.")
            sys.exit(1)
        add_connection(args.add, args.name, args.connection_string)
    elif args.test:
        test_connection(args.test)
    elif args.remove:
        remove_connection(args.remove)


if __name__ == "__main__":
    main()
