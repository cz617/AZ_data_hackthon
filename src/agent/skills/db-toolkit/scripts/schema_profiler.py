"""Schema Profiler for MetaForge's db-toolkit.

This script exports database schemas to a distributed JSON structure.

Usage:
    python .amandax/skills/db-toolkit/scripts/schema_profiler.py --database bird_testing --output-dir artifacts/schemas
    python .amandax/skills/db-toolkit/scripts/schema_profiler.py --database bird_testing --schema public --output-dir artifacts/schemas
    python .amandax/skills/db-toolkit/scripts/schema_profiler.py --database bird_testing --table users --output-dir artifacts/schemas
"""

import argparse
import json
import re
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# Optional database drivers - imported at module level for testability
psycopg2 = None
mysql = None

try:
    import psycopg2
except ImportError:
    pass

try:
    import mysql.connector
    mysql = type(sys)('mysql')
    mysql.connector = mysql.connector
except ImportError:
    pass


@dataclass
class TableDefinition:
    """Represents a database table definition.

    Attributes:
        database: The database name
        schema: The schema name
        name: The table name
        columns: Dictionary of column definitions
        constraints: Dictionary of constraint definitions
        indexes: Dictionary of index definitions
        statistics: Dictionary of table statistics
        comment: Table comment/description
    """

    database: str
    schema: str
    name: str
    columns: dict[str, Any]
    constraints: dict[str, Any] = field(default_factory=dict)
    indexes: dict[str, Any] = field(default_factory=dict)
    statistics: dict[str, Any] = field(default_factory=dict)
    comment: str = ""


class SchemaProfiler:
    """Profiles database schemas and exports them to JSON structure.

    Supports SQLite, PostgreSQL, and MySQL databases.
    Reads connection settings from settings.json.
    """

    VERSION = "1.0"
    DEFAULT_SOURCE = "postgresql"

    def __init__(self, connection_name: str, output_dir: str = "artifacts/schemas"):
        """Initialize the SchemaProfiler.

        Args:
            connection_name: The name of the database connection in settings.json
            output_dir: Base directory for output schema files
        """
        self.connection_name = connection_name
        self.output_dir = Path(output_dir)
        self.db_dir = self.output_dir / connection_name
        self.connection: Any | None = None
        self.db_type: str | None = None
        self.connection_string: str | None = None
        self._schemas: set[str] = set()

    def _ensure_directories(self) -> None:
        """Create the database directory structure."""
        self.db_dir.mkdir(parents=True, exist_ok=True)

    def _write_database_metadata(self) -> Path:
        """Write the database metadata file.

        Creates the _metadata.json file and special directories.

        Returns:
            Path to the created metadata file
        """
        self._ensure_directories()

        metadata = {
            "database": self.connection_name,
            "version": self.VERSION,
            "created_at": datetime.now(UTC).isoformat(),
            "schemas": sorted(self._schemas),
        }

        metadata_path = self.db_dir / "_metadata.json"
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

        return metadata_path

    def _write_table_file(self, table: TableDefinition) -> Path:
        """Write a table definition to the schema directory.

        Args:
            table: The TableDefinition to write

        Returns:
            Path to the created table JSON file
        """
        # Track schema for metadata
        self._schemas.add(table.schema)

        # Create schema directory (flat structure, no tables/ subdirectory)
        schema_dir = self.db_dir / table.schema
        schema_dir.mkdir(parents=True, exist_ok=True)

        # Build simplified table JSON structure
        table_data = {
            "version": self.VERSION,
            "database": table.database,
            "schema": table.schema,
            "name": table.name,
            "columns": table.columns,
            "constraints": table.constraints,
            "indexes": table.indexes,
            "statistics": table.statistics,
            "comment": table.comment,
            "extracted_at": datetime.now(UTC).isoformat(),
        }

        # Write table file
        table_path = schema_dir / f"{table.name}.json"
        with open(table_path, "w", encoding="utf-8") as f:
            json.dump(table_data, f, indent=2)

        return table_path

    def _write_relationships(self, relationships: list[dict]) -> Path:
        """Write foreign key relationships to _relationships.json.

        Args:
            relationships: List of relationship dictionaries with dbt-style format

        Returns:
            Path to the created relationships file
        """
        rel_file = self.db_dir / "_relationships.json"

        # Convert to dbt-style format
        formatted_rels = []
        for rel in relationships:
            source_schema = rel.get("from_schema", "public")
            source_table = rel.get("from_table", "")
            source_column = rel.get("from_column", "")
            target_schema = rel.get("to_schema", "public")
            target_table = rel.get("to_table", "")
            target_column = rel.get("to_column", "")

            formatted_rels.append({
                "unique_id": f"{source_schema}.{source_table}.{source_column}__{target_schema}.{target_table}.{target_column}",
                "source": {
                    "schema": source_schema,
                    "table": source_table,
                    "column": source_column,
                },
                "target": {
                    "schema": target_schema,
                    "table": target_table,
                    "column": target_column,
                },
                "type": rel.get("type", "foreign_key"),
                "metadata": rel.get("metadata", {}),
            })

        with open(rel_file, "w", encoding="utf-8") as f:
            json.dump({
                "version": "1.1",
                "database": self.connection_name,
                "generated_at": datetime.now(UTC).isoformat(),
                "relationships": formatted_rels
            }, f, indent=2)

        return rel_file

    def _write_lineages(self, lineages: list[dict]) -> Path:
        """Write data lineage to _lineages.json.

        Args:
            lineages: List of lineage dictionaries

        Returns:
            Path to the created lineages file
        """
        lineage_file = self.db_dir / "_lineages.json"

        with open(lineage_file, "w", encoding="utf-8") as f:
            json.dump({
                "version": "1.0",
                "database": self.connection_name,
                "generated_at": datetime.now(UTC).isoformat(),
                "lineages": lineages
            }, f, indent=2)

        return lineage_file

    def _validate_identifier(self, name: str) -> str:
        """Validate that a name is a safe SQL identifier.

        Args:
            name: The identifier to validate

        Returns:
            The validated identifier

        Raises:
            ValueError: If the identifier contains invalid characters
        """
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', name):
            raise ValueError(f"Invalid SQL identifier: {name}")
        return name

    def _load_settings(self) -> dict[str, Any]:
        """Load settings from settings.json.

        Looks for settings.json in .amandax/ directory in current working directory.

        Returns:
            Dictionary containing settings

        Raises:
            FileNotFoundError: If settings.json is not found
            ValueError: If connection is not found in settings
        """
        # Look for settings.json in .amandax/ directory
        settings_path = Path.cwd() / ".amandax" / "settings.json"

        if not settings_path.exists():
            # Try parent directories
            current = Path.cwd()
            for _ in range(3):  # Look up 3 levels
                parent = current.parent
                settings_path = parent / ".amandax" / "settings.json"
                if settings_path.exists():
                    break
                current = parent
            else:
                raise FileNotFoundError(
                    "settings.json not found in .amandax/ directory"
                )

        with open(settings_path, encoding="utf-8") as f:
            settings = json.load(f)

        return settings

    def _get_connection_config(self) -> dict[str, Any]:
        """Get connection configuration for the named connection.

        Returns:
            Dictionary with 'type' and 'connectionString' keys

        Raises:
            ValueError: If connection name is not found in settings
        """
        settings = self._load_settings()

        if "database" not in settings:
            raise ValueError("No 'database' section in settings.json")

        db_settings = settings["database"]

        if self.connection_name not in db_settings:
            raise ValueError(
                f"Connection '{self.connection_name}' not found in settings.json"
            )

        conn_config = db_settings[self.connection_name]

        # Handle both direct connectionString and nested config
        if isinstance(conn_config, str):
            # Infer type from connection string prefix
            if conn_config.startswith("sqlite"):
                db_type = "sqlite"
            elif conn_config.startswith("postgresql") or conn_config.startswith("postgres"):
                db_type = "postgresql"
            elif conn_config.startswith("mysql"):
                db_type = "mysql"
            else:
                db_type = "unknown"
            return {"type": db_type, "connectionString": conn_config}

        return conn_config

    def connect(self) -> None:
        """Connect to the database using settings.json configuration.

        Raises:
            ValueError: If connection configuration is invalid
            ImportError: If required database driver is not installed
        """
        config = self._get_connection_config()
        self.db_type = config.get("type", "unknown").lower()
        self.connection_string = config.get("connectionString")

        if not self.connection_string:
            raise ValueError(f"No connectionString for '{self.connection_name}'")

        if self.db_type == "sqlite":
            self._connect_sqlite()
        elif self.db_type == "postgresql":
            self._connect_postgresql()
        elif self.db_type == "mysql":
            self._connect_mysql()
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")

    def _connect_sqlite(self) -> None:
        """Connect to SQLite database."""
        # Parse sqlite:///path or file:///path
        conn_str = self.connection_string
        if conn_str.startswith("sqlite:///"):
            db_path = conn_str[10:]
        elif conn_str.startswith("file:///"):
            db_path = conn_str[8:]
        else:
            db_path = conn_str

        self.connection = sqlite3.connect(db_path)
        # Enable foreign key support
        self.connection.execute("PRAGMA foreign_keys = ON")

    def _connect_postgresql(self) -> None:
        """Connect to PostgreSQL database."""
        global psycopg2
        if psycopg2 is None:
            raise ImportError(
                "psycopg2 is required for PostgreSQL support. "
                "Install with: pip install psycopg2-binary"
            )

        # psycopg2 can use the connection string directly
        conn_str = self.connection_string
        if conn_str.startswith("postgresql+psycopg2://"):
            # Convert SQLAlchemy style to psycopg2 style
            conn_str = conn_str.replace("postgresql+psycopg2://", "postgresql://")

        self.connection = psycopg2.connect(conn_str)

    def _connect_mysql(self) -> None:
        """Connect to MySQL database."""
        global mysql
        if mysql is None:
            raise ImportError(
                "mysql-connector-python is required for MySQL support. "
                "Install with: pip install mysql-connector-python"
            )

        # Parse mysql://user:pass@host:port/db format
        conn_str = self.connection_string
        if conn_str.startswith("mysql://"):
            conn_str = conn_str[8:]
        elif conn_str.startswith("mysql+mysqlconnector://"):
            conn_str = conn_str[23:]

        # Parse credentials
        if "@" in conn_str:
            creds, host_db = conn_str.split("@", 1)
            if ":" in creds:
                user, password = creds.split(":", 1)
            else:
                user, password = creds, ""

            if "/" in host_db:
                host_port, database = host_db.split("/", 1)
                if ":" in host_port:
                    host, port = host_port.split(":", 1)
                    port = int(port)
                else:
                    host, port = host_port, 3306
            else:
                host, port, database = host_db, 3306, ""

            self.connection = mysql.connector.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database
            )
        else:
            # Try connection string directly
            self.connection = mysql.connector.connect(host=conn_str)

    def get_tables(self, schema: str | None) -> list[dict]:
        """List all tables in the database.

        Args:
            schema: Schema/database name (optional for SQLite)

        Returns:
            List of dictionaries with 'name' and 'schema' keys for each table
        """
        if self.db_type == "sqlite":
            return self._get_tables_sqlite()
        elif self.db_type == "postgresql":
            return self._get_tables_postgresql(schema or "public")
        elif self.db_type == "mysql":
            return self._get_tables_mysql(schema)
        else:
            return []

    def _get_tables_sqlite(self) -> list[dict]:
        """Get tables for SQLite database."""
        cursor = self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
        return [{"name": row[0], "schema": "main"} for row in cursor.fetchall()]

    def _get_tables_postgresql(self, schema: str) -> list[dict]:
        """Get tables for PostgreSQL database."""
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            """,
            (schema,)
        )
        return [{"name": row[0], "schema": schema} for row in cursor.fetchall()]

    def _get_tables_mysql(self, schema: str | None) -> list[dict]:
        """Get tables for MySQL database."""
        cursor = self.connection.cursor()
        if schema:
            cursor.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
                (schema,)
            )
        else:
            cursor.execute("SHOW TABLES")
        return [{"name": row[0], "schema": schema or "main"} for row in cursor.fetchall()]

    def get_columns(self, table_name: str, schema: str) -> dict[str, Any]:
        """Get column definitions for a table.

        Args:
            table_name: Name of the table
            schema: Schema/database name

        Returns:
            Dictionary mapping column names to column definitions
        """
        if self.db_type == "sqlite":
            return self._get_columns_sqlite(table_name)
        elif self.db_type == "postgresql":
            return self._get_columns_postgresql(table_name, schema or "public")
        elif self.db_type == "mysql":
            return self._get_columns_mysql(table_name, schema)
        else:
            return {}

    def _get_columns_sqlite(self, table_name: str) -> dict[str, Any]:
        """Get columns for SQLite table."""
        cursor = self.connection.execute(f'PRAGMA table_info("{self._validate_identifier(table_name)}")')
        columns = {}
        for row in cursor.fetchall():
            # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
            _, name, col_type, notnull, default, pk = row
            is_pk = bool(pk)
            # In SQLite, PRIMARY KEY implies NOT NULL
            is_nullable = not bool(notnull) and not is_pk
            columns[name] = {
                "type": col_type,
                "nullable": is_nullable,
                "default": default,
                "primary_key": is_pk,
            }
        return columns

    def _get_columns_postgresql(self, table_name: str, schema: str) -> dict[str, Any]:
        """Get columns for PostgreSQL table."""
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table_name)
        )
        columns = {}
        for row in cursor.fetchall():
            name, col_type, is_nullable, default = row
            columns[name] = {
                "type": col_type,
                "nullable": is_nullable == "YES",
                "default": default,
            }
        return columns

    def _get_columns_mysql(self, table_name: str, schema: str | None) -> dict[str, Any]:
        """Get columns for MySQL table."""
        cursor = self.connection.cursor()
        if schema:
            cursor.execute(
                """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema, table_name)
            )
        else:
            cursor.execute(f"DESCRIBE {self._validate_identifier(table_name)}")
        columns = {}
        for row in cursor.fetchall():
            if schema:
                name, col_type, is_nullable, default = row
                columns[name] = {
                    "type": col_type,
                    "nullable": is_nullable == "YES",
                    "default": default,
                }
            else:
                # DESCRIBE output: Field, Type, Null, Key, Default, Extra
                name, col_type, is_nullable, _, default, _ = row
                columns[name] = {
                    "type": col_type,
                    "nullable": is_nullable == "YES",
                    "default": default,
                }
        return columns

    def get_foreign_keys(self, table_name: str, schema: str) -> list[dict]:
        """Get foreign key definitions for a table.

        Args:
            table_name: Name of the table
            schema: Schema/database name

        Returns:
            List of foreign key dictionaries with 'column', 'referenced_table',
            and 'referenced_column' keys
        """
        if self.db_type == "sqlite":
            return self._get_foreign_keys_sqlite(table_name)
        elif self.db_type == "postgresql":
            return self._get_foreign_keys_postgresql(table_name, schema or "public")
        elif self.db_type == "mysql":
            return self._get_foreign_keys_mysql(table_name, schema)
        else:
            return []

    def _get_foreign_keys_sqlite(self, table_name: str) -> list[dict]:
        """Get foreign keys for SQLite table."""
        cursor = self.connection.execute(
            f'PRAGMA foreign_key_list("{self._validate_identifier(table_name)}")'
        )
        fks = []
        for row in cursor.fetchall():
            # PRAGMA foreign_key_list returns:
            # id, seq, table, from, to, on_update, on_delete, match
            _, _, ref_table, from_col, to_col, _, _, _ = row
            fks.append({
                "column": from_col,
                "referenced_table": ref_table,
                "referenced_column": to_col,
            })
        return fks

    def _get_foreign_keys_postgresql(self, table_name: str, schema: str) -> list[dict]:
        """Get foreign keys for PostgreSQL table."""
        cursor = self.connection.cursor()
        cursor.execute(
            """
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_name = %s
                AND tc.table_schema = %s
            """,
            (table_name, schema)
        )
        return [
            {
                "column": row[0],
                "referenced_table": row[1],
                "referenced_column": row[2],
            }
            for row in cursor.fetchall()
        ]

    def _get_foreign_keys_mysql(self, table_name: str, schema: str | None) -> list[dict]:
        """Get foreign keys for MySQL table."""
        cursor = self.connection.cursor()
        if schema:
            cursor.execute(
                """
                SELECT
                    column_name,
                    referenced_table_name,
                    referenced_column_name
                FROM information_schema.key_column_usage
                WHERE table_name = %s
                    AND table_schema = %s
                    AND referenced_table_name IS NOT NULL
                """,
                (table_name, schema)
            )
        else:
            cursor.execute(
                """
                SELECT
                    column_name,
                    referenced_table_name,
                    referenced_column_name
                FROM information_schema.key_column_usage
                WHERE table_name = %s
                    AND referenced_table_name IS NOT NULL
                """,
                (table_name,)
            )
        return [
            {
                "column": row[0],
                "referenced_table": row[1],
                "referenced_column": row[2],
            }
            for row in cursor.fetchall()
        ]

    def export_table(self, table_name: str, schema: str) -> Path:
        """Export a single table to the schema structure.

        Args:
            table_name: Name of the table to export
            schema: Schema/database name

        Returns:
            Path to the created table JSON file
        """
        columns = self.get_columns(table_name, schema)
        foreign_keys = self.get_foreign_keys(table_name, schema)

        # Build constraints from foreign keys
        constraints = {}
        if foreign_keys:
            constraints["foreign_keys"] = foreign_keys

        # Add primary key constraint if found in columns
        pk_columns = [c for c, info in columns.items() if info.get("primary_key")]
        if pk_columns:
            constraints["primary_key"] = {"columns": pk_columns}

        table = TableDefinition(
            database=self.connection_name,
            schema=schema or "main",
            name=table_name,
            columns=columns,
            constraints=constraints,
        )

        return self._write_table_file(table)

    def export_all(self, target_schema: str | None = None) -> None:
        """Export all tables to the schema structure.

        Args:
            target_schema: Specific schema to export (None for all)
        """
        # Write initial metadata
        self._write_database_metadata()

        tables = self.get_tables(target_schema)

        for table_info in tables:
            table_name = table_info["name"]
            schema = table_info.get("schema", target_schema or "main")
            if target_schema and schema != target_schema:
                continue
            path = self.export_table(table_name, schema)
            print(f"Exported: {path}")

        # Export relationships
        self._export_relationships(tables)

        # Update metadata with collected schemas
        self._write_database_metadata()

    def _export_relationships(self, tables: list[dict]) -> None:
        """Export foreign key relationships to _relationships.json.

        Note: Lineages are not auto-detected from database introspection.
        They must be manually defined or imported from ETL tools.

        Args:
            tables: List of table dictionaries
        """
        all_relationships = []

        for table_info in tables:
            table_name = table_info["name"]
            schema = table_info.get("schema", "main")
            fks = self.get_foreign_keys(table_name, schema)

            for fk in fks:
                all_relationships.append({
                    "from_schema": schema,
                    "from_table": table_name,
                    "from_column": fk["column"],
                    "to_schema": schema,  # Assume same schema for FK
                    "to_table": fk["referenced_table"],
                    "to_column": fk["referenced_column"],
                    "type": "foreign_key",
                    "metadata": {},
                })

        if all_relationships:
            path = self._write_relationships(all_relationships)
            print(f"Exported relationships: {path}")

        # Create empty lineages file for future use
        lineage_path = self._write_lineages([])
        print(f"Created empty lineages file: {lineage_path}")

    def close(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None


def main():
    """CLI entry point for schema_profiler."""
    parser = argparse.ArgumentParser(
        description="Export database schema to distributed JSON structure"
    )
    parser.add_argument(
        "--database",
        required=True,
        help="Database connection name from settings.json",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        default="artifacts/schemas",
        help="Output directory for schema files (default: artifacts/schemas)",
    )
    parser.add_argument(
        "--schema",
        help="Target schema to export (default: all schemas)",
    )
    parser.add_argument(
        "--table",
        help="Export a single table instead of all tables",
    )

    args = parser.parse_args()

    profiler = SchemaProfiler(args.database, args.output_dir)

    try:
        profiler.connect()

        if args.table:
            # Export single table
            schema = args.schema or "main"
            path = profiler.export_table(args.table, schema)
            print(f"Exported table to: {path}")
        else:
            # Export all tables
            profiler.export_all(args.schema)
            print(f"Exported all tables to: {args.output_dir}/{args.database}")
    finally:
        profiler.close()


if __name__ == "__main__":
    main()
