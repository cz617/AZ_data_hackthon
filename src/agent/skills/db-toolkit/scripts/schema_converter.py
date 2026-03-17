"""Schema Converter for MetaForge db-toolkit.

Converts between DDL SQL, single JSON, and distributed file formats.

Usage:
    # DDL to distributed JSON
    python .amandax/skills/db-toolkit/scripts/schema_converter.py --input artifacts/ddl/schema.sql --from ddl --to distributed --database mydb --output-dir artifacts/schemas

    # Distributed to single JSON
    python .amandax/skills/db-toolkit/scripts/schema_converter.py --input-dir artifacts/schemas/mydb --from distributed --to json --output artifacts/schema.json
"""

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass
class TableDefinition:
    """Represents a database table definition."""
    database: str
    schema: str
    name: str
    columns: dict[str, Any]
    constraints: dict[str, Any] = field(default_factory=dict)
    indexes: dict[str, Any] = field(default_factory=dict)
    statistics: dict[str, Any] = field(default_factory=dict)
    comment: str = ""


class DDLParser:
    """Parses DDL SQL statements into structured table definitions."""

    def __init__(self):
        """Initialize the DDL parser."""
        self.tables: dict[str, Any] = {}

    def parse(self, ddl: str) -> dict[str, Any]:
        """Parse CREATE TABLE statements from DDL.

        Args:
            ddl: DDL SQL string containing CREATE TABLE statements

        Returns:
            Dictionary mapping table names to their definitions
        """
        self.tables = {}

        if not ddl or not ddl.strip():
            return self.tables

        # Normalize line endings and remove comments
        ddl = self._normalize_ddl(ddl)

        # Find all CREATE TABLE statements
        create_pattern = re.compile(
            r'CREATE\s+TABLE\s+(?:"?([^"\s]+)"?\.)?"?([^"\s(]+)"?\s*\((.*?)\)\s*;',
            re.IGNORECASE | re.DOTALL
        )

        for match in create_pattern.finditer(ddl):
            schema_name = match.group(1) or "public"
            table_name = self._strip_quotes(match.group(2))
            body = match.group(3)

            table_def = {
                "name": table_name,
                "schema": schema_name,
                "columns": {},
                "constraints": {},
            }

            # Parse columns and inline constraints
            columns = self._parse_columns(body)
            table_def["columns"] = columns

            # Parse table-level constraints
            constraints = self._parse_constraints(body)
            table_def["constraints"].update(constraints)

            self.tables[table_name] = table_def

        return self.tables

    def _normalize_ddl(self, ddl: str) -> str:
        """Normalize DDL by removing comments and extra whitespace."""
        # Remove SQL comments (-- style)
        ddl = re.sub(r'--[^\n]*', '', ddl)
        # Remove /* */ style comments
        ddl = re.sub(r'/\*.*?\*/', '', ddl, flags=re.DOTALL)
        # Normalize whitespace
        ddl = re.sub(r'\s+', ' ', ddl)
        return ddl.strip()

    def _strip_quotes(self, identifier: str) -> str:
        """Remove quotes from identifier if present."""
        if identifier.startswith('"') and identifier.endswith('"'):
            return identifier[1:-1]
        if identifier.startswith('`') and identifier.endswith('`'):
            return identifier[1:-1]
        if identifier.startswith('[') and identifier.endswith(']'):
            return identifier[1:-1]
        return identifier

    def _parse_columns(self, body: str) -> dict[str, Any]:
        """Extract column definitions from table body.

        Args:
            body: The content inside CREATE TABLE parentheses

        Returns:
            Dictionary mapping column names to their definitions
        """
        columns = {}

        # Split by comma, but be careful with nested parentheses
        column_defs = self._split_columns(body)

        for col_def in column_defs:
            col_def = col_def.strip()
            if not col_def:
                continue

            # Skip table-level constraints (PRIMARY KEY, FOREIGN KEY, etc.)
            if re.match(r'^(PRIMARY\s+KEY|FOREIGN\s+KEY|UNIQUE|CONSTRAINT|CHECK)\b',
                       col_def, re.IGNORECASE):
                continue

            # Parse column name and type
            col_match = re.match(
                r'"?([^"\s]+)"?\s+(\w+(?:\([^)]*\))?)',
                col_def,
                re.IGNORECASE
            )

            if not col_match:
                continue

            col_name = self._strip_quotes(col_match.group(1))
            type_str = col_match.group(2)

            col_info = self._parse_column_type(type_str)
            col_info["nullable"] = True  # Default

            # Parse constraints
            constraints_str = col_def[len(col_match.group(0)):].strip()

            # NOT NULL / NULL
            if re.search(r'\bNOT\s+NULL\b', constraints_str, re.IGNORECASE):
                col_info["nullable"] = False
            elif re.search(r'\bNULL\b', constraints_str, re.IGNORECASE):
                col_info["nullable"] = True

            # PRIMARY KEY
            if re.search(r'\bPRIMARY\s+KEY\b', constraints_str, re.IGNORECASE):
                col_info["primary_key"] = True
                col_info["nullable"] = False

            # UNIQUE
            if re.search(r'\bUNIQUE\b', constraints_str, re.IGNORECASE):
                col_info["unique"] = True

            # DEFAULT value
            default_match = re.search(
                r'\bDEFAULT\s+([^\s,]+(?:\s+[^\s,]+)*)',
                constraints_str,
                re.IGNORECASE
            )
            if default_match:
                col_info["default"] = default_match.group(1).strip()

            # FOREIGN KEY (column level)
            fk_match = re.search(
                r'REFERENCES\s+"?([^"\s(]+)"?(?:\s*\(\s*"?([^"\s)]+)"?\s*\))?',
                constraints_str,
                re.IGNORECASE
            )
            if fk_match:
                ref_table = self._strip_quotes(fk_match.group(1))
                ref_column = fk_match.group(2) or "id"
                ref_column = self._strip_quotes(ref_column)
                col_info["foreign_key"] = {
                    "table": ref_table,
                    "column": ref_column
                }

            columns[col_name] = col_info

        return columns

    def _split_columns(self, body: str) -> list[str]:
        """Split column definitions by comma, respecting parentheses."""
        result = []
        current = ""
        depth = 0

        for char in body:
            if char == '(':
                depth += 1
                current += char
            elif char == ')':
                depth -= 1
                current += char
            elif char == ',' and depth == 0:
                result.append(current)
                current = ""
            else:
                current += char

        if current.strip():
            result.append(current)

        return result

    def _parse_column_type(self, type_str: str) -> dict[str, Any]:
        """Parse a column type string into structured info."""
        type_str = type_str.strip().lower()

        # bigint, int, integer
        if re.match(r'^(bigint|int|integer)\b', type_str):
            return {"type": type_str.split()[0]}

        # varchar with optional length
        varchar_match = re.match(r'^varchar(?:\((\d+)\))?', type_str)
        if varchar_match:
            result = {"type": "varchar"}
            if varchar_match.group(1):
                result["length"] = int(varchar_match.group(1))
            return result

        # text, string
        if re.match(r'^(text|string)\b', type_str):
            return {"type": type_str.split()[0]}

        # decimal/numeric with precision and scale
        decimal_match = re.match(r'^(decimal|numeric)(?:\((\d+),\s*(\d+)\))?', type_str)
        if decimal_match:
            result = {"type": decimal_match.group(1)}
            if decimal_match.group(2):
                result["precision"] = int(decimal_match.group(2))
            if decimal_match.group(3):
                result["scale"] = int(decimal_match.group(3))
            return result

        # timestamp, datetime, date
        if re.match(r'^(timestamp|datetime|date)\b', type_str):
            return {"type": type_str.split()[0]}

        # boolean, bool
        if re.match(r'^(boolean|bool)\b', type_str):
            return {"type": type_str.split()[0]}

        # Unknown type - store as-is
        return {"type": type_str}

    def _parse_constraints(self, body: str) -> dict[str, Any]:
        """Extract table-level constraints from table body.

        Args:
            body: The content inside CREATE TABLE parentheses

        Returns:
            Dictionary of constraint definitions
        """
        constraints = {}

        # Split column definitions
        parts = self._split_columns(body)

        for part in parts:
            part = part.strip()
            if not part:
                continue

            # PRIMARY KEY (table level)
            pk_match = re.match(
                r'PRIMARY\s+KEY\s*\(\s*([^)]+)\s*\)',
                part,
                re.IGNORECASE
            )
            if pk_match:
                columns = [self._strip_quotes(c.strip())
                          for c in pk_match.group(1).split(',')]
                constraints["primary_key"] = {"columns": columns}
                continue

            # FOREIGN KEY (table level)
            fk_match = re.match(
                r'FOREIGN\s+KEY\s*\(\s*"?([^"\s)]+)"?\s*\)\s*REFERENCES\s*"?([^"\s(]+)"?(?:\s*\(\s*"?([^"\s)]+)"?\s*\))?',
                part,
                re.IGNORECASE
            )
            if fk_match:
                if "foreign_keys" not in constraints:
                    constraints["foreign_keys"] = []

                col_name = self._strip_quotes(fk_match.group(1))
                ref_table = self._strip_quotes(fk_match.group(2))
                ref_column = fk_match.group(3) or "id"
                ref_column = self._strip_quotes(ref_column)

                constraints["foreign_keys"].append({
                    "column": col_name,
                    "references_table": ref_table,
                    "references_column": ref_column
                })
                continue

            # UNIQUE constraint (table level)
            unique_match = re.match(
                r'UNIQUE\s*\(\s*([^)]+)\s*\)',
                part,
                re.IGNORECASE
            )
            if unique_match:
                if "unique" not in constraints:
                    constraints["unique"] = []
                columns = [self._strip_quotes(c.strip())
                          for c in unique_match.group(1).split(',')]
                constraints["unique"].append({"columns": columns})

        return constraints

    def _extract_default(self, constraints: str) -> str | None:
        """Extract DEFAULT value from constraint string.

        Args:
            constraints: Constraint string after column type

        Returns:
            Default value string or None
        """
        match = re.search(r'DEFAULT\s+(.+?)(?:\s+(?:NOT\s+)?NULL|\s+PRIMARY|\s+UNIQUE|\s+REFERENCES|$)',
                         constraints, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return None


class SchemaConverter:
    """Converts between DDL, JSON, and distributed file formats."""

    VERSION = "1.0"
    DEFAULT_SOURCE = "ddl"

    def __init__(self):
        """Initialize the schema converter."""
        self.parser = DDLParser()

    def _ensure_directories(self, db_dir: Path) -> None:
        """Create the database directory structure."""
        db_dir.mkdir(parents=True, exist_ok=True)
        (db_dir / "_relationships").mkdir(exist_ok=True)
        (db_dir / "_lineage").mkdir(exist_ok=True)

    def _write_database_metadata(self, db_dir: Path, database_name: str, schemas: set) -> Path:
        """Write the database metadata file."""
        self._ensure_directories(db_dir)

        metadata = {
            "database": database_name,
            "version": self.VERSION,
            "created_at": datetime.now(UTC).isoformat(),
            "schemas": sorted(schemas),
        }

        metadata_path = db_dir / "_metadata.json"
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

        return metadata_path

    def _write_table_file(self, db_dir: Path, table: TableDefinition) -> Path:
        """Write a table definition to the schema directory."""
        # Create schema directory structure
        schema_dir = db_dir / table.schema / "tables"
        schema_dir.mkdir(parents=True, exist_ok=True)

        # Build table JSON structure
        table_data = {
            "version": self.VERSION,
            "identity": {
                "database": table.database,
                "schema": table.schema,
                "name": table.name,
                "full_name": f"{table.database}.{table.schema}.{table.name}",
            },
            "extracted_at": datetime.now(UTC).isoformat(),
            "source": self.DEFAULT_SOURCE,
            "definition": {
                "type": "table",
                "comment": table.comment,
            },
            "columns": table.columns,
            "constraints": table.constraints,
            "indexes": table.indexes,
            "statistics": table.statistics,
        }

        # Write table file
        table_path = schema_dir / f"{table.name}.json"
        with open(table_path, "w", encoding="utf-8") as f:
            json.dump(table_data, f, indent=2)

        return table_path

    def ddl_to_distributed(self, ddl: str, output_dir: str, database_name: str) -> None:
        """Convert DDL SQL to distributed file format.

        Args:
            ddl: DDL SQL string
            output_dir: Base directory for output
            database_name: Name of the database
        """
        # Parse DDL
        tables = self.parser.parse(ddl)

        # Setup directory structure
        db_dir = Path(output_dir) / database_name
        schemas: set = set()

        # Write each table
        for table_name, table_def in tables.items():
            schema = table_def.get("schema", "public")
            schemas.add(schema)

            # Convert to TableDefinition
            table = TableDefinition(
                database=database_name,
                schema=schema,
                name=table_name,
                columns=table_def.get("columns", {}),
                constraints=table_def.get("constraints", {}),
                indexes=table_def.get("indexes", {}),
                statistics=table_def.get("statistics", {}),
                comment=table_def.get("comment", "")
            )
            self._write_table_file(db_dir, table)

        # Write metadata
        self._write_database_metadata(db_dir, database_name, schemas)

    def distributed_to_json(self, input_dir: str, output_file: str) -> None:
        """Convert distributed files to single JSON.

        Args:
            input_dir: Directory containing distributed schema files
            output_file: Path to output JSON file
        """
        input_path = Path(input_dir)

        # Read metadata
        metadata_path = input_path / "_metadata.json"
        if not metadata_path.exists():
            raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

        with open(metadata_path, encoding='utf-8') as f:
            metadata = json.load(f)

        database_name = metadata.get("database", "unknown")

        # Collect all table files
        tables = []
        for schema_dir in input_path.iterdir():
            if not schema_dir.is_dir() or schema_dir.name.startswith('_'):
                continue

            tables_dir = schema_dir / "tables"
            if not tables_dir.exists():
                continue

            for table_file in tables_dir.glob("*.json"):
                with open(table_file, encoding='utf-8') as f:
                    table_data = json.load(f)

                # Extract relevant info
                table_info = {
                    "name": table_data.get("identity", {}).get("name", table_file.stem),
                    "schema": table_data.get("identity", {}).get("schema", schema_dir.name),
                    "columns": table_data.get("columns", {}),
                    "constraints": table_data.get("constraints", {}),
                    "indexes": table_data.get("indexes", {}),
                    "comment": table_data.get("definition", {}).get("comment", "")
                }
                tables.append(table_info)

        # Create combined output
        output = {
            "database": database_name,
            "version": metadata.get("version", "1.0"),
            "extracted_at": metadata.get("created_at", ""),
            "tables": tables
        }

        # Write output
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Convert between DDL SQL, JSON, and distributed schema formats"
    )

    parser.add_argument(
        "--input",
        help="Input DDL file path"
    )
    parser.add_argument(
        "--input-dir",
        help="Input directory (for distributed format)"
    )
    parser.add_argument(
        "--from",
        dest="from_format",
        choices=["ddl", "distributed"],
        required=True,
        help="Source format"
    )
    parser.add_argument(
        "--to",
        dest="to_format",
        choices=["distributed", "json"],
        required=True,
        help="Target format"
    )
    parser.add_argument(
        "--database",
        help="Database name (for DDL to distributed)"
    )
    parser.add_argument(
        "--output-dir",
        help="Output directory (for distributed format)"
    )
    parser.add_argument(
        "--output",
        help="Output file path (for JSON format)"
    )

    args = parser.parse_args()

    converter = SchemaConverter()

    if args.from_format == "ddl" and args.to_format == "distributed":
        if not args.input:
            print("Error: --input required for DDL input", file=sys.stderr)
            sys.exit(1)
        if not args.database:
            print("Error: --database required for DDL to distributed conversion", file=sys.stderr)
            sys.exit(1)
        if not args.output_dir:
            print("Error: --output-dir required for distributed output", file=sys.stderr)
            sys.exit(1)

        ddl = Path(args.input).read_text(encoding='utf-8')
        converter.ddl_to_distributed(ddl, args.output_dir, args.database)
        print(f"Converted DDL to distributed format in {args.output_dir}/{args.database}")

    elif args.from_format == "distributed" and args.to_format == "json":
        if not args.input_dir:
            print("Error: --input-dir required for distributed input", file=sys.stderr)
            sys.exit(1)
        if not args.output:
            print("Error: --output required for JSON output", file=sys.stderr)
            sys.exit(1)

        converter.distributed_to_json(args.input_dir, args.output)
        print(f"Converted distributed format to JSON: {args.output}")

    else:
        print(f"Error: Conversion from {args.from_format} to {args.to_format} not supported",
              file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
