"""Schema Validator for MetaForge's db-toolkit.

Validates schema JSON files (format + quality checks).

Usage:
    python .amandax/skills/db-toolkit/scripts/schema_validator.py --schema-dir artifacts/schemas/mydb
    python .amandax/skills/db-toolkit/scripts/schema_validator.py --schema-dir artifacts/schemas/mydb --verbose
    python .amandax/skills/db-toolkit/scripts/schema_validator.py --schema-dir artifacts/schemas/mydb --output report.json

Output format:
    ERROR|WARN: <file> | <object> | <issue>
"""

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


class SchemaValidator:
    """Validates schema JSON files: format compliance + quality checks."""

    RELATIONSHIPS_VERSION = "1.1"
    LINEAGES_VERSION = "1.0"

    def __init__(self, schema_dir: str, verbose: bool = False):
        self.schema_dir = Path(schema_dir)
        self.verbose = verbose
        self.errors: list[dict] = []
        self.warnings: list[dict] = []
        self.tables: list[dict] = []

    def _error(self, file: str, obj: str, issue: str) -> None:
        """Record an error with structured format."""
        entry = {"file": file, "object": obj, "issue": issue}
        self.errors.append(entry)
        print(f"ERROR: {file} | {obj} | {issue}")

    def _warn(self, file: str, obj: str, issue: str) -> None:
        """Record a warning with structured format."""
        entry = {"file": file, "object": obj, "issue": issue}
        self.warnings.append(entry)
        print(f"WARN: {file} | {obj} | {issue}")

    def _log(self, msg: str) -> None:
        """Verbose logging only."""
        if self.verbose:
            print(f"INFO: {msg}")

    def validate(self) -> dict[str, Any]:
        """Run all checks and return result."""
        print(f"Schema: {self.schema_dir}")
        print("-" * 60)

        if not self.schema_dir.exists():
            self._error(str(self.schema_dir), "directory", "not found")
            return self._build_result()

        # Format checks
        self._check_metadata()
        self._check_relationships()
        self._check_lineages()
        self._check_tables()

        # Quality checks (only if no format errors)
        if not self.errors:
            self._check_quality()

        # Summary
        print("-" * 60)
        if self.errors:
            print(f"FAILED: {len(self.errors)} errors, {len(self.warnings)} warnings")
        else:
            print(f"PASSED: {len(self.warnings)} warnings")

        return self._build_result()

    def _build_result(self) -> dict[str, Any]:
        """Build result dictionary."""
        return {
            "valid": len(self.errors) == 0,
            "schema_dir": str(self.schema_dir),
            "errors": self.errors,
            "warnings": self.warnings,
            "summary": {
                "total_errors": len(self.errors),
                "total_warnings": len(self.warnings),
                "tables_checked": len(self.tables),
            },
        }

    # --- Format Checks ---

    def _check_metadata(self) -> None:
        file_name = "_metadata.json"
        path = self.schema_dir / file_name

        if not path.exists():
            self._warn(file_name, "file", "not found (optional)")
            return

        self._log(f"Checking {file_name}")
        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError as e:
            self._error(file_name, "file", f"invalid JSON: {e}")
            return

        if "version" not in data:
            self._error(file_name, "root", "missing field 'version'")
        if "database" not in data:
            self._error(file_name, "root", "missing field 'database'")

    def _check_relationships(self) -> None:
        file_name = "_relationships.json"
        path = self.schema_dir / file_name

        if not path.exists():
            self._log(f"{file_name} not found (empty relationships)")
            return

        self._log(f"Checking {file_name}")
        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError as e:
            self._error(file_name, "file", f"invalid JSON: {e}")
            return

        if "version" not in data:
            self._error(file_name, "root", "missing field 'version'")
        if "database" not in data:
            self._error(file_name, "root", "missing field 'database'")
        if "relationships" not in data:
            self._error(file_name, "root", "missing field 'relationships'")
            return

        for i, rel in enumerate(data.get("relationships", [])):
            obj = f"relationships[{i}]"
            for f in ["unique_id", "source", "target", "type"]:
                if f not in rel:
                    self._error(file_name, obj, f"missing field '{f}'")
            for key in ["source", "target"]:
                if key in rel:
                    for f in ["schema", "table", "column"]:
                        if f not in rel[key]:
                            self._error(file_name, f"{obj}.{key}", f"missing field '{f}'")

    def _check_lineages(self) -> None:
        file_name = "_lineages.json"
        path = self.schema_dir / file_name

        if not path.exists():
            self._log(f"{file_name} not found (empty lineages)")
            return

        self._log(f"Checking {file_name}")
        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError as e:
            self._error(file_name, "file", f"invalid JSON: {e}")
            return

        for f in ["version", "database", "lineages"]:
            if f not in data:
                self._error(file_name, "root", f"missing field '{f}'")

    def _check_tables(self) -> None:
        self._log("Checking table files")
        schema_dirs = [d for d in self.schema_dir.iterdir() if d.is_dir() and not d.name.startswith("_")]

        if not schema_dirs:
            self._warn("(root)", "directory", "no schema subdirectories found")
            return

        for schema_dir in schema_dirs:
            for table_file in schema_dir.glob("*.json"):
                self._check_table_file(schema_dir.name, table_file)

    def _check_table_file(self, schema_name: str, path: Path) -> None:
        file_name = f"{schema_name}/{path.stem}.json"

        try:
            data = json.loads(path.read_text())
        except json.JSONDecodeError as e:
            self._error(file_name, "file", f"invalid JSON: {e}")
            return

        self.tables.append({"file": file_name, "schema": schema_name, "data": data})

        if "schema" not in data:
            self._error(file_name, "root", "missing field 'schema'")
        if "name" not in data:
            self._error(file_name, "root", "missing field 'name'")
        if "columns" not in data:
            self._error(file_name, "root", "missing field 'columns'")
            return

        for col_name, col_def in data.get("columns", {}).items():
            obj = f"column '{col_name}'"
            if not isinstance(col_def, dict):
                self._error(file_name, obj, "not an object")
            elif "type" not in col_def:
                self._error(file_name, obj, "missing field 'type'")

    # --- Quality Checks ---

    def _check_quality(self) -> None:
        self._log("Running quality checks")

        snake_pattern = re.compile(r"^[a-z][a-z0-9_]*$")
        numeric_pattern = re.compile(r"price|amount|cost|total|quantity|num_|count|sum|value", re.IGNORECASE)

        for table_info in self.tables:
            file_name = table_info["file"]
            table = table_info["data"]
            table_name = table.get("name", "unknown")

            # Primary key check
            has_pk = False
            constraints = table.get("constraints", {})
            if constraints.get("primary_key"):
                has_pk = True
            for col_def in table.get("columns", {}).values():
                if isinstance(col_def, dict) and col_def.get("pk"):
                    has_pk = True
            if not has_pk:
                self._warn(file_name, f"table '{table_name}'", "no primary key")

            # Naming check
            if not snake_pattern.match(table_name):
                self._warn(file_name, f"table '{table_name}'", "name not snake_case")

            for col_name in table.get("columns", {}).keys():
                if not snake_pattern.match(col_name):
                    self._warn(file_name, f"column '{table_name}.{col_name}'", "name not snake_case")

            # Type and comment checks
            for col_name, col_def in table.get("columns", {}).items():
                if not isinstance(col_def, dict):
                    continue

                col_type = col_def.get("type", "").lower()
                obj = f"column '{table_name}.{col_name}'"

                # Numeric data as string
                if col_type in ("varchar", "text", "char") and numeric_pattern.search(col_name):
                    self._warn(file_name, obj, f"type '{col_type}' for numeric-sounding column")

                # No comment
                if not col_def.get("comment"):
                    self._warn(file_name, obj, "missing comment")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate schema JSON files")
    parser.add_argument("--schema-dir", required=True, help="Schema directory path")
    parser.add_argument("--verbose", action="store_true", help="Show info messages")
    parser.add_argument("--output", "-o", help="Output JSON report")

    args = parser.parse_args()

    validator = SchemaValidator(args.schema_dir, args.verbose)
    result = validator.validate()

    if args.output:
        Path(args.output).write_text(json.dumps(result, indent=2))
        print(f"Report: {args.output}")

    return 0 if result["valid"] else 1


if __name__ == "__main__":
    sys.exit(main())
