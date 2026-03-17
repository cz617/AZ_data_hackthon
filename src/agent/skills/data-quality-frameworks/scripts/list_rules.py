#!/usr/bin/env python3
"""
Query GE Suite rules with pattern matching support.

Usage:
    # List all rules for a table
    python list_rules.py --table orders

    # Filter by type pattern
    python list_rules.py --table orders --type "*unique*"

    # Filter by column pattern
    python list_rules.py --table orders --column "*_id"

    # Filter by severity
    python list_rules.py --table orders --severity error

    # List all tables
    python list_rules.py --all

    # Markdown output
    python list_rules.py --table orders --format markdown
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

from ge_validation import match_pattern

GE_DIR = Path("artifacts/great_expectations")


def load_suite(table: str) -> dict:
    """Load suite from JSON file."""
    suite_path = GE_DIR / "expectations" / f"{table}_suite.json"

    if not suite_path.exists():
        return {"error": f"Suite not found: {table}"}

    with open(suite_path) as f:
        return json.load(f)


def get_suite_info(
    table: str,
    type_pattern: Optional[str] = None,
    column_pattern: Optional[str] = None,
    severity: Optional[str] = None,
    format: str = "json",
) -> str:
    """Get single suite info with pattern matching filters."""
    suite = load_suite(table)

    if "error" in suite:
        return format_output(suite, format)

    rules = []
    for exp in suite.get("expectations", []):
        meta = exp.get("meta", {})
        expectation_type = exp["expectation_type"]
        column = exp["kwargs"].get("column")
        rule_severity = meta.get("severity", "error")

        # Apply severity filter (exact match)
        if severity and rule_severity != severity:
            continue

        # Apply type pattern filter
        if type_pattern and not match_pattern(expectation_type, type_pattern):
            continue

        # Apply column pattern filter
        if column_pattern:
            if column is None:
                continue
            if not match_pattern(column, column_pattern):
                continue

        rules.append({
            "rule_id": meta.get("rule_id", "unknown"),
            "expectation_type": expectation_type,
            "column": column,
            "kwargs": {k: v for k, v in exp["kwargs"].items() if k != "column"},
            "severity": rule_severity,
        })

    result = {
        "table": table,
        "suite_name": suite["expectation_suite_name"],
        "total_rules": len(rules),
        "rules": rules,
    }

    return format_output(result, format)


def list_rules(
    table: str = None,
    all_tables: bool = False,
    type: Optional[str] = None,
    column: Optional[str] = None,
    severity: Optional[str] = None,
    format: str = "json",
    **kwargs,
) -> str:
    """
    Query rules from GE Suite.

    Args:
        table: Table name to query
        all_tables: List all tables if True
        type: Pattern to match expectation_type (supports * and ? wildcards)
        column: Pattern to match column name (supports * and ? wildcards)
        severity: Exact severity filter (error, warning, info)
        format: Output format (json or markdown)

    Returns:
        Formatted output string
    """
    if all_tables:
        results = []
        if (GE_DIR / "expectations").exists():
            for suite_path in (GE_DIR / "expectations").glob("*_suite.json"):
                table_name = suite_path.stem.replace("_suite", "")
                suite = load_suite(table_name)
                if "error" not in suite:
                    info = get_suite_info(
                        table_name,
                        type_pattern=type,
                        column_pattern=column,
                        severity=severity,
                        format="json",
                    )
                    info_dict = json.loads(info)
                    results.append(info_dict)
        return format_output(results, format, multi=True)

    return get_suite_info(
        table,
        type_pattern=type,
        column_pattern=column,
        severity=severity,
        format=format,
    )


def format_output(data, format: str, multi: bool = False) -> str:
    """Format output as JSON or markdown."""
    if format == "json":
        return json.dumps(data, indent=2, ensure_ascii=False)
    elif format == "markdown":
        return to_markdown(data, multi)
    else:
        return str(data)


def to_markdown(data, multi: bool = False) -> str:
    """Convert to markdown."""
    if multi:
        lines = ["# Data Quality Rules\n"]
        for suite in data:
            lines.extend(format_suite_markdown(suite))
            lines.append("")
        return "\n".join(lines)
    else:
        return "\n".join(format_suite_markdown(data))


def format_suite_markdown(suite: dict) -> list:
    """Format single suite as markdown."""
    if "error" in suite:
        return [f"## Error: {suite['error']}"]

    lines = [
        f"## {suite['table']}",
        f"**Suite**: `{suite['suite_name']}`  ",
        f"**Total Rules**: {suite['total_rules']}",
        "",
        "| Rule ID | Type | Column | Severity |",
        "|---------|------|--------|----------|",
    ]

    for rule in suite.get("rules", []):
        lines.append(
            f"| {rule['rule_id']} | `{rule['expectation_type']}` | {rule['column'] or '-'} | {rule['severity']} |"
        )

    return lines


def main():
    parser = argparse.ArgumentParser(
        description="List data quality rules with pattern matching support"
    )
    parser.add_argument("--table", help="Table name")
    parser.add_argument("--all", action="store_true", help="List all tables")
    parser.add_argument(
        "--type",
        dest="type_pattern",
        help="Filter by expectation_type pattern (supports * and ? wildcards)",
    )
    parser.add_argument(
        "--column",
        dest="column_pattern",
        help="Filter by column name pattern (supports * and ? wildcards)",
    )
    parser.add_argument(
        "--severity",
        choices=["error", "warning", "info"],
        help="Filter by severity (exact match)",
    )
    parser.add_argument(
        "--format",
        choices=["json", "markdown"],
        default="json",
        help="Output format",
    )

    args = parser.parse_args()

    if not args.table and not args.all:
        parser.error("Either --table or --all required")

    if args.table and args.all:
        parser.error("--table and --all are mutually exclusive")

    # Validate project
    if not Path(".amandax").exists():
        print("Error: Not a valid AmandaX project (.amandax not found)")
        sys.exit(1)

    output = list_rules(
        table=args.table,
        all_tables=args.all,
        type=args.type_pattern,
        column=args.column_pattern,
        severity=args.severity,
        format=args.format,
    )

    print(output)


if __name__ == "__main__":
    main()
