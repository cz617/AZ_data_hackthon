#!/usr/bin/env python3
"""
Batch delete rules from GE Suite with pattern matching support.

Usage:
    # Delete by rule_id pattern
    python delete_rules.py --table orders --rule-id "r_abc*"

    # Delete by expectation type pattern
    python delete_rules.py --table orders --type "*unique*"

    # Delete by column pattern
    python delete_rules.py --table orders --column "*_id"

    # Delete all rules
    python delete_rules.py --table orders --all

    # Dry run (preview without deleting)
    python delete_rules.py --table orders --type "*null*" --dry-run
"""

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

from ge_validation import match_pattern

GE_DIR = Path("artifacts/great_expectations")


def load_suite(table: str) -> dict:
    """Load suite from JSON file."""
    suite_path = GE_DIR / "expectations" / f"{table}_suite.json"

    if not suite_path.exists():
        return {"error": f"Suite not found: {table}"}

    with open(suite_path) as f:
        return json.load(f)


def save_suite(table: str, suite: dict) -> Path:
    """Save suite to JSON file."""
    suite_path = GE_DIR / "expectations" / f"{table}_suite.json"

    with open(suite_path, "w") as f:
        json.dump(suite, f, indent=2)

    return suite_path


def delete_rules(
    table: str,
    rule_id_pattern: Optional[str] = None,
    type_pattern: Optional[str] = None,
    column_pattern: Optional[str] = None,
    delete_all: bool = False,
    dry_run: bool = False,
) -> dict:
    """Batch delete rules from GE Suite with pattern matching."""

    suite = load_suite(table)

    if "error" in suite:
        return {"success": False, "error": suite["error"], "table": table}

    original_expectations = suite.get("expectations", [])
    to_delete = []
    remaining = []

    for exp in original_expectations:
        should_delete = False
        meta = exp.get("meta", {})
        kwargs = exp.get("kwargs", {})

        # Delete all rules
        if delete_all:
            should_delete = True
        # Match by rule_id pattern
        elif rule_id_pattern:
            rule_id = meta.get("rule_id", "")
            if rule_id and match_pattern(rule_id, rule_id_pattern):
                should_delete = True
        # Match by expectation_type pattern
        elif type_pattern:
            exp_type = exp.get("expectation_type", "")
            if exp_type and match_pattern(exp_type, type_pattern):
                should_delete = True
        # Match by column pattern
        elif column_pattern:
            column = kwargs.get("column", "")
            if column and match_pattern(column, column_pattern):
                should_delete = True

        if should_delete:
            to_delete.append(exp)
        else:
            remaining.append(exp)

    # Build deleted_rules info
    deleted_rules = [
        {
            "rule_id": e.get("meta", {}).get("rule_id", "unknown"),
            "type": e.get("expectation_type", ""),
            "column": e.get("kwargs", {}).get("column"),
        }
        for e in to_delete
    ]

    # Update suite only if not dry_run
    if not dry_run and to_delete:
        suite["expectations"] = remaining
        save_suite(table, suite)

    return {
        "success": True,
        "table": table,
        "deleted": len(to_delete),
        "remaining": len(remaining),
        "deleted_rules": deleted_rules,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Delete data quality rules with pattern matching support"
    )
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview rules to delete without actually deleting",
    )

    # Mutually exclusive filter group
    filter_group = parser.add_mutually_exclusive_group(required=True)
    filter_group.add_argument(
        "--rule-id",
        dest="rule_id_pattern",
        help="Delete by rule_id pattern (supports * and ? wildcards)",
    )
    filter_group.add_argument(
        "--type",
        dest="type_pattern",
        help="Delete by expectation_type pattern (supports * and ? wildcards)",
    )
    filter_group.add_argument(
        "--column",
        dest="column_pattern",
        help="Delete by column name pattern (supports * and ? wildcards)",
    )
    filter_group.add_argument(
        "--all",
        dest="delete_all",
        action="store_true",
        help="Delete all rules in the suite",
    )

    args = parser.parse_args()

    # Validate project
    if not Path(".amandax").exists():
        print(
            json.dumps(
                {"success": False, "error": "Not a valid AmandaX project (.amandax not found)"}
            )
        )
        sys.exit(1)

    result = delete_rules(
        table=args.table,
        rule_id_pattern=args.rule_id_pattern,
        type_pattern=args.type_pattern,
        column_pattern=args.column_pattern,
        delete_all=args.delete_all,
        dry_run=args.dry_run,
    )

    print(json.dumps(result, indent=2))

    # Exit with error code if operation failed
    if not result.get("success", False):
        sys.exit(1)


if __name__ == "__main__":
    main()
