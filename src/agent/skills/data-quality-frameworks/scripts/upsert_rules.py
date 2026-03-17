#!/usr/bin/env python3
"""
Upsert data quality rules to GE Suite.

Replaces add_rules.py with validation-first approach and upsert logic.
Supports JSON array input, validation against GE built-in expectations,
and add-or-update semantics.

Usage:
    # Add single rule
    python upsert_rules.py --table orders --rules '[{"expectation_type": "expect_column_values_to_be_unique", "kwargs": {"column": "order_id"}}]'

    # Add multiple rules
    python upsert_rules.py --table orders --rules '[{"expectation_type": "...", "kwargs": {...}}, ...]'

    # Read from file
    python upsert_rules.py --table orders --file rules.json

    # Dry run (validate without saving)
    python upsert_rules.py --table orders --rules '[...]' --dry-run

Output format (success):
    {
        "success": true,
        "table": "orders",
        "suite_name": "orders_suite",
        "suite_path": "artifacts/great_expectations/expectations/orders_suite.json",
        "added": 1,
        "updated": 1,
        "total_expectations": 5,
        "added_rules": [...],
        "updated_rules": [...]
    }

Output format (validation error):
    {
        "error": "Validation failed. Fix the following rules:",
        "valid_rules": 1,
        "invalid_rules": 1,
        "validation_errors": [
            {"index": 0, "rule": {...}, "error": "Missing required parameter 'column' for 'expect_column_values_to_be_unique'"}
        ],
        "hint": "Use GE built-in expectations. See SKILL.md for supported types."
    }
"""

import argparse
import hashlib
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

from ge_validation import validate_rules_batch, validate_expectation

GE_DIR = Path("artifacts/great_expectations")


def generate_rule_id(exp_type: str, column: str) -> str:
    """Generate consistent rule_id from expectation type and column."""
    content = f"{exp_type}_{column}"
    return f"r_{hashlib.md5(content.encode()).hexdigest()[:6]}"


def load_suite(table: str) -> Dict:
    """Load existing suite or create new one."""
    suite_name = f"{table}_suite"
    suite_path = GE_DIR / "expectations" / f"{suite_name}.json"

    if suite_path.exists():
        with open(suite_path) as f:
            return json.load(f)

    # Return GE Suite format
    return {
        "data_asset_type": None,
        "expectation_suite_name": suite_name,
        "expectations": [],
        "meta": {
            "great_expectations_version": "0.18.0",
            "created_at": datetime.now().isoformat(),
        },
    }


def save_suite(table: str, suite: Dict) -> Path:
    """Save suite to JSON file."""
    suite_name = f"{table}_suite"
    suite_path = GE_DIR / "expectations" / f"{suite_name}.json"
    suite_path.parent.mkdir(parents=True, exist_ok=True)

    with open(suite_path, "w") as f:
        json.dump(suite, f, indent=2)

    return suite_path


def _extract_column_from_rule(rule: Dict) -> Optional[str]:
    """Extract column name from rule kwargs."""
    # Most column-based expectations have 'column' in kwargs
    kwargs = rule.get("kwargs", {})
    return kwargs.get("column")


def _normalize_rule(rule: Dict) -> Dict:
    """
    Normalize rule format for validation.

    GE validation expects kwargs at top level, but input may have nested kwargs.
    This function flattens the structure and keeps meta separate.

    Args:
        rule: Rule dict with optional nested 'kwargs'

    Returns:
        Tuple of (normalized_rule_for_validation, meta_dict)
        - normalized_rule: Dict with expectation_type and kwargs at top level (no meta)
        - meta_dict: Meta dict or empty dict
    """
    # Handle missing expectation_type
    if "expectation_type" not in rule:
        return rule, {}  # Return as-is, validation will catch the error

    normalized = {"expectation_type": rule["expectation_type"]}
    meta = {}

    # Check if kwargs are nested or at top level
    if "kwargs" in rule:
        # Nested kwargs format: {"expectation_type": "...", "kwargs": {...}}
        normalized.update(rule["kwargs"])
        # Extract meta if present at top level
        if "meta" in rule:
            meta = rule["meta"]
    else:
        # Flat format: {"expectation_type": "...", "column": "...", "meta": {...}}
        for key, value in rule.items():
            if key == "meta":
                meta = value
            elif key != "expectation_type":
                normalized[key] = value

    return normalized, meta


def upsert_rules(
    table: str,
    rules_input: Union[List[Dict], Dict],
    dry_run: bool = False,
) -> Dict:
    """
    Upsert rules to GE Suite with validation.

    Args:
        table: Table name
        rules_input: Single rule dict or list of rules
        dry_run: If True, validate but don't save

    Returns:
        Result dict with success status, counts, and details
    """
    # Normalize input to list
    if isinstance(rules_input, dict):
        rules = [rules_input]
    else:
        rules = rules_input

    # Normalize all rules (flatten nested kwargs, extract meta)
    normalized_rules = []
    rules_meta = []
    for r in rules:
        norm, meta = _normalize_rule(r)
        normalized_rules.append(norm)
        rules_meta.append(meta)

    # Validate all rules first
    valid_rules, invalid_rules = validate_rules_batch(normalized_rules)

    if invalid_rules:
        # Build validation error response
        validation_errors = []
        for idx, rule in enumerate(normalized_rules):
            is_valid, error = validate_expectation(rule)
            if not is_valid:
                validation_errors.append({
                    "index": idx,
                    "rule": rules[idx],  # Return original rule for reference
                    "error": error,
                })

        return {
            "success": False,
            "error": "Validation failed. Fix the following rules:",
            "valid_rules": len(valid_rules),
            "invalid_rules": len(invalid_rules),
            "validation_errors": validation_errors,
            "hint": "Use GE built-in expectations. See SKILL.md for supported types.",
        }

    # Load existing suite
    suite = load_suite(table)
    suite_name = suite["expectation_suite_name"]

    # Build index of existing rules by (expectation_type, column)
    existing_by_key = {}
    for idx, exp in enumerate(suite["expectations"]):
        exp_type = exp["expectation_type"]
        column = exp["kwargs"].get("column")
        key = (exp_type, column)
        existing_by_key[key] = idx

    added_rules = []
    updated_rules = []

    # Build a list of (normalized_rule, original_index) for valid rules
    valid_rule_indices = []
    for idx, rule in enumerate(normalized_rules):
        is_valid, _ = validate_expectation(rule)
        if is_valid:
            valid_rule_indices.append(idx)

    # Process each valid rule
    for idx in valid_rule_indices:
        rule = normalized_rules[idx]
        rule_meta = rules_meta[idx]

        exp_type = rule["expectation_type"]

        # Extract kwargs (all fields except expectation_type)
        exp_kwargs = {k: v for k, v in rule.items() if k != "expectation_type"}
        column = exp_kwargs.get("column")
        key = (exp_type, column)

        # Generate metadata
        rule_id = rule_meta.get("rule_id") or generate_rule_id(exp_type, column or "")

        meta = {
            "rule_id": rule_id,
            "added_at": datetime.now().isoformat(),
            "added_by": "llm",
            "severity": rule_meta.get("severity", "error"),
            **{
                k: v
                for k, v in rule_meta.items()
                if k not in ["rule_id", "added_at", "added_by", "severity"]
            },
        }

        # Create expectation in GE format (with nested kwargs)
        expectation = {
            "expectation_type": exp_type,
            "kwargs": exp_kwargs,
            "meta": meta,
        }

        if key in existing_by_key:
            # Update existing rule
            idx = existing_by_key[key]
            suite["expectations"][idx] = expectation
            updated_rules.append({
                "rule_id": rule_id,
                "type": exp_type,
                "column": column,
                "severity": meta["severity"],
            })
        else:
            # Add new rule
            suite["expectations"].append(expectation)
            added_rules.append({
                "rule_id": rule_id,
                "type": exp_type,
                "column": column,
                "severity": meta["severity"],
            })

    # Save suite (unless dry run)
    suite_path = None
    if not dry_run:
        suite_path = save_suite(table, suite)

    return {
        "success": True,
        "table": table,
        "suite_name": suite_name,
        "suite_path": str(suite_path) if suite_path else None,
        "added": len(added_rules),
        "updated": len(updated_rules),
        "total_expectations": len(suite["expectations"]),
        "added_rules": added_rules,
        "updated_rules": updated_rules,
        **({"dry_run": True} if dry_run else {}),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Upsert data quality rules with validation"
    )
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--rules", help="JSON array of rules (or single object)")
    parser.add_argument("--file", help="JSON file with rules")
    parser.add_argument(
        "--dry-run", action="store_true", help="Validate without saving"
    )

    args = parser.parse_args()

    # Validate project
    if not Path(".amandax").exists():
        print(json.dumps({
            "success": False,
            "error": "Not a valid AmandaX project (.amandax not found)",
        }))
        sys.exit(1)

    # Load rules from argument or file
    rules_input = None
    if args.rules:
        rules_input = json.loads(args.rules)
    elif args.file:
        with open(args.file) as f:
            rules_input = json.load(f)
    else:
        print(json.dumps({
            "success": False,
            "error": "Must provide --rules or --file",
        }))
        sys.exit(1)

    # Execute upsert
    result = upsert_rules(
        table=args.table,
        rules_input=rules_input,
        dry_run=args.dry_run,
    )

    print(json.dumps(result, indent=2))

    # Exit with error code if validation failed
    if not result.get("success"):
        sys.exit(1)


if __name__ == "__main__":
    main()
