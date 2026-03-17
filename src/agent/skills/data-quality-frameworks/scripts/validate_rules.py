#!/usr/bin/env python3
"""
Validate existing data quality rules in GE Suite files.

This script checks existing rule files for:
- Invalid expectation types (not GE built-in)
- Missing required parameters
- Invalid parameter types
- Duplicate rules (same type + column)
- Malformed JSON structure
- Missing meta fields (rule_id, severity)

Usage:
    # Validate all rules
    python validate_rules.py --all

    # Validate specific table
    python validate_rules.py --table orders

    # Validate and auto-fix issues (removes invalid rules)
    python validate_rules.py --table orders --fix

    # Detailed output
    python validate_rules.py --table orders --verbose

Output format (success):
    {
        "success": true,
        "tables_checked": 3,
        "total_rules": 15,
        "valid_rules": 14,
        "invalid_rules": 1,
        "issues": [
            {
                "table": "orders",
                "suite_path": "artifacts/great_expectations/expectations/orders_suite.json",
                "rule_index": 2,
                "expectation_type": "invalid_expectation",
                "column": "status",
                "issue": "Unknown expectation type: 'invalid_expectation'",
                "severity": "error"
            }
        ]
    }

Output format (no rules found):
    {
        "success": true,
        "message": "No rules found. Use upsert_rules.py to add rules.",
        "tables_checked": 0,
        "total_rules": 0
    }
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ge_validation import (
    GE_BUILTIN_EXPECTATIONS,
    validate_expectation,
    match_pattern,
)

GE_DIR = Path("artifacts/great_expectations")


def find_suite_files(table_pattern: Optional[str] = None) -> List[Tuple[str, Path]]:
    """
    Find all suite files matching pattern.

    Args:
        table_pattern: Optional pattern to filter tables (supports * and ?)

    Returns:
        List of (table_name, suite_path) tuples
    """
    suites_dir = GE_DIR / "expectations"
    if not suites_dir.exists():
        return []

    results = []
    for suite_path in suites_dir.glob("*_suite.json"):
        # Extract table name from suite filename (e.g., "orders_suite.json" -> "orders")
        table_name = suite_path.stem.replace("_suite", "")

        if table_pattern:
            if not match_pattern(table_name, table_pattern):
                continue

        results.append((table_name, suite_path))

    return sorted(results, key=lambda x: x[0])


def load_suite(suite_path: Path) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Load suite from JSON file.

    Args:
        suite_path: Path to suite JSON file

    Returns:
        Tuple of (suite_dict, error_message)
        - (suite, None) on success
        - (None, error_message) on failure
    """
    try:
        with open(suite_path) as f:
            suite = json.load(f)
        return suite, None
    except json.JSONDecodeError as e:
        return None, f"Invalid JSON: {str(e)}"
    except Exception as e:
        return None, f"Failed to read file: {str(e)}"


def validate_suite_structure(suite: Dict) -> List[str]:
    """
    Validate suite has required GE structure.

    Args:
        suite: Suite dictionary

    Returns:
        List of structural issues found
    """
    issues = []

    if "expectation_suite_name" not in suite:
        issues.append("Missing 'expectation_suite_name' field")

    if "expectations" not in suite:
        issues.append("Missing 'expectations' array")
    elif not isinstance(suite.get("expectations"), list):
        issues.append("'expectations' must be an array")

    return issues


def validate_rule(rule: Dict, rule_index: int) -> List[Dict]:
    """
    Validate a single rule and return all issues found.

    Args:
        rule: Rule dictionary
        rule_index: Index in expectations array

    Returns:
        List of issue dictionaries
    """
    issues = []

    # Check expectation_type exists
    exp_type = rule.get("expectation_type")
    if not exp_type:
        return [{
            "issue": "Missing 'expectation_type' field",
            "severity": "error",
            "rule_index": rule_index,
            "expectation_type": None,
            "column": None,
        }]

    # Check kwargs exists
    kwargs = rule.get("kwargs")
    if kwargs is None:
        issues.append({
            "issue": "Missing 'kwargs' field",
            "severity": "error",
            "rule_index": rule_index,
            "expectation_type": exp_type,
            "column": None,
        })
        return issues

    if not isinstance(kwargs, dict):
        issues.append({
            "issue": "'kwargs' must be an object",
            "severity": "error",
            "rule_index": rule_index,
            "expectation_type": exp_type,
            "column": None,
        })
        return issues

    column = kwargs.get("column")

    # Validate against GE built-in expectations
    # Flatten for validation (kwargs at top level)
    flat_rule = {"expectation_type": exp_type, **kwargs}
    is_valid, error = validate_expectation(flat_rule)

    if not is_valid:
        issues.append({
            "issue": error,
            "severity": "error",
            "rule_index": rule_index,
            "expectation_type": exp_type,
            "column": column,
        })

    # Check meta fields
    meta = rule.get("meta", {})
    if not meta.get("rule_id"):
        issues.append({
            "issue": "Missing 'rule_id' in meta (recommended for traceability)",
            "severity": "warning",
            "rule_index": rule_index,
            "expectation_type": exp_type,
            "column": column,
        })

    if not meta.get("severity"):
        issues.append({
            "issue": "Missing 'severity' in meta (recommended for prioritization)",
            "severity": "warning",
            "rule_index": rule_index,
            "expectation_type": exp_type,
            "column": column,
        })

    return issues


def check_duplicates(suite: Dict) -> List[Dict]:
    """
    Check for duplicate rules (same expectation_type + column).

    Args:
        suite: Suite dictionary

    Returns:
        List of duplicate issues
    """
    issues = []
    seen = {}  # (exp_type, column) -> [indices]

    for idx, exp in enumerate(suite.get("expectations", [])):
        exp_type = exp.get("expectation_type")
        kwargs = exp.get("kwargs", {})
        column = kwargs.get("column")

        key = (exp_type, column)
        if key not in seen:
            seen[key] = []
        seen[key].append(idx)

    # Report duplicates
    for (exp_type, column), indices in seen.items():
        if len(indices) > 1:
            issues.append({
                "issue": f"Duplicate rule: {len(indices)} rules with same type '{exp_type}' and column '{column}'",
                "severity": "warning",
                "rule_indices": indices,
                "expectation_type": exp_type,
                "column": column,
            })

    return issues


def validate_table(
    table: str,
    suite_path: Path,
    fix: bool = False,
) -> Dict:
    """
    Validate all rules in a table's suite.

    Args:
        table: Table name
        suite_path: Path to suite JSON file
        fix: If True, remove invalid rules and save fixed suite

    Returns:
        Validation result dictionary
    """
    # Load suite
    suite, load_error = load_suite(suite_path)
    if load_error:
        return {
            "table": table,
            "suite_path": str(suite_path),
            "success": False,
            "error": load_error,
            "total_rules": 0,
            "valid_rules": 0,
            "invalid_rules": 0,
            "issues": [],
        }

    # Validate structure
    struct_issues = validate_suite_structure(suite)
    if struct_issues:
        return {
            "table": table,
            "suite_path": str(suite_path),
            "success": False,
            "error": "Invalid suite structure: " + "; ".join(struct_issues),
            "total_rules": 0,
            "valid_rules": 0,
            "invalid_rules": 0,
            "issues": [],
        }

    # Validate each rule
    all_issues = []
    expectations = suite.get("expectations", [])
    valid_indices = []

    for idx, rule in enumerate(expectations):
        rule_issues = validate_rule(rule, idx)
        all_issues.extend(rule_issues)

        # Check if rule has any error-level issues
        has_error = any(i.get("severity") == "error" for i in rule_issues)
        if not has_error:
            valid_indices.append(idx)

    # Check for duplicates
    duplicate_issues = check_duplicates(suite)
    all_issues.extend(duplicate_issues)

    # Count errors and warnings
    error_count = sum(1 for i in all_issues if i.get("severity") == "error")
    warning_count = sum(1 for i in all_issues if i.get("severity") == "warning")

    result = {
        "table": table,
        "suite_path": str(suite_path),
        "success": error_count == 0,
        "total_rules": len(expectations),
        "valid_rules": len(valid_indices),
        "invalid_rules": len(expectations) - len(valid_indices),
        "error_count": error_count,
        "warning_count": warning_count,
        "issues": all_issues,
    }

    # Auto-fix if requested
    if fix and error_count > 0:
        fixed_suite = suite.copy()
        fixed_suite["expectations"] = [expectations[i] for i in valid_indices]

        with open(suite_path, "w") as f:
            json.dump(fixed_suite, f, indent=2)

        result["fixed"] = True
        result["removed_rules"] = len(expectations) - len(valid_indices)
        result["remaining_rules"] = len(valid_indices)

    return result


def validate_all(
    table_pattern: Optional[str] = None,
    fix: bool = False,
    verbose: bool = False,
) -> Dict:
    """
    Validate all rules across all tables.

    Args:
        table_pattern: Optional pattern to filter tables
        fix: If True, auto-fix issues by removing invalid rules
        verbose: If True, include all issues in output

    Returns:
        Aggregated validation result
    """
    suite_files = find_suite_files(table_pattern)

    if not suite_files:
        return {
            "success": True,
            "message": "No rules found. Use upsert_rules.py to add rules.",
            "tables_checked": 0,
            "total_rules": 0,
        }

    results = []
    total_rules = 0
    total_valid = 0
    total_invalid = 0
    total_errors = 0
    total_warnings = 0
    all_issues = []

    for table, suite_path in suite_files:
        result = validate_table(table, suite_path, fix=fix)
        results.append(result)

        total_rules += result["total_rules"]
        total_valid += result["valid_rules"]
        total_invalid += result["invalid_rules"]
        total_errors += result.get("error_count", 0)
        total_warnings += result.get("warning_count", 0)

        if verbose or not result["success"]:
            all_issues.extend([
                {**issue, "table": table}
                for issue in result.get("issues", [])
            ])

    return {
        "success": total_errors == 0,
        "tables_checked": len(suite_files),
        "total_rules": total_rules,
        "valid_rules": total_valid,
        "invalid_rules": total_invalid,
        "error_count": total_errors,
        "warning_count": total_warnings,
        **({
            "issues": all_issues,
            "details": results,
        } if verbose else {
            "tables_with_errors": [r["table"] for r in results if not r["success"]],
        }),
        **({"fixed": True, "message": f"Fixed {total_invalid} invalid rules"} if fix else {}),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Validate data quality rules in GE Suite files"
    )
    parser.add_argument(
        "--table",
        help="Table name or pattern (supports * and ? wildcards)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Validate all tables",
    )
    parser.add_argument(
        "--fix",
        action="store_true",
        help="Auto-fix by removing invalid rules",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed output including all issues",
    )

    args = parser.parse_args()

    # Validate project
    if not Path(".amandax").exists():
        print(json.dumps({
            "success": False,
            "error": "Not a valid AmandaX project (.amandax not found)",
        }))
        sys.exit(1)

    # Determine pattern
    if args.all:
        table_pattern = None
    elif args.table:
        table_pattern = args.table
    else:
        # Default: validate all
        table_pattern = None

    # Run validation
    result = validate_all(
        table_pattern=table_pattern,
        fix=args.fix,
        verbose=args.verbose,
    )

    print(json.dumps(result, indent=2))

    # Exit with error code if any errors found
    if not result.get("success"):
        sys.exit(1)


if __name__ == "__main__":
    main()
