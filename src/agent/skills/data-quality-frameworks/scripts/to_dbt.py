#!/usr/bin/env python3
"""
Export GE Suite to dbt tests.

Usage:
    python to_dbt.py --table orders --output models/staging/_orders__tests.yml
"""

import argparse
import json
import sys
from pathlib import Path

GE_DIR = Path("artifacts/great_expectations")

GE_TO_DBT = {
    "expect_column_values_to_be_unique": "unique",
    "expect_column_values_to_not_be_null": "not_null",
    "expect_column_values_to_be_in_set": "accepted_values",
    "expect_column_values_to_be_between": "expression_is_true"
}


def convert_exp_to_dbt(exp: dict):
    """Convert single expectation to dbt test."""

    exp_type = exp["expectation_type"]
    kwargs = exp["kwargs"]

    if exp_type == "expect_column_values_to_be_unique":
        return "unique"

    elif exp_type == "expect_column_values_to_not_be_null":
        return "not_null"

    elif exp_type == "expect_column_values_to_be_in_set":
        return {
            "accepted_values": {
                "values": kwargs["value_set"]
            }
        }

    elif exp_type == "expect_column_values_to_be_between":
        column = kwargs["column"]
        min_val = kwargs.get("min_value")
        max_val = kwargs.get("max_value")

        if min_val is not None and max_val is not None:
            expr = f"{column} >= {min_val} and {column} <= {max_val}"
        elif min_val is not None:
            expr = f"{column} >= {min_val}"
        elif max_val is not None:
            expr = f"{column} <= {max_val}"
        else:
            return None

        return {
            "dbt_utils.expression_is_true": {
                "expression": expr
            }
        }

    return None


def to_dbt(table: str, output: str = None) -> str:
    """Export GE Suite to dbt tests."""

    suite_path = GE_DIR / "expectations" / f"{table}_suite.json"

    if not suite_path.exists():
        raise FileNotFoundError(f"Suite not found: {table}")

    with open(suite_path) as f:
        suite = json.load(f)

    # Group by column
    columns = {}

    for exp in suite.get("expectations", []):
        dbt_test = convert_exp_to_dbt(exp)
        if not dbt_test:
            continue

        column = exp["kwargs"].get("column", "unknown")
        if column not in columns:
            columns[column] = []
        columns[column].append(dbt_test)

    # Build dbt schema
    dbt_schema = {
        "version": 2,
        "models": [{
            "name": table,
            "columns": [
                {"name": col, "tests": tests}
                for col, tests in columns.items()
            ]
        }]
    }

    output_path = output or f"models/staging/_{table}__tests.yml"
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        import yaml
        yaml.dump(dbt_schema, f, default_flow_style=False, sort_keys=False)

    return str(output_path)


def main():
    parser = argparse.ArgumentParser(description="Export to dbt")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--output", help="Output path")

    args = parser.parse_args()

    # Validate project
    if not Path(".amandax").exists():
        print("Error: Not a valid AmandaX project (.amandax not found)")
        sys.exit(1)

    try:
        result = to_dbt(args.table, args.output)
        print(f"dbt tests exported to: {result}")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
