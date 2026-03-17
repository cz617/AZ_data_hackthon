#!/usr/bin/env python3
"""
Export GE Suite to executable SQL.

Usage:
    python to_sql.py --table orders --output artifacts/data_quality/exports/orders.sql
"""

import argparse
import json
import sys
from pathlib import Path

GE_DIR = Path("artifacts/great_expectations")
EXPORTS_DIR = Path("artifacts/data_quality/exports")

SQL_TEMPLATES = {
    "expect_column_values_to_be_unique": """
-- Rule: {rule_id} | {exp_type}
SELECT
  '{rule_id}' as rule_id,
  '{column}' as column_name,
  'unique' as check_type,
  COUNT(*) - COUNT(DISTINCT {column}) as violation_count,
  CASE WHEN COUNT(*) = COUNT(DISTINCT {column}) THEN 'PASS' ELSE 'FAIL' END as result
FROM {table}""",

    "expect_column_values_to_be_in_set": """
-- Rule: {rule_id} | {exp_type}
SELECT
  '{rule_id}' as rule_id,
  '{column}' as column_name,
  'enum' as check_type,
  SUM(CASE WHEN {column} NOT IN ({values}) THEN 1 ELSE 0 END) as violation_count,
  CASE WHEN SUM(CASE WHEN {column} NOT IN ({values}) THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END as result
FROM {table}
WHERE {column} IS NOT NULL""",

    "expect_column_values_to_not_be_null": """
-- Rule: {rule_id} | {exp_type}
SELECT
  '{rule_id}' as rule_id,
  '{column}' as column_name,
  'not_null' as check_type,
  SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) as violation_count,
  CASE WHEN SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END as result
FROM {table}""",

    "expect_column_values_to_be_between": """
-- Rule: {rule_id} | {exp_type}
SELECT
  '{rule_id}' as rule_id,
  '{column}' as column_name,
  'range' as check_type,
  SUM(CASE WHEN {condition} THEN 1 ELSE 0 END) as violation_count,
  CASE WHEN SUM(CASE WHEN {condition} THEN 1 ELSE 0 END) = 0 THEN 'PASS' ELSE 'FAIL' END as result
FROM {table}
WHERE {column} IS NOT NULL"""
}


def to_sql(table: str, output: str = None) -> str:
    """Export GE Suite to SQL."""

    suite_path = GE_DIR / "expectations" / f"{table}_suite.json"

    if not suite_path.exists():
        raise FileNotFoundError(f"Suite not found: {table}")

    with open(suite_path) as f:
        suite = json.load(f)

    sqls = []
    for exp in suite.get("expectations", []):
        template = SQL_TEMPLATES.get(exp["expectation_type"])
        if not template:
            continue

        kwargs = exp["kwargs"]
        exp_type = exp["expectation_type"]

        # Build condition for range check
        condition = ""
        if exp_type == "expect_column_values_to_be_between":
            min_val = kwargs.get("min_value")
            max_val = kwargs.get("max_value")
            conditions = []
            if min_val is not None:
                conditions.append(f"{kwargs['column']} < {min_val}")
            if max_val is not None:
                conditions.append(f"{kwargs['column']} > {max_val}")
            condition = " OR ".join(conditions) if conditions else "FALSE"

        sql = template.format(
            rule_id=exp.get("meta", {}).get("rule_id", "unknown"),
            exp_type=exp_type,
            table=table,
            column=kwargs.get("column"),
            values=", ".join(f"'{v}'" for v in kwargs.get("value_set", [])),
            condition=condition
        )
        sqls.append(sql.strip())

    full_sql = f"-- Data Quality Checks for {table}\n-- Generated from: {suite['expectation_suite_name']}\n-- Total checks: {len(sqls)}\n\n"
    full_sql += ";\n\n".join(sqls) + ";"

    output_path = output or EXPORTS_DIR / f"{table}_checks.sql"
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(full_sql, encoding="utf-8")

    return str(output_path)


def main():
    parser = argparse.ArgumentParser(description="Export to SQL")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--output", help="Output path")

    args = parser.parse_args()

    # Validate project
    if not Path(".amandax").exists():
        print("Error: Not a valid AmandaX project (.amandax not found)")
        sys.exit(1)

    try:
        result = to_sql(args.table, args.output)
        print(f"SQL exported to: {result}")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
