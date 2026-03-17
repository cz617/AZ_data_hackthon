#!/usr/bin/env python3
"""
Execute GE Suite against database.

Usage:
    # Run all rules for a table
    python run_suite.py --table orders

    # Run with type pattern filter
    python run_suite.py --table orders --type "*null*"

    # Run with severity filter
    python run_suite.py --table orders --severity error

    # Generate HTML report
    python run_suite.py --table orders --report html

    # CI mode (exit 1 on failure)
    python run_suite.py --table orders --fail-on-error
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from ge_validation import match_pattern


def is_safe_identifier(name: str) -> bool:
    """
    Validate that identifier contains only safe characters.

    Args:
        name: Table or column name to validate

    Returns:
        True if identifier is safe, False otherwise
    """
    if not name:
        return False
    # Allow alphanumeric, underscore, and dot (for schema.table)
    # Must start with letter or underscore
    return bool(re.match(r'^[a-zA-Z_][a-zA-Z0-9_.]*$', name))

GE_DIR = Path("artifacts/great_expectations")
EXPORTS_DIR = Path("artifacts/data_quality/exports")


def load_settings() -> dict:
    """Load settings from .amandax/settings.json."""
    settings_paths = [
        Path(".amandax/settings.json"),
        Path.home() / ".amandax/settings.json",
    ]
    for path in settings_paths:
        if path.exists():
            with open(path) as f:
                return json.load(f)
    return {}


def get_db_connection(settings: dict):
    """Get database connection from settings."""
    db_config = settings.get("database", {})

    # Use default datasource or first available
    datasource = db_config.get("default") or list(db_config.values())[0] if db_config else None

    if not datasource:
        raise ValueError("No database configured in settings")

    db_type = datasource.get("type", "postgresql")
    conn_string = datasource.get("connectionString")

    if not conn_string:
        # Build from components
        host = datasource.get("host", "localhost")
        port = datasource.get("port", 5432)
        database = datasource.get("database")
        username = datasource.get("username")
        password = datasource.get("password")

        if db_type == "postgresql":
            conn_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        elif db_type == "mysql":
            conn_string = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
        elif db_type == "sqlite":
            conn_string = f"sqlite:///{database}"

    return db_type, conn_string


def filter_expectations(
    expectations: List[Dict],
    type_pattern: Optional[str] = None,
    severity: Optional[str] = None,
) -> List[Dict]:
    """
    Filter expectations by type pattern and severity.

    Args:
        expectations: List of expectation dicts
        type_pattern: Pattern for expectation_type (supports * and ? wildcards)
        severity: Exact severity match (error, warning, info)

    Returns:
        Filtered list of expectations
    """
    filtered = []

    for exp in expectations:
        exp_type = exp["expectation_type"]
        meta = exp.get("meta", {})
        exp_severity = meta.get("severity", "error")

        # Apply type pattern filter
        if type_pattern and not match_pattern(exp_type, type_pattern):
            continue

        # Apply severity filter (exact match)
        if severity and exp_severity != severity:
            continue

        filtered.append(exp)

    return filtered


def run_validation(db_type: str, conn_string: str, table: str, expectations: List[dict]) -> dict:
    """Run validation against database."""

    # Validate table name
    if not is_safe_identifier(table):
        return {"error": f"Invalid table name: {table}"}

    try:
        import sqlalchemy as sa
    except ImportError:
        return {
            "error": "sqlalchemy required for validation. pip install sqlalchemy"
        }

    engine = sa.create_engine(conn_string)

    failures = []
    successful = 0
    unsuccessful = 0

    # Get column info from database
    try:
        with engine.connect() as conn:
            # Check table exists
            result = conn.execute(
                sa.text(f"SELECT COUNT(*) FROM {table}")
            )
            row_count = result.scalar()
    except Exception as e:
        return {"error": f"Failed to connect to table {table}: {e}"}

    # Run each expectation
    for exp in expectations:
        exp_type = exp["expectation_type"]
        kwargs = exp.get("kwargs", {})
        meta = exp.get("meta", {})
        column = kwargs.get("column")

        # Validate column name if present
        if column and not is_safe_identifier(column):
            unsuccessful += 1
            failures.append({
                "rule_id": meta.get("rule_id", "unknown"),
                "expectation_type": exp_type,
                "column": column,
                "severity": meta.get("severity", "error"),
                "error": f"Invalid column name: {column}",
            })
            continue

        try:
            with engine.connect() as conn:
                if exp_type == "expect_column_values_to_be_unique":
                    result = conn.execute(
                        sa.text(f"""
                            SELECT COUNT(*) - COUNT(DISTINCT {column})
                            FROM {table}
                        """)
                    )
                    dup_count = result.scalar() or 0
                    success = dup_count == 0
                    if not success:
                        failures.append({
                            "rule_id": meta.get("rule_id", "unknown"),
                            "expectation_type": exp_type,
                            "column": column,
                            "severity": meta.get("severity", "error"),
                            "unexpected_count": dup_count,
                            "unexpected_percent": (dup_count / row_count * 100) if row_count else 0,
                        })

                elif exp_type == "expect_column_values_to_not_be_null":
                    result = conn.execute(
                        sa.text(f"""
                            SELECT COUNT(*) - COUNT({column})
                            FROM {table}
                        """)
                    )
                    null_count = result.scalar() or 0
                    success = null_count == 0
                    if not success:
                        failures.append({
                            "rule_id": meta.get("rule_id", "unknown"),
                            "expectation_type": exp_type,
                            "column": column,
                            "severity": meta.get("severity", "error"),
                            "unexpected_count": null_count,
                            "unexpected_percent": (null_count / row_count * 100) if row_count else 0,
                        })

                elif exp_type == "expect_column_values_to_be_in_set":
                    value_set = kwargs.get("value_set", [])
                    if value_set:
                        # Use parameterized query to prevent SQL injection
                        params = {f"val_{i}": v for i, v in enumerate(value_set)}
                        placeholders = ", ".join([f":val_{i}" for i in range(len(value_set))])
                        result = conn.execute(
                            sa.text(f"""
                                SELECT COUNT(*)
                                FROM {table}
                                WHERE {column} NOT IN ({placeholders})
                            """),
                            params
                        )
                        invalid_count = result.scalar() or 0
                        success = invalid_count == 0
                        if not success:
                            # Get sample of unexpected values (reuse same params)
                            sample_result = conn.execute(
                                sa.text(f"""
                                    SELECT DISTINCT {column}
                                    FROM {table}
                                    WHERE {column} NOT IN ({placeholders})
                                    LIMIT 5
                                """),
                                params
                            )
                            unexpected_list = [row[0] for row in sample_result]
                            failures.append({
                                "rule_id": meta.get("rule_id", "unknown"),
                                "expectation_type": exp_type,
                                "column": column,
                                "severity": meta.get("severity", "error"),
                                "unexpected_count": invalid_count,
                                "unexpected_percent": (invalid_count / row_count * 100) if row_count else 0,
                                "partial_unexpected_list": unexpected_list,
                            })
                    else:
                        success = True

                elif exp_type == "expect_column_values_to_be_between":
                    min_val = kwargs.get("min_value")
                    max_val = kwargs.get("max_value")
                    conditions = []
                    params = {}

                    if min_val is not None:
                        params["min_val"] = min_val
                        conditions.append(f"{column} < :min_val")
                    if max_val is not None:
                        params["max_val"] = max_val
                        conditions.append(f"{column} > :max_val")

                    if conditions:
                        where_clause = " OR ".join(conditions)
                        result = conn.execute(
                            sa.text(f"""
                                SELECT COUNT(*)
                                FROM {table}
                                WHERE {where_clause}
                            """),
                            params
                        )
                        out_of_range = result.scalar() or 0
                        success = out_of_range == 0
                        if not success:
                            failures.append({
                                "rule_id": meta.get("rule_id", "unknown"),
                                "expectation_type": exp_type,
                                "column": column,
                                "severity": meta.get("severity", "error"),
                                "unexpected_count": out_of_range,
                                "unexpected_percent": (out_of_range / row_count * 100) if row_count else 0,
                            })
                    else:
                        success = True

                else:
                    # Unknown expectation type - skip
                    success = True

                if success:
                    successful += 1
                else:
                    unsuccessful += 1

        except Exception as e:
            unsuccessful += 1
            failures.append({
                "rule_id": meta.get("rule_id", "unknown"),
                "expectation_type": exp_type,
                "column": column,
                "severity": meta.get("severity", "error"),
                "error": str(e),
            })

    total = successful + unsuccessful
    success_percent = (successful / total * 100) if total else 100

    return {
        "success": unsuccessful == 0,
        "statistics": {
            "evaluated_expectations": total,
            "successful_expectations": successful,
            "unsuccessful_expectations": unsuccessful,
            "success_percent": round(success_percent, 1),
        },
        "failures": failures,
    }


def run_suite(
    table: str,
    type_pattern: Optional[str] = None,
    severity: Optional[str] = None,
    report: Optional[str] = None,
    output: Optional[str] = None,
    fail_on_error: bool = False,
) -> dict:
    """
    Execute GE Suite with optional filtering.

    Args:
        table: Table name (required)
        type_pattern: Filter by expectation_type pattern (supports * and ? wildcards)
        severity: Filter by severity (exact match: error, warning, info)
        report: Generate report file (html, json)
        output: Report output path
        fail_on_error: Exit code 1 on validation failure

    Returns:
        Result dict with validation statistics and failures
    """

    if not table:
        raise ValueError("Table name required")

    # Load suite
    suite_path = GE_DIR / "expectations" / f"{table}_suite.json"
    if not suite_path.exists():
        return {"error": f"Suite not found: {table}"}

    with open(suite_path) as f:
        suite = json.load(f)

    # Apply filters
    all_expectations = suite.get("expectations", [])
    filtered_expectations = filter_expectations(
        all_expectations,
        type_pattern=type_pattern,
        severity=severity,
    )

    if not filtered_expectations:
        return {
            "success": True,
            "table": table,
            "run_time": datetime.now().isoformat(),
            "filters": {
                "type": type_pattern,
                "severity": severity,
            },
            "statistics": {
                "evaluated_expectations": 0,
                "successful_expectations": 0,
                "unsuccessful_expectations": 0,
                "success_percent": 100.0,
            },
            "failures": [],
            "warning": "No expectations matched the filters",
        }

    # Get database connection
    settings = load_settings()
    try:
        db_type, conn_string = get_db_connection(settings)
    except ValueError as e:
        return {"error": str(e)}

    # Run validation
    validation_result = run_validation(db_type, conn_string, table, filtered_expectations)

    if "error" in validation_result:
        return validation_result

    # Build result
    result = {
        "success": validation_result["success"],
        "table": table,
        "run_time": datetime.now().isoformat(),
        "filters": {
            "type": type_pattern,
            "severity": severity,
        },
        "statistics": validation_result["statistics"],
        "failures": validation_result["failures"],
    }

    # Generate report
    if report:
        output_path = output or EXPORTS_DIR / f"{table}_report.{report}"
        generate_report(result, report, output_path)
        result["report_path"] = str(output_path)

    # CI mode
    if fail_on_error and not result["success"]:
        print(json.dumps(result, indent=2))
        sys.exit(1)

    return result


def generate_report(result: dict, format: str, output_path: str):
    """Generate report file."""

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    if format == "html":
        # Build failures table rows
        failure_rows = ""
        for f in result.get("failures", []):
            failure_rows += f"""
        <tr>
            <td>{f.get('rule_id', 'unknown')}</td>
            <td><code>{f['expectation_type']}</code></td>
            <td>{f.get('column', '-')}</td>
            <td>{f.get('severity', 'error')}</td>
            <td>{f.get('unexpected_count', 'N/A')}</td>
            <td>{f.get('error', '')}</td>
        </tr>"""

        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>DQ Report: {result['table']}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif; margin: 40px; background: #f5f5f5; }}
        .container {{ max-width: 1000px; margin: 0 auto; background: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        h1 {{ color: #333; border-bottom: 2px solid #4CAF50; padding-bottom: 10px; }}
        .success {{ color: #4CAF50; font-weight: bold; }}
        .failure {{ color: #f44336; font-weight: bold; }}
        .meta {{ color: #666; font-size: 14px; margin-bottom: 20px; }}
        .filters {{ background: #f9f9f9; padding: 15px; border-radius: 4px; margin-bottom: 20px; }}
        .filters code {{ background: #e0e0e0; padding: 2px 6px; border-radius: 3px; }}
        .stats {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin: 20px 0; }}
        .stat-box {{ background: #f5f5f5; padding: 15px; border-radius: 4px; text-align: center; }}
        .stat-value {{ font-size: 24px; font-weight: bold; color: #333; }}
        .stat-label {{ font-size: 12px; color: #666; text-transform: uppercase; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
        th {{ background-color: #4CAF50; color: white; }}
        tr:nth-child(even) {{ background-color: #f9f9f9; }}
        tr:hover {{ background-color: #f1f1f1; }}
        .severity-error {{ color: #f44336; font-weight: bold; }}
        .severity-warning {{ color: #ff9800; font-weight: bold; }}
        .severity-info {{ color: #2196F3; font-weight: bold; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Data Quality Report: {result['table']}</h1>
        <div class="meta">
            <strong>Run Time:</strong> {result['run_time']}<br>
            <strong>Status:</strong> <span class="{'success' if result['success'] else 'failure'}">{'PASSED' if result['success'] else 'FAILED'}</span>
        </div>
"""

        # Add filters section if filters applied
        if result.get("filters", {}).get("type") or result.get("filters", {}).get("severity"):
            html += f"""
        <div class="filters">
            <strong>Filters Applied:</strong><br>
            {'Type Pattern: <code>' + result['filters']['type'] + '</code><br>' if result['filters'].get('type') else ''}
            {'Severity: <code>' + result['filters']['severity'] + '</code>' if result['filters'].get('severity') else ''}
        </div>
"""

        html += f"""
        <div class="stats">
            <div class="stat-box">
                <div class="stat-value">{result['statistics']['evaluated_expectations']}</div>
                <div class="stat-label">Total</div>
            </div>
            <div class="stat-box">
                <div class="stat-value" style="color: #4CAF50;">{result['statistics']['successful_expectations']}</div>
                <div class="stat-label">Passed</div>
            </div>
            <div class="stat-box">
                <div class="stat-value" style="color: #f44336;">{result['statistics']['unsuccessful_expectations']}</div>
                <div class="stat-label">Failed</div>
            </div>
            <div class="stat-box">
                <div class="stat-value">{result['statistics']['success_percent']:.1f}%</div>
                <div class="stat-label">Success Rate</div>
            </div>
        </div>
"""

        if result.get("failures"):
            html += f"""
        <h2>Failures ({len(result['failures'])})</h2>
        <table>
            <tr>
                <th>Rule ID</th>
                <th>Type</th>
                <th>Column</th>
                <th>Severity</th>
                <th>Unexpected</th>
                <th>Details</th>
            </tr>
            {failure_rows}
        </table>
"""

        html += """
    </div>
</body>
</html>"""
        Path(output_path).write_text(html, encoding="utf-8")

    elif format == "json":
        with open(output_path, "w") as f:
            json.dump(result, f, indent=2)


def main():
    parser = argparse.ArgumentParser(
        description="Run data quality suite with optional filtering",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run all rules for a table
  python run_suite.py --table orders

  # Run with type pattern filter (supports * and ? wildcards)
  python run_suite.py --table orders --type "*null*"

  # Run with severity filter (exact match)
  python run_suite.py --table orders --severity error

  # Combine filters
  python run_suite.py --table orders --type "*unique*" --severity error

  # Generate HTML report
  python run_suite.py --table orders --report html

  # Generate JSON report to custom path
  python run_suite.py --table orders --report json --output /path/to/report.json

  # CI mode (exit 1 on failure)
  python run_suite.py --table orders --fail-on-error
        """
    )
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument(
        "--type",
        dest="type_pattern",
        help="Filter by expectation_type pattern (supports * and ? wildcards)",
    )
    parser.add_argument(
        "--severity",
        choices=["error", "warning", "info"],
        help="Filter by severity (exact match)",
    )
    parser.add_argument(
        "--report",
        choices=["html", "json"],
        help="Generate report file",
    )
    parser.add_argument(
        "--output",
        help="Report output path (default: artifacts/data_quality/exports/{table}_report.{format})",
    )
    parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="Exit with code 1 on validation failure",
    )

    args = parser.parse_args()

    # Validate project
    if not Path(".amandax").exists():
        print(json.dumps({
            "success": False,
            "error": "Not a valid AmandaX project (.amandax not found)",
        }, indent=2))
        sys.exit(1)

    result = run_suite(
        table=args.table,
        type_pattern=args.type_pattern,
        severity=args.severity,
        report=args.report,
        output=args.output,
        fail_on_error=args.fail_on_error,
    )

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
