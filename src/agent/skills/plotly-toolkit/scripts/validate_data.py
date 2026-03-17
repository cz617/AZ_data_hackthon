#!/usr/bin/env python3
"""
plotly-toolkit/scripts/validate_data.py
=======================================

Validate data files and chart parameters before Plotly chart generation.

Ensures:
1. Data file exists and is readable
2. Data is not empty
3. Required columns exist
4. Data types are compatible with chart type
5. Chart type is appropriate for data characteristics
6. Output path is writable

Usage:
    python validate_data.py --data results.csv --chart-type bar
    python validate_data.py --data results.csv --chart-type bar --x-column category --y-column revenue
    python validate_data.py --data results.csv --auto  # Auto-detect chart type
    python validate_data.py --data results.csv --output charts/chart.html  # Also validate output path

Exit codes:
    0 = valid
    1 = validation errors
    2 = file not found or critical error
"""

import argparse
import sys
from pathlib import Path
from typing import Any

import pandas as pd


# =============================================================================
# Constants
# =============================================================================

MAX_ROWS_WARNING = 10000
MAX_ROWS_SAMPLING = 100000
VALID_CHART_TYPES = ["bar", "line", "scatter", "pie", "histogram", "box", "area"]

# Common date column name patterns for datetime detection
DATE_COLUMN_PATTERNS = [
    "date", "time", "datetime", "timestamp", "created_at", "updated_at",
    "date_time", "created", "modified", "timestamp", "ts"
]


# =============================================================================
# Validation Results
# =============================================================================

class ValidationResult:
    """Container for validation results."""

    def __init__(self):
        self.errors: list[str] = []
        self.warnings: list[str] = []
        self.suggestions: list[str] = []
        self.data_info: dict[str, Any] = {}

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    def add_error(self, msg: str) -> None:
        self.errors.append(msg)

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def add_suggestion(self, msg: str) -> None:
        self.suggestions.append(msg)


# =============================================================================
# Validators
# =============================================================================

def validate_file_exists(filepath: Path, result: ValidationResult) -> pd.DataFrame | None:
    """Validate that the data file exists and is readable."""
    if not filepath.exists():
        result.add_error(f"Data file not found: {filepath}")
        return None

    if filepath.stat().st_size == 0:
        result.add_error(f"Data file is empty: {filepath}")
        return None

    # Check file size for large files
    file_size_mb = filepath.stat().st_size / (1024 * 1024)
    if file_size_mb > 100:
        result.add_warning(
            f"Large file ({file_size_mb:.1f} MB) - may take time to load"
        )

    # Try to read the file
    try:
        suffix = filepath.suffix.lower()
        if suffix == ".csv":
            # Try to detect dates in CSV
            df = pd.read_csv(filepath, parse_dates=True)
        elif suffix in [".xlsx", ".xls"]:
            df = pd.read_excel(filepath)
        elif suffix == ".json":
            df = pd.read_json(filepath)
        elif suffix == ".parquet":
            df = pd.read_parquet(filepath)
        else:
            # Default to CSV
            df = pd.read_csv(filepath)
        return df
    except pd.errors.EmptyDataError:
        result.add_error(f"Data file has no content: {filepath}")
        return None
    except MemoryError:
        result.add_error(
            f"File too large to load into memory. "
            f"Consider splitting the file or using chunked reading."
        )
        return None
    except Exception as e:
        result.add_error(f"Failed to read data file: {e}")
        return None


def validate_data_not_empty(df: pd.DataFrame, result: ValidationResult) -> None:
    """Validate that the dataframe is not empty."""
    if len(df) == 0:
        result.add_error("DataFrame is empty (0 rows)")
        return

    if len(df.columns) == 0:
        result.add_error("DataFrame has no columns")
        return

    result.data_info["row_count"] = len(df)
    result.data_info["column_count"] = len(df.columns)


def validate_columns_exist(
    df: pd.DataFrame,
    x_column: str | None,
    y_column: str | None,
    result: ValidationResult
) -> None:
    """Validate that specified columns exist in the dataframe."""
    columns = set(df.columns)

    if x_column and x_column not in columns:
        similar = [c for c in columns if x_column.lower() in c.lower()]
        hint = f" Similar columns: {similar}" if similar else ""
        result.add_error(
            f"X column '{x_column}' not found.{hint}"
        )

    if y_column and y_column not in columns:
        similar = [c for c in columns if y_column.lower() in c.lower()]
        hint = f" Similar columns: {similar}" if similar else ""
        result.add_error(
            f"Y column '{y_column}' not found.{hint}"
        )


def try_parse_datetime_columns(df: pd.DataFrame, result: ValidationResult) -> pd.DataFrame:
    """
    Attempt to parse columns that look like dates but weren't auto-detected.
    Returns the DataFrame with parsed datetime columns.
    """
    df = df.copy()

    for col in df.columns:
        # Skip if already datetime
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue

        # Check if column name suggests it's a date
        col_lower = str(col).lower()
        is_date_name = any(pattern in col_lower for pattern in DATE_COLUMN_PATTERNS)

        if is_date_name:
            try:
                # Try to parse as datetime
                parsed = pd.to_datetime(df[col], errors='coerce')
                # If more than 50% parsed successfully, use it
                if parsed.notna().sum() / len(df) > 0.5:
                    df[col] = parsed
                    result.add_suggestion(
                        f"Column '{col}' parsed as datetime"
                    )
            except Exception:
                pass

    return df


def analyze_data_types(df: pd.DataFrame, result: ValidationResult) -> dict[str, list[str]]:
    """Analyze and categorize columns by data type."""
    analysis = {
        "numeric": df.select_dtypes(include=["number"]).columns.tolist(),
        "categorical": df.select_dtypes(include=["object", "category", "bool"]).columns.tolist(),
        "datetime": df.select_dtypes(include=["datetime64", "datetime64[ns]", "period"]).columns.tolist(),
    }

    result.data_info["numeric_columns"] = analysis["numeric"]
    result.data_info["categorical_columns"] = analysis["categorical"]
    result.data_info["datetime_columns"] = analysis["datetime"]

    return analysis


def validate_chart_type_compatibility(
    df: pd.DataFrame,
    chart_type: str,
    x_column: str | None,
    y_column: str | None,
    result: ValidationResult
) -> None:
    """Validate that the chart type is compatible with the data."""
    analysis = analyze_data_types(df, result)

    if chart_type not in VALID_CHART_TYPES:
        result.add_error(
            f"Invalid chart type '{chart_type}'. Valid types: {VALID_CHART_TYPES}"
        )
        return

    # Get actual columns
    x_col = x_column
    y_col = y_column

    # Auto-suggest columns if not specified
    if not x_col and analysis["datetime_columns"]:
        x_col = analysis["datetime_columns"][0]
    elif not x_col and analysis["categorical_columns"]:
        x_col = analysis["categorical_columns"][0]
    elif not x_col and analysis["numeric_columns"]:
        x_col = analysis["numeric_columns"][0]

    if not y_col and analysis["numeric_columns"]:
        y_col = analysis["numeric_columns"][0]

    # Validate chart-specific requirements
    if chart_type == "bar":
        if x_col and y_col:
            x_is_numeric = x_col in analysis["numeric"]
            y_is_numeric = y_col in analysis["numeric"]

            if x_is_numeric and y_is_numeric:
                result.add_warning(
                    f"Bar chart with two numeric columns ({x_col}, {y_col}) - "
                    f"consider using scatter plot instead"
                )

    elif chart_type == "line":
        if x_col:
            x_is_datetime = x_col in analysis["datetime"]
            x_is_numeric = x_col in analysis["numeric"]

            if not x_is_datetime and not x_is_numeric:
                result.add_warning(
                    f"Line chart with non-numeric X axis '{x_col}' - "
                    f"line charts work best with time series or numeric data"
                )

            # Check if data is sorted for line chart
            if x_is_datetime and x_col in df.columns:
                if not df[x_col].is_monotonic_increasing:
                    result.add_suggestion(
                        f"Data is not sorted by '{x_col}' - line chart may look messy"
                    )

    elif chart_type == "scatter":
        if x_col and y_col:
            x_is_numeric = x_col in analysis["numeric"]
            y_is_numeric = y_col in analysis["numeric"]

            if not x_is_numeric or not y_is_numeric:
                result.add_warning(
                    f"Scatter plot requires two numeric columns. "
                    f"'{x_col}' is {'numeric' if x_is_numeric else 'not numeric'}, "
                    f"'{y_col}' is {'numeric' if y_is_numeric else 'not numeric'}"
                )

    elif chart_type == "pie":
        if x_col:
            unique_count = df[x_col].nunique() if x_col in df.columns else 0
            if unique_count > 10:
                result.add_warning(
                    f"Pie chart with {unique_count} categories - "
                    f"consider using bar chart for better readability"
                )
            if unique_count < 2:
                result.add_error(
                    f"Pie chart requires at least 2 categories, found {unique_count}"
                )
            if y_col and y_col in df.columns:
                # Check for negative values
                if df[y_col].min() < 0:
                    result.add_error(
                        f"Pie chart cannot have negative values in '{y_col}'"
                    )

    elif chart_type == "histogram":
        if x_col:
            x_is_numeric = x_col in analysis["numeric"]
            if not x_is_numeric:
                result.add_error(
                    f"Histogram requires a numeric column, '{x_col}' is not numeric"
                )

    elif chart_type == "box":
        if y_col:
            y_is_numeric = y_col in analysis["numeric"]
            if not y_is_numeric:
                result.add_error(
                    f"Box plot requires a numeric Y column, '{y_col}' is not numeric"
                )

    elif chart_type == "area":
        if x_col:
            x_is_datetime = x_col in analysis["datetime"]
            x_is_numeric = x_col in analysis["numeric"]

            if not x_is_datetime and not x_is_numeric:
                result.add_warning(
                    f"Area chart with non-numeric X axis '{x_col}' - "
                    f"area charts work best with time series or numeric data"
                )


def validate_data_quality(df: pd.DataFrame, result: ValidationResult) -> None:
    """Validate data quality and report issues."""
    # Check for null values
    null_counts = df.isnull().sum()
    null_columns = null_counts[null_counts > 0]

    if len(null_columns) > 0:
        for col, count in null_columns.items():
            pct = count / len(df) * 100
            if pct > 50:
                result.add_warning(
                    f"Column '{col}' has {count} null values ({pct:.1f}%)"
                )
            else:
                result.add_suggestion(
                    f"Column '{col}' has {count} null values ({pct:.1f}%)"
                )

    # Check for duplicate rows
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        pct = duplicates / len(df) * 100
        result.add_suggestion(
            f"Found {duplicates} duplicate rows ({pct:.1f}%)"
        )

    # Check for constant columns
    for col in df.columns:
        unique_count = df[col].nunique()
        if unique_count == 1:
            result.add_suggestion(
                f"Column '{col}' has only one unique value - may not be useful for visualization"
            )

    # Check for very large datasets
    if len(df) > MAX_ROWS_SAMPLING:
        result.add_warning(
            f"Very large dataset ({len(df)} rows) - will be sampled to {MAX_ROWS_SAMPLING} rows for visualization"
        )
    elif len(df) > MAX_ROWS_WARNING:
        result.add_warning(
            f"Large dataset ({len(df)} rows) - consider sampling for visualization"
        )


def validate_output_path(output_path: Path | None, result: ValidationResult) -> None:
    """Validate that the output path is writable."""
    if output_path is None:
        return

    # Check parent directory exists
    parent = output_path.parent
    if not parent.exists():
        result.add_suggestion(
            f"Output directory '{parent}' does not exist - will be created"
        )
    elif not parent.is_dir():
        result.add_error(
            f"Output parent path '{parent}' exists but is not a directory"
        )


def suggest_chart_type(df: pd.DataFrame, result: ValidationResult) -> str:
    """Suggest the most appropriate chart type based on data characteristics."""
    analysis = analyze_data_types(df, result)

    numeric_count = len(analysis["numeric"])
    categorical_count = len(analysis["categorical"])
    datetime_count = len(analysis["datetime"])

    # Decision logic
    if datetime_count > 0 and numeric_count > 0:
        result.add_suggestion("Recommended chart type: line (time series)")
        return "line"

    if categorical_count > 0 and numeric_count > 0:
        # Check if it's a part-to-whole scenario
        if len(df) <= 8 and numeric_count == 1:
            result.add_suggestion("Recommended chart type: pie (part-to-whole)")
            return "pie"
        else:
            result.add_suggestion("Recommended chart type: bar (comparison)")
            return "bar"

    if numeric_count >= 2:
        result.add_suggestion("Recommended chart type: scatter (correlation)")
        return "scatter"

    if numeric_count == 1:
        result.add_suggestion("Recommended chart type: histogram (distribution)")
        return "histogram"

    if categorical_count > 0:
        result.add_suggestion("Recommended chart type: bar (count)")
        return "bar"

    result.add_suggestion("Recommended chart type: bar (default)")
    return "bar"


# =============================================================================
# Main Validation Function
# =============================================================================

def validate_data_for_chart(
    data_path: Path,
    chart_type: str | None = None,
    x_column: str | None = None,
    y_column: str | None = None,
    auto_suggest: bool = False,
    output_path: Path | None = None
) -> ValidationResult:
    """
    Validate data file and chart parameters.

    Args:
        data_path: Path to the data file
        chart_type: Chart type (bar, line, scatter, pie, histogram, box, area)
        x_column: X axis column name
        y_column: Y axis column name
        auto_suggest: Whether to auto-suggest chart type
        output_path: Optional output file path to validate

    Returns:
        ValidationResult with errors, warnings, and suggestions
    """
    result = ValidationResult()

    # Validate file exists and is readable
    df = validate_file_exists(data_path, result)
    if df is None:
        return result

    # Validate data is not empty
    validate_data_not_empty(df, result)
    if not result.is_valid:
        return result

    # Try to parse datetime columns
    df = try_parse_datetime_columns(df, result)

    # Validate columns exist
    validate_columns_exist(df, x_column, y_column, result)

    # Auto-suggest chart type if requested or if not specified
    if auto_suggest or not chart_type:
        suggested = suggest_chart_type(df, result)
        if not chart_type:
            chart_type = suggested
            result.data_info["suggested_chart_type"] = suggested

    # Validate chart type compatibility
    if chart_type:
        result.data_info["chart_type"] = chart_type
        validate_chart_type_compatibility(df, chart_type, x_column, y_column, result)

    # Validate data quality
    validate_data_quality(df, result)

    # Validate output path if specified
    validate_output_path(output_path, result)

    # Store data summary
    result.data_info["columns"] = list(df.columns)
    result.data_info["sample_rows"] = len(df)
    result.data_info["dataframe"] = df  # Store for use in generation

    return result


# =============================================================================
# Integration with generate_chart.py
# =============================================================================

def validate_before_generation(
    data_path: Path,
    chart_type: str | None = None,
    x_column: str | None = None,
    y_column: str | None = None,
    output_path: Path | None = None
) -> tuple[bool, pd.DataFrame | None, str]:
    """
    Validate data before chart generation.

    Returns:
        Tuple of (is_valid, dataframe or None, suggested_chart_type)
    """
    result = validate_data_for_chart(
        data_path=data_path,
        chart_type=chart_type,
        x_column=x_column,
        y_column=y_column,
        auto_suggest=True,
        output_path=output_path
    )

    df = result.data_info.get("dataframe")
    suggested_type = result.data_info.get("suggested_chart_type", chart_type or "bar")

    if not result.is_valid:
        print(f"\n❌ Data validation failed:")
        for err in result.errors:
            print(f"   • {err}")
        return False, None, suggested_type

    if result.warnings:
        print(f"\n⚠️  Warnings:")
        for warn in result.warnings:
            print(f"   • {warn}")

    return True, df, suggested_type


# =============================================================================
# CLI
# =============================================================================

def print_result(data_path: Path, result: ValidationResult, quiet: bool = False) -> None:
    """Print validation result."""
    if quiet:
        if result.is_valid:
            print(f"✅ {data_path.name}: valid")
        else:
            print(f"❌ {data_path.name}: {len(result.errors)} error(s)")
        return

    print(f"\n{'=' * 60}")
    print(f"Data File: {data_path}")
    print(f"{'=' * 60}")

    # Print data info
    if result.data_info:
        print("\n📊 Data Summary:")
        if "row_count" in result.data_info:
            print(f"  • Rows: {result.data_info['row_count']}")
        if "column_count" in result.data_info:
            print(f"  • Columns: {result.data_info['column_count']}")
        if "numeric_columns" in result.data_info:
            cols = result.data_info['numeric_columns']
            print(f"  • Numeric columns: {cols[:5]}{'...' if len(cols) > 5 else ''}")
        if "categorical_columns" in result.data_info:
            cols = result.data_info['categorical_columns']
            print(f"  • Categorical columns: {cols[:5]}{'...' if len(cols) > 5 else ''}")
        if "datetime_columns" in result.data_info:
            cols = result.data_info['datetime_columns']
            if cols:
                print(f"  • Datetime columns: {cols}")
        if "chart_type" in result.data_info:
            print(f"  • Chart type: {result.data_info['chart_type']}")
        if "suggested_chart_type" in result.data_info:
            print(f"  • Suggested chart: {result.data_info['suggested_chart_type']}")

    if result.errors:
        print("\n❌ ERRORS:")
        for err in result.errors:
            print(f"  • {err}")

    if result.warnings:
        print("\n⚠️  WARNINGS:")
        for warn in result.warnings:
            print(f"  • {warn}")

    if result.suggestions:
        print("\n💡 SUGGESTIONS:")
        for sug in result.suggestions:
            print(f"  • {sug}")

    if result.is_valid:
        print(f"\n✅ Data is valid for chart generation")

    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate data file and chart parameters for Plotly chart generation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate data for specific chart type
  python validate_data.py --data sales.csv --chart-type bar

  # Validate with column specifications
  python validate_data.py --data sales.csv --chart-type bar --x-column product --y-column revenue

  # Auto-suggest chart type
  python validate_data.py --data sales.csv --auto

  # Also validate output path
  python validate_data.py --data sales.csv --output charts/sales.html

  # Quiet mode for scripting
  python validate_data.py --data sales.csv --quiet
        """
    )

    parser.add_argument("--data", "-d", required=True, help="Path to data file (CSV, JSON, Excel, Parquet)")
    parser.add_argument("--chart-type", "-t", choices=VALID_CHART_TYPES,
                       help="Chart type")
    parser.add_argument("--x-column", "-x", help="X axis column name")
    parser.add_argument("--y-column", "-y", help="Y axis column name")
    parser.add_argument("--auto", "-a", action="store_true", help="Auto-suggest chart type")
    parser.add_argument("--output", "-o", help="Output file path (also validates parent directory)")
    parser.add_argument("--quiet", "-q", action="store_true", help="Minimal output")

    args = parser.parse_args()

    output_path = Path(args.output) if args.output else None

    result = validate_data_for_chart(
        data_path=Path(args.data),
        chart_type=args.chart_type,
        x_column=args.x_column,
        y_column=args.y_column,
        auto_suggest=args.auto,
        output_path=output_path
    )

    print_result(Path(args.data), result, quiet=args.quiet)

    sys.exit(0 if result.is_valid else 1)


if __name__ == "__main__":
    main()