#!/usr/bin/env python
"""Insert sample data into SQLite database."""
import sqlite3
import sys
from pathlib import Path

import pandas as pd

# SQLite database path
DB_PATH = "data/detection.db"

def get_connection():
    """Create and return a SQLite connection."""
    return sqlite3.connect(DB_PATH)


def read_csv_file(file_path: str) -> pd.DataFrame:
    """Read CSV file and return DataFrame."""
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None


def insert_dataframe_to_sqlite(
    df: pd.DataFrame,
    table_name: str,
    truncate_first: bool = False,
) -> bool:
    """
    Insert DataFrame into SQLite table.

    Args:
        df: DataFrame to insert
        table_name: Target table name
        truncate_first: Whether to truncate table before insertion

    Returns:
        True if successful, False otherwise
    """
    if df is None or df.empty:
        print(f"Skipping {table_name}: No data")
        return False

    conn = get_connection()

    try:
        # Truncate table if requested
        if truncate_first:
            print(f"Dropping table {table_name}...")
            cursor = conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.commit()

        # Insert data using pandas to_sql
        print(f"Inserting {len(df)} rows into {table_name}...")
        df.to_sql(table_name, conn, if_exists='append', index=False)
        conn.commit()
        print(f"Successfully inserted {len(df)} rows into {table_name}")
        return True

    except Exception as e:
        print(f"Error inserting data into {table_name}: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        conn.close()


def main():
    """Main function to insert all sample data."""
    sample_data_dir = Path(__file__).parent.parent / "000_客户提供的资料/02_ SAMPLE DATA"

    print("=" * 80)
    print("SAMPLE DATA INSERTION SCRIPT (SQLite)")
    print("=" * 80)
    print(f"\nSample data directory: {sample_data_dir}")

    # Define table files mapping
    table_files = {
        "DIM_ACCOUNT": "DIM_ACCOUNT_sample.csv",
        "DIM_SCENARIO": "DIM_SCENARIO_sample.csv",
        "FACT_PNL_BASE_BRAND": "FACT_PNL_BASE_BRAND_sample.csv",
    }

    results = {}

    for table_name, file_name in table_files.items():
        file_path = sample_data_dir / file_name

        if not file_path.exists():
            print(f"\n⚠ Skipping {table_name}: File not found ({file_name})")
            results[table_name] = "skipped"
            continue

        print(f"\n{'=' * 80}")
        print(f"Processing: {table_name}")
        print(f"File: {file_name}")
        print(f"{'=' * 80}")

        df = read_csv_file(str(file_path))

        if df is not None:
            success = insert_dataframe_to_sqlite(
                df,
                table_name,
                truncate_first=True,
            )
            results[table_name] = "success" if success else "failed"
        else:
            results[table_name] = "failed"

    # Summary
    print(f"\n{'=' * 80}")
    print("INSERTION SUMMARY")
    print(f"{'=' * 80}")

    success_count = sum(1 for v in results.values() if v == "success")
    failed_count = sum(1 for v in results.values() if v == "failed")
    skipped_count = sum(1 for v in results.values() if v == "skipped")

    print(f"\nTotal tables: {len(table_files)}")
    print(f"Successful: {success_count}")
    print(f"Failed: {failed_count}")
    print(f"Skipped: {skipped_count}")

    if failed_count > 0:
        print("\nFailed tables:")
        for table, result in results.items():
            if result == "failed":
                print(f"  - {table}")

    print(f"\n{'=' * 80}")
    print("Sample data insertion completed!")
    print(f"{'=' * 80}")


if __name__ == "__main__":
    main()
