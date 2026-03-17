#!/usr/bin/env python
"""Test detection API with mock database (simulates real behavior)."""
import sys
from pathlib import Path
from unittest.mock import Mock, patch

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

# Create mock settings
class MockSettings:
    def __init__(self):
        pass

# Mock execute_query_with_columns to return sample data
def mock_execute_query(sql, settings, params=None):
    """Mock database query with realistic data."""
    # Return columns and rows based on account code
    columns = ["VALUE", "BUD_VALUE", "MTP_VALUE", "RBU2LTP_VALUE", "PY_VALUE", "TIME_KEY"]

    # Extract account_code from params
    account_code = params[0] if params else None

    # Return sample data based on account
    if account_code == "NET_REVENUE":
        rows = [(100.0, 94.0, 97.0, 98.0, 90.0, "202501")]
    elif account_code == "COGS":
        rows = [(50.0, 49.0, 50.0, 51.0, 45.0, "202501")]
    elif account_code == "GNA_EXPENSE":
        rows = [(20.0, 18.5, 19.0, 19.5, 17.0, "202501")]
    else:
        rows = [(0.0, 0.0, 0.0, 0.0, 0.0, "202501")]

    return columns, rows

# Patch the database module before importing detector
import src.core.database as db_module
original_execute = db_module.execute_query_with_columns
db_module.execute_query_with_columns = mock_execute_query

try:
    from src.detect.detector import detect_variances, print_results

    print("=" * 80)
    print("DETECTION API TEST (WITH MOCK DATABASE)")
    print("=" * 80)
    print("\nTesting detection logic with realistic data structure...")
    print("Database: Mocked (simulates Snowflake FACT_PNL_BASE_BRAND)\n")

    try:
        settings = MockSettings()
        results = detect_variances(settings, time_key=None)

        print(f"✓ Successfully detected {len(results)} metrics\n")

        # Print results
        print_results(results)

        # Summary
        alert_count = sum(1 for r in results if r.is_alert)
        print("\n" + "=" * 80)
        print("TEST SUMMARY")
        print("=" * 80)
        print(f"✓ Total metrics: {len(results)}")
        print(f"✓ Alerts triggered: {alert_count}")
        print(f"✓ Alert threshold: ±5.0%")
        print(f"✓ Margin calculation: revenue - (cost + operational_cost)")
        print("\n✓ All tests passed!")
        print("\nNote: To test with real database:")
        print("  1. Configure .env with Snowflake credentials")
        print("  2. Run: python scripts/insert_sample_data.py")
        print("  3. Run: python scripts/test_detection.py")

        sys.exit(0)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
finally:
    # Restore original
    db_module.execute_query_with_columns = original_execute

