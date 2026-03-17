#!/usr/bin/env python
"""Test detection with mock data (no database required)."""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))
sys.path.insert(0, str(project_root))

# Mock the database functions
class MockSettings:
    """Mock settings for testing."""
    def __init__(self):
        pass

def mock_execute_query_with_columns(sql, settings):
    """Mock database query with sample data."""
    # Return mock data for revenue account
    columns = ["VALUE", "BUD_VALUE", "MTP_VALUE", "RBU2LTP_VALUE", "PY_VALUE", "TIME_KEY"]

    # Different values for different accounts
    if "NET_REVENUE" in sql:
        rows = [(100.0, 95.0, 97.0, 98.0, 90.0, "202501")]
    elif "COGS" in sql:
        rows = [(50.0, 48.0, 49.0, 50.0, 45.0, "202501")]
    elif "GNA_EXPENSE" in sql:
        rows = [(20.0, 19.0, 19.5, 20.0, 18.0, "202501")]
    else:
        rows = [(0.0, 0.0, 0.0, 0.0, 0.0, "202501")]

    return columns, rows

# Patch imports before importing detector
import core.database as db_module
original_execute = db_module.execute_query_with_columns
db_module.execute_query_with_columns = mock_execute_query_with_columns

from detect.detector import create_metric_configs, detect_single_metric, print_results


def main():
    """Run detection test with mock data."""
    print("=" * 80)
    print("VARIANCE DETECTION TEST (MOCK DATA)")
    print("=" * 80)

    try:
        settings = MockSettings()
        configs = create_metric_configs()

        print(f"\nTotal metric configurations: {len(configs)}")
        print("\nRunning detection...\n")

        results = []

        # Detect for first 5 metrics (to keep output manageable)
        for config in configs[:5]:
            result = detect_single_metric(config, settings, time_key="202501")
            if result:
                results.append(result)
                print(f"✓ Detected: {result.account} - {result.description}")

        # Print results
        print("\n" + "-" * 80)
        print("DETECTION RESULTS")
        print("-" * 80)

        print_results(results)

        # Summary
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)

        alert_count = sum(1 for r in results if r.is_alert)
        print(f"\nTotal metrics detected: {len(results)}")
        print(f"Alerts triggered (≥5%): {alert_count}")
        print(f"Alert threshold: ±5.0%")

        if alert_count > 0:
            print("\nAlerts:")
            for i, result in enumerate(results, 1):
                if result.is_alert:
                    print(f"  {i}. {result.account.upper()} - {result.description}")
                    print(f"     Variance: {result.variance_percent:+.2f}%")
        else:
            print("\nNo alerts triggered. All metrics within threshold.")

        print("\n" + "=" * 80)
        print("Mock test completed successfully!")
        print("=" * 80)
        print("\nNote: This test uses mock data. To test with real database:")
        print("  1. Configure Snowflake credentials in .env file")
        print("  2. Run: python scripts/insert_sample_data.py")
        print("  3. Run: python scripts/test_detection.py")

        return 0

    except Exception as e:
        print(f"\n❌ Error during mock detection test: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        # Restore original function
        db_module.execute_query_with_columns = original_execute


if __name__ == "__main__":
    sys.exit(main())
