#!/usr/bin/env python
"""Test script for variance detection."""
import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

# Now we can import from src
from src.core.config import get_settings
from src.detect.detector import detect_variances, print_results


def main():
    """Run detection and print results."""
    print("=" * 80)
    print("VARIANCE DETECTION TEST")
    print("=" * 80)

    try:
        settings = get_settings()
        print("\nConfiguration loaded successfully")

        # Run detection (optional time_key parameter can be added)
        results = detect_variances(settings, time_key=None)

        # Print results to console
        print_results(results)

        # Print summary
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

        print("\n" + "=" * 80)
        print("Test completed successfully!")
        print("=" * 80)

        return 0

    except Exception as e:
        print(f"\n❌ Error during detection test: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
