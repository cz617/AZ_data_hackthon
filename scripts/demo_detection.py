#!/usr/bin/env python
"""Standalone demo of detection logic (no dependencies needed)."""
from datetime import datetime
from dataclasses import dataclass
from typing import Literal


@dataclass
class DetectionResult:
    """Result of variance detection for a single metric."""
    account: str
    description: str
    actual_value: float
    comparison_value: float
    variance: float
    variance_percent: float
    is_alert: bool
    threshold_percent: float = 5.0
    detected_at: datetime = None

    def __post_init__(self):
        if self.detected_at is None:
            self.detected_at = datetime.utcnow()


def create_mock_data():
    """Create mock financial data."""
    return {
        "revenue": {
            "actual": 100.0,
            "budget": 94.0,    # 6.38% variance (alert)
            "mtp": 97.0,       # 3.09% variance
            "rbu2ltp": 98.0,  # 2.04% variance
            "prior_year": 90.0, # 11.11% variance (alert)
            "prev_month": 99.0, # 1.01% variance
        },
        "cost": {
            "actual": 50.0,
            "budget": 49.0,     # 2.04% variance
            "mtp": 50.0,        # 0% variance
            "rbu2ltp": 51.0,   # 1.96% variance
            "prior_year": 45.0, # 11.11% variance (alert)
            "prev_month": 51.0, # 1.96% variance
        },
        "operational_cost": {
            "actual": 20.0,
            "budget": 18.5,     # 8.11% variance (alert)
            "mtp": 19.0,        # 5.26% variance (alert)
            "rbu2ltp": 19.5,    # 2.56% variance
            "prior_year": 17.0,  # 17.65% variance (alert)
            "prev_month": 20.0,  # 0% variance
        }
    }


def calculate_margin(revenue: float, cost: float, operational_cost: float) -> float:
    """Calculate margin: revenue - (cost + operational_cost)."""
    return revenue - (cost + operational_cost)


def detect_metrics():
    """Detect variances for all 20 metrics."""
    data = create_mock_data()
    results = []

    # Define metric configurations (20 total)
    metrics = [
        # Revenue (5)
        ("revenue", "Actual vs Budget", "budget"),
        ("revenue", "Actual vs MTP", "mtp"),
        ("revenue", "Actual vs RBU2LTP", "rbu2ltp"),
        ("revenue", "Actual vs Prior Year", "prior_year"),
        ("revenue", "Actual vs Previous Month", "prev_month"),

        # Cost (5)
        ("cost", "Actual vs Budget", "budget"),
        ("cost", "Actual vs MTP", "mtp"),
        ("cost", "Actual vs RBU2LTP", "rbu2ltp"),
        ("cost", "Actual vs Prior Year", "prior_year"),
        ("cost", "Actual vs Previous Month", "prev_month"),

        # Operational Cost (5)
        ("operational_cost", "Actual vs Budget", "budget"),
        ("operational_cost", "Actual vs MTP", "mtp"),
        ("operational_cost", "Actual vs RBU2LTP", "rbu2ltp"),
        ("operational_cost", "Actual vs Prior Year", "prior_year"),
        ("operational_cost", "Actual vs Previous Month", "prev_month"),

        # Margin (5 - calculated)
        ("margin", "Actual vs Budget", "budget"),
        ("margin", "Actual vs MTP", "mtp"),
        ("margin", "Actual vs RBU2LTP", "rbu2ltp"),
        ("margin", "Actual vs Prior Year", "prior_year"),
        ("margin", "Actual vs Previous Month", "prev_month"),
    ]

    for account, description, comparison_scenario in metrics:
        if account == "margin":
            # Calculate margin
            actual = calculate_margin(
                data["revenue"]["actual"],
                data["cost"]["actual"],
                data["operational_cost"]["actual"]
            )
            comparison = calculate_margin(
                data["revenue"][comparison_scenario],
                data["cost"][comparison_scenario],
                data["operational_cost"][comparison_scenario]
            )
        else:
            actual = data[account]["actual"]
            comparison = data[account][comparison_scenario]

        # Calculate variance
        if comparison != 0:
            variance = actual - comparison
            variance_percent = (variance / abs(comparison)) * 100
        else:
            variance = 0.0
            variance_percent = 0.0

        # Check alert threshold (5%)
        is_alert = abs(variance_percent) >= 5.0

        results.append(DetectionResult(
            account=account,
            description=description,
            actual_value=actual,
            comparison_value=comparison,
            variance=variance,
            variance_percent=variance_percent,
            is_alert=is_alert,
        ))

    return results


def print_results(results):
    """Print detection results to console."""
    print("\n" + "=" * 80)
    print("VARIANCE DETECTION RESULTS")
    print("=" * 80)

    alert_count = sum(1 for r in results if r.is_alert)
    print(f"\nTotal Metrics: {len(results)}")
    print(f"Alerts Triggered: {alert_count}")
    print(f"Alert Threshold: ±5.0%")

    print("\n" + "-" * 80)
    print("DETAILED RESULTS")
    print("-" * 80)

    for i, result in enumerate(results, 1):
        alert_marker = "🚨 ALERT" if result.is_alert else "✓ OK"
        print(f"\n{i}. [{alert_marker}] {result.account.upper()} - {result.description}")
        print(f"   Actual Value:      {result.actual_value:,.2f}")
        print(f"   Comparison Value:  {result.comparison_value:,.2f}")
        print(f"   Variance:          {result.variance:,.2f} ({result.variance_percent:+.2f}%)")

    print("\n" + "=" * 80)
    print(f"Detection completed at {results[0].detected_at if results else 'N/A'}")
    print("=" * 80)


def main():
    """Run the demo."""
    print("=" * 80)
    print("VARIANCE DETECTION DEMO (STANDALONE)")
    print("=" * 80)
    print("\nThis demo shows the detection logic with mock data.")
    print("No database or external dependencies required.")

    try:
        results = detect_metrics()
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
            print("\nAlert Details:")
            for i, result in enumerate(results, 1):
                if result.is_alert:
                    print(f"  {i}. {result.account.upper()} - {result.description}")
                    print(f"     Variance: {result.variance_percent:+.2f}% (threshold: ±5.0%)")

        print("\n" + "=" * 80)
        print("Demo completed successfully!")
        print("=" * 80)

        # Show alert configuration
        print("\n📋 ALERT CONFIGURATION")
        print("-" * 80)
        print(f"Alert Threshold: ±5.0% variance")
        print(f"  - Actual revenue lower than budget by 5% → Alert")
        print(f"  - Actual cost higher than budget by 5% → Alert")
        print(f"  - Any metric variance ≥ ±5% → Alert")

        return 0

    except Exception as e:
        print(f"\n❌ Error during demo: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
