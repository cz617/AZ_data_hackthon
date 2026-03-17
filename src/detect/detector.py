"""Main variance detection logic."""

from typing import Any

from src.core.config import Settings
from src.core.database import execute_query_with_columns
from src.detect.models import (
    ACCOUNT_MAPPINGS,
    DetectionResult,
    MetricConfig,
    SCENARIO_MAPPINGS,
)


def create_metric_configs() -> list[MetricConfig]:
    """
    Create all metric configurations based on Excel specification.

    Returns:
        List of 20 metric configurations
    """
    configs = []

    # Revenue metrics (5)
    revenue_account = "revenue"
    configs.extend([
        MetricConfig(
            account_name=revenue_account,
            description="Actual vs Budget",
            comparison_scenario="budget",
            actual_field="VALUE",
            comparison_field="BUD_VALUE",
        ),
        MetricConfig(
            account_name=revenue_account,
            description="Actual vs MTP",
            comparison_scenario="mtp",
            actual_field="VALUE",
            comparison_field="MTP_VALUE",
        ),
        MetricConfig(
            account_name=revenue_account,
            description="Actual vs RBU2LTP",
            comparison_scenario="rbu2ltp",
            actual_field="VALUE",
            comparison_field="RBU2LTP_VALUE",
        ),
        MetricConfig(
            account_name=revenue_account,
            description="Actual vs Prior Year",
            comparison_scenario="actual",
            actual_field="VALUE",
            comparison_field="PY_VALUE",
        ),
        MetricConfig(
            account_name=revenue_account,
            description="Actual vs Previous Month",
            comparison_scenario="actual",
            actual_field="VALUE",
            comparison_field="PREVIOUS_MONTH_VALUE",
        ),
    ])

    # Cost metrics (5)
    cost_account = "cost"
    configs.extend([
        MetricConfig(
            account_name=cost_account,
            description="Actual vs Budget",
            comparison_scenario="budget",
            actual_field="VALUE",
            comparison_field="BUD_VALUE",
        ),
        MetricConfig(
            account_name=cost_account,
            description="Actual vs MTP",
            comparison_scenario="mtp",
            actual_field="VALUE",
            comparison_field="MTP_VALUE",
        ),
        MetricConfig(
            account_name=cost_account,
            description="Actual vs RBU2LTP",
            comparison_scenario="rbu2ltp",
            actual_field="VALUE",
            comparison_field="RBU2LTP_VALUE",
        ),
        MetricConfig(
            account_name=cost_account,
            description="Actual vs Prior Year",
            comparison_scenario="actual",
            actual_field="VALUE",
            comparison_field="PY_VALUE",
        ),
        MetricConfig(
            account_name=cost_account,
            description="Actual vs Previous Month",
            comparison_scenario="actual",
            actual_field="VALUE",
            comparison_field="PREVIOUS_MONTH_VALUE",
        ),
    ])

    # Operational Cost metrics (5)
    operational_cost_account = "operational_cost"
    configs.extend([
        MetricConfig(
            account_name=operational_cost_account,
            description="Actual vs Budget",
            comparison_scenario="budget",
            actual_field="VALUE",
            comparison_field="BUD_VALUE",
        ),
        MetricConfig(
            account_name=operational_cost_account,
            description="Actual vs MTP",
            comparison_scenario="mtp",
            actual_field="VALUE",
            comparison_field="MTP_VALUE",
        ),
        MetricConfig(
            account_name=operational_cost_account,
            description="Actual vs RBU2LTP",
            comparison_scenario="rbu2ltp",
            actual_field="VALUE",
            comparison_field="RBU2LTP_VALUE",
        ),
        MetricConfig(
            account_name=operational_cost_account,
            description="Actual vs Prior Year",
            comparison_scenario="actual",
            actual_field="VALUE",
            comparison_field="PY_VALUE",
        ),
        MetricConfig(
            account_name=operational_cost_account,
            description="Actual vs Previous Month",
            comparison_scenario="actual",
            actual_field="VALUE",
            comparison_field="PREVIOUS_MONTH_VALUE",
        ),
    ])

    # Margin metrics (5 - formula based)
    margin_accounts = ["revenue", "cost", "operational_cost"]
    configs.extend([
        MetricConfig(
            account_name="margin",
            description="Actual vs Budget",
            comparison_scenario="budget",
            actual_field="CALCULATED_MARGIN",
            comparison_field="CALCULATED_BUD_MARGIN",
            is_formula=True,
            formula_accounts=margin_accounts,
        ),
        MetricConfig(
            account_name="margin",
            description="Actual vs MTP",
            comparison_scenario="mtp",
            actual_field="CALCULATED_MARGIN",
            comparison_field="CALCULATED_MTP_MARGIN",
            is_formula=True,
            formula_accounts=margin_accounts,
        ),
        MetricConfig(
            account_name="margin",
            description="Actual vs RBU2LTP",
            comparison_scenario="rbu2ltp",
            actual_field="CALCULATED_MARGIN",
            comparison_field="CALCULATED_RBU2LTP_MARGIN",
            is_formula=True,
            formula_accounts=margin_accounts,
        ),
        MetricConfig(
            account_name="margin",
            description="Actual vs Prior Year",
            comparison_scenario="actual",
            actual_field="CALCULATED_MARGIN",
            comparison_field="CALCULATED_PY_MARGIN",
            is_formula=True,
            formula_accounts=margin_accounts,
        ),
        MetricConfig(
            account_name="margin",
            description="Actual vs Previous Month",
            comparison_scenario="actual",
            actual_field="CALCULATED_MARGIN",
            comparison_field="CALCULATED_PREVIOUS_MONTH_MARGIN",
            is_formula=True,
            formula_accounts=margin_accounts,
        ),
    ])

    return configs


def get_account_code(account_name: str) -> str:
    """Get account code from mapping."""
    return ACCOUNT_MAPPINGS.get(account_name, ACCOUNT_MAPPINGS["revenue"]).code


def get_scenario_key(scenario_name: str) -> int:
    """Get scenario key from mapping."""
    return SCENARIO_MAPPINGS.get(scenario_name, SCENARIO_MAPPINGS["actual"]).key


def fetch_account_data(
    account_name: str,
    settings: Settings,
    time_key: str = None,
) -> dict[str, Any]:
    """
    Fetch account data from FACT_PNL_BASE_BRAND.

    Note: Always queries SCENARIO_KEY = 1 (Actual) as comparison values
    (BUD_VALUE, MTP_VALUE, etc.) are stored in the same row.

    Args:
        account_name: Account name (e.g., "revenue")
        settings: Application settings
        time_key: Optional time key to filter

    Returns:
        Dictionary with account data
    """
    account_code = get_account_code(account_name)

    sql = """
    SELECT
        VALUE,
        BUD_VALUE,
        MTP_VALUE,
        RBU2LTP_VALUE,
        PY_VALUE,
        TIME_KEY
    FROM FACT_PNL_BASE_BRAND
    WHERE ACCOUNT_KEY = (
        SELECT ACCOUNT_KEY FROM DIM_ACCOUNT WHERE ACCOUNT_CODE = ?
    )
    AND SCENARIO_KEY = 1
    """

    params = [account_code]

    if time_key:
        sql += " AND TIME_KEY = ?"
        params.append(time_key)

    sql += " LIMIT 1"

    try:
        columns, rows = execute_query_with_columns(sql, settings, params=params)
        if rows and len(rows) > 0:
            return dict(zip(columns, rows[0]))
        return {}
    except Exception as e:
        print(f"Error fetching account data for {account_name}: {e}")
        return {}


def calculate_margin(
    revenue: float,
    cost: float,
    operational_cost: float,
) -> float:
    """Calculate margin: revenue - (cost + operational_cost)."""
    return revenue - (cost + operational_cost)


def detect_single_metric(
    metric: MetricConfig,
    settings: Settings,
    time_key: str = None,
) -> DetectionResult | None:
    """
    Detect variance for a single metric.

    Args:
        metric: Metric configuration
        settings: Application settings
        time_key: Optional time key for filtering

    Returns:
        Detection result or None if data unavailable
    """
    try:
        if metric.is_formula and metric.formula_accounts:
            # For margin metrics, calculate using formula
            revenue_data = fetch_account_data("revenue", settings, time_key)
            cost_data = fetch_account_data("cost", settings, time_key)
            operational_data = fetch_account_data("operational_cost", settings, time_key)

            # Get comparison data based on scenario
            if "budget" in metric.comparison_field.lower():
                comparison_revenue = revenue_data.get("BUD_VALUE", 0.0)
                comparison_cost = cost_data.get("BUD_VALUE", 0.0)
                comparison_operational = operational_data.get("BUD_VALUE", 0.0)
            elif "mtp" in metric.comparison_field.lower():
                comparison_revenue = revenue_data.get("MTP_VALUE", 0.0)
                comparison_cost = cost_data.get("MTP_VALUE", 0.0)
                comparison_operational = operational_data.get("MTP_VALUE", 0.0)
            elif "rbu2ltp" in metric.comparison_field.lower():
                comparison_revenue = revenue_data.get("RBU2LTP_VALUE", 0.0)
                comparison_cost = cost_data.get("RBU2LTP_VALUE", 0.0)
                comparison_operational = operational_data.get("RBU2LTP_VALUE", 0.0)
            elif "prior_year" in metric.comparison_field.lower() or "py" in metric.comparison_field.lower():
                comparison_revenue = revenue_data.get("PY_VALUE", 0.0)
                comparison_cost = cost_data.get("PY_VALUE", 0.0)
                comparison_operational = operational_data.get("PY_VALUE", 0.0)
            else:
                # Previous month (simplified)
                comparison_revenue = revenue_data.get("VALUE", 0.0) * 0.95  # Placeholder
                comparison_cost = cost_data.get("VALUE", 0.0) * 0.95
                comparison_operational = operational_data.get("VALUE", 0.0) * 0.95

            actual_value = calculate_margin(
                revenue_data.get("VALUE", 0.0),
                cost_data.get("VALUE", 0.0),
                operational_data.get("VALUE", 0.0),
            )
            comparison_value = calculate_margin(
                comparison_revenue,
                comparison_cost,
                comparison_operational,
            )

        else:
            # For direct field comparison
            account_data = fetch_account_data(metric.account_name, settings, time_key)

            if not account_data:
                return None

            actual_value = account_data.get("VALUE", 0.0)

            # Get comparison value based on field name
            if metric.comparison_field == "PREVIOUS_MONTH_VALUE":
                # Simplified: use 95% of actual as placeholder
                comparison_value = actual_value * 0.95
            else:
                comparison_value = account_data.get(metric.comparison_field, 0.0)

        # Calculate variance
        if comparison_value != 0:
            variance = actual_value - comparison_value
            variance_percent = (variance / abs(comparison_value)) * 100
        else:
            variance = 0.0
            variance_percent = 0.0

        # Check alert threshold (5%)
        is_alert = abs(variance_percent) >= 5.0

        return DetectionResult(
            account=metric.account_name,
            description=metric.description,
            actual_value=actual_value,
            comparison_value=comparison_value,
            variance=variance,
            variance_percent=variance_percent,
            is_alert=is_alert,
        )

    except Exception as e:
        print(f"Error detecting metric {metric.account_name} - {metric.description}: {e}")
        return None


def detect_variances(
    settings: Settings,
    time_key: str = None,
) -> list[DetectionResult]:
    """
    Run variance detection for all metrics.

    Args:
        settings: Application settings
        time_key: Optional time key for filtering

    Returns:
        List of detection results
    """
    configs = create_metric_configs()
    results = []

    for config in configs:
        result = detect_single_metric(config, settings, time_key)
        if result:
            results.append(result)

    return results


def print_results(results: list[DetectionResult]) -> None:
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
