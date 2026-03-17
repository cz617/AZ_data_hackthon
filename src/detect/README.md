# Variance Detection System

This package implements financial metric variance detection based on the business logic defined in `docs/偏差计算.xlsx`.

## Overview

The detection system monitors 20 metrics across 4 account types (Revenue, Cost, Operational Cost, Margin) with 5 comparison scenarios each (Budget, MTP, RBU2LTP, Prior Year, Previous Month).

**Alert Threshold**: ±5.0% variance triggers an alert.

## Architecture

### Components

1. **models.py** - Data models and mappings
   - `AccountMapping`: Account name to code mappings
   - `ScenarioMapping`: Scenario name to key mappings
   - `MetricConfig`: Metric configuration
   - `DetectionResult`: Detection result

2. **detector.py** - Core detection logic
   - `detect_variances()`: Main detection function
   - `create_metric_configs()`: Generates 20 metric configurations
   - `detect_single_metric()`: Detects variance for a single metric
   - `print_results()`: Prints results to console

3. **api.py** - HTTP API endpoint
   - `GET /detect`: Triggers detection and returns JSON results
   - `GET /health`: Health check endpoint

## Usage

### 1. Insert Sample Data

```bash
python scripts/insert_sample_data.py
```

This will insert sample data from `000_客户提供的资料/02_ SAMPLE DATA` into Snowflake.

### 2. Run Detection Test

```bash
python scripts/test_detection.py
```

This runs detection and prints results to console.

### 3. Start HTTP API

```bash
python -m src.detect.api
```

Or using uvicorn:

```bash
uvicorn src.detect.api:app --host 0.0.0.0 --port 8000
```

### 4. Trigger Detection via HTTP

```bash
curl http://localhost:8000/detect
```

With optional time filter:

```bash
curl "http://localhost:8000/detect?time_key=202501"
```

## Configuration

### Account Mappings (Placeholders)

Located in `src/detect/models.py`:

```python
ACCOUNT_MAPPINGS = {
    "revenue": AccountMapping("Net Product Sales", "NET_REVENUE"),
    "cost": AccountMapping("Cost of Goods Sold", "COGS"),
    "operational_cost": AccountMapping("General & Admin Expense", "GNA_EXPENSE"),
}
```

Update these mappings based on actual account codes in `DIM_ACCOUNT` table.

### Scenario Mappings

```python
SCENARIO_MAPPINGS = {
    "actual": ScenarioMapping("Actual", 1),
    "budget": ScenarioMapping("Budget", 2),
    "mtp": ScenarioMapping("Mid-Term Plan", 3),
    "rbu2ltp": ScenarioMapping("RBU2LTP", 4),
}
```

## Metric Details

### Direct Field Metrics (15)

For Revenue, Cost, and Operational Cost:

1. Actual vs Budget: `VALUE` vs `BUD_VALUE`
2. Actual vs MTP: `VALUE` vs `MTP_VALUE`
3. Actual vs RBU2LTP: `VALUE` vs `RBU2LTP_VALUE`
4. Actual vs Prior Year: `VALUE` vs `PY_VALUE`
5. Actual vs Previous Month: `VALUE` vs previous month's `VALUE`

### Formula Metrics (5)

For Margin:
- Formula: `margin = revenue - (cost + operational_cost)`

Applies to the same 5 comparison scenarios (Budget, MTP, RBU2LTP, Prior Year, Previous Month).

## Detection Result Format

```python
{
    "account": "revenue",
    "description": "Actual vs Budget",
    "actual_value": 42.5,
    "comparison_value": 40.0,
    "variance": 2.5,
    "variance_percent": 6.25,
    "is_alert": true,
    "threshold_percent": 5.0,
    "detected_at": "2026-03-17T13:30:00.000000"
}
```

## Console Output Example

```
================================================================================
VARIANCE DETECTION RESULTS
================================================================================

Total Metrics: 20
Alerts Triggered: 8
Alert Threshold: ±5.0%

--------------------------------------------------------------------------------
DETAILED RESULTS
--------------------------------------------------------------------------------

1. [🚨 ALERT] REVENUE - Actual vs Budget
   Actual Value:      42.50
   Comparison Value:  40.00
   Variance:          2.50 (+6.25%)

...

================================================================================
Detection completed at 2026-03-17T13:30:00
================================================================================
```

## API Response Example

```json
{
  "status": "success",
  "total_metrics": 20,
  "alerts_triggered": 8,
  "threshold_percent": 5.0,
  "results": [
    {
      "account": "revenue",
      "description": "Actual vs Budget",
      "actual_value": 42.5,
      "comparison_value": 40.0,
      "variance": 2.5,
      "variance_percent": 6.25,
      "is_alert": true,
      "threshold_percent": 5.0,
      "detected_at": "2026-03-17T13:30:00.000000"
    }
  ]
}
```

## Notes

- All detection logic is implemented as direct Python code (no dynamic SQL generation)
- Account and scenario codes are configurable via dictionaries
- System follows incremental development approach - no changes to existing code
- Detection results are printed to console and returned via HTTP API
- Previous month comparison uses a simplified placeholder logic (95% of current value)
