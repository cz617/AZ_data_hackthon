"""Models for variance detection."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal


@dataclass
class AccountMapping:
    """Mapping between account names and codes."""
    name: str
    code: str


@dataclass
class ScenarioMapping:
    """Mapping between scenario names and keys."""
    name: str
    key: int


@dataclass
class MetricConfig:
    """Configuration for a single metric to detect."""
    account_name: str
    description: str
    comparison_scenario: str
    actual_field: str
    comparison_field: str
    is_formula: bool = False
    formula_accounts: list[str] | None = None


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


# Account and scenario code mappings (placeholders)
ACCOUNT_MAPPINGS = {
    "revenue": AccountMapping("Net Product Sales", "NET_REVENUE"),
    "cost": AccountMapping("Cost of Goods Sold", "COGS"),
    "operational_cost": AccountMapping("General & Admin Expense", "GNA_EXPENSE"),
}

SCENARIO_MAPPINGS = {
    "actual": ScenarioMapping("Actual", 1),
    "budget": ScenarioMapping("Budget", 2),
    "mtp": ScenarioMapping("Mid-Term Plan", 3),
    "rbu2ltp": ScenarioMapping("RBU2LTP", 4),
}
