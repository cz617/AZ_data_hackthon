"""Execute metric SQL queries."""
from typing import Optional

from src.core.config import Settings
from src.core.database import execute_query_with_columns
from src.monitor.models import Metric


def execute_metric_sql(metric: Metric, settings: Settings) -> Optional[float]:
    """
    Execute a metric's SQL template and return the result value.

    Args:
        metric: Metric definition with SQL template
        settings: Application settings for database connection

    Returns:
        Float value from query, or None if no result
    """
    columns, rows = execute_query_with_columns(metric.sql_template, settings)

    if not rows or not rows[0]:
        return None

    value = rows[0][0]

    if value is None:
        return None

    return float(value)