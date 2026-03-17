"""Alert engine for threshold checking and alert creation."""
from datetime import datetime
from typing import Optional

from src.core.config import Settings
from src.monitor.executor import execute_metric_sql
from src.monitor.models import (
    AlertQueue,
    AlertStatus,
    Metric,
    MetricResult,
    ThresholdOperator,
    get_session,
)
from src.agent.middleware.alert_trigger import get_alert_handler


def check_threshold(
    actual_value: float,
    threshold_value: float,
    operator: ThresholdOperator,
) -> bool:
    """
    Check if actual value triggers an alert based on threshold.

    Args:
        actual_value: The measured value
        threshold_value: The threshold to compare against
        operator: Comparison operator

    Returns:
        True if alert should be triggered
    """
    if actual_value is None:
        return False

    comparisons = {
        ThresholdOperator.GT: actual_value > threshold_value,
        ThresholdOperator.LT: actual_value < threshold_value,
        ThresholdOperator.EQ: actual_value == threshold_value,
        ThresholdOperator.GTE: actual_value >= threshold_value,
        ThresholdOperator.LTE: actual_value <= threshold_value,
    }

    return comparisons.get(operator, False)


def process_metric(
    metric: Metric,
    settings: Settings,
    db_path: str = "data/monitor.db",
) -> MetricResult:
    """
    Execute a metric and create result record.
    If alert is triggered, call Agent for automatic analysis.

    Args:
        metric: Metric to process
        settings: Application settings
        db_path: Path to SQLite database

    Returns:
        Created MetricResult
    """
    actual_value = execute_metric_sql(metric, settings)

    is_alert = check_threshold(
        actual_value,
        metric.threshold_value,
        metric.threshold_operator,
    )

    session = get_session(db_path)
    try:
        result = MetricResult(
            metric_id=metric.id,
            actual_value=actual_value,
            threshold_value=metric.threshold_value,
            is_alert=is_alert,
        )
        session.add(result)
        session.commit()
        session.refresh(result)

        if is_alert:
            # Create alert record with PROCESSING status
            alert = AlertQueue(
                metric_id=metric.id,
                result_id=result.id,
                status=AlertStatus.PROCESSING,
            )
            session.add(alert)
            session.commit()
            session.refresh(alert)

            # Call Agent for analysis
            alert_handler = get_alert_handler()
            try:
                analysis_result = alert_handler.on_alert(alert, metric, result)
                # Update alert status to COMPLETED
                alert.status = AlertStatus.COMPLETED
                alert.analysis_result = analysis_result
            except Exception as e:
                # Update alert status to FAILED
                alert.status = AlertStatus.FAILED
                alert.analysis_result = f"Analysis failed: {str(e)}"
            finally:
                alert.processed_at = datetime.utcnow()
                session.commit()

    finally:
        session.close()

    return result


def get_pending_alerts(db_path: str = "data/monitor.db") -> list[AlertQueue]:
    """Get all pending alerts from the queue."""
    session = get_session(db_path)
    try:
        return session.query(AlertQueue).filter(
            AlertQueue.status == AlertStatus.PENDING
        ).all()
    finally:
        session.close()


def mark_alert_processing(alert_id: int, db_path: str = "data/monitor.db") -> None:
    """Mark an alert as being processed."""
    session = get_session(db_path)
    try:
        alert = session.query(AlertQueue).filter(AlertQueue.id == alert_id).first()
        if alert:
            alert.status = AlertStatus.PROCESSING
            session.commit()
    finally:
        session.close()


def complete_alert(
    alert_id: int,
    analysis_result: str,
    db_path: str = "data/monitor.db",
) -> None:
    """Mark an alert as completed with analysis result."""
    session = get_session(db_path)
    try:
        alert = session.query(AlertQueue).filter(AlertQueue.id == alert_id).first()
        if alert:
            alert.status = AlertStatus.COMPLETED
            alert.analysis_result = analysis_result
            alert.processed_at = datetime.utcnow()
            session.commit()
    finally:
        session.close()