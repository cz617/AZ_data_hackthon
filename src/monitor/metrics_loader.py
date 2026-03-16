"""Load metrics configuration from YAML files."""
from pathlib import Path
from typing import Any

import yaml

from src.monitor.models import (
    Metric,
    ThresholdOperator,
    ThresholdType,
    get_session,
)


def load_metrics_from_yaml(yaml_path: str) -> list[dict[str, Any]]:
    """
    Load metrics definitions from a YAML file.

    Args:
        yaml_path: Path to the YAML configuration file

    Returns:
        List of metric dictionaries
    """
    path = Path(yaml_path)
    if not path.exists():
        raise FileNotFoundError(f"Metrics config file not found: {yaml_path}")

    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    return config.get("metrics", [])


def load_metrics_into_database(
    metrics_data: list[dict[str, Any]],
    db_path: str = "data/monitor.db",
) -> list[Metric]:
    """
    Load metrics from data into the database.

    Args:
        metrics_data: List of metric dictionaries
        db_path: Path to SQLite database

    Returns:
        List of created Metric objects
    """
    session = get_session(db_path)
    created_metrics = []

    try:
        for metric_dict in metrics_data:
            metric = Metric(
                name=metric_dict["name"],
                description=metric_dict.get("description"),
                category=metric_dict.get("category"),
                sql_template=metric_dict["sql_template"],
                threshold_type=ThresholdType(metric_dict["threshold_type"]),
                threshold_value=metric_dict["threshold_value"],
                threshold_operator=ThresholdOperator(metric_dict["threshold_operator"]),
                is_active=metric_dict.get("is_active", True),
            )
            session.add(metric)
            created_metrics.append(metric)

        session.commit()

        for metric in created_metrics:
            session.refresh(metric)

    finally:
        session.close()

    return created_metrics


def reload_metrics_from_config(
    yaml_path: str = "config/metrics_template.yaml",
    db_path: str = "data/monitor.db",
) -> list[Metric]:
    """
    Reload metrics from YAML config into database.

    Args:
        yaml_path: Path to YAML configuration
        db_path: Path to SQLite database

    Returns:
        List of created Metric objects
    """
    metrics_data = load_metrics_from_yaml(yaml_path)

    session = get_session(db_path)
    try:
        session.query(Metric).delete()
        session.commit()
    finally:
        session.close()

    return load_metrics_into_database(metrics_data, db_path)