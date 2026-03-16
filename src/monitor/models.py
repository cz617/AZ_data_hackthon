"""SQLAlchemy models for monitor database."""
from datetime import datetime
from enum import Enum as PyEnum
from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    create_engine,
)
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker, relationship


class ThresholdType(str, PyEnum):
    """Types of threshold comparisons."""

    ABSOLUTE = "absolute"
    PERCENTAGE = "percentage"
    CHANGE = "change"


class ThresholdOperator(str, PyEnum):
    """Comparison operators for thresholds."""

    GT = "gt"  # greater than
    LT = "lt"  # less than
    EQ = "eq"  # equal
    GTE = "gte"  # greater than or equal
    LTE = "lte"  # less than or equal


class AlertStatus(str, PyEnum):
    """Status of alerts in the queue."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class Base(DeclarativeBase):
    """Base class for all models."""

    pass


class Metric(Base):
    """Definition of a monitoring metric."""

    __tablename__ = "metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=True)
    category = Column(String(100), nullable=True)
    sql_template = Column(Text, nullable=False)
    threshold_type = Column(Enum(ThresholdType), nullable=False)
    threshold_value = Column(Float, nullable=False)
    threshold_operator = Column(Enum(ThresholdOperator), nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    # Relationships
    results = relationship("MetricResult", back_populates="metric")
    alerts = relationship("AlertQueue", back_populates="metric")

    def __repr__(self) -> str:
        return f"<Metric(id={self.id}, name='{self.name}')>"


class MetricResult(Base):
    """Result of a metric execution."""

    __tablename__ = "metric_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    metric_id = Column(Integer, ForeignKey("metrics.id"), nullable=False)
    executed_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    actual_value = Column(Float, nullable=True)
    threshold_value = Column(Float, nullable=True)
    is_alert = Column(Boolean, default=False, nullable=False)

    # Relationships
    metric = relationship("Metric", back_populates="results")

    def __repr__(self) -> str:
        return f"<MetricResult(id={self.id}, metric_id={self.metric_id}, is_alert={self.is_alert})>"


class AlertQueue(Base):
    """Queue of alerts waiting to be processed."""

    __tablename__ = "alert_queue"

    id = Column(Integer, primary_key=True, autoincrement=True)
    metric_id = Column(Integer, ForeignKey("metrics.id"), nullable=False)
    result_id = Column(Integer, ForeignKey("metric_results.id"), nullable=False)
    status = Column(Enum(AlertStatus), default=AlertStatus.PENDING, nullable=False)
    analysis_result = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    processed_at = Column(DateTime, nullable=True)

    # Relationships
    metric = relationship("Metric", back_populates="alerts")
    result = relationship("MetricResult")

    def __repr__(self) -> str:
        return f"<AlertQueue(id={self.id}, status={self.status})>"


# Global engine and session factory
_engine = None
_SessionFactory = None


def init_database(db_path: str = "data/monitor.db") -> None:
    """
    Initialize the database, creating all tables.

    Args:
        db_path: Path to SQLite database file
    """
    global _engine, _SessionFactory

    import os
    os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else ".", exist_ok=True)

    _engine = create_engine(f"sqlite:///{db_path}")
    Base.metadata.create_all(_engine)
    _SessionFactory = sessionmaker(bind=_engine)


def get_session(db_path: str = "data/monitor.db") -> Session:
    """
    Get a database session.

    Args:
        db_path: Path to SQLite database file

    Returns:
        SQLAlchemy Session
    """
    global _engine, _SessionFactory

    if _SessionFactory is None:
        init_database(db_path)

    return _SessionFactory()