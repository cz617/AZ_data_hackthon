"""Variance detection package for financial metrics."""

from src.detect.detector import detect_variances
from src.detect.models import DetectionResult, MetricConfig

__all__ = ["detect_variances", "DetectionResult", "MetricConfig"]
