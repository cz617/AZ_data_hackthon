"""Scheduler service for periodic metric execution."""
import time
from typing import Optional

from apscheduler.schedulers.background import BackgroundScheduler

from src.core.config import Settings, get_settings
from src.monitor.alert_engine import process_metric
from src.monitor.models import Metric, get_session


class MonitorScheduler:
    """Scheduler for periodic metric monitoring."""

    def __init__(
        self,
        interval_seconds: Optional[int] = None,
        db_path: str = "data/monitor.db",
        settings: Optional[Settings] = None,
    ):
        self.settings = settings or get_settings()
        self.interval_seconds = interval_seconds or (
            self.settings.monitor_interval_minutes * 60
        )
        self.db_path = db_path
        self.scheduler: Optional[BackgroundScheduler] = None
        self._is_running = False

    @property
    def is_running(self) -> bool:
        """Check if scheduler is running."""
        return self._is_running

    def get_active_metrics(self) -> list[Metric]:
        """Get all active metrics from database."""
        session = get_session(self.db_path)
        try:
            return session.query(Metric).filter(Metric.is_active == True).all()
        finally:
            session.close()

    def run_once(self) -> None:
        """Execute all active metrics once."""
        metrics = self.get_active_metrics()

        for metric in metrics:
            try:
                process_metric(metric, self.settings, self.db_path)
            except Exception as e:
                print(f"Error processing metric {metric.name}: {e}")

    def _run_iteration(self) -> None:
        """Internal method called by scheduler."""
        self.run_once()

    def start(self) -> None:
        """Start the scheduler."""
        if self._is_running:
            return

        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(
            self._run_iteration,
            "interval",
            seconds=self.interval_seconds,
        )
        self.scheduler.start()
        self._is_running = True

    def stop(self) -> None:
        """Stop the scheduler."""
        if self.scheduler:
            self.scheduler.shutdown()
            self.scheduler = None
        self._is_running = False

    def run_forever(self) -> None:
        """Run the scheduler in blocking mode."""
        self.start()
        try:
            while self._is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()


def start_monitor_service(
    db_path: str = "data/monitor.db",
    settings: Optional[Settings] = None,
) -> MonitorScheduler:
    """Create and start a monitor scheduler."""
    scheduler = MonitorScheduler(db_path=db_path, settings=settings)
    scheduler.start()
    return scheduler