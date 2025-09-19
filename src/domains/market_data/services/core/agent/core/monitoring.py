"""
Monitoring module for the Data Agent.

This module provides metrics collection, logging, and alerting capabilities
for the data agent orchestrator to track performance and reliability.
"""

import logging
import time
import os
from datetime import datetime
from typing import Dict, Any, List, Optional, Union
import asyncio
import json

from market_data.agent.alert_handlers import (
    AlertHandler, AlertSeverity, LoggingAlertHandler,
    SlackAlertHandler, EmailAlertHandler, CompositeAlertHandler
)

try:
    from market_data.agent.prometheus_metrics import (
        setup_prometheus_monitoring
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

logger = logging.getLogger(__name__)

class DataAgentMetrics:
    """
    Collects and reports metrics for the data agent.

    Metrics tracked:
    - Processing time per data point
    - Success/failure rates
    - Data source availability
    - Reconciliation statistics
    - Batch processing performance
    """

    def __init__(self, metrics_prefix: str = "data_agent"):
        """
        Initialize the metrics collector.

        Args:
            metrics_prefix: Prefix for all metrics names
        """
        self.metrics_prefix = metrics_prefix
        self.reset()

    def reset(self):
        """Reset all metrics counters."""
        # Core metrics
        self.data_points_processed = 0
        self.data_points_failed = 0
        self.processing_times = []  # in seconds

        # Source metrics
        self.source_success: Dict[str, int] = {}
        self.source_failure: Dict[str, int] = {}
        self.source_latency: Dict[str, List[float]] = {}

        # Reconciliation metrics
        self.reconciliation_conflicts = 0
        self.reconciliation_no_data = 0
        self.reconciliation_single_source = 0
        self.reconciliation_multi_source = 0

        # Batch metrics
        self.batch_sizes = []
        self.batch_processing_times = []

        # Time tracking
        self.start_time = datetime.now()
        self.last_report_time = self.start_time

    def record_data_point_processed(self, success: bool, processing_time: float):
        """
        Record a processed data point.

        Args:
            success: Whether processing was successful
            processing_time: Time taken to process in seconds
        """
        if success:
            self.data_points_processed += 1
        else:
            self.data_points_failed += 1

        self.processing_times.append(processing_time)

    def record_source_result(self, source_name: str, success: bool, latency: float):
        """
        Record a result from a data source.

        Args:
            source_name: Name of the data source
            success: Whether the fetch was successful
            latency: Time taken for the fetch in seconds
        """
        if source_name not in self.source_success:
            self.source_success[source_name] = 0
            self.source_failure[source_name] = 0
            self.source_latency[source_name] = []

        if success:
            self.source_success[source_name] += 1
        else:
            self.source_failure[source_name] += 1

        self.source_latency[source_name].append(latency)

    def record_reconciliation(self, num_sources: int, had_conflict: bool):
        """
        Record a reconciliation result.

        Args:
            num_sources: Number of sources that provided data
            had_conflict: Whether there was a conflict that needed resolution
        """
        if num_sources == 0:
            self.reconciliation_no_data += 1
        elif num_sources == 1:
            self.reconciliation_single_source += 1
        else:
            self.reconciliation_multi_source += 1

        if had_conflict:
            self.reconciliation_conflicts += 1

    def record_batch(self, batch_size: int, processing_time: float):
        """
        Record a batch processing result.

        Args:
            batch_size: Size of the batch
            processing_time: Time taken to process the batch in seconds
        """
        self.batch_sizes.append(batch_size)
        self.batch_processing_times.append(processing_time)

    def get_metrics_report(self) -> Dict[str, Any]:
        """
        Get a report of all metrics.

        Returns:
            Dictionary containing all metrics
        """
        now = datetime.now()
        elapsed = (now - self.start_time).total_seconds()

        # Calculate averages and rates
        avg_processing_time = sum(self.processing_times) / max(len(self.processing_times), 1)
        points_per_second = (self.data_points_processed + self.data_points_failed) / max(elapsed, 1)
        failure_rate = self.data_points_failed / max(self.data_points_processed + self.data_points_failed, 1)

        # Source metrics
        source_metrics = {}
        for source in self.source_success.keys():
            total_calls = self.source_success[source] + self.source_failure[source]
            avg_latency = sum(self.source_latency[source]) / max(len(self.source_latency[source]), 1)
            success_rate = self.source_success[source] / max(total_calls, 1)

            source_metrics[source] = {
                f"{self.metrics_prefix}.source.calls": total_calls,
                f"{self.metrics_prefix}.source.success_rate": success_rate,
                f"{self.metrics_prefix}.source.avg_latency": avg_latency
            }

        # Batch metrics
        avg_batch_size = sum(self.batch_sizes) / max(len(self.batch_sizes), 1)
        avg_batch_time = sum(self.batch_processing_times) / max(len(self.batch_processing_times), 1)

        # Compile full report
        report = {
            f"{self.metrics_prefix}.processed": self.data_points_processed,
            f"{self.metrics_prefix}.failed": self.data_points_failed,
            f"{self.metrics_prefix}.avg_processing_time": avg_processing_time,
            f"{self.metrics_prefix}.points_per_second": points_per_second,
            f"{self.metrics_prefix}.failure_rate": failure_rate,
            f"{self.metrics_prefix}.reconciliation.conflicts": self.reconciliation_conflicts,
            f"{self.metrics_prefix}.reconciliation.no_data": self.reconciliation_no_data,
            f"{self.metrics_prefix}.reconciliation.single_source": self.reconciliation_single_source,
            f"{self.metrics_prefix}.reconciliation.multi_source": self.reconciliation_multi_source,
            f"{self.metrics_prefix}.batch.avg_size": avg_batch_size,
            f"{self.metrics_prefix}.batch.avg_time": avg_batch_time,
            f"{self.metrics_prefix}.uptime": elapsed,
            "sources": source_metrics
        }

        return report

    def log_metrics(self, level=logging.INFO):
        """
        Log all metrics at the specified level.

        Args:
            level: Logging level to use
        """
        report = self.get_metrics_report()
        logger.log(level, f"Data Agent Metrics: {json.dumps(report, indent=2)}")
        self.last_report_time = datetime.now()

    def should_report(self, interval_seconds: int = 300) -> bool:
        """
        Check if metrics should be reported based on time interval.

        Args:
            interval_seconds: Reporting interval in seconds

        Returns:
            True if it's time to report metrics
        """
        now = datetime.now()
        return (now - self.last_report_time).total_seconds() >= interval_seconds


class DataAgentMonitor:
    """
    Monitors the data agent and provides alerting capabilities.

    Features:
    - Periodic metrics reporting
    - Error rate alerting
    - Data source availability monitoring
    - Performance degradation detection
    """

    def __init__(
        self,
        metrics: DataAgentMetrics,
        reporting_interval: int = 300,  # 5 minutes
        alert_handler: Optional[Union[AlertHandler, List[AlertHandler]]] = None,
        enable_prometheus: bool = False,
        prometheus_port: int = 8000
    ):
        """
        Initialize the monitor.

        Args:
            metrics: Metrics collector to use
            reporting_interval: How often to report metrics (seconds)
            alert_handler: Alert handler or list of handlers for sending alerts
            enable_prometheus: Whether to enable Prometheus metrics export
            prometheus_port: Port to expose Prometheus metrics on
        """
        self.metrics = metrics
        self.reporting_interval = reporting_interval
        self.monitoring_task = None
        self.is_running = False

        # Set up alert handlers
        if alert_handler is None:
            # Create default composite handler with logging
            self.alert_handler = CompositeAlertHandler()
            self.alert_handler.add_handler(LoggingAlertHandler())

            # Add Slack handler if webhook URL is configured
            if os.environ.get("SLACK_WEBHOOK_URL"):
                self.alert_handler.add_handler(SlackAlertHandler())

            # Add Email handler if recipients are configured
            if os.environ.get("ALERT_EMAIL_RECIPIENTS"):
                self.alert_handler.add_handler(EmailAlertHandler())
        elif isinstance(alert_handler, list):
            # Create composite handler from list
            composite = CompositeAlertHandler()
            for handler in alert_handler:
                composite.add_handler(handler)
            self.alert_handler = composite
        else:
            # Use provided handler
            self.alert_handler = alert_handler

        # Set up Prometheus monitoring if enabled
        self.prometheus_monitor = None
        if enable_prometheus and PROMETHEUS_AVAILABLE:
            try:
                self.prometheus_monitor = setup_prometheus_monitoring(
                    metrics_getter=self.metrics.get_metrics_report,
                    port=prometheus_port,
                    update_interval=min(15, reporting_interval),  # Update at least every 15 seconds
                    metrics_prefix=self.metrics.metrics_prefix
                )
                logger.info(f"Prometheus metrics server started on port {prometheus_port}")
            except Exception as e:
                logger.error(f"Failed to set up Prometheus monitoring: {e}")

        # Alert thresholds with severity levels
        self.thresholds = {
            "failure_rate": {
                AlertSeverity.WARNING: 0.05,  # 5%
                AlertSeverity.CRITICAL: 0.15   # 15%
            },
            "source_success_rate": {
                AlertSeverity.WARNING: 0.9,    # Below 90%
                AlertSeverity.CRITICAL: 0.7    # Below 70%
            },
            "processing_time": {
                AlertSeverity.WARNING: 1.0,    # Over 1 second
                AlertSeverity.CRITICAL: 3.0    # Over 3 seconds
            },
            "points_per_second": {
                AlertSeverity.WARNING: 5.0,    # Below 5 points/sec
                AlertSeverity.CRITICAL: 1.0    # Below 1 point/sec
            }
        }

    def check_alerts(self):
        """Check for alert conditions and trigger alerts if needed"""
        report = self.metrics.get_metrics_report()

        # Check for high failure rate
        failure_rate = report.get(f"{self.metrics.metrics_prefix}.failure_rate", 0)
        self._check_metric("failure_rate", failure_rate,
                          "High Failure Rate", "Data agent failure rate is {value:.2%}",
                          higher_is_worse=True,
                          metadata={
                              "failed": report.get(f"{self.metrics.metrics_prefix}.failed", 0),
                              "processed": report.get(f"{self.metrics.metrics_prefix}.processed", 0)
                          })

        # Check for source availability
        for source_name, source_metrics in report.get("sources", {}).items():
            success_rate = source_metrics.get(f"{self.metrics.metrics_prefix}.source.success_rate", 1.0)
            self._check_metric("source_success_rate", success_rate,
                              f"{source_name} Availability Issue",
                              f"{source_name} success rate is {{value:.2%}}",
                              higher_is_worse=False,
                              metadata={
                                  "source": source_name,
                                  "calls": source_metrics.get(f"{self.metrics.metrics_prefix}.source.calls", 0),
                                  "avg_latency": source_metrics.get(f"{self.metrics.metrics_prefix}.source.avg_latency", 0)
                              })

        # Check for slow processing
        avg_time = report.get(f"{self.metrics.metrics_prefix}.avg_processing_time", 0)
        self._check_metric("processing_time", avg_time,
                          "Slow Processing", "Average processing time is {value:.2f}s",
                          higher_is_worse=True)

        # Check for low throughput
        points_per_second = report.get(f"{self.metrics.metrics_prefix}.points_per_second", 0)
        self._check_metric("points_per_second", points_per_second,
                          "Low Throughput", "Processing rate is {value:.2f} points/sec",
                          higher_is_worse=False)

        # Check for reconciliation issues
        conflicts = report.get(f"{self.metrics.metrics_prefix}.reconciliation.conflicts", 0)
        total_reconciliations = conflicts + \
                              report.get(f"{self.metrics.metrics_prefix}.reconciliation.no_data", 0) + \
                              report.get(f"{self.metrics.metrics_prefix}.reconciliation.single_source", 0) + \
                              report.get(f"{self.metrics.metrics_prefix}.reconciliation.multi_source", 0)

        if total_reconciliations > 0:
            conflict_rate = conflicts / total_reconciliations
            if conflict_rate > 0.3:  # More than 30% conflicts
                severity = AlertSeverity.WARNING if conflict_rate < 0.5 else AlertSeverity.CRITICAL
                self.alert_handler.send_alert(
                    "High Reconciliation Conflicts",
                    f"Reconciliation conflict rate is {conflict_rate:.2%}",
                    severity,
                    {
                        "conflicts": conflicts,
                        "total_reconciliations": total_reconciliations,
                        "no_data": report.get(f"{self.metrics.metrics_prefix}.reconciliation.no_data", 0),
                        "single_source": report.get(f"{self.metrics.metrics_prefix}.reconciliation.single_source", 0),
                        "multi_source": report.get(f"{self.metrics.metrics_prefix}.reconciliation.multi_source", 0)
                    }
                )

    def _check_metric(self, metric_name, value, title_template, message_template,
                      higher_is_worse=True, metadata=None):
        """Check a metric against thresholds and send alerts if needed"""
        if metric_name not in self.thresholds:
            return

        thresholds = self.thresholds[metric_name]
        severity = None

        # Determine severity based on thresholds
        if higher_is_worse:
            # For metrics where higher values are worse (e.g., failure rate)
            if value >= thresholds.get(AlertSeverity.CRITICAL, float('inf')):
                severity = AlertSeverity.CRITICAL
            elif value >= thresholds.get(AlertSeverity.WARNING, float('inf')):
                severity = AlertSeverity.WARNING
        else:
            # For metrics where lower values are worse (e.g., success rate)
            if value <= thresholds.get(AlertSeverity.CRITICAL, float('-inf')):
                severity = AlertSeverity.CRITICAL
            elif value <= thresholds.get(AlertSeverity.WARNING, float('-inf')):
                severity = AlertSeverity.WARNING

        # Send alert if severity threshold was crossed
        if severity:
            title = title_template
            message = message_template.format(value=value)
            self.alert_handler.send_alert(title, message, severity, metadata)

    def _trigger_alert(self, alert_type: str, details: Dict[str, Any], severity: str):
        """
        Legacy method for backwards compatibility.

        Args:
            alert_type: Type of alert
            details: Alert details
            severity: Severity of the alert
        """
        # Convert to new alert format
        title = alert_type.replace('_', ' ').title()
        message = f"{title} detected"
        self.alert_handler.send_alert(title, message, severity, details)

    async def _monitoring_loop(self):
        """Background task that periodically reports metrics and checks alerts."""
        while self.is_running:
            try:
                if self.metrics.should_report(self.reporting_interval):
                    self.metrics.log_metrics()
                    self.check_alerts()

                await asyncio.sleep(10)  # Check every 10 seconds
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(30)  # Back off on error

    def start(self):
        """Start the monitoring loop."""
        if self.monitoring_task is None or self.monitoring_task.done():
            self.is_running = True
            self.monitoring_task = asyncio.create_task(self._monitoring_loop())
            logger.info("Data agent monitoring started")

    def stop(self):
        """Stop the monitoring loop."""
        self.is_running = False
        if self.monitoring_task and not self.monitoring_task.done():
            self.monitoring_task.cancel()

        # Stop Prometheus monitoring if enabled
        if self.prometheus_monitor:
            try:
                self.prometheus_monitor.stop()
            except Exception as e:
                logger.error(f"Error stopping Prometheus monitor: {e}")

        logger.info("Data agent monitoring stopped")


# Decorator for timing functions and recording metrics
def timed_operation(metrics: DataAgentMetrics, operation_name: str):
    """
    Decorator for timing operations and recording metrics.

    Args:
        metrics: Metrics collector to use
        operation_name: Name of the operation for logging

    Returns:
        Decorator function
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            success = False
            try:
                result = await func(*args, **kwargs)
                success = True
                return result
            finally:
                end_time = time.time()
                elapsed = end_time - start_time
                if operation_name == "data_point":
                    metrics.record_data_point_processed(success, elapsed)
                elif operation_name == "batch":
                    # For batch operations, extract batch size from args or kwargs
                    batch_size = kwargs.get("batch_size", 0)
                    if not batch_size and len(args) > 0 and hasattr(args[0], "__len__"):
                        batch_size = len(args[0])
                    metrics.record_batch_processed(batch_size, elapsed)
                elif operation_name.startswith("source:"):
                    source_name = operation_name.split(":", 1)[1]
                    metrics.record_source_result(source_name, success, elapsed)

                # Alert on slow operations
                if elapsed > 5.0:  # Log slow operations
                    logger.warning(f"Slow operation: {operation_name} took {elapsed:.2f}s")

                    # If metrics has a monitor attached, send an alert
                    if hasattr(metrics, "monitor") and metrics.monitor is not None:
                        metadata = {
                            "operation": operation_name,
                            "duration": elapsed,
                            "success": success
                        }
                        severity = AlertSeverity.WARNING if elapsed < 10.0 else AlertSeverity.CRITICAL
                        metrics.monitor.alert_handler.send_alert(
                            "Slow Operation Detected",
                            f"Operation '{operation_name}' took {elapsed:.2f}s to complete",
                            severity,
                            metadata
                        )

        return wrapper
    return decorator
