"""
Prometheus metrics integration for the Data Agent.

This module provides a Prometheus metrics exporter for the data agent metrics,
allowing integration with Prometheus monitoring system and Grafana dashboards.
"""

import logging
import time
import threading
from typing import Dict, Any

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logging.warning("prometheus_client not installed. Prometheus metrics will not be available.")

logger = logging.getLogger(__name__)

class PrometheusMetricsExporter:
    """
    Exports data agent metrics to Prometheus.
    
    This class creates Prometheus metrics for the data agent and
    provides methods to update them based on the data agent metrics.
    """
    
    def __init__(self, metrics_prefix: str = "data_agent", port: int = 8000):
        """
        Initialize the Prometheus metrics exporter.
        
        Args:
            metrics_prefix: Prefix for all metrics names
            port: Port to expose Prometheus metrics on
        """
        self.metrics_prefix = metrics_prefix
        self.port = port
        self.server_started = False
        
        if not PROMETHEUS_AVAILABLE:
            logger.warning("PrometheusMetricsExporter initialized but prometheus_client not available")
            return
        
        # Core metrics
        self.processed_counter = Counter(
            f"{metrics_prefix}_processed_total",
            "Total number of data points processed"
        )
        self.failed_counter = Counter(
            f"{metrics_prefix}_failed_total",
            "Total number of data points that failed processing"
        )
        self.processing_time = Histogram(
            f"{metrics_prefix}_processing_time_seconds",
            "Time taken to process data points",
            buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0]
        )
        
        # Source metrics
        self.source_calls = Counter(
            f"{metrics_prefix}_source_calls_total",
            "Total number of calls to data sources",
            ["source"]
        )
        self.source_success = Counter(
            f"{metrics_prefix}_source_success_total",
            "Total number of successful calls to data sources",
            ["source"]
        )
        self.source_latency = Histogram(
            f"{metrics_prefix}_source_latency_seconds",
            "Latency of data source calls",
            ["source"],
            buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0]
        )
        
        # Reconciliation metrics
        self.reconciliation_conflicts = Counter(
            f"{metrics_prefix}_reconciliation_conflicts_total",
            "Total number of reconciliation conflicts"
        )
        self.reconciliation_no_data = Counter(
            f"{metrics_prefix}_reconciliation_no_data_total",
            "Total number of reconciliations with no data"
        )
        self.reconciliation_single_source = Counter(
            f"{metrics_prefix}_reconciliation_single_source_total",
            "Total number of reconciliations with a single source"
        )
        self.reconciliation_multi_source = Counter(
            f"{metrics_prefix}_reconciliation_multi_source_total",
            "Total number of reconciliations with multiple sources"
        )
        
        # Batch metrics
        self.batch_size = Histogram(
            f"{metrics_prefix}_batch_size",
            "Size of processed batches",
            buckets=[1, 5, 10, 20, 50, 100, 200, 500, 1000]
        )
        self.batch_processing_time = Histogram(
            f"{metrics_prefix}_batch_processing_time_seconds",
            "Time taken to process batches",
            buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0]
        )
        
        # Performance metrics
        self.points_per_second = Gauge(
            f"{metrics_prefix}_points_per_second",
            "Number of data points processed per second"
        )
        self.failure_rate = Gauge(
            f"{metrics_prefix}_failure_rate",
            "Failure rate of data point processing"
        )
        self.uptime = Gauge(
            f"{metrics_prefix}_uptime_seconds",
            "Uptime of the data agent"
        )
        
    def start_server(self):
        """Start the Prometheus metrics server"""
        if not PROMETHEUS_AVAILABLE:
            logger.warning("Cannot start Prometheus server: prometheus_client not available")
            return
            
        if self.server_started:
            return
            
        try:
            start_http_server(self.port)
            self.server_started = True
            logger.info(f"Prometheus metrics server started on port {self.port}")
        except Exception as e:
            logger.error(f"Failed to start Prometheus metrics server: {e}")
    
    def update_metrics(self, metrics_report: Dict[str, Any]):
        """
        Update Prometheus metrics from a metrics report.
        
        Args:
            metrics_report: Metrics report from DataAgentMetrics.get_metrics_report()
        """
        if not PROMETHEUS_AVAILABLE:
            return
            
        # Start server if not already started
        if not self.server_started:
            self.start_server()
        
        # Update core metrics
        processed = metrics_report.get(f"{self.metrics_prefix}.processed", 0)
        failed = metrics_report.get(f"{self.metrics_prefix}.failed", 0)
        
        # Set counters to the current values (they're cumulative)
        self.processed_counter._value.set(processed)
        self.failed_counter._value.set(failed)
        
        # Update performance metrics
        self.points_per_second.set(metrics_report.get(f"{self.metrics_prefix}.points_per_second", 0))
        self.failure_rate.set(metrics_report.get(f"{self.metrics_prefix}.failure_rate", 0))
        self.uptime.set(metrics_report.get(f"{self.metrics_prefix}.uptime", 0))
        
        # Update reconciliation metrics
        conflicts = metrics_report.get(f"{self.metrics_prefix}.reconciliation.conflicts", 0)
        no_data = metrics_report.get(f"{self.metrics_prefix}.reconciliation.no_data", 0)
        single_source = metrics_report.get(f"{self.metrics_prefix}.reconciliation.single_source", 0)
        multi_source = metrics_report.get(f"{self.metrics_prefix}.reconciliation.multi_source", 0)
        
        self.reconciliation_conflicts._value.set(conflicts)
        self.reconciliation_no_data._value.set(no_data)
        self.reconciliation_single_source._value.set(single_source)
        self.reconciliation_multi_source._value.set(multi_source)
        
        # Update source metrics
        for source_name, source_metrics in metrics_report.get("sources", {}).items():
            calls = source_metrics.get(f"{self.metrics_prefix}.source.calls", 0)
            success_rate = source_metrics.get(f"{self.metrics_prefix}.source.success_rate", 1.0)
            successes = int(calls * success_rate)
            
            self.source_calls.labels(source=source_name)._value.set(calls)
            self.source_success.labels(source=source_name)._value.set(successes)

class PrometheusMonitor:
    """
    Monitor that exports data agent metrics to Prometheus.
    
    This class periodically updates Prometheus metrics based on the
    data agent metrics.
    """
    
    def __init__(self, metrics_exporter: PrometheusMetricsExporter, 
                 metrics_getter, update_interval: int = 15):
        """
        Initialize the Prometheus monitor.
        
        Args:
            metrics_exporter: PrometheusMetricsExporter instance
            metrics_getter: Function that returns a metrics report
            update_interval: Interval in seconds between updates
        """
        self.metrics_exporter = metrics_exporter
        self.metrics_getter = metrics_getter
        self.update_interval = update_interval
        self.is_running = False
        self.thread = None
        
    def start(self):
        """Start the Prometheus monitor"""
        if self.is_running:
            return
            
        self.is_running = True
        self.thread = threading.Thread(target=self._monitoring_loop)
        self.thread.daemon = True
        self.thread.start()
        logger.info("Prometheus monitoring started")
        
    def stop(self):
        """Stop the Prometheus monitor"""
        self.is_running = False
        if self.thread:
            self.thread.join(timeout=1.0)
        logger.info("Prometheus monitoring stopped")
        
    def _monitoring_loop(self):
        """Background thread that periodically updates Prometheus metrics"""
        while self.is_running:
            try:
                metrics_report = self.metrics_getter()
                self.metrics_exporter.update_metrics(metrics_report)
            except Exception as e:
                logger.error(f"Error updating Prometheus metrics: {e}")
                
            time.sleep(self.update_interval)

def setup_prometheus_monitoring(metrics_getter, port: int = 8000, 
                               update_interval: int = 15, 
                               metrics_prefix: str = "data_agent"):
    """
    Set up Prometheus monitoring for the data agent.
    
    Args:
        metrics_getter: Function that returns a metrics report
        port: Port to expose Prometheus metrics on
        update_interval: Interval in seconds between updates
        metrics_prefix: Prefix for all metrics names
        
    Returns:
        PrometheusMonitor instance or None if Prometheus is not available
    """
    if not PROMETHEUS_AVAILABLE:
        logger.warning("Cannot set up Prometheus monitoring: prometheus_client not available")
        return None
        
    exporter = PrometheusMetricsExporter(metrics_prefix=metrics_prefix, port=port)
    monitor = PrometheusMonitor(exporter, metrics_getter, update_interval)
    monitor.start()
    return monitor
