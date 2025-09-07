#!/usr/bin/env python3
"""
WSL System Monitoring Script with Slack Alerts

Monitors WSL system health and sends Slack alerts when system stress is detected.
Tracks CPU, memory, disk usage, docker containers, database connections, and process health.

Features:
- Comprehensive system metrics collection
- Configurable stress thresholds
- Slack webhook notifications with rich formatting
- Rate limiting to prevent alert spam
- Historical data logging for trend analysis
- Auto-recovery detection and notifications
- WSL-specific monitoring (memory pressure, disk space)
"""

import os
import sys
import json
import time
import psutil
import subprocess
import requests
from datetime import datetime, timedelta
from pathlib import Path
import logging
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Any
import threading
from collections import deque
import socket

# Configure logging
log_dir = Path('/mnt/d/ats-logs/monitoring')
log_dir.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / 'wsl_system_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("wsl_system_monitor")

@dataclass
class SystemMetrics:
    """System performance metrics."""
    timestamp: datetime
    hostname: str

    # CPU metrics
    cpu_percent: float
    cpu_count: int
    load_avg: Optional[List[float]]

    # Memory metrics
    memory_total: int
    memory_available: int
    memory_percent: float
    memory_used: int
    swap_total: int
    swap_used: int
    swap_percent: float

    # Disk metrics
    disk_total: int
    disk_used: int
    disk_free: int
    disk_percent: float

    # Network metrics
    network_sent: int
    network_recv: int

    # Process metrics
    process_count: int
    docker_containers: int
    docker_running: int

    # Database metrics
    postgres_connections: Optional[int]
    postgres_status: str

    # ATS specific metrics
    ats_backfill_active: bool
    ats_data_size_gb: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary with serializable values."""
        result = asdict(self)
        result['timestamp'] = self.timestamp.isoformat()
        return result

@dataclass
class AlertThresholds:
    """Configurable thresholds for system stress alerts."""

    # CPU thresholds
    cpu_warning: float = 80.0      # 80% CPU usage
    cpu_critical: float = 95.0     # 95% CPU usage
    cpu_duration_minutes: int = 5   # Sustained for 5 minutes

    # Memory thresholds
    memory_warning: float = 85.0    # 85% memory usage
    memory_critical: float = 95.0   # 95% memory usage
    memory_available_mb: int = 500  # Less than 500MB available

    # Disk thresholds
    disk_warning: float = 85.0      # 85% disk usage
    disk_critical: float = 95.0     # 95% disk usage
    disk_free_gb: float = 5.0       # Less than 5GB free

    # Network thresholds
    network_mb_per_sec: float = 100.0  # 100MB/s network usage

    # Process thresholds
    max_processes: int = 500        # Too many processes

    # Database thresholds
    max_db_connections: int = 100   # PostgreSQL connection limit

    # ATS specific thresholds
    ats_data_max_gb: float = 500.0  # ATS data directory size limit

class SlackNotifier:
    """Handles Slack webhook notifications with rate limiting."""

    def __init__(self, webhook_url: str, rate_limit_minutes: int = 15):
        self.webhook_url = webhook_url
        self.rate_limit_minutes = rate_limit_minutes
        self.last_alerts: Dict[str, datetime] = {}

    def should_send_alert(self, alert_type: str) -> bool:
        """Check if we should send alert based on rate limiting."""
        now = datetime.now()
        last_sent = self.last_alerts.get(alert_type)

        if not last_sent:
            return True

        time_since_last = now - last_sent
        return time_since_last > timedelta(minutes=self.rate_limit_minutes)

    def send_alert(self, alert_type: str, title: str, message: str,
                   metrics: SystemMetrics, severity: str = "warning") -> bool:
        """Send formatted Slack alert."""

        if not self.should_send_alert(alert_type):
            logger.info(f"Rate limited: {alert_type} alert not sent")
            return False

        # Color coding for severity
        colors = {
            "info": "#36a64f",      # Green
            "warning": "#ff9500",   # Orange
            "critical": "#ff0000",  # Red
            "recovery": "#36a64f"   # Green
        }

        # Create rich Slack message
        payload = {
            "username": "ATS System Monitor",
            "icon_emoji": ":warning:" if severity in ["warning", "critical"] else ":white_check_mark:",
            "attachments": [
                {
                    "color": colors.get(severity, "#ff9500"),
                    "title": f"{severity.upper()}: {title}",
                    "text": message,
                    "fields": [
                        {
                            "title": "System",
                            "value": f"Host: {metrics.hostname}\nTime: {metrics.timestamp.strftime('%Y-%m-%d %H:%M:%S')}",
                            "short": True
                        },
                        {
                            "title": "CPU & Memory",
                            "value": f"CPU: {metrics.cpu_percent:.1f}%\nMemory: {metrics.memory_percent:.1f}% ({metrics.memory_available//1024//1024:,}MB available)",
                            "short": True
                        },
                        {
                            "title": "Disk & Network",
                            "value": f"Disk: {metrics.disk_percent:.1f}% ({metrics.disk_free//1024//1024//1024:.1f}GB free)\nProcesses: {metrics.process_count}",
                            "short": True
                        },
                        {
                            "title": "ATS Status",
                            "value": f"Backfill Active: {'✅' if metrics.ats_backfill_active else '❌'}\nData Size: {metrics.ats_data_size_gb:.1f}GB\nDB Status: {metrics.postgres_status}",
                            "short": True
                        }
                    ],
                    "footer": "ATS WSL System Monitor",
                    "ts": int(metrics.timestamp.timestamp())
                }
            ]
        }

        try:
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10,
                headers={'Content-Type': 'application/json'}
            )

            if response.status_code == 200:
                self.last_alerts[alert_type] = datetime.now()
                logger.info(f"Slack alert sent: {alert_type}")
                return True
            else:
                logger.error(f"Slack webhook failed: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
            return False

class WSLSystemMonitor:
    """Main WSL system monitoring class."""

    def __init__(self, slack_webhook_url: str, config_file: str = None):
        self.slack_notifier = SlackNotifier(slack_webhook_url)
        self.thresholds = AlertThresholds()
        self.config_file = config_file

        # Historical data for trend analysis
        self.metrics_history = deque(maxlen=100)  # Keep last 100 measurements
        self.stress_periods: Dict[str, datetime] = {}

        # State tracking
        self.last_alert_states: Dict[str, bool] = {}

        # Load configuration if provided
        if config_file and Path(config_file).exists():
            self.load_config()

    def load_config(self):
        """Load configuration from JSON file."""
        try:
            with open(self.config_file, 'r') as f:
                config = json.load(f)

            # Update thresholds
            if 'thresholds' in config:
                for key, value in config['thresholds'].items():
                    if hasattr(self.thresholds, key):
                        setattr(self.thresholds, key, value)

            logger.info(f"Configuration loaded from {self.config_file}")

        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")

    def get_system_metrics(self) -> SystemMetrics:
        """Collect comprehensive system metrics."""

        # Basic system info
        hostname = socket.gethostname()
        timestamp = datetime.now()

        # CPU metrics
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_count = psutil.cpu_count()

        try:
            load_avg = list(os.getloadavg()) if hasattr(os, 'getloadavg') else None
        except:
            load_avg = None

        # Memory metrics
        memory = psutil.virtual_memory()
        swap = psutil.swap_memory()

        # Disk metrics (root filesystem)
        disk = psutil.disk_usage('/')

        # Network metrics
        network = psutil.net_io_counters()

        # Process metrics
        process_count = len(psutil.pids())

        # Docker metrics
        docker_containers, docker_running = self.get_docker_metrics()

        # Database metrics
        postgres_connections, postgres_status = self.get_postgres_metrics()

        # ATS specific metrics
        ats_backfill_active = self.check_ats_backfill_status()
        ats_data_size_gb = self.get_ats_data_size()

        return SystemMetrics(
            timestamp=timestamp,
            hostname=hostname,
            cpu_percent=cpu_percent,
            cpu_count=cpu_count,
            load_avg=load_avg,
            memory_total=memory.total,
            memory_available=memory.available,
            memory_percent=memory.percent,
            memory_used=memory.used,
            swap_total=swap.total,
            swap_used=swap.used,
            swap_percent=swap.percent,
            disk_total=disk.total,
            disk_used=disk.used,
            disk_free=disk.free,
            disk_percent=(disk.used / disk.total) * 100,
            network_sent=network.bytes_sent,
            network_recv=network.bytes_recv,
            process_count=process_count,
            docker_containers=docker_containers,
            docker_running=docker_running,
            postgres_connections=postgres_connections,
            postgres_status=postgres_status,
            ats_backfill_active=ats_backfill_active,
            ats_data_size_gb=ats_data_size_gb
        )

    def get_docker_metrics(self) -> tuple[int, int]:
        """Get Docker container metrics."""
        try:
            result = subprocess.run(['docker', 'ps', '-a', '--format', '{{.Status}}'],
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                statuses = result.stdout.strip().split('\n')
                total = len([s for s in statuses if s.strip()])
                running = len([s for s in statuses if s.startswith('Up')])
                return total, running
        except Exception as e:
            logger.warning(f"Failed to get Docker metrics: {e}")

        return 0, 0

    def get_postgres_metrics(self) -> tuple[Optional[int], str]:
        """Get PostgreSQL connection metrics."""
        try:
            result = subprocess.run([
                'python3', 'scripts/run_dev.py', 'query',
                '--query', 'SELECT count(*) FROM pg_stat_activity'
            ], capture_output=True, text=True, timeout=10)

            if result.returncode == 0:
                # Parse connection count from output
                lines = result.stdout.strip().split('\n')
                for line in lines:
                    if line.strip().isdigit():
                        return int(line.strip()), "connected"

        except Exception as e:
            logger.warning(f"Failed to get PostgreSQL metrics: {e}")

        return None, "unavailable"

    def check_ats_backfill_status(self) -> bool:
        """Check if ATS backfill is currently active."""
        try:
            result = subprocess.run(['pgrep', '-f', 'polygon_30year_minute_backfill'],
                                  capture_output=True, timeout=5)
            return result.returncode == 0
        except:
            return False

    def get_ats_data_size(self) -> float:
        """Get ATS data directory size in GB."""
        try:
            ats_data_path = Path('/mnt/d/ats-data')
            if ats_data_path.exists():
                result = subprocess.run(['du', '-sb', str(ats_data_path)],
                                      capture_output=True, text=True, timeout=30)
                if result.returncode == 0:
                    size_bytes = int(result.stdout.split()[0])
                    return size_bytes / (1024**3)  # Convert to GB
        except Exception as e:
            logger.warning(f"Failed to get ATS data size: {e}")

        return 0.0

    def analyze_stress_conditions(self, metrics: SystemMetrics) -> List[Dict[str, Any]]:
        """Analyze current metrics for stress conditions."""
        alerts = []

        # CPU stress analysis
        if metrics.cpu_percent >= self.thresholds.cpu_critical:
            alerts.append({
                'type': 'cpu_critical',
                'severity': 'critical',
                'title': 'Critical CPU Usage',
                'message': f'CPU usage at {metrics.cpu_percent:.1f}% (threshold: {self.thresholds.cpu_critical}%)'
            })
        elif metrics.cpu_percent >= self.thresholds.cpu_warning:
            alerts.append({
                'type': 'cpu_warning',
                'severity': 'warning',
                'title': 'High CPU Usage',
                'message': f'CPU usage at {metrics.cpu_percent:.1f}% (threshold: {self.thresholds.cpu_warning}%)'
            })

        # Memory stress analysis
        if metrics.memory_percent >= self.thresholds.memory_critical:
            alerts.append({
                'type': 'memory_critical',
                'severity': 'critical',
                'title': 'Critical Memory Usage',
                'message': f'Memory usage at {metrics.memory_percent:.1f}% with only {metrics.memory_available//1024//1024:,}MB available'
            })
        elif (metrics.memory_percent >= self.thresholds.memory_warning or
              metrics.memory_available < self.thresholds.memory_available_mb * 1024 * 1024):
            alerts.append({
                'type': 'memory_warning',
                'severity': 'warning',
                'title': 'High Memory Usage',
                'message': f'Memory usage at {metrics.memory_percent:.1f}% with {metrics.memory_available//1024//1024:,}MB available'
            })

        # Disk stress analysis
        if metrics.disk_percent >= self.thresholds.disk_critical:
            alerts.append({
                'type': 'disk_critical',
                'severity': 'critical',
                'title': 'Critical Disk Usage',
                'message': f'Disk usage at {metrics.disk_percent:.1f}% with only {metrics.disk_free//1024//1024//1024:.1f}GB free'
            })
        elif (metrics.disk_percent >= self.thresholds.disk_warning or
              metrics.disk_free < self.thresholds.disk_free_gb * 1024**3):
            alerts.append({
                'type': 'disk_warning',
                'severity': 'warning',
                'title': 'High Disk Usage',
                'message': f'Disk usage at {metrics.disk_percent:.1f}% with {metrics.disk_free//1024//1024//1024:.1f}GB free'
            })

        # Process analysis
        if metrics.process_count > self.thresholds.max_processes:
            alerts.append({
                'type': 'process_count',
                'severity': 'warning',
                'title': 'High Process Count',
                'message': f'Running {metrics.process_count} processes (threshold: {self.thresholds.max_processes})'
            })

        # Database analysis
        if (metrics.postgres_connections and
            metrics.postgres_connections > self.thresholds.max_db_connections):
            alerts.append({
                'type': 'db_connections',
                'severity': 'warning',
                'title': 'High Database Connections',
                'message': f'PostgreSQL has {metrics.postgres_connections} active connections (threshold: {self.thresholds.max_db_connections})'
            })

        # ATS data size analysis
        if metrics.ats_data_size_gb > self.thresholds.ats_data_max_gb:
            alerts.append({
                'type': 'ats_data_size',
                'severity': 'warning',
                'title': 'Large ATS Data Directory',
                'message': f'ATS data directory is {metrics.ats_data_size_gb:.1f}GB (threshold: {self.thresholds.ats_data_max_gb}GB)'
            })

        return alerts

    def check_recovery_conditions(self, metrics: SystemMetrics) -> List[Dict[str, Any]]:
        """Check if any stress conditions have recovered."""
        recoveries = []

        stress_checks = {
            'cpu_critical': metrics.cpu_percent < self.thresholds.cpu_warning,
            'cpu_warning': metrics.cpu_percent < self.thresholds.cpu_warning,
            'memory_critical': metrics.memory_percent < self.thresholds.memory_warning,
            'memory_warning': metrics.memory_percent < self.thresholds.memory_warning,
            'disk_critical': metrics.disk_percent < self.thresholds.disk_warning,
            'disk_warning': metrics.disk_percent < self.thresholds.disk_warning
        }

        for condition, is_recovered in stress_checks.items():
            was_stressed = self.last_alert_states.get(condition, False)
            if was_stressed and is_recovered:
                recoveries.append({
                    'type': f'{condition}_recovery',
                    'severity': 'recovery',
                    'title': f'{condition.replace("_", " ").title()} Recovered',
                    'message': f'System has recovered from {condition.replace("_", " ")} condition'
                })

        return recoveries

    def run_monitoring_cycle(self):
        """Run one complete monitoring cycle."""
        try:
            # Collect metrics
            metrics = self.get_system_metrics()
            self.metrics_history.append(metrics)

            # Log basic metrics
            logger.info(f"System Status - CPU: {metrics.cpu_percent:.1f}%, "
                       f"Memory: {metrics.memory_percent:.1f}%, "
                       f"Disk: {metrics.disk_percent:.1f}%, "
                       f"Processes: {metrics.process_count}, "
                       f"ATS Backfill: {'Active' if metrics.ats_backfill_active else 'Inactive'}")

            # Analyze stress conditions
            stress_alerts = self.analyze_stress_conditions(metrics)
            recovery_alerts = self.check_recovery_conditions(metrics)

            # Send alerts
            all_alerts = stress_alerts + recovery_alerts
            for alert in all_alerts:
                self.slack_notifier.send_alert(
                    alert_type=alert['type'],
                    title=alert['title'],
                    message=alert['message'],
                    metrics=metrics,
                    severity=alert['severity']
                )

                # Update alert state tracking
                if alert['severity'] in ['warning', 'critical']:
                    self.last_alert_states[alert['type']] = True
                elif alert['severity'] == 'recovery':
                    original_condition = alert['type'].replace('_recovery', '')
                    self.last_alert_states[original_condition] = False

            # Save metrics to file for historical analysis
            self.save_metrics_history(metrics)

        except Exception as e:
            logger.error(f"Error in monitoring cycle: {e}")

    def save_metrics_history(self, metrics: SystemMetrics):
        """Save metrics to historical data file."""
        try:
            history_file = Path('/mnt/d/ats-logs/monitoring/system_metrics_history.jsonl')
            history_file.parent.mkdir(parents=True, exist_ok=True)

            with open(history_file, 'a') as f:
                f.write(json.dumps(metrics.to_dict()) + '\n')

        except Exception as e:
            logger.warning(f"Failed to save metrics history: {e}")

    def run_continuous_monitoring(self, interval_seconds: int = 60):
        """Run continuous monitoring loop."""
        logger.info(f"Starting WSL system monitoring (interval: {interval_seconds}s)")
        logger.info(f"Monitoring thresholds: CPU {self.thresholds.cpu_warning}%/{self.thresholds.cpu_critical}%, "
                   f"Memory {self.thresholds.memory_warning}%/{self.thresholds.memory_critical}%, "
                   f"Disk {self.thresholds.disk_warning}%/{self.thresholds.disk_critical}%")

        while True:
            try:
                self.run_monitoring_cycle()
                time.sleep(interval_seconds)

            except KeyboardInterrupt:
                logger.info("Monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"Unexpected error in monitoring loop: {e}")
                time.sleep(30)  # Wait before retrying

def main():
    """Main entry point for WSL system monitor."""
    import argparse

    parser = argparse.ArgumentParser(description="WSL System Monitor with Slack Alerts")

    parser.add_argument(
        "--slack-webhook",
        required=True,
        help="Slack webhook URL for notifications"
    )

    parser.add_argument(
        "--config-file",
        help="JSON configuration file path"
    )

    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Monitoring interval in seconds (default: 60)"
    )

    parser.add_argument(
        "--test-alert",
        action="store_true",
        help="Send a test alert and exit"
    )

    args = parser.parse_args()

    # Initialize monitor
    monitor = WSLSystemMonitor(
        slack_webhook_url=args.slack_webhook,
        config_file=args.config_file
    )

    if args.test_alert:
        # Send test alert
        logger.info("Sending test alert...")
        metrics = monitor.get_system_metrics()
        success = monitor.slack_notifier.send_alert(
            alert_type="test_alert",
            title="WSL System Monitor Test",
            message="This is a test alert from the WSL System Monitor. If you receive this, the monitoring system is working correctly!",
            metrics=metrics,
            severity="info"
        )
        if success:
            print("✅ Test alert sent successfully!")
        else:
            print("❌ Failed to send test alert")
        return

    # Run continuous monitoring
    monitor.run_continuous_monitoring(interval_seconds=args.interval)

if __name__ == "__main__":
    main()