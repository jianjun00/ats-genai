#!/usr/bin/env python3
"""
Real-Time Training Data Monitoring Dashboard
Provides live monitoring and alerting for training data generation and quality.
"""

import asyncio
import json
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from enum import Enum
try:
    import websockets
    WEBSOCKETS_AVAILABLE = True
except ImportError:
    WEBSOCKETS_AVAILABLE = False
from collections import deque
import logging

class AlertLevel(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

@dataclass
class SystemMetric:
    """Real-time system metric."""
    name: str
    value: float
    unit: str
    timestamp: datetime
    trend: str = "stable"  # increasing, decreasing, stable
    alert_level: AlertLevel = AlertLevel.INFO

@dataclass
class GenerationStatus:
    """Real-time generation status."""
    run_id: str
    symbol: str
    start_time: datetime
    current_stage: str
    progress_percentage: float
    estimated_completion: Optional[datetime] = None
    records_processed: int = 0
    current_timeframe: str = ""
    status: str = "running"  # running, completed, failed, paused

@dataclass
class QualityAlert:
    """Data quality alert."""
    dataset_id: str
    symbol: str
    quality_score: float
    alert_level: AlertLevel
    message: str
    timestamp: datetime
    acknowledged: bool = False

class RealTimeMonitor:
    """
    Real-time monitoring system for training data operations.
    Provides live metrics, alerts, and status updates.
    """

    def __init__(self, environment: str = "dev"):
        self.environment = environment
        self.running = False
        self.metrics_history = deque(maxlen=1000)  # Last 1000 metrics
        self.active_generations = {}  # run_id -> GenerationStatus
        self.alerts = deque(maxlen=100)  # Last 100 alerts
        self.websocket_clients = set()

        # Monitoring thresholds
        self.thresholds = {
            'quality_score_warning': 0.7,
            'quality_score_critical': 0.5,
            'generation_time_warning': 1800,  # 30 minutes
            'generation_time_critical': 3600,  # 1 hour
            'disk_usage_warning': 80.0,  # 80%
            'disk_usage_critical': 90.0,  # 90%
            'memory_usage_warning': 85.0,  # 85%
            'memory_usage_critical': 95.0,  # 95%
        }

        self.logger = logging.getLogger(__name__)

    async def start_monitoring(self):
        """Start the real-time monitoring system."""
        self.running = True
        self.logger.info("🔄 Starting real-time training data monitor")

        # Start monitoring tasks
        tasks = [
            self._monitor_system_metrics(),
            self._monitor_generation_status(),
            self._monitor_data_quality(),
            self._cleanup_old_data(),
            self._websocket_server()
        ]

        await asyncio.gather(*tasks)

    async def stop_monitoring(self):
        """Stop the monitoring system."""
        self.running = False
        self.logger.info("🛑 Stopping real-time training data monitor")

    async def _monitor_system_metrics(self):
        """Monitor system-level metrics."""
        while self.running:
            try:
                # Collect system metrics
                metrics = await self._collect_system_metrics()

                for metric in metrics:
                    # Check thresholds and generate alerts
                    alert = self._check_metric_threshold(metric)
                    if alert:
                        await self._add_alert(alert)

                    # Add to history
                    self.metrics_history.append(metric)

                # Broadcast to websocket clients
                await self._broadcast_metrics(metrics)

            except Exception as e:
                self.logger.error(f"Error in system metrics monitoring: {e}")

            await asyncio.sleep(10)  # Update every 10 seconds

    async def _monitor_generation_status(self):
        """Monitor active training data generation processes."""
        while self.running:
            try:
                # Update status of active generations
                for run_id in list(self.active_generations.keys()):
                    status = await self._get_generation_status(run_id)

                    if status:
                        self.active_generations[run_id] = status

                        # Check for timeouts or issues
                        alert = self._check_generation_health(status)
                        if alert:
                            await self._add_alert(alert)
                    else:
                        # Generation completed or failed
                        del self.active_generations[run_id]

                # Broadcast status updates
                await self._broadcast_generation_status()

            except Exception as e:
                self.logger.error(f"Error in generation monitoring: {e}")

            await asyncio.sleep(5)  # Update every 5 seconds

    async def _monitor_data_quality(self):
        """Monitor data quality metrics and trends."""
        while self.running:
            try:
                # Check recent datasets for quality issues
                quality_alerts = await self._check_quality_trends()

                for alert in quality_alerts:
                    await self._add_alert(alert)

            except Exception as e:
                self.logger.error(f"Error in quality monitoring: {e}")

            await asyncio.sleep(30)  # Update every 30 seconds

    async def _collect_system_metrics(self) -> List[SystemMetric]:
        """Collect current system metrics."""
        metrics = []
        current_time = datetime.now()

        try:
            import psutil
            import shutil

            # CPU Usage
            cpu_percent = psutil.cpu_percent(interval=1)
            metrics.append(SystemMetric(
                name="cpu_usage",
                value=cpu_percent,
                unit="%",
                timestamp=current_time,
                trend=self._calculate_trend("cpu_usage", cpu_percent),
                alert_level=AlertLevel.WARNING if cpu_percent > 80 else AlertLevel.INFO
            ))

            # Memory Usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            metrics.append(SystemMetric(
                name="memory_usage",
                value=memory_percent,
                unit="%",
                timestamp=current_time,
                trend=self._calculate_trend("memory_usage", memory_percent),
                alert_level=self._get_alert_level("memory_usage", memory_percent)
            ))

            # Disk Usage
            disk = shutil.disk_usage('/data')
            disk_percent = (disk.used / disk.total) * 100
            metrics.append(SystemMetric(
                name="disk_usage",
                value=disk_percent,
                unit="%",
                timestamp=current_time,
                trend=self._calculate_trend("disk_usage", disk_percent),
                alert_level=self._get_alert_level("disk_usage", disk_percent)
            ))

            # Active Generations
            metrics.append(SystemMetric(
                name="active_generations",
                value=len(self.active_generations),
                unit="count",
                timestamp=current_time,
                trend="stable"
            ))

            # Recent Quality Score Average
            avg_quality = await self._get_recent_quality_average()
            metrics.append(SystemMetric(
                name="avg_quality_score",
                value=avg_quality,
                unit="score",
                timestamp=current_time,
                trend=self._calculate_trend("avg_quality_score", avg_quality),
                alert_level=self._get_alert_level("quality_score", avg_quality)
            ))

        except ImportError:
            # Mock metrics if psutil not available
            metrics = self._get_mock_metrics(current_time)
        except Exception as e:
            self.logger.error(f"Error collecting metrics: {e}")
            metrics = self._get_mock_metrics(current_time)

        return metrics

    def _get_mock_metrics(self, current_time: datetime) -> List[SystemMetric]:
        """Generate mock metrics for demonstration."""
        import random

        # Generate realistic mock data
        base_time = time.time()

        return [
            SystemMetric(
                name="cpu_usage",
                value=random.uniform(15, 45),
                unit="%",
                timestamp=current_time,
                trend="stable"
            ),
            SystemMetric(
                name="memory_usage",
                value=random.uniform(60, 75),
                unit="%",
                timestamp=current_time,
                trend="increasing" if random.random() > 0.7 else "stable"
            ),
            SystemMetric(
                name="disk_usage",
                value=random.uniform(45, 65),
                unit="%",
                timestamp=current_time,
                trend="increasing"
            ),
            SystemMetric(
                name="active_generations",
                value=len(self.active_generations),
                unit="count",
                timestamp=current_time,
                trend="stable"
            ),
            SystemMetric(
                name="avg_quality_score",
                value=random.uniform(0.85, 0.95),
                unit="score",
                timestamp=current_time,
                trend="stable"
            )
        ]

    def _calculate_trend(self, metric_name: str, current_value: float) -> str:
        """Calculate trend based on recent values."""
        recent_values = [
            m.value for m in list(self.metrics_history)[-10:]
            if m.name == metric_name
        ]

        if len(recent_values) < 3:
            return "stable"

        # Simple trend calculation
        avg_recent = sum(recent_values[-3:]) / 3
        avg_older = sum(recent_values[-6:-3]) / 3 if len(recent_values) >= 6 else avg_recent

        if avg_recent > avg_older * 1.05:
            return "increasing"
        elif avg_recent < avg_older * 0.95:
            return "decreasing"
        else:
            return "stable"

    def _get_alert_level(self, metric_name: str, value: float) -> AlertLevel:
        """Determine alert level based on thresholds."""
        if metric_name == "memory_usage" or metric_name == "disk_usage":
            if value >= self.thresholds[f'{metric_name}_critical']:
                return AlertLevel.CRITICAL
            elif value >= self.thresholds[f'{metric_name}_warning']:
                return AlertLevel.WARNING
        elif metric_name == "quality_score":
            if value <= self.thresholds['quality_score_critical']:
                return AlertLevel.CRITICAL
            elif value <= self.thresholds['quality_score_warning']:
                return AlertLevel.WARNING

        return AlertLevel.INFO

    def _check_metric_threshold(self, metric: SystemMetric) -> Optional[QualityAlert]:
        """Check if metric violates thresholds."""
        if metric.alert_level in [AlertLevel.WARNING, AlertLevel.CRITICAL]:
            return QualityAlert(
                dataset_id="system",
                symbol="SYSTEM",
                quality_score=metric.value,
                alert_level=metric.alert_level,
                message=f"{metric.name} is {metric.value:.1f}{metric.unit} (trend: {metric.trend})",
                timestamp=metric.timestamp
            )
        return None

    async def _get_generation_status(self, run_id: str) -> Optional[GenerationStatus]:
        """Get current status of a generation process."""
        # Mock generation status - in real implementation would query actual process
        import random

        if run_id in self.active_generations:
            current = self.active_generations[run_id]
            # Update progress
            current.progress_percentage = min(100, current.progress_percentage + random.uniform(1, 5))
            current.records_processed += random.randint(100, 500)

            if current.progress_percentage >= 100:
                current.status = "completed"
                return None  # Remove from active list

            return current

        return None

    def _check_generation_health(self, status: GenerationStatus) -> Optional[QualityAlert]:
        """Check if generation process is healthy."""
        current_time = datetime.now()
        duration = (current_time - status.start_time).total_seconds()

        if duration > self.thresholds['generation_time_critical']:
            return QualityAlert(
                dataset_id=status.run_id,
                symbol=status.symbol,
                quality_score=0.0,
                alert_level=AlertLevel.CRITICAL,
                message=f"Generation for {status.symbol} running for {duration/3600:.1f} hours",
                timestamp=current_time
            )
        elif duration > self.thresholds['generation_time_warning']:
            return QualityAlert(
                dataset_id=status.run_id,
                symbol=status.symbol,
                quality_score=0.0,
                alert_level=AlertLevel.WARNING,
                message=f"Generation for {status.symbol} running for {duration/60:.1f} minutes",
                timestamp=current_time
            )

        return None

    async def _check_quality_trends(self) -> List[QualityAlert]:
        """Check for quality trend issues."""
        alerts = []

        # Mock quality trend analysis
        import random

        if random.random() < 0.1:  # 10% chance of quality alert
            alerts.append(QualityAlert(
                dataset_id=f"dataset_{int(time.time())}",
                symbol=random.choice(['AAPL', 'TSLA', 'MSFT']),
                quality_score=random.uniform(0.4, 0.7),
                alert_level=AlertLevel.WARNING,
                message="Quality score below normal range",
                timestamp=datetime.now()
            ))

        return alerts

    async def _get_recent_quality_average(self) -> float:
        """Get average quality score from recent datasets."""
        # Mock average quality - in real implementation would query database
        import random
        return random.uniform(0.85, 0.95)

    async def _add_alert(self, alert: QualityAlert):
        """Add new alert to the system."""
        self.alerts.append(alert)
        self.logger.warning(f"🚨 {alert.alert_level.value.upper()}: {alert.message}")

        # Broadcast alert to websocket clients
        await self._broadcast_alert(alert)

    async def _cleanup_old_data(self):
        """Clean up old monitoring data."""
        while self.running:
            try:
                current_time = datetime.now()
                cutoff_time = current_time - timedelta(hours=24)

                # Remove old alerts
                self.alerts = deque([
                    alert for alert in self.alerts
                    if alert.timestamp > cutoff_time
                ], maxlen=100)

            except Exception as e:
                self.logger.error(f"Error in cleanup: {e}")

            await asyncio.sleep(3600)  # Clean up every hour

    async def _websocket_server(self):
        """WebSocket server for real-time updates."""
        if not WEBSOCKETS_AVAILABLE:
            self.logger.warning("📡 WebSocket server not available (websockets package not installed)")
            return

        async def handle_client(websocket, path):
            self.websocket_clients.add(websocket)
            try:
                # Send initial data
                await self._send_initial_data(websocket)

                # Keep connection alive
                async for message in websocket:
                    # Handle client messages (commands, acknowledgments, etc.)
                    await self._handle_client_message(websocket, message)

            except Exception:  # Handle websockets.exceptions.ConnectionClosed and other exceptions
                pass
            finally:
                self.websocket_clients.discard(websocket)

        # Start WebSocket server
        try:
            await websockets.serve(handle_client, "localhost", 8765)
            self.logger.info("📡 WebSocket server started on ws://localhost:8765")
        except Exception as e:
            self.logger.error(f"Failed to start WebSocket server: {e}")

    async def _broadcast_metrics(self, metrics: List[SystemMetric]):
        """Broadcast metrics to all connected clients."""
        if not self.websocket_clients:
            return

        message = {
            "type": "metrics",
            "data": [asdict(metric) for metric in metrics],
            "timestamp": datetime.now().isoformat()
        }

        await self._broadcast_message(message)

    async def _broadcast_generation_status(self):
        """Broadcast generation status to clients."""
        if not self.websocket_clients:
            return

        message = {
            "type": "generation_status",
            "data": [asdict(status) for status in self.active_generations.values()],
            "timestamp": datetime.now().isoformat()
        }

        await self._broadcast_message(message)

    async def _broadcast_alert(self, alert: QualityAlert):
        """Broadcast alert to clients."""
        if not self.websocket_clients:
            return

        message = {
            "type": "alert",
            "data": asdict(alert),
            "timestamp": datetime.now().isoformat()
        }

        await self._broadcast_message(message)

    async def _broadcast_message(self, message: Dict[str, Any]):
        """Broadcast message to all connected clients."""
        if not self.websocket_clients:
            return

        message_json = json.dumps(message, default=str)

        # Send to all clients
        disconnected = set()
        for client in self.websocket_clients:
            try:
                await client.send(message_json)
            except websockets.exceptions.ConnectionClosed:
                disconnected.add(client)

        # Remove disconnected clients
        self.websocket_clients -= disconnected

    async def _send_initial_data(self, websocket):
        """Send initial data to newly connected client."""
        # Send recent metrics
        recent_metrics = list(self.metrics_history)[-10:]
        await websocket.send(json.dumps({
            "type": "initial_metrics",
            "data": [asdict(metric) for metric in recent_metrics]
        }, default=str))

        # Send recent alerts
        recent_alerts = list(self.alerts)[-10:]
        await websocket.send(json.dumps({
            "type": "initial_alerts",
            "data": [asdict(alert) for alert in recent_alerts]
        }, default=str))

        # Send active generations
        await websocket.send(json.dumps({
            "type": "initial_generations",
            "data": [asdict(status) for status in self.active_generations.values()]
        }, default=str))

    async def _handle_client_message(self, websocket, message: str):
        """Handle message from client."""
        try:
            data = json.loads(message)
            command = data.get("command")

            if command == "acknowledge_alert":
                alert_id = data.get("alert_id")
                # Mark alert as acknowledged
                for alert in self.alerts:
                    if str(id(alert)) == alert_id:
                        alert.acknowledged = True
                        break

            elif command == "get_status":
                # Send current status
                await self._send_initial_data(websocket)

        except json.JSONDecodeError:
            await websocket.send(json.dumps({"error": "Invalid JSON"}))
        except Exception as e:
            await websocket.send(json.dumps({"error": str(e)}))

    def add_active_generation(self, run_id: str, symbol: str):
        """Add a new active generation to monitor."""
        self.active_generations[run_id] = GenerationStatus(
            run_id=run_id,
            symbol=symbol,
            start_time=datetime.now(),
            current_stage="initializing",
            progress_percentage=0.0,
            records_processed=0
        )

    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get current dashboard data for web interface."""
        return {
            "metrics": [asdict(metric) for metric in list(self.metrics_history)[-20:]],
            "active_generations": [asdict(status) for status in self.active_generations.values()],
            "recent_alerts": [asdict(alert) for alert in list(self.alerts)[-10:]],
            "system_status": {
                "running": self.running,
                "connected_clients": len(self.websocket_clients),
                "total_alerts": len(self.alerts),
                "uptime": "active" if self.running else "stopped"
            }
        }

def demo_monitoring_system():
    """Demonstrate the monitoring system."""
    print("📊 Real-Time Training Data Monitoring System Demo")
    print("=" * 55)

    monitor = RealTimeMonitor()

    # Add some mock active generations
    monitor.add_active_generation("run_001", "AAPL")
    monitor.add_active_generation("run_002", "TSLA")

    # Get dashboard data
    dashboard_data = monitor.get_dashboard_data()

    print(f"🎛️ Dashboard Data:")
    print(f"  • Active Generations: {len(dashboard_data['active_generations'])}")
    print(f"  • Recent Alerts: {len(dashboard_data['recent_alerts'])}")
    print(f"  • System Status: {dashboard_data['system_status']}")

    print(f"\n🚨 Alert Thresholds:")
    for key, value in monitor.thresholds.items():
        print(f"  • {key}: {value}")

    print(f"\n📡 Features:")
    print(f"  • Real-time metrics collection (10s interval)")
    print(f"  • Generation process monitoring (5s interval)")
    print(f"  • Quality trend analysis (30s interval)")
    print(f"  • WebSocket server (ws://localhost:8765)")
    print(f"  • Alert system with 4 severity levels")
    print(f"  • Automatic cleanup (24h retention)")

    print(f"\n💡 To start monitoring:")
    print(f"     asyncio.run(monitor.start_monitoring())")

if __name__ == "__main__":
    demo_monitoring_system()