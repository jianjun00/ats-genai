"""
Event System Monitoring - Health checks, metrics, and observability
"""

import json
import logging
import time
import psutil
from datetime import datetime, timedelta
from typing import Dict, List, Any
from dataclasses import dataclass, asdict

from events.database import EventStorage
from events.producer import EventProducer
from events.correlation import CorrelationEngine

logger = logging.getLogger(__name__)

@dataclass
class SystemHealth:
    """System health status"""
    status: str  # healthy, degraded, unhealthy
    timestamp: str
    uptime_seconds: float
    components: Dict[str, Dict[str, Any]]
    metrics: Dict[str, Any]
    alerts: List[str]

@dataclass
class EventMetrics:
    """Event processing metrics"""
    total_events: int
    events_per_hour: float
    events_by_type: Dict[str, int]
    events_by_source: Dict[str, int]
    average_processing_time: float
    queue_depths: Dict[str, int]
    correlations_found: int
    error_rate: float

class EventSystemMonitor:
    """Comprehensive monitoring for event system"""

    def __init__(self):
        """Initialize event system monitor"""
        self.start_time = time.time()
        self.event_storage = None
        self.event_producer = None
        self.correlation_engine = None

        # Metrics history
        self.metrics_history = []
        self.max_history_size = 1000

        # Alert thresholds
        self.alert_thresholds = {
            'queue_depth': 1000,
            'error_rate': 0.05,  # 5%
            'processing_latency': 30000,  # 30 seconds
            'memory_usage': 0.85,  # 85%
            'disk_usage': 0.90,  # 90%
        }

        self._initialize_components()

    def _initialize_components(self):
        """Initialize monitoring components"""
        try:
            self.event_storage = EventStorage()
            self.event_producer = EventProducer()
            self.correlation_engine = CorrelationEngine(self.event_storage)
            logger.info("✅ Event system monitor initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize monitor components: {e}")

    def get_system_health(self) -> SystemHealth:
        """Get comprehensive system health status"""
        timestamp = datetime.utcnow().isoformat()
        uptime = time.time() - self.start_time

        components = {}
        alerts = []
        overall_status = "healthy"

        # Check database health
        db_health = self._check_database_health()
        components['database'] = db_health
        if db_health['status'] != 'healthy':
            overall_status = 'degraded'
            alerts.extend(db_health.get('alerts', []))

        # Check Redis/queue health
        redis_health = self._check_redis_health()
        components['redis'] = redis_health
        if redis_health['status'] != 'healthy':
            overall_status = 'degraded'
            alerts.extend(redis_health.get('alerts', []))

        # Check system resources
        system_health = self._check_system_resources()
        components['system'] = system_health
        if system_health['status'] != 'healthy':
            if overall_status == 'healthy':
                overall_status = 'degraded'
            alerts.extend(system_health.get('alerts', []))

        # Check processing pipeline
        processing_health = self._check_processing_pipeline()
        components['processing'] = processing_health
        if processing_health['status'] != 'healthy':
            if overall_status == 'healthy':
                overall_status = 'degraded'
            alerts.extend(processing_health.get('alerts', []))

        # Overall metrics
        metrics = self._collect_system_metrics()

        # Determine final status
        if len(alerts) > 5 or any('critical' in alert.lower() for alert in alerts):
            overall_status = 'unhealthy'

        return SystemHealth(
            status=overall_status,
            timestamp=timestamp,
            uptime_seconds=uptime,
            components=components,
            metrics=metrics,
            alerts=alerts
        )

    def _check_database_health(self) -> Dict[str, Any]:
        """Check database connectivity and performance"""
        try:
            start_time = time.time()
            stats = self.event_storage.get_event_stats()
            response_time = (time.time() - start_time) * 1000

            alerts = []
            status = "healthy"

            if 'error' in stats:
                status = "unhealthy"
                alerts.append(f"Database error: {stats['error']}")
            elif response_time > 1000:  # 1 second
                status = "degraded"
                alerts.append(f"Database response time high: {response_time:.0f}ms")

            return {
                'status': status,
                'response_time_ms': response_time,
                'total_events': stats.get('total_events', 0),
                'recent_events': stats.get('recent_events_24h', 0),
                'alerts': alerts,
                'last_check': datetime.utcnow().isoformat()
            }

        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'alerts': [f'Database connection failed: {str(e)}'],
                'last_check': datetime.utcnow().isoformat()
            }

    def _check_redis_health(self) -> Dict[str, Any]:
        """Check Redis connectivity and queue depths"""
        try:
            start_time = time.time()
            queue_stats = self.event_producer.get_queue_stats()
            response_time = (time.time() - start_time) * 1000

            alerts = []
            status = "healthy"

            # Check queue depths
            for queue, depth in queue_stats.items():
                if depth < 0:  # Error condition
                    status = "unhealthy"
                    alerts.append(f"Queue {queue} connection error")
                elif depth > self.alert_thresholds['queue_depth']:
                    if status == "healthy":
                        status = "degraded"
                    alerts.append(f"Queue {queue} depth high: {depth} events")

            if response_time > 500:  # 500ms
                if status == "healthy":
                    status = "degraded"
                alerts.append(f"Redis response time high: {response_time:.0f}ms")

            return {
                'status': status,
                'response_time_ms': response_time,
                'queue_stats': queue_stats,
                'alerts': alerts,
                'last_check': datetime.utcnow().isoformat()
            }

        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'alerts': [f'Redis connection failed: {str(e)}'],
                'last_check': datetime.utcnow().isoformat()
            }

    def _check_system_resources(self) -> Dict[str, Any]:
        """Check system resource usage"""
        try:
            # Memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent / 100

            # Disk usage
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent / 100

            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1) / 100

            alerts = []
            status = "healthy"

            if memory_percent > self.alert_thresholds['memory_usage']:
                status = "degraded"
                alerts.append(f"High memory usage: {memory_percent:.1%}")

            if disk_percent > self.alert_thresholds['disk_usage']:
                status = "degraded"
                alerts.append(f"High disk usage: {disk_percent:.1%}")

            if cpu_percent > 0.90:  # 90%
                status = "degraded"
                alerts.append(f"High CPU usage: {cpu_percent:.1%}")

            return {
                'status': status,
                'memory_usage': memory_percent,
                'disk_usage': disk_percent,
                'cpu_usage': cpu_percent,
                'alerts': alerts,
                'last_check': datetime.utcnow().isoformat()
            }

        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'alerts': [f'System resource check failed: {str(e)}'],
                'last_check': datetime.utcnow().isoformat()
            }

    def _check_processing_pipeline(self) -> Dict[str, Any]:
        """Check event processing pipeline health"""
        try:
            # Get recent processing metrics
            recent_events = self.event_storage.query_events(
                after_timestamp=datetime.utcnow() - timedelta(hours=1),
                limit=1000
            )

            alerts = []
            status = "healthy"

            # Check processing rate
            events_per_hour = len(recent_events)
            if events_per_hour == 0:
                status = "degraded"
                alerts.append("No events processed in the last hour")

            # Check for processing errors (simplified)
            error_count = sum(1 for event in recent_events
                            if event.get('processing_metadata', {}).get('retry_count', 0) > 2)
            error_rate = error_count / max(len(recent_events), 1)

            if error_rate > self.alert_thresholds['error_rate']:
                status = "degraded"
                alerts.append(f"High error rate: {error_rate:.1%}")

            return {
                'status': status,
                'events_processed_last_hour': events_per_hour,
                'error_rate': error_rate,
                'alerts': alerts,
                'last_check': datetime.utcnow().isoformat()
            }

        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'alerts': [f'Processing pipeline check failed: {str(e)}'],
                'last_check': datetime.utcnow().isoformat()
            }

    def _collect_system_metrics(self) -> Dict[str, Any]:
        """Collect comprehensive system metrics"""
        try:
            # Basic system info
            uptime = time.time() - self.start_time
            memory = psutil.virtual_memory()

            # Event metrics
            db_stats = self.event_storage.get_event_stats()
            queue_stats = self.event_producer.get_queue_stats()

            metrics = {
                'uptime_seconds': uptime,
                'memory_usage_percent': memory.percent,
                'total_events': db_stats.get('total_events', 0),
                'recent_events_24h': db_stats.get('recent_events_24h', 0),
                'queue_depths': queue_stats,
                'correlations_total': db_stats.get('total_correlations', 0),
                'timestamp': datetime.utcnow().isoformat()
            }

            return metrics

        except Exception as e:
            logger.error(f"❌ Error collecting metrics: {e}")
            return {'error': str(e)}

    def get_event_metrics(self, hours_back: int = 24) -> EventMetrics:
        """Get detailed event processing metrics"""
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
            recent_events = self.event_storage.query_events(
                after_timestamp=cutoff_time,
                limit=10000
            )

            if not recent_events:
                return EventMetrics(
                    total_events=0,
                    events_per_hour=0.0,
                    events_by_type={},
                    events_by_source={},
                    average_processing_time=0.0,
                    queue_depths={},
                    correlations_found=0,
                    error_rate=0.0
                )

            # Calculate metrics
            total_events = len(recent_events)
            events_per_hour = total_events / hours_back

            # Group by type and source
            events_by_type = {}
            events_by_source = {}
            total_processing_time = 0
            processing_time_count = 0
            error_count = 0

            for event in recent_events:
                # Type grouping
                event_type = event.get('event_type', 'unknown')
                events_by_type[event_type] = events_by_type.get(event_type, 0) + 1

                # Source grouping
                source = event.get('source', 'unknown')
                events_by_source[source] = events_by_source.get(source, 0) + 1

                # Processing time
                metadata = event.get('processing_metadata', {})
                processing_time = metadata.get('processing_time_ms')
                if processing_time:
                    total_processing_time += processing_time
                    processing_time_count += 1

                # Error counting
                retry_count = metadata.get('retry_count', 0)
                if retry_count > 2:
                    error_count += 1

            avg_processing_time = (total_processing_time / processing_time_count
                                 if processing_time_count > 0 else 0.0)
            error_rate = error_count / total_events if total_events > 0 else 0.0

            # Get current queue depths
            queue_depths = self.event_producer.get_queue_stats()

            # Count correlations (simplified)
            correlations_found = 0  # Would query correlation table

            return EventMetrics(
                total_events=total_events,
                events_per_hour=events_per_hour,
                events_by_type=events_by_type,
                events_by_source=events_by_source,
                average_processing_time=avg_processing_time,
                queue_depths=queue_depths,
                correlations_found=correlations_found,
                error_rate=error_rate
            )

        except Exception as e:
            logger.error(f"❌ Error getting event metrics: {e}")
            return EventMetrics(
                total_events=0,
                events_per_hour=0.0,
                events_by_type={'error': str(e)},
                events_by_source={},
                average_processing_time=0.0,
                queue_depths={},
                correlations_found=0,
                error_rate=1.0
            )

    def log_metrics(self):
        """Log current metrics for monitoring"""
        try:
            health = self.get_system_health()
            metrics = self.get_event_metrics()

            # Store in history
            metric_snapshot = {
                'timestamp': datetime.utcnow().isoformat(),
                'health': asdict(health),
                'metrics': asdict(metrics)
            }

            self.metrics_history.append(metric_snapshot)

            # Trim history
            if len(self.metrics_history) > self.max_history_size:
                self.metrics_history = self.metrics_history[-self.max_history_size:]

            # Log summary
            logger.info(f"📊 System Health: {health.status} | "
                       f"Events/hr: {metrics.events_per_hour:.1f} | "
                       f"Queue depth: {sum(metrics.queue_depths.values())} | "
                       f"Alerts: {len(health.alerts)}")

            if health.alerts:
                for alert in health.alerts[:3]:  # Log first 3 alerts
                    logger.warning(f"⚠️ {alert}")

        except Exception as e:
            logger.error(f"❌ Error logging metrics: {e}")

    def export_metrics_to_file(self, filepath: str):
        """Export metrics history to file"""
        try:
            with open(filepath, 'w') as f:
                json.dump(self.metrics_history, f, indent=2)

            logger.info(f"📁 Exported {len(self.metrics_history)} metric snapshots to {filepath}")

        except Exception as e:
            logger.error(f"❌ Error exporting metrics: {e}")

    def get_metrics_summary(self, hours_back: int = 24) -> Dict[str, Any]:
        """Get summary of metrics over time period"""
        if not self.metrics_history:
            return {'error': 'No metrics history available'}

        cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)

        relevant_metrics = [
            m for m in self.metrics_history
            if datetime.fromisoformat(m['timestamp']) >= cutoff_time
        ]

        if not relevant_metrics:
            return {'error': f'No metrics in last {hours_back} hours'}

        # Calculate summary statistics
        event_rates = [m['metrics']['events_per_hour'] for m in relevant_metrics]
        error_rates = [m['metrics']['error_rate'] for m in relevant_metrics]
        processing_times = [m['metrics']['average_processing_time'] for m in relevant_metrics]

        summary = {
            'period_hours': hours_back,
            'snapshots_count': len(relevant_metrics),
            'events_per_hour': {
                'min': min(event_rates) if event_rates else 0,
                'max': max(event_rates) if event_rates else 0,
                'avg': sum(event_rates) / len(event_rates) if event_rates else 0
            },
            'error_rate': {
                'min': min(error_rates) if error_rates else 0,
                'max': max(error_rates) if error_rates else 0,
                'avg': sum(error_rates) / len(error_rates) if error_rates else 0
            },
            'processing_time_ms': {
                'min': min(processing_times) if processing_times else 0,
                'max': max(processing_times) if processing_times else 0,
                'avg': sum(processing_times) / len(processing_times) if processing_times else 0
            }
        }

        return summary

    def close(self):
        """Close monitoring connections"""
        if self.event_storage:
            self.event_storage.close()
        if self.event_producer:
            self.event_producer.close()

# CLI monitoring interface
def run_monitoring_loop(interval_seconds: int = 300):
    """Run continuous monitoring loop"""
    monitor = EventSystemMonitor()

    logger.info(f"🔍 Starting event system monitoring (interval: {interval_seconds}s)")

    try:
        while True:
            monitor.log_metrics()
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        logger.info("⏹️ Monitoring stopped by user")
    except Exception as e:
        logger.error(f"❌ Monitoring error: {e}")
    finally:
        monitor.close()

if __name__ == "__main__":
    import sys

    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "health":
            monitor = EventSystemMonitor()
            health = monitor.get_system_health()
            print(f"System Health: {json.dumps(asdict(health), indent=2)}")
            monitor.close()

        elif command == "metrics":
            monitor = EventSystemMonitor()
            metrics = monitor.get_event_metrics()
            print(f"Event Metrics: {json.dumps(asdict(metrics), indent=2)}")
            monitor.close()

        elif command == "monitor":
            interval = int(sys.argv[2]) if len(sys.argv) > 2 else 300
            run_monitoring_loop(interval)

        else:
            print("Unknown command. Available: health, metrics, monitor [interval_seconds]")
    else:
        print("Event System Monitor")
        print("Usage: python monitoring.py [health|metrics|monitor [interval]]")