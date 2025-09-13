"""
Service Performance Monitoring and Metrics Collection

Provides comprehensive monitoring capabilities for service-based architecture:
1. Performance metrics collection (latency, throughput, error rates)
2. Health check monitoring and alerting
3. Resource utilization tracking
4. Business metrics and KPIs
5. Real-time dashboards and reporting
"""

import time
import asyncio
import logging
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import defaultdict, deque
import json
import psutil
from contextlib import asynccontextmanager


logger = logging.getLogger(__name__)


# ========================================================================================
# METRICS DATA MODELS
# ========================================================================================

@dataclass
class ServiceMetric:
    """Individual service metric data point"""
    service_name: str
    operation: str
    metric_type: str  # 'latency', 'throughput', 'error_rate', 'resource_usage'
    value: float
    timestamp: datetime
    labels: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PerformanceBenchmark:
    """Performance benchmark thresholds"""
    service_name: str
    operation: str
    latency_p50_ms: float = 50.0
    latency_p95_ms: float = 100.0
    latency_p99_ms: float = 200.0
    error_rate_threshold: float = 0.01  # 1%
    throughput_min_ops_sec: float = 10.0
    memory_usage_mb_max: float = 512.0
    cpu_usage_percent_max: float = 70.0


@dataclass
class ServiceHealth:
    """Service health status"""
    service_name: str
    status: str  # 'healthy', 'degraded', 'unhealthy', 'critical'
    last_check: datetime
    response_time_ms: float
    error_count: int
    uptime_seconds: float
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AlertRule:
    """Alert rule configuration"""
    name: str
    service_name: str
    metric_type: str
    condition: str  # 'greater_than', 'less_than', 'equals'
    threshold: float
    duration_seconds: int = 300  # Alert if condition persists for 5 minutes
    severity: str = 'warning'  # 'info', 'warning', 'error', 'critical'
    enabled: bool = True


# ========================================================================================
# PERFORMANCE METRICS COLLECTOR
# ========================================================================================

class ServiceMetricsCollector:
    """
    Collects and aggregates service performance metrics.

    Features:
    - Real-time metric collection
    - Statistical aggregation (percentiles, averages)
    - Time-series data storage
    - Performance benchmarking
    - Alert evaluation
    """

    def __init__(self, max_metrics_history: int = 10000):
        self.max_metrics_history = max_metrics_history
        self.metrics: deque = deque(maxlen=max_metrics_history)
        self.service_stats: Dict[str, Dict] = defaultdict(lambda: {
            'operation_counts': defaultdict(int),
            'latencies': defaultdict(list),
            'error_counts': defaultdict(int),
            'last_activity': None,
            'total_requests': 0,
            'total_errors': 0
        })
        self.benchmarks: Dict[str, PerformanceBenchmark] = {}
        self.health_status: Dict[str, ServiceHealth] = {}
        self.alert_rules: List[AlertRule] = []
        self._active_alerts: Dict[str, datetime] = {}

    def add_benchmark(self, benchmark: PerformanceBenchmark):
        """Add performance benchmark for a service operation"""
        key = f"{benchmark.service_name}:{benchmark.operation}"
        self.benchmarks[key] = benchmark
        logger.info(f"Added benchmark for {key}")

    def add_alert_rule(self, rule: AlertRule):
        """Add alert rule for monitoring"""
        self.alert_rules.append(rule)
        logger.info(f"Added alert rule: {rule.name}")

    def record_metric(self, metric: ServiceMetric):
        """Record a service metric"""
        self.metrics.append(metric)

        # Update service statistics
        service_key = metric.service_name
        stats = self.service_stats[service_key]

        stats['last_activity'] = metric.timestamp
        stats['operation_counts'][metric.operation] += 1
        stats['total_requests'] += 1

        if metric.metric_type == 'latency':
            stats['latencies'][metric.operation].append(metric.value)
            # Keep only recent latencies for memory efficiency
            if len(stats['latencies'][metric.operation]) > 1000:
                stats['latencies'][metric.operation] = stats['latencies'][metric.operation][-500:]

        elif metric.metric_type == 'error_rate' and metric.value > 0:
            stats['error_counts'][metric.operation] += int(metric.value)
            stats['total_errors'] += int(metric.value)

    def get_service_stats(self, service_name: str, operation: Optional[str] = None) -> Dict[str, Any]:
        """Get aggregated statistics for a service"""
        if service_name not in self.service_stats:
            return {}

        stats = self.service_stats[service_name].copy()

        # Calculate latency percentiles
        latency_stats = {}
        for op, latencies in stats['latencies'].items():
            if operation and op != operation:
                continue

            if latencies:
                sorted_latencies = sorted(latencies)
                n = len(sorted_latencies)
                latency_stats[op] = {
                    'count': n,
                    'min': min(latencies),
                    'max': max(latencies),
                    'avg': sum(latencies) / n,
                    'p50': sorted_latencies[int(n * 0.5)] if n > 0 else 0,
                    'p95': sorted_latencies[int(n * 0.95)] if n > 1 else 0,
                    'p99': sorted_latencies[int(n * 0.99)] if n > 1 else 0
                }

        # Calculate error rates
        error_rates = {}
        for op, error_count in stats['error_counts'].items():
            if operation and op != operation:
                continue

            total_requests = stats['operation_counts'][op]
            error_rates[op] = (error_count / total_requests) if total_requests > 0 else 0

        return {
            'service_name': service_name,
            'total_requests': stats['total_requests'],
            'total_errors': stats['total_errors'],
            'overall_error_rate': stats['total_errors'] / stats['total_requests'] if stats['total_requests'] > 0 else 0,
            'last_activity': stats['last_activity'],
            'operation_counts': dict(stats['operation_counts']),
            'latency_stats': latency_stats,
            'error_rates': error_rates,
            'uptime_status': 'active' if stats['last_activity'] and
                           (datetime.utcnow() - stats['last_activity']).seconds < 300 else 'inactive'
        }

    def get_benchmark_violations(self, service_name: str, operation: str) -> List[str]:
        """Check for benchmark violations"""
        key = f"{service_name}:{operation}"
        benchmark = self.benchmarks.get(key)
        if not benchmark:
            return []

        violations = []
        stats = self.get_service_stats(service_name, operation)

        if operation in stats.get('latency_stats', {}):
            latency_stats = stats['latency_stats'][operation]

            if latency_stats['p95'] > benchmark.latency_p95_ms:
                violations.append(f"P95 latency {latency_stats['p95']:.1f}ms exceeds benchmark {benchmark.latency_p95_ms}ms")

            if latency_stats['p99'] > benchmark.latency_p99_ms:
                violations.append(f"P99 latency {latency_stats['p99']:.1f}ms exceeds benchmark {benchmark.latency_p99_ms}ms")

        if operation in stats.get('error_rates', {}):
            error_rate = stats['error_rates'][operation]
            if error_rate > benchmark.error_rate_threshold:
                violations.append(f"Error rate {error_rate*100:.2f}% exceeds benchmark {benchmark.error_rate_threshold*100:.2f}%")

        return violations

    def evaluate_alerts(self) -> List[Dict[str, Any]]:
        """Evaluate alert rules and return active alerts"""
        active_alerts = []
        current_time = datetime.utcnow()

        for rule in self.alert_rules:
            if not rule.enabled:
                continue

            # Get current metric value
            current_value = self._get_current_metric_value(rule.service_name, rule.metric_type)
            if current_value is None:
                continue

            # Check condition
            alert_triggered = False
            if rule.condition == 'greater_than' and current_value > rule.threshold:
                alert_triggered = True
            elif rule.condition == 'less_than' and current_value < rule.threshold:
                alert_triggered = True
            elif rule.condition == 'equals' and current_value == rule.threshold:
                alert_triggered = True

            alert_key = f"{rule.name}:{rule.service_name}"

            if alert_triggered:
                # Check if alert should fire based on duration
                if alert_key not in self._active_alerts:
                    self._active_alerts[alert_key] = current_time

                alert_start_time = self._active_alerts[alert_key]
                if (current_time - alert_start_time).total_seconds() >= rule.duration_seconds:
                    active_alerts.append({
                        'rule_name': rule.name,
                        'service_name': rule.service_name,
                        'metric_type': rule.metric_type,
                        'current_value': current_value,
                        'threshold': rule.threshold,
                        'severity': rule.severity,
                        'duration': (current_time - alert_start_time).total_seconds(),
                        'message': f"{rule.service_name} {rule.metric_type} {current_value} {rule.condition} {rule.threshold}"
                    })
            else:
                # Clear alert if condition is no longer met
                if alert_key in self._active_alerts:
                    del self._active_alerts[alert_key]

        return active_alerts

    def _get_current_metric_value(self, service_name: str, metric_type: str) -> Optional[float]:
        """Get the most recent metric value for a service"""
        # Look for recent metrics (within last 5 minutes)
        cutoff_time = datetime.utcnow() - timedelta(minutes=5)
        recent_metrics = [
            m for m in reversed(self.metrics)
            if m.service_name == service_name and
               m.metric_type == metric_type and
               m.timestamp > cutoff_time
        ]

        if not recent_metrics:
            return None

        # Return average of recent values
        return sum(m.value for m in recent_metrics) / len(recent_metrics)


# ========================================================================================
# PERFORMANCE MONITORING DECORATOR
# ========================================================================================

class ServicePerformanceMonitor:
    """
    Decorator and context manager for automatic service performance monitoring.

    Usage as decorator:
        @monitor_performance('UserService', 'create_user')
        async def create_user(self, user_data):
            # Service implementation

    Usage as context manager:
        async with ServicePerformanceMonitor('UserService', 'get_user'):
            # Monitored operation
    """

    def __init__(self, service_name: str, operation: str, collector: Optional[ServiceMetricsCollector] = None):
        self.service_name = service_name
        self.operation = operation
        self.collector = collector or get_global_metrics_collector()
        self.start_time = None
        self.end_time = None

    def __call__(self, func: Callable):
        """Decorator implementation"""
        if asyncio.iscoroutinefunction(func):
            async def async_wrapper(*args, **kwargs):
                async with self:
                    return await func(*args, **kwargs)
            return async_wrapper
        else:
            def sync_wrapper(*args, **kwargs):
                with self:
                    return func(*args, **kwargs)
            return sync_wrapper

    def __enter__(self):
        """Synchronous context manager entry"""
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Synchronous context manager exit"""
        self._record_metrics(exc_type is not None)

    async def __aenter__(self):
        """Asynchronous context manager entry"""
        self.start_time = time.time()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Asynchronous context manager exit"""
        self._record_metrics(exc_type is not None)

    def _record_metrics(self, had_error: bool):
        """Record performance metrics"""
        if self.start_time is None:
            return

        self.end_time = time.time()
        duration_ms = (self.end_time - self.start_time) * 1000

        # Record latency metric
        self.collector.record_metric(ServiceMetric(
            service_name=self.service_name,
            operation=self.operation,
            metric_type='latency',
            value=duration_ms,
            timestamp=datetime.utcnow(),
            labels={'status': 'error' if had_error else 'success'}
        ))

        # Record error metric if applicable
        if had_error:
            self.collector.record_metric(ServiceMetric(
                service_name=self.service_name,
                operation=self.operation,
                metric_type='error_rate',
                value=1.0,
                timestamp=datetime.utcnow()
            ))


# ========================================================================================
# HEALTH CHECK MONITORING
# ========================================================================================

class ServiceHealthMonitor:
    """
    Monitors service health status and provides health check capabilities.

    Features:
    - Periodic health checks
    - Health status aggregation
    - Dependency health tracking
    - Health history and trends
    """

    def __init__(self, check_interval_seconds: int = 30):
        self.check_interval_seconds = check_interval_seconds
        self.health_checks: Dict[str, Callable] = {}
        self.health_history: Dict[str, List[ServiceHealth]] = defaultdict(list)
        self.dependency_map: Dict[str, List[str]] = {}
        self._monitoring_task = None
        self._running = False

    def register_health_check(self, service_name: str, health_check_func: Callable):
        """Register a health check function for a service"""
        self.health_checks[service_name] = health_check_func
        logger.info(f"Registered health check for service: {service_name}")

    def register_dependency(self, service_name: str, dependency_name: str):
        """Register a service dependency"""
        if service_name not in self.dependency_map:
            self.dependency_map[service_name] = []
        self.dependency_map[service_name].append(dependency_name)
        logger.info(f"Registered dependency: {service_name} -> {dependency_name}")

    async def check_service_health(self, service_name: str) -> ServiceHealth:
        """Perform health check for a specific service"""
        health_check_func = self.health_checks.get(service_name)
        if not health_check_func:
            return ServiceHealth(
                service_name=service_name,
                status='unknown',
                last_check=datetime.utcnow(),
                response_time_ms=0,
                error_count=0,
                uptime_seconds=0,
                details={'error': 'No health check registered'}
            )

        start_time = time.time()
        try:
            # Execute health check
            health_data = await health_check_func() if asyncio.iscoroutinefunction(health_check_func) else health_check_func()
            response_time_ms = (time.time() - start_time) * 1000

            # Parse health check response
            if isinstance(health_data, dict):
                status = health_data.get('status', 'unknown')
                details = health_data
            else:
                status = 'healthy' if health_data else 'unhealthy'
                details = {'result': health_data}

            health = ServiceHealth(
                service_name=service_name,
                status=status,
                last_check=datetime.utcnow(),
                response_time_ms=response_time_ms,
                error_count=0,
                uptime_seconds=self._calculate_uptime(service_name),
                details=details
            )

        except Exception as e:
            logger.error(f"Health check failed for {service_name}: {e}")
            health = ServiceHealth(
                service_name=service_name,
                status='error',
                last_check=datetime.utcnow(),
                response_time_ms=(time.time() - start_time) * 1000,
                error_count=1,
                uptime_seconds=0,
                details={'error': str(e)}
            )

        # Store health history
        self.health_history[service_name].append(health)
        # Keep only recent history (last 100 checks)
        if len(self.health_history[service_name]) > 100:
            self.health_history[service_name] = self.health_history[service_name][-50:]

        return health

    async def get_overall_health(self) -> Dict[str, Any]:
        """Get overall system health status"""
        service_healths = {}
        overall_status = 'healthy'

        # Check all registered services
        for service_name in self.health_checks.keys():
            health = await self.check_service_health(service_name)
            service_healths[service_name] = health

            # Update overall status based on individual service status
            if health.status in ['error', 'critical']:
                overall_status = 'critical'
            elif health.status == 'unhealthy' and overall_status == 'healthy':
                overall_status = 'degraded'
            elif health.status == 'degraded' and overall_status == 'healthy':
                overall_status = 'degraded'

        return {
            'overall_status': overall_status,
            'timestamp': datetime.utcnow(),
            'services': service_healths,
            'summary': {
                'total_services': len(service_healths),
                'healthy_services': len([h for h in service_healths.values() if h.status == 'healthy']),
                'degraded_services': len([h for h in service_healths.values() if h.status == 'degraded']),
                'unhealthy_services': len([h for h in service_healths.values() if h.status in ['unhealthy', 'error', 'critical']])
            }
        }

    def _calculate_uptime(self, service_name: str) -> float:
        """Calculate service uptime based on health history"""
        history = self.health_history.get(service_name, [])
        if len(history) < 2:
            return 0.0

        # Calculate uptime as percentage of healthy checks in recent history
        recent_checks = history[-20:]  # Last 20 checks
        healthy_checks = len([h for h in recent_checks if h.status == 'healthy'])

        return (healthy_checks / len(recent_checks)) * 100 if recent_checks else 0.0

    async def start_monitoring(self):
        """Start continuous health monitoring"""
        if self._running:
            return

        self._running = True
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        logger.info(f"Started health monitoring with {self.check_interval_seconds}s interval")

    async def stop_monitoring(self):
        """Stop continuous health monitoring"""
        self._running = False
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
        logger.info("Stopped health monitoring")

    async def _monitoring_loop(self):
        """Continuous monitoring loop"""
        while self._running:
            try:
                # Check health of all registered services
                for service_name in self.health_checks.keys():
                    await self.check_service_health(service_name)

                # Wait for next check interval
                await asyncio.sleep(self.check_interval_seconds)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health monitoring loop: {e}")
                await asyncio.sleep(5)  # Brief pause before retrying


# ========================================================================================
# RESOURCE MONITORING
# ========================================================================================

class ResourceMonitor:
    """
    Monitors system resource utilization (CPU, memory, disk, network).

    Features:
    - Real-time resource monitoring
    - Process-specific monitoring
    - Resource usage trends
    - Resource-based alerting
    """

    def __init__(self, collector: Optional[ServiceMetricsCollector] = None):
        self.collector = collector or get_global_metrics_collector()
        self._monitoring_task = None
        self._running = False

    def get_current_resource_usage(self) -> Dict[str, float]:
        """Get current system resource usage"""
        try:
            # CPU usage
            cpu_percent = psutil.cpu_percent(interval=1)

            # Memory usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            memory_mb = memory.used / (1024 * 1024)

            # Disk usage
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent

            # Network I/O
            network = psutil.net_io_counters()

            return {
                'cpu_percent': cpu_percent,
                'memory_percent': memory_percent,
                'memory_mb': memory_mb,
                'disk_percent': disk_percent,
                'network_bytes_sent': network.bytes_sent,
                'network_bytes_recv': network.bytes_recv,
                'timestamp': time.time()
            }

        except Exception as e:
            logger.error(f"Error getting resource usage: {e}")
            return {}

    def get_process_resource_usage(self, process_name: Optional[str] = None) -> Dict[str, float]:
        """Get resource usage for specific process"""
        try:
            current_process = psutil.Process()

            # CPU and memory for current process
            cpu_percent = current_process.cpu_percent()
            memory_info = current_process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)

            return {
                'process_cpu_percent': cpu_percent,
                'process_memory_mb': memory_mb,
                'process_threads': current_process.num_threads(),
                'timestamp': time.time()
            }

        except Exception as e:
            logger.error(f"Error getting process resource usage: {e}")
            return {}

    async def record_resource_metrics(self, service_name: str = "system"):
        """Record current resource metrics"""
        timestamp = datetime.utcnow()

        # System resource metrics
        system_resources = self.get_current_resource_usage()
        for metric_name, value in system_resources.items():
            if metric_name != 'timestamp':
                self.collector.record_metric(ServiceMetric(
                    service_name=service_name,
                    operation='resource_monitoring',
                    metric_type=f'system_{metric_name}',
                    value=value,
                    timestamp=timestamp
                ))

        # Process resource metrics
        process_resources = self.get_process_resource_usage()
        for metric_name, value in process_resources.items():
            if metric_name != 'timestamp':
                self.collector.record_metric(ServiceMetric(
                    service_name=service_name,
                    operation='resource_monitoring',
                    metric_type=metric_name,
                    value=value,
                    timestamp=timestamp
                ))

    async def start_monitoring(self, interval_seconds: int = 60, service_name: str = "system"):
        """Start continuous resource monitoring"""
        if self._running:
            return

        self._running = True

        async def monitoring_loop():
            while self._running:
                try:
                    await self.record_resource_metrics(service_name)
                    await asyncio.sleep(interval_seconds)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error in resource monitoring: {e}")
                    await asyncio.sleep(5)

        self._monitoring_task = asyncio.create_task(monitoring_loop())
        logger.info(f"Started resource monitoring with {interval_seconds}s interval")

    async def stop_monitoring(self):
        """Stop continuous resource monitoring"""
        self._running = False
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
        logger.info("Stopped resource monitoring")


# ========================================================================================
# GLOBAL METRICS COLLECTOR SINGLETON
# ========================================================================================

_global_metrics_collector: Optional[ServiceMetricsCollector] = None


def get_global_metrics_collector() -> ServiceMetricsCollector:
    """Get or create the global metrics collector instance"""
    global _global_metrics_collector
    if _global_metrics_collector is None:
        _global_metrics_collector = ServiceMetricsCollector()
    return _global_metrics_collector


def setup_default_benchmarks():
    """Set up default performance benchmarks for common operations"""
    collector = get_global_metrics_collector()

    # Default benchmarks for common service operations
    default_benchmarks = [
        PerformanceBenchmark(
            service_name="InstrumentService",
            operation="create_instrument",
            latency_p95_ms=100.0,
            error_rate_threshold=0.01
        ),
        PerformanceBenchmark(
            service_name="InstrumentService",
            operation="get_instrument_by_id",
            latency_p95_ms=50.0,
            error_rate_threshold=0.005
        ),
        PerformanceBenchmark(
            service_name="InstrumentService",
            operation="list_instruments",
            latency_p95_ms=200.0,
            error_rate_threshold=0.01
        )
    ]

    for benchmark in default_benchmarks:
        collector.add_benchmark(benchmark)

    logger.info(f"Set up {len(default_benchmarks)} default benchmarks")


def setup_default_alerts():
    """Set up default alert rules"""
    collector = get_global_metrics_collector()

    # Default alert rules
    default_alerts = [
        AlertRule(
            name="High Error Rate",
            service_name="*",  # Apply to all services
            metric_type="error_rate",
            condition="greater_than",
            threshold=0.05,  # 5%
            duration_seconds=300,
            severity="warning"
        ),
        AlertRule(
            name="High CPU Usage",
            service_name="system",
            metric_type="system_cpu_percent",
            condition="greater_than",
            threshold=80.0,
            duration_seconds=300,
            severity="warning"
        ),
        AlertRule(
            name="High Memory Usage",
            service_name="system",
            metric_type="system_memory_percent",
            condition="greater_than",
            threshold=85.0,
            duration_seconds=300,
            severity="error"
        )
    ]

    for alert in default_alerts:
        collector.add_alert_rule(alert)

    logger.info(f"Set up {len(default_alerts)} default alert rules")


# ========================================================================================
# CONVENIENCE FUNCTIONS AND DECORATORS
# ========================================================================================

def monitor_performance(service_name: str, operation: str):
    """Decorator for automatic performance monitoring"""
    def decorator(func):
        return ServicePerformanceMonitor(service_name, operation)(func)
    return decorator


@asynccontextmanager
async def performance_monitor(service_name: str, operation: str):
    """Async context manager for performance monitoring"""
    async with ServicePerformanceMonitor(service_name, operation):
        yield


# ========================================================================================
# USAGE EXAMPLES
# ========================================================================================

"""
USAGE EXAMPLES:

1. Decorator Usage:
    @monitor_performance('UserService', 'create_user')
    async def create_user(self, user_data):
        # Implementation here
        return result

2. Context Manager Usage:
    async with performance_monitor('UserService', 'get_user'):
        user = await dao.get_user_by_id(user_id)
        return user

3. Manual Metrics:
    collector = get_global_metrics_collector()
    collector.record_metric(ServiceMetric(
        service_name='CustomService',
        operation='custom_operation',
        metric_type='business_metric',
        value=123.45,
        timestamp=datetime.utcnow()
    ))

4. Health Monitoring:
    health_monitor = ServiceHealthMonitor()
    health_monitor.register_health_check('MyService', my_health_check_func)
    await health_monitor.start_monitoring()

5. Resource Monitoring:
    resource_monitor = ResourceMonitor()
    await resource_monitor.start_monitoring(interval_seconds=30)

6. Getting Statistics:
    collector = get_global_metrics_collector()
    stats = collector.get_service_stats('UserService')
    violations = collector.get_benchmark_violations('UserService', 'create_user')
    alerts = collector.evaluate_alerts()
"""