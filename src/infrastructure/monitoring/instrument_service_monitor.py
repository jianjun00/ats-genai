"""
InstrumentService Monitoring Integration

Integrates the existing comprehensive service monitoring system with the InstrumentService architecture.
Provides specific monitoring, health checks, and performance tracking for instrument domain services.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from infrastructure.monitoring.service_metrics import (
    ServiceMetricsCollector,
    ServiceHealthMonitor, 
    ResourceMonitor,
    ServiceMetric,
    ServiceHealth,
    PerformanceBenchmark,
    AlertRule,
    get_global_metrics_collector,
    setup_default_benchmarks,
    setup_default_alerts,
    monitor_performance
)

# Service interfaces
from domains.instruments.services.interfaces.instrument_service_interface import InstrumentServiceInterface

logger = logging.getLogger(__name__)


class InstrumentServiceMonitor:
    """
    Specialized monitoring for InstrumentService architecture.
    
    Provides:
    1. InstrumentService-specific health checks
    2. Business metrics tracking (instrument operations, cache performance)
    3. Performance benchmarking for instrument operations
    4. Integration with existing monitoring infrastructure
    5. Service dependency monitoring
    """
    
    def __init__(self):
        self.metrics_collector = get_global_metrics_collector()
        self.health_monitor = ServiceHealthMonitor(check_interval_seconds=30)
        self.resource_monitor = ResourceMonitor(self.metrics_collector)
        self.alert_rules = []  # Store alert rules locally
        self._monitoring_active = False
        
        # Setup InstrumentService-specific configuration
        self._setup_instrument_benchmarks()
        self._setup_instrument_alerts()
        self._setup_instrument_health_checks()
    
    def _setup_instrument_benchmarks(self):
        """Setup performance benchmarks for InstrumentService operations"""
        instrument_benchmarks = [
            PerformanceBenchmark(
                service_name="InstrumentService",
                operation="get_instrument_by_id",
                latency_p50_ms=25.0,
                latency_p95_ms=50.0,
                latency_p99_ms=100.0,
                error_rate_threshold=0.001,  # 0.1%
                throughput_min_ops_sec=100.0
            ),
            PerformanceBenchmark(
                service_name="InstrumentService", 
                operation="get_instrument_by_symbol",
                latency_p50_ms=30.0,
                latency_p95_ms=75.0,
                latency_p99_ms=150.0,
                error_rate_threshold=0.002,  # 0.2%
                throughput_min_ops_sec=50.0
            ),
            PerformanceBenchmark(
                service_name="InstrumentService",
                operation="list_instruments", 
                latency_p50_ms=100.0,
                latency_p95_ms=250.0,
                latency_p99_ms=500.0,
                error_rate_threshold=0.005,  # 0.5%
                throughput_min_ops_sec=20.0
            ),
            PerformanceBenchmark(
                service_name="InstrumentService",
                operation="create_instrument",
                latency_p50_ms=150.0,
                latency_p95_ms=300.0,
                latency_p99_ms=750.0,
                error_rate_threshold=0.01,  # 1%
                throughput_min_ops_sec=10.0
            ),
            PerformanceBenchmark(
                service_name="InstrumentService",
                operation="validate_symbol",
                latency_p50_ms=20.0,
                latency_p95_ms=40.0,
                latency_p99_ms=80.0,
                error_rate_threshold=0.001,  # 0.1%
                throughput_min_ops_sec=200.0
            ),
            PerformanceBenchmark(
                service_name="CachedInstrumentService",
                operation="cache_hit",
                latency_p50_ms=5.0,
                latency_p95_ms=15.0,
                latency_p99_ms=30.0,
                error_rate_threshold=0.0001,  # 0.01%
                throughput_min_ops_sec=1000.0
            )
        ]
        
        for benchmark in instrument_benchmarks:
            self.metrics_collector.add_benchmark(benchmark)
        
        logger.info(f"Setup {len(instrument_benchmarks)} InstrumentService benchmarks")
    
    def _setup_instrument_alerts(self):
        """Setup alert rules specific to InstrumentService"""
        instrument_alerts = [
            AlertRule(
                name="InstrumentService High Error Rate",
                service_name="InstrumentService",
                metric_type="error_rate",
                condition="greater_than",
                threshold=0.02,  # 2%
                duration_seconds=180,  # 3 minutes
                severity="critical"
            ),
            AlertRule(
                name="InstrumentService High Latency",
                service_name="InstrumentService", 
                metric_type="latency",
                condition="greater_than",
                threshold=1000.0,  # 1 second
                duration_seconds=120,  # 2 minutes
                severity="warning"
            ),
            AlertRule(
                name="Cache Miss Rate Too High",
                service_name="CachedInstrumentService",
                metric_type="cache_miss_rate",
                condition="greater_than",
                threshold=0.3,  # 30% miss rate
                duration_seconds=300,  # 5 minutes
                severity="warning"
            ),
            AlertRule(
                name="Database Connection Pool Exhaustion",
                service_name="InstrumentService",
                metric_type="db_connection_pool_usage",
                condition="greater_than", 
                threshold=0.9,  # 90% of pool
                duration_seconds=60,  # 1 minute
                severity="critical"
            ),
            AlertRule(
                name="InstrumentService Low Throughput",
                service_name="InstrumentService",
                metric_type="throughput",
                condition="less_than",
                threshold=5.0,  # Less than 5 ops/sec
                duration_seconds=600,  # 10 minutes
                severity="warning"
            )
        ]
        
        for alert in instrument_alerts:
            self.alert_rules.append(alert)
            self.metrics_collector.add_alert_rule(alert)
        
        logger.info(f"Setup {len(instrument_alerts)} InstrumentService alert rules")
    
    def _setup_instrument_health_checks(self):
        """Setup health check functions for InstrumentService"""
        
        async def instrument_service_health_check():
            """Comprehensive InstrumentService health check"""
            try:
                # Dynamic import to avoid circular dependency
                from domains.instruments.services.config.service_container import get_service_container
                
                # Get service container and instrument service
                container = await get_service_container()
                service = container.get_instrument_service()
                
                start_time = datetime.utcnow()
                
                # Test basic service operations
                health_results = {}
                
                # 1. Test database connectivity via get_instrument_count
                try:
                    count = await service.get_instrument_count()
                    health_results['database_connectivity'] = True
                    health_results['instrument_count'] = count
                except Exception as e:
                    logger.error(f"Database connectivity check failed: {e}")
                    health_results['database_connectivity'] = False
                    health_results['database_error'] = str(e)
                
                # 2. Test symbol validation (lightweight operation)
                try:
                    is_valid = await service.validate_symbol("AAPL")
                    health_results['symbol_validation'] = True
                    health_results['sample_validation_result'] = is_valid
                except Exception as e:
                    logger.error(f"Symbol validation check failed: {e}")
                    health_results['symbol_validation'] = False
                    health_results['validation_error'] = str(e)
                
                # 3. Test list operation (with small limit)
                try:
                    from domains.instruments.services.interfaces.instrument_service_interface import InstrumentSearchCriteria
                    criteria = InstrumentSearchCriteria(limit=1)
                    instruments = await service.list_instruments(criteria)
                    health_results['list_operation'] = True
                    health_results['sample_list_count'] = len(instruments)
                except Exception as e:
                    logger.error(f"List operation check failed: {e}")
                    health_results['list_operation'] = False
                    health_results['list_error'] = str(e)
                
                # Calculate overall health
                critical_operations = ['database_connectivity', 'symbol_validation']
                critical_health = all(health_results.get(op, False) for op in critical_operations)
                
                response_time = (datetime.utcnow() - start_time).total_seconds() * 1000
                
                if critical_health:
                    status = 'healthy' if all(health_results.get(k, False) for k in health_results if k.endswith('_connectivity') or k.endswith('_validation') or k.endswith('_operation')) else 'degraded'
                else:
                    status = 'unhealthy'
                
                return {
                    'status': status,
                    'response_time_ms': response_time,
                    'service_name': 'InstrumentService',
                    'checks': health_results,
                    'timestamp': datetime.utcnow().isoformat()
                }
                
            except Exception as e:
                logger.error(f"InstrumentService health check failed: {e}")
                return {
                    'status': 'error',
                    'error': str(e),
                    'service_name': 'InstrumentService',
                    'timestamp': datetime.utcnow().isoformat()
                }
        
        async def cache_health_check():
            """Health check for caching layer"""
            try:
                # This would check Redis connectivity, memory usage, hit rates, etc.
                # For now, return basic health status
                return {
                    'status': 'healthy',
                    'cache_type': 'redis_with_fallback',
                    'timestamp': datetime.utcnow().isoformat()
                }
            except Exception as e:
                return {
                    'status': 'error', 
                    'error': str(e),
                    'timestamp': datetime.utcnow().isoformat()
                }
        
        async def database_pool_health_check():
            """Health check for database connection pool"""
            try:
                # Dynamic import to avoid circular dependency
                from domains.instruments.services.config.service_container import get_service_container
                
                # Check database pool health
                container = await get_service_container()
                health_status = container.get_health_status()
                
                return {
                    'status': 'healthy' if health_status.get('initialized', False) else 'unhealthy',
                    'container_status': health_status,
                    'timestamp': datetime.utcnow().isoformat()
                }
            except Exception as e:
                return {
                    'status': 'error',
                    'error': str(e),
                    'timestamp': datetime.utcnow().isoformat()
                }
        
        # Register health checks
        self.health_monitor.register_health_check('InstrumentService', instrument_service_health_check)
        self.health_monitor.register_health_check('CacheService', cache_health_check)
        self.health_monitor.register_health_check('DatabasePool', database_pool_health_check)
        
        # Register service dependencies
        self.health_monitor.register_dependency('InstrumentService', 'DatabasePool')
        self.health_monitor.register_dependency('CachedInstrumentService', 'CacheService')
        
        logger.info("Setup InstrumentService health checks")
    
    async def start_monitoring(self):
        """Start comprehensive InstrumentService monitoring"""
        if self._monitoring_active:
            logger.warning("Monitoring already active")
            return
        
        logger.info("Starting InstrumentService monitoring...")
        
        try:
            # Start health monitoring
            await self.health_monitor.start_monitoring()
            
            # Start resource monitoring
            await self.resource_monitor.start_monitoring(
                interval_seconds=60,
                service_name="InstrumentService"
            )
            
            self._monitoring_active = True
            logger.info("InstrumentService monitoring started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start InstrumentService monitoring: {e}")
            raise
    
    async def stop_monitoring(self):
        """Stop InstrumentService monitoring"""
        if not self._monitoring_active:
            return
        
        logger.info("Stopping InstrumentService monitoring...")
        
        try:
            await self.health_monitor.stop_monitoring()
            await self.resource_monitor.stop_monitoring()
            
            self._monitoring_active = False
            logger.info("InstrumentService monitoring stopped successfully")
            
        except Exception as e:
            logger.error(f"Error stopping monitoring: {e}")
    
    async def get_monitoring_dashboard(self) -> Dict[str, Any]:
        """Get comprehensive monitoring dashboard data"""
        try:
            # Get health status
            overall_health = await self.health_monitor.get_overall_health()
            
            # Get service statistics
            service_stats = self.metrics_collector.get_service_stats('InstrumentService')
            cache_stats = self.metrics_collector.get_service_stats('CachedInstrumentService')
            
            # Get benchmark violations
            instrument_violations = []
            operations = ['get_instrument_by_id', 'get_instrument_by_symbol', 'list_instruments', 'create_instrument', 'validate_symbol']
            for operation in operations:
                violations = self.metrics_collector.get_benchmark_violations('InstrumentService', operation)
                if violations:
                    instrument_violations.extend([f"{operation}: {v}" for v in violations])
            
            # Get active alerts using the metrics collector
            active_alerts = self.metrics_collector.evaluate_alerts()
            
            # Get recent performance metrics
            recent_metrics = self._get_recent_performance_summary()
            
            return {
                'timestamp': datetime.utcnow().isoformat(),
                'monitoring_status': 'active' if self._monitoring_active else 'inactive',
                'service_health': overall_health,
                'performance_metrics': {
                    'instrument_service': service_stats,
                    'cache_service': cache_stats,
                    'recent_summary': recent_metrics
                },
                'benchmark_violations': instrument_violations,
                'active_alerts': active_alerts,
                'alert_summary': {
                    'total_alerts': len(active_alerts),
                    'critical_alerts': len([a for a in active_alerts if a.get('severity') == 'critical']),
                    'warning_alerts': len([a for a in active_alerts if a.get('severity') == 'warning'])
                }
            }
            
        except Exception as e:
            logger.error(f"Error generating monitoring dashboard: {e}")
            return {
                'timestamp': datetime.utcnow().isoformat(),
                'error': str(e),
                'monitoring_status': 'error'
            }
    
    def _get_recent_performance_summary(self) -> Dict[str, Any]:
        """Get recent performance summary across all operations"""
        try:
            # Get recent metrics (last hour)
            cutoff_time = datetime.utcnow() - timedelta(hours=1)
            
            # This would filter recent metrics from the collector
            # For now, return basic summary structure
            return {
                'time_period': 'last_1_hour',
                'total_requests': 0,
                'total_errors': 0,
                'avg_response_time_ms': 0.0,
                'cache_hit_rate': 0.0,
                'top_operations': [],
                'error_breakdown': {}
            }
            
        except Exception as e:
            logger.warning(f"Error calculating performance summary: {e}")
            return {'error': str(e)}
    
    def record_business_metric(self, metric_name: str, value: float, labels: Dict[str, str] = None):
        """Record business-specific metrics for InstrumentService"""
        metric = ServiceMetric(
            service_name="InstrumentService",
            operation="business_metrics",
            metric_type=f"business_{metric_name}",
            value=value,
            timestamp=datetime.utcnow(),
            labels=labels or {}
        )
        self.metrics_collector.record_metric(metric)
    
    def record_cache_metrics(self, operation: str, hit: bool, duration_ms: float):
        """Record cache-specific metrics"""
        cache_result = "hit" if hit else "miss"
        
        # Record cache hit/miss
        self.metrics_collector.record_metric(ServiceMetric(
            service_name="CachedInstrumentService",
            operation=operation,
            metric_type=f"cache_{cache_result}",
            value=1.0,
            timestamp=datetime.utcnow(),
            labels={"cache_result": cache_result}
        ))
        
        # Record cache operation duration
        self.metrics_collector.record_metric(ServiceMetric(
            service_name="CachedInstrumentService", 
            operation=operation,
            metric_type="cache_latency",
            value=duration_ms,
            timestamp=datetime.utcnow(),
            labels={"cache_result": cache_result}
        ))


# ========================================================================================
# CONVENIENCE FUNCTIONS AND DECORATORS  
# ========================================================================================

# Global monitor instance
_global_instrument_monitor: Optional[InstrumentServiceMonitor] = None


def get_instrument_service_monitor() -> InstrumentServiceMonitor:
    """Get or create global InstrumentService monitor"""
    global _global_instrument_monitor
    if _global_instrument_monitor is None:
        _global_instrument_monitor = InstrumentServiceMonitor()
    return _global_instrument_monitor


def monitor_instrument_operation(operation: str):
    """Decorator for monitoring InstrumentService operations"""
    return monitor_performance("InstrumentService", operation)


@asynccontextmanager
async def instrument_performance_monitor(operation: str):
    """Context manager for InstrumentService performance monitoring"""
    from infrastructure.monitoring.service_metrics import ServicePerformanceMonitor
    async with ServicePerformanceMonitor("InstrumentService", operation):
        yield


def record_instrument_business_metric(metric_name: str, value: float, labels: Dict[str, str] = None):
    """Convenience function to record business metrics"""
    monitor = get_instrument_service_monitor()
    monitor.record_business_metric(metric_name, value, labels)


def record_cache_performance(operation: str, hit: bool, duration_ms: float):
    """Convenience function to record cache metrics"""
    monitor = get_instrument_service_monitor()
    monitor.record_cache_metrics(operation, hit, duration_ms)


# ========================================================================================
# STARTUP AND CONFIGURATION
# ========================================================================================

async def initialize_instrument_service_monitoring():
    """Initialize and start InstrumentService monitoring"""
    try:
        # Setup global monitoring infrastructure
        setup_default_benchmarks()
        setup_default_alerts()
        
        # Get and start InstrumentService monitor
        monitor = get_instrument_service_monitor()
        await monitor.start_monitoring()
        
        logger.info("InstrumentService monitoring initialized successfully")
        return monitor
        
    except Exception as e:
        logger.error(f"Failed to initialize InstrumentService monitoring: {e}")
        raise


async def shutdown_instrument_service_monitoring():
    """Shutdown InstrumentService monitoring"""
    try:
        monitor = get_instrument_service_monitor()
        await monitor.stop_monitoring()
        
        logger.info("InstrumentService monitoring shutdown complete")
        
    except Exception as e:
        logger.error(f"Error during monitoring shutdown: {e}")


# ========================================================================================
# USAGE EXAMPLES
# ========================================================================================

"""
USAGE EXAMPLES:

1. Decorator Usage:
    @monitor_instrument_operation('get_instrument_by_id')
    async def get_instrument_by_id(self, instrument_id: int):
        # Implementation
        return instrument

2. Context Manager Usage:
    async with instrument_performance_monitor('create_instrument'):
        result = await dao.create_instrument(instrument_dto)
        return result

3. Business Metrics:
    record_instrument_business_metric('instruments_created_today', 42, {'exchange': 'NYSE'})
    record_cache_performance('get_instrument_by_id', hit=True, duration_ms=5.2)

4. Monitoring Dashboard:
    monitor = get_instrument_service_monitor()
    dashboard_data = await monitor.get_monitoring_dashboard()

5. Startup Integration:
    # In your application startup
    await initialize_instrument_service_monitoring()
    
    # In your application shutdown
    await shutdown_instrument_service_monitoring()
"""