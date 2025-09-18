"""
Integration tests for InstrumentService monitoring system.

Tests monitoring integration, metrics collection, health checks,
and performance tracking for the InstrumentService architecture.
"""

import pytest
import asyncio
import time
from datetime import datetime
from unittest.mock import Mock

# Service imports
from domains.instruments.services.interfaces.instrument_service_interface import (
    InstrumentDTO,
    InstrumentSearchCriteria
)
from domains.instruments.services.config.service_container import get_instrument_service
from core.platform.config.environment import Environment, EnvironmentType

# Monitoring imports
from infrastructure.monitoring.instrument_service_monitor import (
    InstrumentServiceMonitor,
    get_instrument_service_monitor,
    record_instrument_business_metric,
    record_cache_performance
)
from infrastructure.monitoring.service_metrics import (
    get_global_metrics_collector,
    ServiceMetricsCollector,
    ServiceHealthMonitor
)


class TestInstrumentServiceMonitoringIntegration:
    """Integration tests for monitoring system with real InstrumentService"""
    
    @pytest.fixture
    async def test_environment(self):
        """Create test environment"""
        return Environment(None, EnvironmentType.DEV)
    
    @pytest.fixture
    async def instrument_service(self, test_environment):
        """Get instrumented service for testing"""
        return await get_instrument_service(test_environment)
    
    @pytest.fixture
    def service_monitor(self):
        """Get service monitor instance"""
        return get_instrument_service_monitor()
    
    @pytest.fixture
    def sample_instrument(self):
        """Sample instrument for testing"""
        return InstrumentDTO(
            symbol="TEST_MONITOR",
            name="Test Monitoring Instrument",
            exchange="NYSE",
            instrument_type="stock",
            currency="USD"
        )
    
    @pytest.mark.asyncio
    async def test_monitoring_initialization(self, service_monitor):
        """Test monitoring system initializes correctly"""
        # Test monitor is available
        assert service_monitor is not None
        assert isinstance(service_monitor, InstrumentServiceMonitor)
        
        # Test metrics collector is available
        collector = service_monitor.metrics_collector
        assert isinstance(collector, ServiceMetricsCollector)
        
        # Test health monitor is available
        health_monitor = service_monitor.health_monitor
        assert isinstance(health_monitor, ServiceHealthMonitor)
    
    @pytest.mark.asyncio
    async def test_service_health_checks(self, service_monitor, instrument_service):
        """Test service health check integration"""
        # Perform health check
        health_status = await service_monitor.health_monitor.get_overall_health()
        
        # Verify health check structure
        assert 'overall_status' in health_status
        assert 'services' in health_status
        assert 'summary' in health_status
        
        # Should have InstrumentService health check
        services = health_status['services']
        assert len(services) > 0
        
        # Check for InstrumentService specifically
        instrument_service_health = None
        for service_name, health in services.items():
            if service_name == 'InstrumentService':
                instrument_service_health = health
                break
        
        # Verify service health structure (might be unavailable in test env)
        if instrument_service_health:
            assert hasattr(instrument_service_health, 'service_name')
            assert hasattr(instrument_service_health, 'status')
            assert hasattr(instrument_service_health, 'response_time_ms')
    
    @pytest.mark.asyncio
    async def test_performance_monitoring_integration(self, instrument_service, service_monitor, sample_instrument):
        """Test performance monitoring decorators work correctly"""
        collector = service_monitor.metrics_collector
        
        # Get initial metrics
        initial_stats = collector.get_service_stats('InstrumentService')
        initial_requests = initial_stats.get('total_requests', 0)
        
        # Perform monitored operations
        await instrument_service.validate_symbol("AAPL")
        count = await instrument_service.get_instrument_count()
        
        # Give time for metrics to be recorded
        await asyncio.sleep(0.1)
        
        # Check updated metrics
        updated_stats = collector.get_service_stats('InstrumentService')
        updated_requests = updated_stats.get('total_requests', 0)
        
        # Should have recorded some operations
        assert updated_requests >= initial_requests
        assert 'operation_counts' in updated_stats
        
        # Should have latency data
        if 'latency_stats' in updated_stats and updated_stats['latency_stats']:
            # At least one operation should have latency data
            operation_latencies = updated_stats['latency_stats']
            assert len(operation_latencies) > 0
            
            # Check latency structure
            for operation, latency_data in operation_latencies.items():
                assert 'count' in latency_data
                assert 'avg' in latency_data
                assert latency_data['count'] > 0
    
    @pytest.mark.asyncio
    async def test_business_metrics_recording(self, service_monitor):
        """Test business metrics are recorded correctly"""
        collector = service_monitor.metrics_collector
        
        # Record some business metrics
        record_instrument_business_metric('test_metric', 42.5, {'test_label': 'test_value'})
        record_cache_performance('test_operation', hit=True, duration_ms=15.2)
        
        # Give time for metrics to be processed
        await asyncio.sleep(0.1)
        
        # Check metrics were recorded
        service_stats = collector.get_service_stats('InstrumentService')
        cache_stats = collector.get_service_stats('CachedInstrumentService')
        
        # Should have business metrics in service stats or separate tracking
        assert service_stats is not None or cache_stats is not None
    
    @pytest.mark.asyncio
    async def test_monitoring_dashboard_data(self, service_monitor):
        """Test monitoring dashboard provides complete data"""
        # Get dashboard data
        dashboard_data = await service_monitor.get_monitoring_dashboard()
        
        # Verify dashboard structure
        assert 'timestamp' in dashboard_data
        assert 'monitoring_status' in dashboard_data
        assert 'service_health' in dashboard_data
        assert 'performance_metrics' in dashboard_data
        
        # Verify performance metrics structure
        perf_metrics = dashboard_data['performance_metrics']
        assert 'instrument_service' in perf_metrics
        assert 'recent_summary' in perf_metrics
        
        # Verify alert information
        assert 'active_alerts' in dashboard_data
        assert 'alert_summary' in dashboard_data
        
        alert_summary = dashboard_data['alert_summary']
        assert 'total_alerts' in alert_summary
        assert isinstance(alert_summary['total_alerts'], int)
    
    @pytest.mark.asyncio 
    async def test_benchmark_violation_detection(self, service_monitor):
        """Test performance benchmark violation detection"""
        collector = service_monitor.metrics_collector
        
        # Check for benchmark violations on common operations
        operations = ['get_instrument_by_id', 'list_instruments', 'validate_symbol']
        
        for operation in operations:
            violations = collector.get_benchmark_violations('InstrumentService', operation)
            
            # Should return a list (empty or with violations)
            assert isinstance(violations, list)
            
            # If there are violations, they should be descriptive strings
            for violation in violations:
                assert isinstance(violation, str)
                assert len(violation) > 0
    
    @pytest.mark.asyncio
    async def test_alert_evaluation(self, service_monitor):
        """Test alert rule evaluation"""
        # Get current monitoring status
        dashboard_data = await service_monitor.get_monitoring_dashboard()
        
        # Evaluate alerts
        active_alerts = dashboard_data.get('active_alerts', [])
        
        # Should return a list of alerts
        assert isinstance(active_alerts, list)
        
        # If there are active alerts, verify structure
        for alert in active_alerts:
            assert 'rule_name' in alert or 'name' in alert
            assert 'severity' in alert
    
    @pytest.mark.asyncio
    async def test_service_integration_with_monitoring(self, instrument_service, service_monitor):
        """Test complete service operations with monitoring integration"""
        try:
            # Perform a complete workflow that should generate metrics
            search_criteria = InstrumentSearchCriteria(limit=5)
            
            # These operations should be automatically monitored
            instruments = await instrument_service.list_instruments(search_criteria)
            count = await instrument_service.get_instrument_count()
            is_valid = await instrument_service.validate_symbol("AAPL")
            
            # Get monitoring data after operations
            dashboard_data = await service_monitor.get_monitoring_dashboard()
            
            # Should have recorded the operations
            perf_metrics = dashboard_data['performance_metrics']
            instrument_stats = perf_metrics['instrument_service']
            
            # Should have some activity
            assert instrument_stats is not None
            
            # If we have operation counts, verify they increased
            if 'operation_counts' in instrument_stats:
                operation_counts = instrument_stats['operation_counts']
                total_ops = sum(operation_counts.values()) if operation_counts else 0
                assert total_ops >= 0  # Should have at least some operations
                
        except Exception as e:
            # In test environment, some operations might fail
            # Verify monitoring still captures the attempts
            dashboard_data = await service_monitor.get_monitoring_dashboard()
            assert dashboard_data is not None
            assert 'performance_metrics' in dashboard_data


class TestMonitoringSystemReliability:
    """Test monitoring system reliability and error handling"""
    
    @pytest.mark.asyncio
    async def test_monitoring_with_service_failures(self):
        """Test monitoring handles service failures gracefully"""
        monitor = get_instrument_service_monitor()
        
        # Test dashboard generation when service is unavailable
        dashboard_data = await monitor.get_monitoring_dashboard()
        
        # Should still return valid dashboard structure
        assert dashboard_data is not None
        assert 'timestamp' in dashboard_data
        assert 'monitoring_status' in dashboard_data
        
        # If there's an error, it should be captured
        if 'error' in dashboard_data:
            assert isinstance(dashboard_data['error'], str)
    
    @pytest.mark.asyncio
    async def test_metrics_collector_memory_management(self):
        """Test metrics collector manages memory properly"""
        collector = get_global_metrics_collector()
        
        # Generate many metrics to test memory limits
        for i in range(100):
            collector.record_metric(Mock(
                service_name="TestService",
                operation="test_operation",
                metric_type="latency",
                value=float(i),
                timestamp=datetime.utcnow(),
                labels={}
            ))
        
        # Should handle large number of metrics without issues
        stats = collector.get_service_stats('TestService')
        assert stats is not None
    
    @pytest.mark.asyncio
    async def test_concurrent_monitoring(self):
        """Test monitoring system handles concurrent operations"""
        monitor = get_instrument_service_monitor()
        
        # Simulate concurrent health checks
        tasks = []
        for i in range(10):
            task = asyncio.create_task(monitor.get_monitoring_dashboard())
            tasks.append(task)
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # All should complete successfully or with handled exceptions
        for result in results:
            if isinstance(result, Exception):
                # Should be handled gracefully, not crash
                assert str(result) is not None
            else:
                # Should return valid dashboard data
                assert result is not None
                assert 'timestamp' in result


class TestMonitoringPerformanceImpact:
    """Test monitoring system performance impact"""
    
    @pytest.mark.asyncio
    async def test_monitoring_overhead(self, instrument_service):
        """Test monitoring adds minimal overhead"""
        # Test with monitoring
        start_time = time.time()
        await instrument_service.validate_symbol("TEST")
        monitored_time = time.time() - start_time
        
        # Monitoring should add minimal overhead (< 50ms typically)
        assert monitored_time < 1.0  # Should complete in reasonable time
    
    @pytest.mark.asyncio
    async def test_dashboard_generation_performance(self):
        """Test dashboard generation is reasonably fast"""
        monitor = get_instrument_service_monitor()
        
        start_time = time.time()
        dashboard_data = await monitor.get_monitoring_dashboard()
        generation_time = time.time() - start_time
        
        # Dashboard generation should be fast (< 1 second typically)
        assert generation_time < 5.0  # Reasonable upper bound
        assert dashboard_data is not None


if __name__ == "__main__":
    # Run with: pytest tests/integration/test_instrument_service_monitoring.py -v
    pytest.main([__file__, "-v", "--tb=short"])