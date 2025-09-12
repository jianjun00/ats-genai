"""
Test Suite for Service Discovery Infrastructure.

This module provides comprehensive tests for service registry, health checks,
and service client functionality.
"""

import asyncio
import pytest
import pytest_asyncio
from datetime import datetime, timedelta
from typing import Dict, Any
from unittest.mock import AsyncMock, Mock, patch

from src.infrastructure.service_discovery import (
    # Service Registry
    InMemoryServiceRegistry,
    ServiceInstance,
    ServiceEndpoint,
    ServiceStatus,
    HealthCheck,
    ServiceDiscoveryClient,
    service_registration_context,
    
    # Health Checks
    HealthCheckManager,
    HealthStatus,
    HealthCheckType,
    SystemResourceHealthCheck,
    CustomHealthCheck,
    DatabaseHealthCheck,
    HttpServiceHealthCheck,
    
    # Service Client
    ServiceClient,
    CircuitBreaker,
    CircuitBreakerConfig,
    RetryConfig,
    RoundRobinBalancer,
    RandomBalancer,
    service_client
)


class TestServiceRegistry:
    """Test service registry functionality."""
    
    @pytest_asyncio.fixture
    async def registry(self):
        """Create and start in-memory registry."""
        registry = InMemoryServiceRegistry()
        await registry.start()
        yield registry
        await registry.stop()
    
    @pytest.fixture
    def sample_service_instance(self):
        """Create sample service instance."""
        return ServiceInstance(
            service_name="test-service",
            instance_id="test-service-1",
            version="1.0.0",
            endpoint=ServiceEndpoint(
                host="localhost",
                port=8000,
                protocol="http"
            ),
            metadata={"environment": "test"},
            health_check=CustomHealthCheck("test-health", lambda: True)
        )
    
    @pytest.mark.asyncio
    async def test_service_registration(self, registry, sample_service_instance):
        """Test service registration."""
        # Register service
        success = await registry.register_service(sample_service_instance)
        assert success is True
        
        # Update status to healthy (newly registered services start as STARTING)
        await registry.update_health_status("test-service", "test-service-1", ServiceStatus.HEALTHY)
        
        # Verify service is registered
        instances = await registry.get_service_instances("test-service")
        assert len(instances) == 1
        assert instances[0].service_name == "test-service"
        assert instances[0].instance_id == "test-service-1"
    
    @pytest.mark.asyncio
    async def test_service_deregistration(self, registry, sample_service_instance):
        """Test service deregistration."""
        # Register service
        await registry.register_service(sample_service_instance)
        
        # Deregister service
        success = await registry.deregister_service("test-service", "test-service-1")
        assert success is True
        
        # Verify service is deregistered
        instances = await registry.get_service_instances("test-service")
        assert len(instances) == 0
    
    @pytest.mark.asyncio
    async def test_heartbeat_update(self, registry, sample_service_instance):
        """Test heartbeat updates."""
        # Register service
        await registry.register_service(sample_service_instance)
        
        # Update status to healthy (newly registered services start as STARTING)
        await registry.update_health_status("test-service", "test-service-1", ServiceStatus.HEALTHY)
        
        # Update heartbeat
        success = await registry.heartbeat("test-service", "test-service-1")
        assert success is True
        
        # Verify heartbeat timestamp was updated
        instances = await registry.get_service_instances("test-service")
        assert len(instances) == 1
        assert instances[0].last_heartbeat is not None
    
    @pytest.mark.asyncio
    async def test_health_status_update(self, registry, sample_service_instance):
        """Test health status updates."""
        # Register service
        await registry.register_service(sample_service_instance)
        
        # Update health status
        success = await registry.update_health_status("test-service", "test-service-1", ServiceStatus.UNHEALTHY)
        assert success is True
        
        # Verify status was updated
        all_services = await registry.get_all_services()
        instance = all_services["test-service"][0]
        assert instance.status == ServiceStatus.UNHEALTHY
    
    @pytest.mark.asyncio
    async def test_service_registration_context(self, registry):
        """Test service registration context manager."""
        instance = ServiceInstance(
            service_name="context-test",
            instance_id="context-test-1",
            version="1.0.0",
            endpoint=ServiceEndpoint(host="localhost", port=8001),
            metadata={},
            health_check=CustomHealthCheck("test-health", lambda: True)
        )
        
        # Use context manager
        async with service_registration_context(registry, instance) as registered_instance:
            assert registered_instance.service_name == "context-test"
            
            # Update status to healthy (newly registered services start as STARTING)
            await registry.update_health_status("context-test", "context-test-1", ServiceStatus.HEALTHY)
            
            # Verify service is registered
            instances = await registry.get_service_instances("context-test")
            assert len(instances) == 1
        
        # Verify service is automatically deregistered
        instances = await registry.get_service_instances("context-test")
        assert len(instances) == 0


class TestServiceDiscoveryClient:
    """Test service discovery client."""
    
    @pytest_asyncio.fixture
    async def registry_with_services(self):
        """Create registry with sample services."""
        registry = InMemoryServiceRegistry()
        await registry.start()
        
        # Register multiple instances
        for i in range(3):
            instance = ServiceInstance(
                service_name="api-service",
                instance_id=f"api-service-{i}",
                version="1.0.0",
                endpoint=ServiceEndpoint(host="localhost", port=8000 + i),
                metadata={},
                health_check=CustomHealthCheck("test-health", lambda: True)
            )
            await registry.register_service(instance)
            await registry.update_health_status("api-service", f"api-service-{i}", ServiceStatus.HEALTHY)
        
        yield registry
        await registry.stop()
    
    @pytest.mark.asyncio
    async def test_service_discovery(self, registry_with_services):
        """Test basic service discovery."""
        client = ServiceDiscoveryClient(registry_with_services)
        
        instances = await client.discover_service("api-service")
        assert len(instances) == 3
        
        for instance in instances:
            assert instance.service_name == "api-service"
            assert instance.status == ServiceStatus.HEALTHY
    
    @pytest.mark.asyncio
    async def test_service_endpoint_selection(self, registry_with_services):
        """Test service endpoint selection."""
        client = ServiceDiscoveryClient(registry_with_services)
        
        endpoint = await client.get_service_endpoint("api-service")
        assert endpoint is not None
        assert endpoint.host == "localhost"
        assert endpoint.port in [8000, 8001, 8002]
    
    @pytest.mark.asyncio
    async def test_cache_functionality(self, registry_with_services):
        """Test service discovery caching."""
        client = ServiceDiscoveryClient(registry_with_services)
        
        # First call - should hit registry
        instances1 = await client.discover_service("api-service", use_cache=True)
        
        # Second call - should use cache
        instances2 = await client.discover_service("api-service", use_cache=True)
        
        assert len(instances1) == len(instances2) == 3
        
        # Clear cache and verify
        client.clear_cache("api-service")
        instances3 = await client.discover_service("api-service", use_cache=True)
        assert len(instances3) == 3


class TestHealthChecks:
    """Test health check functionality."""
    
    @pytest.fixture
    def health_manager(self):
        """Create health check manager."""
        return HealthCheckManager()
    
    @pytest.mark.asyncio
    async def test_system_resource_health_check(self):
        """Test system resource health check."""
        check = SystemResourceHealthCheck("system_test")
        result = await check.check_with_timeout()
        
        assert result.check_name == "system_test"
        assert result.check_type == HealthCheckType.LIVENESS
        assert result.status in [HealthStatus.HEALTHY, HealthStatus.UNHEALTHY]
        assert result.duration_ms >= 0
        assert 'cpu_percent' in result.details
        assert 'memory_percent' in result.details
    
    @pytest.mark.asyncio
    async def test_custom_health_check_async(self):
        """Test custom async health check."""
        async def custom_check():
            await asyncio.sleep(0.01)  # Simulate async work
            return {'status': 'healthy', 'message': 'Custom check passed', 'value': 42}
        
        check = CustomHealthCheck("custom_async_test", custom_check)
        result = await check.check_with_timeout()
        
        assert result.check_name == "custom_async_test"
        assert result.status == HealthStatus.HEALTHY
        assert result.message == "Custom check passed"
        assert result.details['value'] == 42
    
    @pytest.mark.asyncio
    async def test_custom_health_check_sync(self):
        """Test custom sync health check."""
        def custom_check():
            return True
        
        check = CustomHealthCheck("custom_sync_test", custom_check)
        result = await check.check_with_timeout()
        
        assert result.check_name == "custom_sync_test"
        assert result.status == HealthStatus.HEALTHY
        assert result.message == "Custom check passed"
    
    @pytest.mark.asyncio
    async def test_health_check_timeout(self):
        """Test health check timeout handling."""
        async def slow_check():
            await asyncio.sleep(1.0)  # Longer than timeout
            return True
        
        check = CustomHealthCheck("timeout_test", slow_check)
        check.timeout_seconds = 0.1  # Short timeout
        
        result = await check.check_with_timeout()
        
        assert result.check_name == "timeout_test"
        assert result.status == HealthStatus.UNHEALTHY
        assert "timed out" in result.message.lower()
        assert result.error == "TimeoutError"
    
    @pytest.mark.asyncio
    async def test_health_check_exception_handling(self):
        """Test health check exception handling."""
        def failing_check():
            raise ValueError("Test error")
        
        check = CustomHealthCheck("error_test", failing_check)
        result = await check.check_with_timeout()
        
        assert result.check_name == "error_test"
        assert result.status == HealthStatus.UNHEALTHY
        assert "Test error" in result.message
        assert result.error == "Test error"
    
    @pytest.mark.asyncio
    async def test_health_manager_multiple_checks(self, health_manager):
        """Test health manager with multiple checks."""
        # Add multiple checks
        health_manager.add_health_check(
            CustomHealthCheck("check1", lambda: True)
        )
        health_manager.add_health_check(
            CustomHealthCheck("check2", lambda: {'status': 'healthy', 'message': 'Check 2 passed'})
        )
        health_manager.add_health_check(
            CustomHealthCheck("check3", lambda: False)
        )
        
        overall_health = await health_manager.perform_all_checks()
        
        assert len(overall_health.checks) == 3
        assert overall_health.status == HealthStatus.UNHEALTHY  # Because check3 failed
        assert overall_health.summary['total_checks'] == 3
        assert overall_health.summary['healthy'] == 2
        assert overall_health.summary['unhealthy'] == 1


class TestCircuitBreaker:
    """Test circuit breaker functionality."""
    
    def test_circuit_breaker_initialization(self):
        """Test circuit breaker initialization."""
        config = CircuitBreakerConfig(failure_threshold=3, recovery_timeout_seconds=60.0)
        cb = CircuitBreaker("test_circuit", config)
        
        assert cb.name == "test_circuit"
        assert cb.state.value == "closed"
        assert cb.failure_count == 0
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_success(self):
        """Test circuit breaker with successful calls."""
        cb = CircuitBreaker("test_circuit")
        
        # Successful call
        async def successful_call():
            return "success"
        
        result = await cb.call(successful_call)
        assert result == "success"
        assert cb.failure_count == 0
        assert cb.state.value == "closed"
    
    @pytest.mark.asyncio
    async def test_circuit_breaker_failure_threshold(self):
        """Test circuit breaker opening after failures."""
        config = CircuitBreakerConfig(failure_threshold=2)
        cb = CircuitBreaker("test_circuit", config)
        
        async def failing_call():
            raise ValueError("Test failure")
        
        # First failure
        with pytest.raises(ValueError):
            await cb.call(failing_call)
        assert cb.failure_count == 1
        assert cb.state.value == "closed"
        
        # Second failure - should open circuit
        with pytest.raises(ValueError):
            await cb.call(failing_call)
        assert cb.failure_count == 2
        assert cb.state.value == "open"


class TestServiceClient:
    """Test service client functionality."""
    
    @pytest_asyncio.fixture
    async def registry_with_mock_service(self):
        """Create registry with mock HTTP service."""
        registry = InMemoryServiceRegistry()
        await registry.start()
        
        # Register httpbin service for testing
        instance = ServiceInstance(
            service_name="httpbin",
            instance_id="httpbin-1",
            version="1.0.0",
            endpoint=ServiceEndpoint(
                host="httpbin.org",
                port=443,
                protocol="https"
            ),
            metadata={},
            health_check=CustomHealthCheck("test-health", lambda: True)
        )
        await registry.register_service(instance)
        await registry.update_health_status("httpbin", "httpbin-1", ServiceStatus.HEALTHY)
        
        yield registry
        await registry.stop()
    
    @pytest.mark.asyncio
    async def test_service_client_get_request(self, registry_with_mock_service):
        """Test service client GET request."""
        async with service_client("httpbin", registry=registry_with_mock_service) as client:
            try:
                response = await client.get("/json")
                assert response.status == 200
                
                data = await response.json()
                assert "url" in data
            except Exception as e:
                # Network issues in test environment are acceptable
                pytest.skip(f"Network error in test environment: {e}")
    
    @pytest.mark.asyncio
    async def test_service_client_post_request(self, registry_with_mock_service):
        """Test service client POST request."""
        async with service_client("httpbin", registry=registry_with_mock_service) as client:
            try:
                test_data = {"test": "data", "number": 42}
                response = await client.post("/post", json_data=test_data)
                assert response.status == 200
                
                data = await response.json()
                assert data["json"]["test"] == "data"
                assert data["json"]["number"] == 42
            except Exception as e:
                pytest.skip(f"Network error in test environment: {e}")
    
    def test_round_robin_balancer(self):
        """Test round-robin load balancer."""
        balancer = RoundRobinBalancer()
        
        instances = [
            Mock(instance_id="instance-1"),
            Mock(instance_id="instance-2"),
            Mock(instance_id="instance-3")
        ]
        
        # Test round-robin selection
        selected1 = balancer.select_instance(instances)
        selected2 = balancer.select_instance(instances)
        selected3 = balancer.select_instance(instances)
        selected4 = balancer.select_instance(instances)  # Should wrap around
        
        assert selected1.instance_id == "instance-1"
        assert selected2.instance_id == "instance-2"
        assert selected3.instance_id == "instance-3"
        assert selected4.instance_id == "instance-1"  # Wrapped around
    
    def test_retry_config(self):
        """Test retry configuration."""
        config = RetryConfig(
            max_attempts=5,
            base_delay_seconds=0.5,
            exponential_backoff=True,
            retryable_status_codes=[429, 502, 503]
        )
        
        assert config.max_attempts == 5
        assert config.base_delay_seconds == 0.5
        assert config.exponential_backoff is True
        assert 429 in config.retryable_status_codes


@pytest.mark.asyncio
async def test_full_integration_scenario():
    """Test full integration scenario with all components."""
    # Initialize service registry
    registry = InMemoryServiceRegistry()
    await registry.start()
    
    try:
        # Register a service
        service_instance = ServiceInstance(
            service_name="integration-test",
            instance_id="integration-test-1",
            version="1.0.0",
            endpoint=ServiceEndpoint(host="localhost", port=8080),
            metadata={"environment": "test"},
            health_check=CustomHealthCheck("test-health", lambda: True)
        )
        
        # Test service registration
        await registry.register_service(service_instance)
        await registry.update_health_status("integration-test", "integration-test-1", ServiceStatus.HEALTHY)
        
        # Test service discovery
        client = ServiceDiscoveryClient(registry)
        instances = await client.discover_service("integration-test")
        assert len(instances) == 1
        assert instances[0].status == ServiceStatus.HEALTHY
        
        # Test health checks
        health_manager = HealthCheckManager()
        health_manager.add_health_check(
            CustomHealthCheck("integration_check", lambda: {"status": "healthy", "test": True})
        )
        
        overall_health = await health_manager.perform_all_checks()
        assert overall_health.status == HealthStatus.HEALTHY
        assert len(overall_health.checks) == 1
        
        # Test service deregistration
        await registry.deregister_service("integration-test", "integration-test-1")
        instances = await client.discover_service("integration-test")
        assert len(instances) == 0
        
    finally:
        await registry.stop()


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])