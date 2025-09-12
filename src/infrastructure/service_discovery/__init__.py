"""
Service Discovery Infrastructure Module.

This module provides comprehensive service discovery, health checking, and service client
functionality for distributed service architectures.
"""

from .service_registry import (
    ServiceRegistry,
    InMemoryServiceRegistry,
    ServiceInstance,
    ServiceEndpoint,
    ServiceStatus,
    HealthCheck,
    ServiceDiscoveryClient,
    service_registration_context,
    get_global_registry,
    initialize_service_registry,
    shutdown_service_registry,
    # Load balancing strategies
    round_robin_balancer,
    random_balancer,
    least_connections_balancer
)

from .health_checks import (
    HealthCheck,
    HealthCheckResult,
    HealthCheckType,
    HealthStatus,
    OverallHealth,
    HealthCheckManager,
    DatabaseHealthCheck,
    HttpServiceHealthCheck,
    SystemResourceHealthCheck,
    CustomHealthCheck,
    get_health_manager
)

from .service_client import (
    ServiceClient,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    RetryConfig,
    LoadBalancer,
    RoundRobinBalancer,
    RandomBalancer,
    WeightedBalancer,
    RequestStats,
    service_client,
    call_service,
    get_service_json,
    post_service_json,
    ServiceDiscoveryError,
    CircuitBreakerError
)

__all__ = [
    # Service Registry
    'ServiceRegistry',
    'InMemoryServiceRegistry',
    'ServiceInstance',
    'ServiceEndpoint',
    'ServiceStatus',
    'HealthCheck',
    'ServiceDiscoveryClient',
    'service_registration_context',
    'get_global_registry',
    'initialize_service_registry',
    'shutdown_service_registry',
    'round_robin_balancer',
    'random_balancer',
    'least_connections_balancer',
    
    # Health Checks
    'HealthCheck',
    'HealthCheckResult',
    'HealthCheckType',
    'HealthStatus',
    'OverallHealth',
    'HealthCheckManager',
    'DatabaseHealthCheck',
    'HttpServiceHealthCheck',
    'SystemResourceHealthCheck',
    'CustomHealthCheck',
    'get_health_manager',
    
    # Service Client
    'ServiceClient',
    'CircuitBreaker',
    'CircuitBreakerConfig',
    'CircuitState',
    'RetryConfig',
    'LoadBalancer',
    'RoundRobinBalancer',
    'RandomBalancer',
    'WeightedBalancer',
    'RequestStats',
    'service_client',
    'call_service',
    'get_service_json',
    'post_service_json',
    'ServiceDiscoveryError',
    'CircuitBreakerError'
]