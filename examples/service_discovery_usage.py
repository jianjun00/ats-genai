"""
Service Discovery Usage Examples.

This module demonstrates how to use the service discovery infrastructure
for registering services, performing health checks, and making service calls.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, Any

from src.infrastructure.service_discovery import (
    # Service Registry
    initialize_service_registry,
    shutdown_service_registry,
    ServiceInstance,
    ServiceEndpoint,
    HealthCheck,
    service_registration_context,

    # Health Checks
    get_health_manager,
    DatabaseHealthCheck,
    HttpServiceHealthCheck,
    SystemResourceHealthCheck,
    CustomHealthCheck,
    HealthCheckType,

    # Service Client
    ServiceClient,
    service_client,
    get_service_json,
    post_service_json,
    RetryConfig,
    CircuitBreakerConfig,
    RoundRobinBalancer
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def example_service_registration():
    """Example: Service registration and discovery."""
    logger.info("=== Service Registration Example ===")

    # Initialize service registry
    registry = await initialize_service_registry()

    # Create service instance
    service_instance = ServiceInstance(
        service_name="example-service",
        instance_id="example-service-1",
        version="1.0.0",
        endpoint=ServiceEndpoint(
            host="localhost",
            port=8001,
            protocol="http"
        ),
        metadata={
            "environment": "dev",
            "weight": 1,
            "tags": ["example", "demo"]
        },
        health_check=HealthCheck(
            endpoint="/health",
            interval_seconds=30,
            timeout_seconds=5
        )
    )

    # Register service using context manager
    async with service_registration_context(registry, service_instance) as instance:
        logger.info(f"Service registered: {instance.service_name}:{instance.instance_id}")

        # Simulate service running
        await asyncio.sleep(2)

        # Discover services
        instances = await registry.get_service_instances("example-service")
        logger.info(f"Discovered {len(instances)} instances of example-service")

        for inst in instances:
            logger.info(f"  - {inst.instance_id} at {inst.endpoint.url}")

    logger.info("Service deregistered automatically")

    # Clean up
    await shutdown_service_registry()


async def example_health_checks():
    """Example: Comprehensive health checking."""
    logger.info("=== Health Checks Example ===")

    health_manager = get_health_manager()

    # Add system resource health check
    system_check = SystemResourceHealthCheck(
        name="system_resources",
        cpu_threshold=80.0,
        memory_threshold=85.0,
        disk_threshold=90.0
    )
    health_manager.add_health_check(system_check)

    # Add custom business logic health check
    async def check_business_logic():
        """Example business logic check."""
        # Simulate some business logic validation
        await asyncio.sleep(0.1)  # Simulate processing time

        return {
            'status': 'healthy',
            'message': 'Business logic functioning normally',
            'processed_items': 42,
            'cache_hit_rate': 95.5
        }

    business_check = CustomHealthCheck(
        name="business_logic",
        check_function=check_business_logic,
        check_type=HealthCheckType.READINESS
    )
    health_manager.add_health_check(business_check)

    # Add HTTP service dependency check
    http_check = HttpServiceHealthCheck(
        name="external_api",
        url="https://httpbin.org/status/200"  # Always returns 200
    )
    health_manager.add_health_check(http_check)

    # Perform all health checks
    overall_health = await health_manager.perform_all_checks()

    logger.info(f"Overall Health Status: {overall_health.status.value}")
    logger.info(f"Health Message: {overall_health.message}")
    logger.info(f"Total Checks: {len(overall_health.checks)}")

    # Display individual check results
    for check in overall_health.checks:
        logger.info(f"  {check.check_name}: {check.status.value} ({check.duration_ms:.1f}ms)")
        if check.message:
            logger.info(f"    Message: {check.message}")


async def example_service_client():
    """Example: Service client with resilience patterns."""
    logger.info("=== Service Client Example ===")

    # Initialize service registry
    registry = await initialize_service_registry()

    # Register a mock service
    mock_service = ServiceInstance(
        service_name="mock-api",
        instance_id="mock-api-1",
        version="1.0.0",
        endpoint=ServiceEndpoint(
            host="httpbin.org",
            port=443,
            protocol="https"
        ),
        metadata={"environment": "demo"},
        health_check=HealthCheck(endpoint="/status/200")
    )

    await registry.register_service(mock_service)

    # Configure retry and circuit breaker
    retry_config = RetryConfig(
        max_attempts=3,
        base_delay_seconds=0.5,
        exponential_backoff=True,
        retryable_status_codes=[429, 502, 503, 504]
    )

    circuit_breaker_config = CircuitBreakerConfig(
        failure_threshold=3,
        recovery_timeout_seconds=30.0
    )

    # Create service client
    async with service_client(
        service_name="mock-api",
        registry=registry,
        load_balancer=RoundRobinBalancer(),
        retry_config=retry_config,
        circuit_breaker_config=circuit_breaker_config
    ) as client:

        logger.info("Making service calls...")

        # Example GET request
        response = await client.get("/json")
        if response.status == 200:
            data = await response.json()
            logger.info(f"GET /json successful: {data.get('url', 'N/A')}")
        test_data = {"message": "Hello from service client", "timestamp": datetime.utcnow().isoformat()}
        response = await client.post("/post", json_data=test_data)
        if response.status == 200:
            result = await response.json()
            logger.info(f"POST /post successful: received {len(result.get('json', {}))} fields")
        stats = client.get_stats()
        logger.info(f"Client Stats: {stats['total_requests']} requests, "
                   f"{stats['success_rate']:.1f}% success rate, "
                   f"avg {stats['average_response_time_ms']:.1f}ms response time")

    # Clean up
    await registry.deregister_service("mock-api", "mock-api-1")
    await shutdown_service_registry()


async def example_convenience_functions():
    """Example: Using convenience functions for service calls."""
    logger.info("=== Convenience Functions Example ===")

    # Initialize service registry
    registry = await initialize_service_registry()

    # Register JSONPlaceholder API service
    jsonplaceholder_service = ServiceInstance(
        service_name="jsonplaceholder",
        instance_id="jsonplaceholder-1",
        version="1.0.0",
        endpoint=ServiceEndpoint(
            host="jsonplaceholder.typicode.com",
            port=443,
            protocol="https"
        ),
        metadata={"environment": "demo"},
        health_check=HealthCheck(endpoint="/posts/1")
    )

    await registry.register_service(jsonplaceholder_service)

    # Get JSON data using convenience function
    post_data = await get_service_json("jsonplaceholder", "/posts/1")
    logger.info(f"Retrieved post: '{post_data.get('title', 'N/A')[:50]}...'")

    # Post JSON data using convenience function
    new_post = {
        "title": "Example Post",
        "body": "This is an example post created via service client",
        "userId": 1
    }

    created_post = await post_service_json("jsonplaceholder", "/posts", new_post)
    logger.info(f"Created post with ID: {created_post.get('id')}")

    await registry.deregister_service("jsonplaceholder", "jsonplaceholder-1")
    await shutdown_service_registry()


async def example_advanced_health_monitoring():
    """Example: Advanced health monitoring with custom checks."""
    logger.info("=== Advanced Health Monitoring Example ===")

    health_manager = get_health_manager()

    # Custom check that simulates application-specific monitoring
    async def check_cache_health():
        """Simulate cache health check."""
        # Simulate cache statistics
        cache_stats = {
            'hit_rate': 92.5,
            'miss_rate': 7.5,
            'size_mb': 128.7,
            'max_size_mb': 256.0,
            'evictions_per_hour': 12
        }

        # Determine health based on cache performance
        if cache_stats['hit_rate'] < 70:
            status = 'unhealthy'
            message = f"Cache hit rate too low: {cache_stats['hit_rate']:.1f}%"
        elif cache_stats['size_mb'] / cache_stats['max_size_mb'] > 0.9:
            status = 'degraded'
            message = f"Cache nearly full: {cache_stats['size_mb']:.1f}MB / {cache_stats['max_size_mb']:.1f}MB"
        else:
            status = 'healthy'
            message = f"Cache performing well: {cache_stats['hit_rate']:.1f}% hit rate"

        return {
            'status': status,
            'message': message,
            'cache_statistics': cache_stats
        }

    cache_check = CustomHealthCheck(
        name="cache_health",
        check_function=check_cache_health,
        check_type=HealthCheckType.CUSTOM
    )
    health_manager.add_health_check(cache_check)

    # Custom check for data freshness
    def check_data_freshness():
        """Simulate data freshness check."""
        # Simulate checking when data was last updated
        import random
        minutes_since_update = random.randint(1, 120)

        if minutes_since_update > 60:
            return {
                'status': 'unhealthy',
                'message': f'Data is stale: last updated {minutes_since_update} minutes ago',
                'minutes_since_update': minutes_since_update,
                'max_staleness_minutes': 60
            }
        elif minutes_since_update > 30:
            return {
                'status': 'degraded',
                'message': f'Data is getting stale: last updated {minutes_since_update} minutes ago',
                'minutes_since_update': minutes_since_update
            }
        else:
            return {
                'status': 'healthy',
                'message': f'Data is fresh: last updated {minutes_since_update} minutes ago',
                'minutes_since_update': minutes_since_update
            }

    freshness_check = CustomHealthCheck(
        name="data_freshness",
        check_function=check_data_freshness,
        check_type=HealthCheckType.READINESS
    )
    health_manager.add_health_check(freshness_check)

    # Perform health checks and display detailed results
    overall_health = await health_manager.perform_all_checks()

    logger.info(f"=== Health Check Results ===")
    logger.info(f"Overall Status: {overall_health.status.value}")
    logger.info(f"Summary: {overall_health.summary}")

    for check in overall_health.checks:
        logger.info(f"\n{check.check_name} ({check.check_type.value}):")
        logger.info(f"  Status: {check.status.value}")
        logger.info(f"  Duration: {check.duration_ms:.1f}ms")
        logger.info(f"  Message: {check.message}")

        if check.details:
            logger.info(f"  Details: {check.details}")


async def main():
    """Run all examples."""
    logger.info("Starting Service Discovery Examples...")

    examples = [
        ("Service Registration", example_service_registration),
        ("Health Checks", example_health_checks),
        ("Service Client", example_service_client),
        ("Convenience Functions", example_convenience_functions),
        ("Advanced Health Monitoring", example_advanced_health_monitoring)
    ]

    for name, example_func in examples:
        logger.info(f"\n{'='*20} {name} {'='*20}")
        await example_func()
        logger.info(f"✅ {name} completed successfully")
        await asyncio.sleep(1)

    logger.info("\n🎉 All service discovery examples completed!")


if __name__ == "__main__":
    asyncio.run(main())