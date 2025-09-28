"""
Caching and Optimization Example

Demonstrates the comprehensive caching and performance optimization features
of the service architecture.
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import List, Dict, Any

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import caching and optimization infrastructure
from src.infrastructure.caching import (
    CacheConfig,
    EvictionPolicy,
    MemoryCache,
    RedisCache,
    MultiLayerCache,
    cached,
    DatabaseCache,
    ConnectionPoolConfig,
    APICacheManager,
    ResponseCacheConfig,
    CacheStrategy,
    register_cache
)

from src.infrastructure.optimization import (
    get_performance_profiler,
    profile_performance
)


class ExampleDataService:
    """Example data service to demonstrate caching patterns."""

    def __init__(self, cache: MultiLayerCache, db_cache: DatabaseCache):
        self.cache = cache
        self.db_cache = db_cache
        self.call_count = 0

    @cached(ttl=3600, cache_name="example")
    async def get_expensive_data(self, data_id: str) -> Dict[str, Any]:
        """Simulate expensive data retrieval with caching."""
        self.call_count += 1

        # Simulate expensive computation
        await asyncio.sleep(0.5)

        logger.info(f"Expensive computation for {data_id} (call #{self.call_count})")

        return {
            'data_id': data_id,
            'computed_value': data_id.upper() * 3,
            'timestamp': datetime.utcnow().isoformat(),
            'computation_number': self.call_count
        }

    @profile_performance("database_query")
    async def query_database(self, query: str, params: List[Any] = None) -> List[Dict[str, Any]]:
        """Simulate database query with caching."""
        # This would normally use the database cache
        logger.info(f"Executing database query: {query[:50]}...")

        # Simulate database delay
        await asyncio.sleep(0.2)

        # Return mock data
        return [
            {'id': i, 'query': query, 'result': f'row_{i}'}
            for i in range(5)
        ]

    async def batch_operation(self, item_ids: List[str]) -> Dict[str, Any]:
        """Demonstrate batch processing with caching."""
        results = {}

        # Check cache for each item
        cached_items = []
        uncached_items = []

        for item_id in item_ids:
            cache_key = f"batch_item:{item_id}"
            cached_result = await self.cache.get(cache_key)

            if cached_result:
                results[item_id] = cached_result
                cached_items.append(item_id)
            else:
                uncached_items.append(item_id)

        # Process uncached items
        if uncached_items:
            logger.info(f"Processing {len(uncached_items)} uncached items")

            for item_id in uncached_items:
                # Simulate processing
                await asyncio.sleep(0.1)

                result = {
                    'item_id': item_id,
                    'processed_at': datetime.utcnow().isoformat(),
                    'value': hash(item_id) % 1000
                }

                results[item_id] = result

                # Cache the result
                cache_key = f"batch_item:{item_id}"
                await self.cache.set(cache_key, result, ttl=1800)

        logger.info(f"Batch operation: {len(cached_items)} from cache, {len(uncached_items)} computed")

        return {
            'results': results,
            'cache_hits': len(cached_items),
            'cache_misses': len(uncached_items),
            'total_items': len(item_ids)
        }


async def demonstrate_multi_layer_caching():
    """Demonstrate multi-layer caching functionality."""
    logger.info("=== Multi-Layer Caching Demo ===")

    # Configure caches
    l1_config = CacheConfig(
        ttl_seconds=300,
        max_size=100,
        eviction_policy=EvictionPolicy.LRU,
        namespace="demo_l1"
    )

    l2_config = CacheConfig(
        ttl_seconds=3600,
        max_size=1000,
        eviction_policy=EvictionPolicy.LRU,
        namespace="demo_l2"
    )

    # Create multi-layer cache
    cache = MultiLayerCache(l1_config, l2_config, redis_url="redis://localhost:6379")
    register_cache("example", cache)

    # Test cache operations
    test_key = "test_key"
    test_value = {"message": "Hello from cache!", "timestamp": datetime.utcnow().isoformat()}

    # Set value
    await cache.set(test_key, test_value)
    logger.info("Value stored in cache")

    # Get value (should hit L1 cache)
    start_time = time.time()
    retrieved_value = await cache.get(test_key)
    l1_time = (time.time() - start_time) * 1000

    logger.info(f"L1 cache retrieval: {l1_time:.2f}ms")
    logger.info(f"Retrieved value: {retrieved_value['message']}")

    # Clear L1 cache to test L2
    await cache.l1_cache.clear()
    logger.info("Cleared L1 cache")

    # Get value again (should hit L2 cache and populate L1)
    start_time = time.time()
    retrieved_value = await cache.get(test_key)
    l2_time = (time.time() - start_time) * 1000

    logger.info(f"L2 cache retrieval with L1 population: {l2_time:.2f}ms")

    # Get cache metrics
    metrics = await cache.get_metrics()
    logger.info("Cache metrics:")
    for layer, layer_metrics in metrics.items():
        logger.info(f"  {layer}: {layer_metrics.hits} hits, {layer_metrics.misses} misses, {layer_metrics.hit_rate:.1f}% hit rate")


async def demonstrate_database_caching():
    """Demonstrate database query caching."""
    logger.info("=== Database Caching Demo ===")

    # Configure database cache (would normally use real database)
    pool_config = ConnectionPoolConfig(
        host="localhost",
        port=5432,
        database="test_db",
        user="test_user",
        password="test_pass",
        min_connections=2,
        max_connections=10
    )

    # Create cache for database
    cache_config = CacheConfig(ttl_seconds=1800, namespace="db_cache")
    cache = MemoryCache(cache_config)

    # This would normally connect to a real database
    db_cache = DatabaseCache(pool_config, cache, cache_config)
    # await db_cache.initialize()  # Skip for demo

    logger.info("Database cache would be initialized here")
    logger.info("Query caching enables:")
    logger.info("  - Automatic result caching based on query + parameters")
    logger.info("  - Intelligent TTL calculation based on query type")
    logger.info("  - Connection pool optimization")
    logger.info("  - Query performance analysis")

async def demonstrate_service_caching():
    """Demonstrate service-level caching with the example service."""
    logger.info("=== Service-Level Caching Demo ===")

    # Setup caching infrastructure
    cache_config = CacheConfig(ttl_seconds=600, max_size=50, namespace="service_demo")
    memory_cache = MemoryCache(cache_config)
    register_cache("example", memory_cache)

    # Mock database cache
    pool_config = ConnectionPoolConfig(
        host="localhost", port=5432, database="demo", user="demo", password="demo"
    )
    db_cache = DatabaseCache(pool_config)

    # Create service
    service = ExampleDataService(memory_cache, db_cache)

    # Test cached operations
    logger.info("Testing cached expensive operations...")

    # First call - should execute computation
    start_time = time.time()
    result1 = await service.get_expensive_data("item_123")
    first_call_time = (time.time() - start_time) * 1000

    logger.info(f"First call: {first_call_time:.1f}ms")
    logger.info(f"Result: {result1['computed_value']}")

    # Second call - should hit cache
    start_time = time.time()
    result2 = await service.get_expensive_data("item_123")
    second_call_time = (time.time() - start_time) * 1000

    logger.info(f"Second call (cached): {second_call_time:.1f}ms")
    logger.info(f"Speed improvement: {first_call_time / second_call_time:.1f}x faster")

    # Test batch operations with mixed cache hits/misses
    logger.info("\nTesting batch operations with caching...")

    item_ids = [f"batch_item_{i}" for i in range(10)]

    # First batch - all cache misses
    start_time = time.time()
    batch_result1 = await service.batch_operation(item_ids)
    batch_time1 = (time.time() - start_time) * 1000

    logger.info(f"First batch: {batch_time1:.1f}ms")
    logger.info(f"Cache performance: {batch_result1['cache_hits']} hits, {batch_result1['cache_misses']} misses")

    # Second batch - all cache hits
    start_time = time.time()
    batch_result2 = await service.batch_operation(item_ids)
    batch_time2 = (time.time() - start_time) * 1000

    logger.info(f"Second batch (cached): {batch_time2:.1f}ms")
    logger.info(f"Cache performance: {batch_result2['cache_hits']} hits, {batch_result2['cache_misses']} misses")
    logger.info(f"Batch speed improvement: {batch_time1 / batch_time2:.1f}x faster")


async def demonstrate_performance_profiling():
    """Demonstrate performance profiling and optimization."""
    logger.info("=== Performance Profiling Demo ===")

    profiler = get_performance_profiler()

    # Profile an operation
    async with profiler.profile_operation("demo_expensive_operation"):
        # Simulate expensive operation
        await asyncio.sleep(0.3)

        # Simulate some CPU work
        total = 0
        for i in range(100000):
            total += i * i

        logger.info(f"Completed expensive operation with result: {total}")

    # Get performance summary
    summary = profiler.get_performance_summary("demo_expensive_operation")
    logger.info("Performance summary:")
    logger.info(f"  Average execution time: {summary['execution_time_stats']['average_ms']:.1f}ms")
    logger.info(f"  Memory usage: {summary['memory_stats']['average_mb']:.2f}MB")
    logger.info(f"  CPU usage: {summary['cpu_stats']['average_percent']:.1f}%")

    # Get detailed profile results
    profile_result = profiler.get_latest_profile_result("demo_expensive_operation")
    if profile_result:
        logger.info("Performance recommendations:")
        for rec in profile_result.recommendations:
            logger.info(f"  - {rec}")

        logger.info("Top time-consuming functions:")
        for func, time_ms, calls in profile_result.top_functions[:3]:
            logger.info(f"  - {func}: {time_ms:.2f}ms ({calls} calls)")


async def demonstrate_cache_invalidation():
    """Demonstrate intelligent cache invalidation."""
    logger.info("=== Cache Invalidation Demo ===")

    cache_config = CacheConfig(ttl_seconds=3600, namespace="invalidation_demo")
    cache = MemoryCache(cache_config)

    # Set some test data with tags
    await cache.set("user:123", {"name": "John Doe", "email": "john@example.com"})
    await cache.set("user:456", {"name": "Jane Smith", "email": "jane@example.com"})
    await cache.set("product:789", {"name": "Widget", "price": 19.99})

    logger.info("Cached user and product data")

    # Verify data is cached
    user_data = await cache.get("user:123")
    logger.info(f"Retrieved user: {user_data['name']}")

    # Simulate user update - invalidate user caches
    logger.info("Simulating user data update...")
    await cache.clear("user:*")
    logger.info("Invalidated all user caches")

    # Verify user cache is cleared but product cache remains
    user_data_after = await cache.get("user:123")
    product_data = await cache.get("product:789")

    logger.info(f"User data after invalidation: {user_data_after}")
    logger.info(f"Product data (should remain): {product_data['name'] if product_data else 'Not found'}")


async def run_comprehensive_demo():
    """Run comprehensive caching and optimization demonstration."""
    logger.info("🚀 Starting Comprehensive Caching and Optimization Demo")

    # Run all demonstrations
    await demonstrate_multi_layer_caching()
    await asyncio.sleep(1)

    await demonstrate_database_caching()
    await asyncio.sleep(1)

    await demonstrate_service_caching()
    await asyncio.sleep(1)

    await demonstrate_performance_profiling()
    await asyncio.sleep(1)

    await demonstrate_cache_invalidation()

    logger.info("✅ All demonstrations completed successfully!")

async def main():
    """Main demonstration function."""
    await run_comprehensive_demo()


if __name__ == "__main__":
    asyncio.run(main())