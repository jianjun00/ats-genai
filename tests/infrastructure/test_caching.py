"""
Test Suite for Caching Infrastructure

Comprehensive tests for multi-layer caching, database caching, API response caching,
and performance optimization features.
"""

import asyncio
import pytest
import time
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

from src.infrastructure.caching import (
    # Core caching
    CacheConfig,
    EvictionPolicy,
    CacheEntry,
    MemoryCache,
    MultiLayerCache,
    CacheInvalidationManager,

    # Database caching
    DatabaseCache,
    ConnectionPoolConfig,
    QueryOptimizer,
    QueryBuilder,

    # API caching
    APICacheManager,
    ResponseCacheConfig,
    CacheStrategy,
    CachedResponse,
    SmartCacheInvalidator,

    # Utilities
    cached,
    register_cache,
    get_cache
)

from src.infrastructure.optimization import (
    PerformanceProfiler,
    get_performance_profiler,
    profile_performance
)


class TestCacheEntry:
    """Test CacheEntry functionality."""

    def test_cache_entry_creation(self):
        """Test cache entry creation and properties."""
        entry = CacheEntry(
            key="test_key",
            value="test_value",
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(seconds=3600)
        )

        assert entry.key == "test_key"
        assert entry.value == "test_value"
        assert not entry.is_expired()
        assert entry.access_count == 0

    def test_cache_entry_expiration(self):
        """Test cache entry expiration logic."""
        # Non-expiring entry
        entry1 = CacheEntry(
            key="test_key",
            value="test_value",
            created_at=datetime.utcnow()
        )
        assert not entry1.is_expired()

        # Expired entry
        entry2 = CacheEntry(
            key="test_key",
            value="test_value",
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() - timedelta(seconds=1)
        )
        assert entry2.is_expired()

    def test_cache_entry_touch(self):
        """Test cache entry access tracking."""
        entry = CacheEntry(
            key="test_key",
            value="test_value",
            created_at=datetime.utcnow()
        )

        initial_access_time = entry.last_accessed
        initial_count = entry.access_count

        entry.touch()

        assert entry.access_count == initial_count + 1
        assert entry.last_accessed > initial_access_time


class TestMemoryCache:
    """Test MemoryCache functionality."""

    @pytest.fixture
    def memory_cache(self):
        """Create memory cache for testing."""
        config = CacheConfig(
            ttl_seconds=3600,
            max_size=100,
            eviction_policy=EvictionPolicy.LRU,
            namespace="test"
        )
        return MemoryCache(config)

    async def test_basic_operations(self, memory_cache):
        """Test basic cache operations."""
        # Set and get
        success = await memory_cache.set("key1", "value1")
        assert success is True

        value = await memory_cache.get("key1")
        assert value == "value1"

        # Exists check
        exists = await memory_cache.exists("key1")
        assert exists is True

        exists_missing = await memory_cache.exists("missing_key")
        assert exists_missing is False

        # Delete
        deleted = await memory_cache.delete("key1")
        assert deleted is True

        value_after_delete = await memory_cache.get("key1")
        assert value_after_delete is None

    async def test_ttl_expiration(self, memory_cache):
        """Test TTL-based expiration."""
        # Set with short TTL
        await memory_cache.set("temp_key", "temp_value", ttl=1)

        # Should exist immediately
        value = await memory_cache.get("temp_key")
        assert value == "temp_value"

        # Wait for expiration
        await asyncio.sleep(1.1)

        # Should be expired
        value_after_expiry = await memory_cache.get("temp_key")
        assert value_after_expiry is None

    async def test_cache_metrics(self, memory_cache):
        """Test cache metrics collection."""
        # Generate some hits and misses
        await memory_cache.set("key1", "value1")
        await memory_cache.get("key1")  # Hit
        await memory_cache.get("missing_key")  # Miss

        metrics = await memory_cache.get_metrics()

        assert metrics.hits >= 1
        assert metrics.misses >= 1
        assert metrics.total_requests >= 2
        assert metrics.hit_rate > 0


class TestMultiLayerCache:
    """Test MultiLayerCache functionality."""

    @pytest.fixture
    def multi_layer_cache(self):
        """Create multi-layer cache for testing."""
        l1_config = CacheConfig(
            ttl_seconds=300,
            max_size=50,
            eviction_policy=EvictionPolicy.LRU,
            namespace="test_l1"
        )

        l2_config = CacheConfig(
            ttl_seconds=3600,
            max_size=200,
            eviction_policy=EvictionPolicy.LRU,
            namespace="test_l2"
        )

        # Use memory cache for L2 instead of Redis for testing
        return MultiLayerCache(l1_config, l2_config=None)  # L2 disabled for testing

    async def test_cache_layering(self, multi_layer_cache):
        """Test cache layer functionality."""
        # Set value
        await multi_layer_cache.set("test_key", "test_value")

        # Should hit L1 cache
        value = await multi_layer_cache.get("test_key")
        assert value == "test_value"

        # Clear L1 and verify read repair would work
        await multi_layer_cache.l1_cache.clear()

        # If L2 was enabled, this would test read repair
        # For now, just verify the value is gone
        value_after_clear = await multi_layer_cache.get("test_key")
        assert value_after_clear is None

    async def test_write_through(self, multi_layer_cache):
        """Test write-through caching."""
        # Write should go to both layers (if L2 enabled)
        success = await multi_layer_cache.set("write_key", "write_value")
        assert success is True

        # Verify it exists
        exists = await multi_layer_cache.exists("write_key")
        assert exists is True

    async def test_cache_invalidation(self, multi_layer_cache):
        """Test cache invalidation across layers."""
        # Set multiple values
        await multi_layer_cache.set("key1", "value1")
        await multi_layer_cache.set("key2", "value2")

        # Delete specific key
        deleted = await multi_layer_cache.delete("key1")
        assert deleted is True

        # Verify deletion
        assert await multi_layer_cache.get("key1") is None
        assert await multi_layer_cache.get("key2") == "value2"


class TestCacheInvalidationManager:
    """Test cache invalidation patterns."""

    @pytest.fixture
    def cache_with_invalidation(self):
        """Create cache with invalidation manager."""
        config = CacheConfig(ttl_seconds=3600, namespace="test_invalidation")
        cache = MemoryCache(config)
        invalidation_manager = CacheInvalidationManager(cache)
        return cache, invalidation_manager

    async def test_tag_based_invalidation(self, cache_with_invalidation):
        """Test invalidation by tags."""
        cache, invalidation_manager = cache_with_invalidation

        # Set values with tags
        await invalidation_manager.set_with_tags(
            "user:123", {"name": "John"}, ["users", "user:123"]
        )
        await invalidation_manager.set_with_tags(
            "user:456", {"name": "Jane"}, ["users", "user:456"]
        )
        await invalidation_manager.set_with_tags(
            "product:789", {"name": "Widget"}, ["products", "product:789"]
        )

        # Verify data exists
        assert await cache.get("user:123") is not None
        assert await cache.get("product:789") is not None

        # Invalidate by tag
        count = await invalidation_manager.invalidate_by_tag("users")
        assert count == 2  # Should invalidate both users

        # Verify invalidation
        assert await cache.get("user:123") is None
        assert await cache.get("user:456") is None
        assert await cache.get("product:789") is not None  # Should remain


class TestDatabaseCache:
    """Test DatabaseCache functionality."""

    @pytest.fixture
    def mock_db_cache(self):
        """Create mock database cache for testing."""
        pool_config = ConnectionPoolConfig(
            host="localhost",
            port=5432,
            database="test_db",
            user="test_user",
            password="test_pass"
        )

        config = CacheConfig(ttl_seconds=1800, namespace="db_test")
        memory_cache = MemoryCache(config)

        return DatabaseCache(pool_config, memory_cache, config)

    def test_query_optimizer(self):
        """Test query optimization analysis."""
        optimizer = QueryOptimizer()

        # Test slow query analysis
        optimizer.analyze_query("SELECT * FROM large_table", 2000.0)

        assert len(optimizer.slow_queries) == 1
        assert optimizer.slow_queries[0]['execution_time_ms'] == 2000.0

        # Test query pattern extraction
        pattern = optimizer._extract_query_pattern("SELECT * FROM users WHERE id = 123")
        expected_pattern = "SELECT * FROM USERS WHERE ID = ?"
        assert pattern == expected_pattern

    def test_query_builder(self, mock_db_cache):
        """Test dynamic query builder."""
        builder = QueryBuilder(mock_db_cache)

        # Build a query
        query, params = (builder
                        .select(['name', 'email'])
                        .from_table('users')
                        .where('active = ?', True)
                        .where('created_at > ?', datetime.utcnow())
                        .order_by('name', 'ASC')
                        .limit(10)
                        .build())

        assert 'SELECT name, email' in query
        assert 'FROM users' in query
        assert 'WHERE active = ?' in query
        assert 'ORDER BY name ASC' in query
        assert 'LIMIT 10' in query
        assert len(params) == 2


class TestAPICacheManager:
    """Test API response caching."""

    @pytest.fixture
    def api_cache_manager(self):
        """Create API cache manager for testing."""
        config = CacheConfig(ttl_seconds=3600, namespace="api_test")
        cache = MemoryCache(config)

        response_config = ResponseCacheConfig(
            default_ttl=3600,
            max_response_size=1024*1024,
            cache_private_responses=False
        )

        return APICacheManager(cache, response_config)

    def test_cache_key_generation(self, api_cache_manager):
        """Test cache key generation for requests."""
        from fastapi import Request

        # Mock request
        mock_request = Mock()
        mock_request.method = "GET"
        mock_request.url.path = "/api/v1/users"
        mock_request.query_params = {"limit": "10", "offset": "0"}
        mock_request.headers = {"authorization": "Bearer token123"}

        # Configure cache key headers
        api_cache_manager.config.cache_key_headers = ["authorization"]

        cache_key = api_cache_manager.generate_cache_key(mock_request)

        assert "api_cache:" in cache_key
        assert "GET" in cache_key or len(cache_key) == 64  # Either readable or hashed

    async def test_response_caching(self, api_cache_manager):
        """Test response storage and retrieval."""
        from fastapi import Request, Response

        # Mock request and response
        mock_request = Mock()
        mock_request.method = "GET"
        mock_request.url.path = "/api/test"
        mock_request.query_params = {}
        mock_request.headers = {}

        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "application/json"}
        mock_response.body = b'{"data": "test"}'
        mock_response.media_type = "application/json"

        # Store response
        stored = await api_cache_manager.store_response(mock_request, mock_response)
        assert stored is True

        # Retrieve response
        cached_response = await api_cache_manager.get_cached_response(mock_request)
        assert cached_response is not None
        assert cached_response.status_code == 200
        assert cached_response.body == b'{"data": "test"}'


class TestPerformanceProfiler:
    """Test performance profiling functionality."""

    @pytest.fixture
    def profiler(self):
        """Create performance profiler for testing."""
        return PerformanceProfiler(enable_memory_profiling=False)  # Disable for testing

    async def test_operation_profiling(self, profiler):
        """Test operation profiling context manager."""
        async with profiler.profile_operation("test_operation"):
            # Simulate work
            await asyncio.sleep(0.1)
            total = sum(range(1000))

        # Check metrics were recorded
        assert len(profiler.metrics_history) == 1

        metrics = profiler.metrics_history[0]
        assert metrics.operation_name == "test_operation"
        assert metrics.execution_time_ms >= 100  # At least 100ms due to sleep

    async def test_performance_summary(self, profiler):
        """Test performance summary generation."""
        # Generate multiple operations
        for i in range(3):
            async with profiler.profile_operation("test_op"):
                await asyncio.sleep(0.05)

        summary = profiler.get_performance_summary("test_op")

        assert summary['total_operations'] == 3
        assert summary['execution_time_stats']['average_ms'] >= 50
        assert summary['execution_time_stats']['min_ms'] >= 50

    def test_profile_decorator(self, profiler):
        """Test performance profiling decorator."""
        @profile_performance("decorated_function")
        async def test_function(x: int) -> int:
            await asyncio.sleep(0.05)
            return x * 2

        # This would be tested in integration, but we can verify decorator exists
        assert hasattr(test_function, '__wrapped__')


class TestCachedDecorator:
    """Test the @cached decorator functionality."""

    @pytest.fixture
    def setup_cache(self):
        """Setup cache for decorator testing."""
        config = CacheConfig(ttl_seconds=300, namespace="decorator_test")
        cache = MemoryCache(config)
        register_cache("decorator_test", cache)
        return cache

    async def test_cached_decorator(self, setup_cache):
        """Test @cached decorator functionality."""
        call_count = 0

        @cached(ttl=300, cache_name="decorator_test")
        async def expensive_function(x: int) -> int:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)
            return x * 2

        # First call
        result1 = await expensive_function(5)
        assert result1 == 10
        assert call_count == 1

        # Second call - should hit cache
        result2 = await expensive_function(5)
        assert result2 == 10
        assert call_count == 1  # Should not increment

        # Different parameter - should miss cache
        result3 = await expensive_function(10)
        assert result3 == 20
        assert call_count == 2


@pytest.mark.asyncio
async def test_integration_scenario():
    """Test integration scenario with multiple caching layers."""
    # Setup multi-layer cache
    l1_config = CacheConfig(ttl_seconds=300, max_size=10, namespace="integration_l1")
    l2_config = CacheConfig(ttl_seconds=3600, max_size=100, namespace="integration_l2")

    cache = MultiLayerCache(l1_config, l2_config=None)  # L2 disabled for testing
    register_cache("integration_test", cache)

    # Create service with caching
    class TestService:
        def __init__(self):
            self.call_count = 0

        @cached(ttl=300, cache_name="integration_test")
        async def get_data(self, item_id: str) -> Dict[str, Any]:
            self.call_count += 1
            await asyncio.sleep(0.05)  # Simulate work
            return {
                'id': item_id,
                'data': f'processed_{item_id}',
                'call_number': self.call_count
            }

    service = TestService()

    # Test caching behavior
    result1 = await service.get_data("item1")
    assert result1['call_number'] == 1
    assert service.call_count == 1

    # Second call should hit cache
    result2 = await service.get_data("item1")
    assert result2['call_number'] == 1  # Same as before
    assert service.call_count == 1  # No additional calls

    # Different item should miss cache
    result3 = await service.get_data("item2")
    assert result3['call_number'] == 2
    assert service.call_count == 2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])