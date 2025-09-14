"""
Tests for Redis caching infrastructure for InstrumentService.

Tests cache functionality, performance, error handling,
and integration with InstrumentService.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch

from infrastructure.caching.redis_cache import (
    CacheKeyBuilder,
    CacheStats,
    InMemoryCache
)
from domains.instruments.services.impl.cached_instrument_service_impl import CachedInstrumentServiceImpl
from domains.instruments.services.impl.instrument_service_impl import InstrumentServiceImpl
from domains.instruments.services.interfaces.instrument_service_interface import (
    InstrumentDTO,
    InstrumentOperationResult
)
from core.platform.config.environment import EnvironmentType


class TestCacheKeyBuilder:
    """Test cache key generation"""

    def test_instrument_by_id_key(self):
        key = CacheKeyBuilder.instrument_by_id(123)
        assert key == "instrument:id:123"

    def test_instrument_by_symbol_key(self):
        key = CacheKeyBuilder.instrument_by_symbol("AAPL", "ticker")
        assert key == "instrument:symbol:ticker:AAPL"

    def test_criteria_hash_deterministic(self):
        criteria1 = {"symbols": ["AAPL", "GOOGL"], "limit": 10}
        criteria2 = {"limit": 10, "symbols": ["AAPL", "GOOGL"]}  # Different order

        hash1 = CacheKeyBuilder.hash_criteria(criteria1)
        hash2 = CacheKeyBuilder.hash_criteria(criteria2)

        assert hash1 == hash2
        assert len(hash1) == 16


class TestCacheStats:
    """Test cache statistics tracking"""

    def test_initial_stats(self):
        stats = CacheStats()
        assert stats.hits == 0
        assert stats.misses == 0
        assert stats.hit_rate == 0.0

    def test_hit_rate_calculation(self):
        stats = CacheStats()
        stats.hits = 7
        stats.misses = 3
        assert stats.hit_rate == 0.7


class TestInMemoryCache:
    """Test fallback in-memory cache"""

    @pytest.fixture
    def cache(self):
        return InMemoryCache(max_size=5, default_ttl=1)

    @pytest.mark.asyncio
    async def test_basic_operations(self, cache):
        # Set
        result = await cache.set("key1", "value1")
        assert result is True

        # Get
        value = await cache.get("key1")
        assert value == "value1"

        # Delete
        deleted = await cache.delete("key1")
        assert deleted is True

        # Get after delete
        value = await cache.get("key1")
        assert value is None


class TestCachedInstrumentService:
    """Test cached instrument service implementation"""

    @pytest.fixture
    def mock_base_service(self):
        service = Mock(spec=InstrumentServiceImpl)
        service.get_instrument_by_id = AsyncMock()
        service.get_instrument_by_symbol = AsyncMock()
        service.get_instrument_count = AsyncMock()
        service.create_instrument = AsyncMock()
        return service

    @pytest.fixture
    def mock_environment(self):
        env = Mock()
        env.env_type = EnvironmentType.DEV
        return env

    @pytest.fixture
    def cached_service(self, mock_base_service, mock_environment):
        with patch('redis.Redis') as mock_redis:
            mock_redis.side_effect = Exception("Redis not available")
            service = CachedInstrumentServiceImpl(mock_base_service, mock_environment)
            return service

    @pytest.fixture
    def sample_instrument(self):
        return InstrumentDTO(
            id=1,
            symbol="AAPL",
            name="Apple Inc.",
            exchange="NASDAQ",
            instrument_type="stock",
            currency="USD"
        )

    @pytest.mark.asyncio
    async def test_get_instrument_by_id_cache_miss(self, cached_service, mock_base_service, sample_instrument):
        mock_base_service.get_instrument_by_id.return_value = sample_instrument

        result = await cached_service.get_instrument_by_id(1)

        assert result == sample_instrument
        mock_base_service.get_instrument_by_id.assert_called_once_with(1)

    @pytest.mark.asyncio
    async def test_get_instrument_by_id_cache_hit(self, cached_service, mock_base_service, sample_instrument):
        mock_base_service.get_instrument_by_id.return_value = sample_instrument

        # First call (cache miss)
        await cached_service.get_instrument_by_id(1)

        # Second call should hit cache
        result = await cached_service.get_instrument_by_id(1)

        assert result.symbol == "AAPL"
        assert mock_base_service.get_instrument_by_id.call_count == 1

    @pytest.mark.asyncio
    async def test_cache_stats(self, cached_service):
        stats = await cached_service.get_cache_stats()

        assert "hits" in stats
        assert "misses" in stats
        assert "cache_health" in stats

    @pytest.mark.asyncio
    async def test_create_instrument_invalidates_cache(self, cached_service, mock_base_service, sample_instrument):
        # Pre-populate cache
        mock_base_service.get_instrument_count.return_value = 1000
        await cached_service.get_instrument_count()

        # Setup successful creation
        operation_result = InstrumentOperationResult(
            success=True,
            instrument_id=1,
            created_count=1
        )
        mock_base_service.create_instrument.return_value = operation_result
        mock_base_service.get_instrument_by_id.return_value = sample_instrument

        # Create instrument
        result = await cached_service.create_instrument(sample_instrument)

        assert result.success is True

        # Cache should be invalidated - count should be fetched again
        count = await cached_service.get_instrument_count()
        assert mock_base_service.get_instrument_count.call_count == 2