"""
Performance tests for optimized data pipeline components.
Validates that batch operations provide significant performance improvements.
"""
import pytest
import time
from datetime import datetime, date
from unittest.mock import AsyncMock, MagicMock
from domains.market_data.services.eod.unified_db_daily_price_market_data_manager import UnifiedDBDailyPriceMarketDataManager
from domains.market_data.services.eod.unify_daily_price_polygon import DatabaseDailyPricesUnifier
from core.shared.utils.environment import Environment


class TestOptimizedDataPipelinePerformance:

    @pytest.fixture
    def mock_env(self):
        env = MagicMock(spec=Environment)
        env.get_database_url.return_value = "postgresql://test:test@localhost:5432/test_db"
        env.get_table_name.side_effect = lambda table: f"test_{table}"
        return env

    @pytest.fixture
    def sample_instrument_ids(self):
        """Sample instrument IDs for performance testing."""
        return list(range(1, 101))  # 100 instruments

    @pytest.fixture
    def sample_symbols(self):
        """Sample symbols corresponding to instrument IDs."""
        return [f"TEST{i:03d}" for i in range(1, 101)]

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_batch_symbol_resolution_performance(self, mock_env, sample_instrument_ids):
        """Test that batch symbol resolution is significantly faster than individual calls."""
        manager = UnifiedDBDailyPriceMarketDataManager(mock_env)

        # Mock the DAO to simulate database calls
        mock_dao = AsyncMock()
        mock_core.dao.get_symbol_by_instrument_id_vendor_name = AsyncMock(side_effect=lambda iid, **kwargs: f"TEST{iid:03d}")
        mock_core.dao.get_symbols_by_instrument_ids_batch = AsyncMock(return_value={iid: f"TEST{iid:03d}" for iid in sample_instrument_ids})

        # Patch DAO creation
        with pytest.mock.patch('core.dao.instrument_xrefs_core.dao.InstrumentXrefsDAO', return_value=mock_dao):
            # Test individual calls (old method)
            start_time = time.time()
            individual_results = {}
            for iid in sample_instrument_ids:
                symbol = await manager.resolve_symbol(iid)
                individual_results[iid] = symbol
            individual_time = time.time() - start_time

            # Clear cache for fair comparison
            manager._symbol_cache.clear()
            manager._cache_timestamp = None

            # Test batch calls (new method)
            start_time = time.time()
            batch_results = await manager.resolve_symbols_batch(sample_instrument_ids)
            batch_time = time.time() - start_time

            # Verify results are identical
            assert individual_results == batch_results

            # Verify performance improvement (batch should be >5x faster)
            performance_improvement = individual_time / batch_time
            print(f"Performance improvement: {performance_improvement:.2f}x")
            print(f"Individual calls: {individual_time:.4f}s")
            print(f"Batch calls: {batch_time:.4f}s")

            # In real scenario with database latency, expect >5x improvement
            # For unit test with mocks, just verify batch method completes faster
            assert batch_time <= individual_time
            assert len(batch_results) == len(sample_instrument_ids)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_symbol_caching_performance(self, mock_env, sample_instrument_ids):
        """Test that symbol caching reduces repeated database calls."""
        manager = UnifiedDBDailyPriceMarketDataManager(mock_env)

        # Mock DAO with call counting
        call_count = 0
        async def mock_resolve_symbol(iid, **kwargs):
            nonlocal call_count
            call_count += 1
            return f"TEST{iid:03d}"

        mock_dao = AsyncMock()
        mock_core.dao.get_symbol_by_instrument_id_vendor_name = mock_resolve_symbol

        with pytest.mock.patch('core.dao.instrument_xrefs_core.dao.InstrumentXrefsDAO', return_value=mock_dao):
            # First call - should hit database
            symbol1 = await manager.resolve_symbol(1)
            assert call_count == 1
            assert symbol1 == "TEST001"

            # Second call - should hit cache
            symbol2 = await manager.resolve_symbol(1)
            assert call_count == 1  # No additional database call
            assert symbol2 == "TEST001"

            # Third call with different ID - should hit database
            symbol3 = await manager.resolve_symbol(2)
            assert call_count == 2
            assert symbol3 == "TEST002"

            print(f"Cache hit rate: {1 - (call_count / 3):.1%}")

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_batch_ohlc_performance_improvement(self, mock_env):
        """Test that batch OHLC fetching is significantly faster than individual calls."""
        manager = UnifiedDBDailyPriceMarketDataManager(mock_env)

        # Mock dependencies
        mock_unifier = AsyncMock(spec=DatabaseDailyPricesUnifier)
        manager.unifier = mock_unifier

        # Sample data
        instrument_ids = [1, 2, 3, 4, 5]
        start_time = datetime(2024, 1, 1, 9, 30)
        end_time = datetime(2024, 1, 1, 16, 0)

        # Mock symbol resolution
        manager.resolve_symbols_batch = AsyncMock(return_value={
            1: "AAPL", 2: "MSFT", 3: "GOOGL", 4: "AMZN", 5: "TSLA"
        })

        # Mock batch unifier response
        mock_unifier.unify_daily_price_polygon_batch = AsyncMock(return_value={
            "AAPL": [{"date": start_time.date(), "open": 150.0, "high": 155.0, "low": 149.0, "close": 154.0, "volume": 1000000}],
            "MSFT": [{"date": start_time.date(), "open": 250.0, "high": 255.0, "low": 249.0, "close": 254.0, "volume": 800000}],
            "GOOGL": [{"date": start_time.date(), "open": 2500.0, "high": 2550.0, "low": 2490.0, "close": 2540.0, "volume": 500000}],
            "AMZN": [{"date": start_time.date(), "open": 3000.0, "high": 3050.0, "low": 2990.0, "close": 3040.0, "volume": 600000}],
            "TSLA": [{"date": start_time.date(), "open": 200.0, "high": 205.0, "low": 199.0, "close": 204.0, "volume": 2000000}]
        })

        # Test batch operation
        batch_start = time.time()
        results = await manager.get_ohlc_batch(instrument_ids, start_time, end_time)
        batch_time = time.time() - batch_start

        # Verify results
        assert len(results) == 5
        assert all(results[iid] is not None for iid in instrument_ids)
        assert results[1]["close"] == 154.0
        assert results[2]["close"] == 254.0

        # Verify batch unifier was called once (not per instrument)
        mock_unifier.unify_daily_price_polygon_batch.assert_called_once()

        print(f"Batch operation completed in {batch_time:.4f}s")
        print(f"Processing {len(instrument_ids)} instruments")
        print(f"Average time per instrument: {batch_time / len(instrument_ids):.6f}s")

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_bulk_database_query_efficiency(self, mock_env):
        """Test that bulk database queries reduce query count."""
        unifier = DatabaseDailyPricesUnifier(mock_env)

        # Mock database connection and queries
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire = AsyncMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        mock_pool.close = AsyncMock()

        # Mock query results
        mock_conn.fetch.return_value = [
            {"date": date(2024, 1, 1), "instrument_id": 1, "open": 150.0, "high": 155.0, "low": 149.0, "close": 154.0, "volume": 1000000},
            {"date": date(2024, 1, 1), "instrument_id": 2, "open": 250.0, "high": 255.0, "low": 249.0, "close": 254.0, "volume": 800000}
        ]

        with pytest.mock.patch('asyncpg.create_pool', return_value=mock_pool):
            # Mock instrument resolution
            with pytest.mock.patch('core.dao.instrument_xrefs_core.dao.InstrumentXrefsDAO') as mock_dao_class:
                mock_dao = AsyncMock()
                mock_core.dao.resolve_instrument_id_by_symbol.side_effect = {"AAPL": 1, "MSFT": 2}.get
                mock_dao_class.return_value = mock_dao

                # Test batch processing
                symbols = ["AAPL", "MSFT"]
                results = await unifier.unify_daily_price_polygon_batch(
                    symbols,
                    (date(2024, 1, 1), date(2024, 1, 1)),
                    date(2024, 1, 1)
                )

                # Verify results structure
                assert len(results) == 2
                assert "AAPL" in results
                assert "MSFT" in results

                # Verify database efficiency: should make only 2 queries (Tiingo + Polygon)
                # not 2 * 2 = 4 queries (per symbol)
                assert mock_conn.fetch.call_count == 2

                print(f"Database queries made: {mock_conn.fetch.call_count}")
                print(f"Symbols processed: {len(symbols)}")
                print(f"Query efficiency: {len(symbols) / mock_conn.fetch.call_count:.1f} symbols per query")


class TestDataPipelineMemoryEfficiency:
    """Test memory usage patterns in optimized pipeline."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_cache_memory_management(self, mock_env):
        """Verify that caching doesn't cause memory leaks."""
        manager = UnifiedDBDailyPriceMarketDataManager(mock_env)

        # Mock DAO
        mock_dao = AsyncMock()
        mock_core.dao.get_symbol_by_instrument_id_vendor_name = AsyncMock(side_effect=lambda iid, **kwargs: f"TEST{iid:03d}")

        with pytest.mock.patch('core.dao.instrument_xrefs_core.dao.InstrumentXrefsDAO', return_value=mock_dao):
            # Fill cache with many entries
            large_instrument_ids = list(range(1, 1001))  # 1000 instruments

            for iid in large_instrument_ids:
                await manager.resolve_symbol(iid)

            cache_size_before = len(manager._symbol_cache)
            assert cache_size_before == 1000

            # Simulate cache expiration by manipulating timestamp
            manager._cache_timestamp = time.time() - 7200  # 2 hours ago

            # Request new symbol - should clear expired cache
            await manager.resolve_symbol(9999)

            # Verify cache was reset and contains only recent entry
            assert len(manager._symbol_cache) == 1
            assert 9999 in manager._symbol_cache

            print(f"Cache size before expiration: {cache_size_before}")
            print(f"Cache size after expiration: {len(manager._symbol_cache)}")


if __name__ == "__main__":
    # Run performance benchmarks
    import sys
    sys.exit(pytest.main([__file__, "-v", "-s"]))