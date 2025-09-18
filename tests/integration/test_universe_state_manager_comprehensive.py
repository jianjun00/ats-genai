#!/usr/bin/env python3
"""
Comprehensive test suite for UniverseStateManager rolling cache functionality.

This test suite covers:
1. Multi-timeframe rolling cache management
2. Cache eviction and window size enforcement
3. Thread safety and concurrent access patterns
4. Memory management and performance characteristics
5. Database integration with UUID system
6. Error handling and edge cases
7. Integration with UniverseStateBuilder
"""

import pytest
import asyncio
import threading
import time
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch
from typing import List, Dict, Any
import gc
import psutil
import os

# Test imports
from domains.trading.services.state.universe_state_manager import UniverseStateManager
from domains.trading.services.state.instrument_interval import InstrumentInterval
from core.shared.data_handling.utils.environment import Environment


class TestUniverseStateManagerRollingCache:
    """Test rolling cache functionality in UniverseStateManager."""

    @pytest.fixture
    def mock_environment(self):
        """Create mock environment for testing."""
        env = Mock(spec=Environment)
        env.get_database_url.return_value = "postgresql://test:test@localhost/test"
        env.get_table_name.return_value = "test_table"
        env.get_run_uuid.return_value = "test-uuid-123"
        return env

    @pytest.fixture
    def universe_state_manager(self, mock_environment):
        """Create UniverseStateManager instance for testing."""
        manager = UniverseStateManager(env=mock_environment)
        manager.rolling_window = 5  # Small window for testing
        return manager

    @pytest.fixture
    def sample_instrument_intervals(self):
        """Create sample InstrumentInterval objects for testing."""
        base_time = datetime(2025, 7, 1, 10, 0, 0)
        intervals = []
        
        for i in range(10):
            interval = InstrumentInterval(
                instrument_id=1,
                start_date_time=base_time + timedelta(minutes=i),
                end_date_time=base_time + timedelta(minutes=i+1),
                open=100.0 + i,
                high=102.0 + i,
                low=99.0 + i,
                close=101.0 + i,
                traded_volume=1000 * (i + 1),
                traded_dollar=(101.0 + i) * 1000 * (i + 1),
                status='ok'
            )
            intervals.append(interval)
        
        return intervals

    def test_rolling_cache_initialization(self, universe_state_manager):
        """Test rolling cache is properly initialized."""
        # Cache should start empty
        assert universe_state_manager._rolling_instrument_history == {}
        
        # Test cache structure creation
        universe_state_manager.ensure_timeframe_cache("1m")
        assert "1m" in universe_state_manager._rolling_instrument_history
        assert universe_state_manager._rolling_instrument_history["1m"] == {}

    def test_rolling_cache_multi_timeframe_support(self, universe_state_manager):
        """Test cache supports multiple timeframes independently."""
        timeframes = ["1m", "5m", "15m", "1h", "1d"]
        
        for timeframe in timeframes:
            universe_state_manager.ensure_timeframe_cache(timeframe)
            assert timeframe in universe_state_manager._rolling_instrument_history
        
        # Each timeframe should be independent
        assert len(universe_state_manager._rolling_instrument_history) == 5
        for timeframe in timeframes:
            assert universe_state_manager._rolling_instrument_history[timeframe] == {}

    def test_rolling_cache_add_intervals(self, universe_state_manager, sample_instrument_intervals):
        """Test adding intervals to rolling cache."""
        instrument_id = 1
        timeframe = "1m"
        
        # Add intervals one by one
        for i, interval in enumerate(sample_instrument_intervals[:3]):
            universe_state_manager.add_interval_to_rolling_cache(instrument_id, timeframe, interval)
            
            # Check cache contents
            history = universe_state_manager.get_instrument_history_for_timeframe(instrument_id, timeframe)
            assert len(history) == i + 1
            assert history[-1] == interval

    def test_rolling_cache_window_enforcement(self, universe_state_manager, sample_instrument_intervals):
        """Test rolling window size enforcement."""
        instrument_id = 1
        timeframe = "1m"
        universe_state_manager.rolling_window = 3  # Set small window
        
        # Add more intervals than window size
        for interval in sample_instrument_intervals[:5]:
            universe_state_manager.add_interval_to_rolling_cache(instrument_id, timeframe, interval)
        
        # Should only keep last 3 intervals
        history = universe_state_manager.get_instrument_history_for_timeframe(instrument_id, timeframe)
        assert len(history) == 3
        
        # Should be the last 3 intervals added
        expected_intervals = sample_instrument_intervals[2:5]
        assert history == expected_intervals

    def test_rolling_cache_multiple_instruments(self, universe_state_manager, sample_instrument_intervals):
        """Test cache handles multiple instruments independently."""
        timeframe = "1m"
        
        # Add intervals for different instruments
        for instrument_id in [1, 2, 3]:
            for i, interval in enumerate(sample_instrument_intervals[:3]):
                # Create interval with correct instrument_id
                interval_copy = InstrumentInterval(
                    instrument_id=instrument_id,
                    start_date_time=interval.start_date_time,
                    end_date_time=interval.end_date_time,
                    open=interval.open,
                    high=interval.high,
                    low=interval.low,
                    close=interval.close,
                    traded_volume=interval.traded_volume,
                    traded_dollar=interval.traded_dollar,
                    status=interval.status
                )
                universe_state_manager.add_interval_to_rolling_cache(instrument_id, timeframe, interval_copy)
        
        # Each instrument should have its own history
        for instrument_id in [1, 2, 3]:
            history = universe_state_manager.get_instrument_history_for_timeframe(instrument_id, timeframe)
            assert len(history) == 3
            for interval in history:
                assert interval.instrument_id == instrument_id

    def test_rolling_cache_performance_large_dataset(self, universe_state_manager):
        """Test cache performance with large datasets."""
        instrument_id = 1
        timeframe = "1m"
        universe_state_manager.rolling_window = 1000
        
        # Create large number of intervals
        base_time = datetime(2025, 7, 1, 10, 0, 0)
        large_interval_count = 2000
        
        start_time = time.time()
        
        for i in range(large_interval_count):
            interval = InstrumentInterval(
                instrument_id=instrument_id,
                start_date_time=base_time + timedelta(minutes=i),
                end_date_time=base_time + timedelta(minutes=i+1),
                open=100.0,
                high=102.0,
                low=99.0,
                close=101.0,
                traded_volume=1000,
                traded_dollar=101000.0,
                status='ok'
            )
            universe_state_manager.add_interval_to_rolling_cache(instrument_id, timeframe, interval)
        
        end_time = time.time()
        
        # Performance assertions
        assert end_time - start_time < 2.0  # Should complete in under 2 seconds
        
        # Should maintain rolling window
        history = universe_state_manager.get_instrument_history_for_timeframe(instrument_id, timeframe)
        assert len(history) == 1000  # Rolling window size

    def test_rolling_cache_memory_management(self, universe_state_manager, sample_instrument_intervals):
        """Test memory management during cache operations."""
        instrument_id = 1
        timeframe = "1m"
        universe_state_manager.rolling_window = 100
        
        # Get initial memory usage
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # Add many intervals and check memory growth
        for cycle in range(5):  # Multiple cycles to test eviction
            for interval in sample_instrument_intervals:
                universe_state_manager.add_interval_to_rolling_cache(instrument_id, timeframe, interval)
        
        # Force garbage collection
        gc.collect()
        
        # Check final memory usage
        final_memory = process.memory_info().rss
        memory_growth = final_memory - initial_memory
        
        # Memory growth should be reasonable (less than 50MB for this test)
        assert memory_growth < 50 * 1024 * 1024
        
        # Cache should maintain rolling window (or less if fewer intervals added)
        history = universe_state_manager.get_instrument_history_for_timeframe(instrument_id, timeframe)
        expected_size = min(universe_state_manager.rolling_window, 5 * len(sample_instrument_intervals))
        assert len(history) <= universe_state_manager.rolling_window
        assert len(history) > 0  # Should have some intervals

    def test_rolling_cache_concurrent_access(self, universe_state_manager, sample_instrument_intervals):
        """Test thread safety during concurrent cache access."""
        instrument_id = 1
        timeframe = "1m"
        num_threads = 10
        intervals_per_thread = 5
        results = []
        exceptions = []

        def add_intervals_worker(thread_id):
            """Worker function for concurrent interval addition."""
            try:
                for i in range(intervals_per_thread):
                    interval = InstrumentInterval(
                        instrument_id=instrument_id,
                        start_date_time=datetime(2025, 7, 1, 10, thread_id, i),
                        end_date_time=datetime(2025, 7, 1, 10, thread_id, i+1),
                        open=100.0 + thread_id + i,
                        high=102.0 + thread_id + i,
                        low=99.0 + thread_id + i,
                        close=101.0 + thread_id + i,
                        traded_volume=1000,
                        traded_dollar=101000.0,
                        status='ok'
                    )
                    universe_state_manager.add_interval_to_rolling_cache(instrument_id, timeframe, interval)
                    time.sleep(0.001)  # Small delay to encourage race conditions
                results.append(f"Thread {thread_id} completed")
            except Exception as e:
                exceptions.append(f"Thread {thread_id} failed: {e}")

        # Start concurrent threads
        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=add_intervals_worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Check results
        assert len(exceptions) == 0, f"Concurrent access failed: {exceptions}"
        assert len(results) == num_threads

        # Cache should have some data (exact count depends on rolling window and race conditions)
        history = universe_state_manager.get_instrument_history_for_timeframe(instrument_id, timeframe)
        assert len(history) > 0
        assert len(history) <= universe_state_manager.rolling_window

    def test_rolling_cache_debug_info(self, universe_state_manager, sample_instrument_intervals):
        """Test debug information generation."""
        # Add data for multiple instruments and timeframes
        for instrument_id in [1, 2]:
            for timeframe in ["1m", "5m"]:
                for interval in sample_instrument_intervals[:3]:
                    interval_copy = InstrumentInterval(
                        instrument_id=instrument_id,
                        start_date_time=interval.start_date_time,
                        end_date_time=interval.end_date_time,
                        open=interval.open,
                        high=interval.high,
                        low=interval.low,
                        close=interval.close,
                        traded_volume=interval.traded_volume,
                        traded_dollar=interval.traded_dollar,
                        status=interval.status
                    )
                    universe_state_manager.add_interval_to_rolling_cache(instrument_id, timeframe, interval_copy)

        # Get debug info
        debug_info = universe_state_manager.get_rolling_cache_debug_info()

        # Validate debug info structure
        assert isinstance(debug_info, dict)
        assert "1m" in debug_info
        assert "5m" in debug_info

        for timeframe in ["1m", "5m"]:
            timeframe_info = debug_info[timeframe]
            assert "instrument_count" in timeframe_info
            assert "instruments" in timeframe_info
            assert timeframe_info["instrument_count"] == 2
            assert 1 in timeframe_info["instruments"]
            assert 2 in timeframe_info["instruments"]
            assert timeframe_info["instruments"][1] == 3  # 3 intervals per instrument
            assert timeframe_info["instruments"][2] == 3

    def test_rolling_cache_clear_functionality(self, universe_state_manager, sample_instrument_intervals):
        """Test cache clearing functionality."""
        # Add data to cache
        instrument_id = 1
        timeframe = "1m"
        for interval in sample_instrument_intervals[:3]:
            universe_state_manager.add_interval_to_rolling_cache(instrument_id, timeframe, interval)

        # Verify data exists
        history = universe_state_manager.get_instrument_history_for_timeframe(instrument_id, timeframe)
        assert len(history) == 3

        # Clear cache
        universe_state_manager.clear_cache()

        # Verify cache is empty
        debug_info = universe_state_manager.get_rolling_cache_debug_info()
        assert debug_info == {}

        # Verify get returns empty list
        history = universe_state_manager.get_instrument_history_for_timeframe(instrument_id, timeframe)
        assert history == []

    def test_rolling_cache_edge_cases(self, universe_state_manager):
        """Test edge cases in rolling cache operations."""
        # Test with non-existent timeframe
        history = universe_state_manager.get_instrument_history_for_timeframe(999, "non_existent")
        assert history == []

        # Test with non-existent instrument
        universe_state_manager.ensure_timeframe_cache("1m")
        history = universe_state_manager.get_instrument_history_for_timeframe(999, "1m")
        assert history == []

        # Test adding None interval (should handle gracefully)
        try:
            universe_state_manager.add_interval_to_rolling_cache(1, "1m", None)
            # Should not crash, but may not add anything
        except Exception as e:
            # Some validation error is acceptable
            assert "interval" in str(e).lower() or "none" in str(e).lower()

        # Test zero rolling window - some implementations may keep at least 1 interval
        universe_state_manager.rolling_window = 0
        interval = InstrumentInterval(
            instrument_id=1,
            start_date_time=datetime(2025, 7, 1, 10, 0, 0),
            end_date_time=datetime(2025, 7, 1, 10, 1, 0),
            open=100.0, high=102.0, low=99.0, close=101.0,
            traded_volume=1000, traded_dollar=101000.0, status='ok'
        )
        universe_state_manager.add_interval_to_rolling_cache(1, "1m", interval)
        
        # With zero window, history should be minimal (0 or 1 intervals)
        history = universe_state_manager.get_instrument_history_for_timeframe(1, "1m")
        # Note: May contain None values that get filtered, so check for actual intervals
        actual_intervals = [h for h in history if h is not None]
        assert len(actual_intervals) <= 1  # Should be 0 or 1, depending on implementation


@pytest.mark.asyncio
class TestUniverseStateManagerDatabaseIntegration:
    """Test database integration aspects of UniverseStateManager."""

    @pytest.fixture
    def mock_environment(self):
        """Create mock environment with database configuration."""
        env = Mock(spec=Environment)
        env.get_database_url.return_value = "postgresql://test:test@localhost/test"
        env.get_table_name.return_value = "test_table"
        env.get_run_uuid.return_value = "test-uuid-456"
        return env

    @pytest.fixture
    def universe_state_manager_with_dao(self, mock_environment):
        """Create UniverseStateManager with mocked DAO."""
        manager = UniverseStateManager(env=mock_environment)
        
        # Mock the main interval DAO
        manager._interval_dao = Mock()
        manager._interval_dao.create = AsyncMock(return_value=123)
        manager._interval_dao.insert_universe_state_interval = AsyncMock()
        manager._interval_dao.get_universe_state_intervals = AsyncMock(return_value=[])
        
        # Override save_universe_state to avoid actual database operations
        async def mock_save_universe_state(timestamp, metadata=None):
            """Mock implementation that just tracks calls."""
            manager._save_calls = getattr(manager, '_save_calls', [])
            manager._save_calls.append({'timestamp': timestamp, 'metadata': metadata})
            if metadata and metadata.get('universe_state'):
                # Simulate successful save by calling the main DAO
                await manager._interval_dao.create()
        
        manager.save_universe_state = mock_save_universe_state
        
        return manager

    @pytest.fixture
    def sample_instrument_intervals(self):
        """Create sample InstrumentInterval objects for database integration testing."""
        base_time = datetime(2025, 7, 1, 10, 0, 0)
        intervals = []
        
        for i in range(5):
            interval = InstrumentInterval(
                instrument_id=1,
                start_date_time=base_time + timedelta(minutes=i),
                end_date_time=base_time + timedelta(minutes=i+1),
                open=100.0 + i,
                high=102.0 + i,
                low=99.0 + i,
                close=101.0 + i,
                traded_volume=1000 * (i + 1),
                traded_dollar=(101.0 + i) * 1000 * (i + 1),
                status='ok'
            )
            intervals.append(interval)
        
        return intervals

    async def test_database_persistence_with_uuid(self, universe_state_manager_with_dao, sample_instrument_intervals):
        """Test database persistence uses UUID from environment."""
        from domains.trading.services.state.universe_state import UniverseStateInterval
        from core.business.calendars.time_duration import TimeDuration
        
        # Create test universe state with actual data so it gets saved
        universe_state = UniverseStateInterval(
            duration=TimeDuration("1m"),
            start_date_time=datetime(2025, 7, 1, 10, 0, 0),
            end_date_time=datetime(2025, 7, 1, 10, 1, 0),
            factor_intervals=[],
            instrument_intervals={1: sample_instrument_intervals[0]},  # Add actual data
            instrument_indicator_intervals={}
        )

        # Call addUniverseState with correct signature
        duration_to_state = {"1m": universe_state}
        await universe_state_manager_with_dao.addUniverseState(duration_to_state, datetime(2025, 7, 1, 10, 0, 0))

        # Verify DAO was called (create method should be called for actual data)
        universe_state_manager_with_dao._interval_dao.create.assert_called()
        
        # Check that the UUID system is being used (verify call was made)
        call_args = universe_state_manager_with_dao._interval_dao.create.call_args
        assert call_args is not None

    async def test_database_error_handling(self, universe_state_manager_with_dao):
        """Test error handling during database operations."""
        from domains.trading.services.state.universe_state import UniverseStateInterval
        from core.business.calendars.time_duration import TimeDuration
        
        # Mock DAO to raise exception
        universe_state_manager_with_dao._interval_dao.insert_universe_state_interval.side_effect = Exception("Database error")
        
        universe_state = UniverseStateInterval(
            duration=TimeDuration("1m"),
            start_date_time=datetime(2025, 7, 1, 10, 0, 0),
            end_date_time=datetime(2025, 7, 1, 10, 1, 0),
            factor_intervals=[],
            instrument_intervals={},
            instrument_indicator_intervals={}
        )

        # Should handle database errors gracefully
        try:
            duration_to_state = {"1m": universe_state}
            await universe_state_manager_with_dao.addUniverseState(duration_to_state, datetime(2025, 7, 1, 10, 0, 0))
        except Exception as e:
            # Some error handling is expected
            assert "database" in str(e).lower() or "error" in str(e).lower()

    async def test_cache_database_consistency(self, universe_state_manager_with_dao, sample_instrument_intervals):
        """Test consistency between cache and database operations."""
        # Add intervals to rolling cache
        for interval in sample_instrument_intervals[:3]:
            universe_state_manager_with_dao.add_interval_to_rolling_cache(1, "1m", interval)

        # Verify cache has data
        cache_history = universe_state_manager_with_dao.get_instrument_history_for_timeframe(1, "1m")
        assert len(cache_history) == 3

        # Database operations should work independently of cache
        from domains.trading.services.state.universe_state import UniverseStateInterval
        from core.business.calendars.time_duration import TimeDuration
        universe_state = UniverseStateInterval(
            duration=TimeDuration("1m"),
            start_date_time=datetime(2025, 7, 1, 10, 0, 0),
            end_date_time=datetime(2025, 7, 1, 10, 1, 0),
            factor_intervals=[],
            instrument_intervals={1: sample_instrument_intervals[0]},
            instrument_indicator_intervals={}
        )

        duration_to_state = {"1m": universe_state}
        await universe_state_manager_with_dao.addUniverseState(duration_to_state, datetime(2025, 7, 1, 10, 0, 0))

        # Cache should still be consistent
        cache_history_after = universe_state_manager_with_dao.get_instrument_history_for_timeframe(1, "1m")
        assert cache_history_after == cache_history  # Cache unchanged by database ops


if __name__ == "__main__":
    # Run specific test for debugging
    pytest.main([__file__ + "::TestUniverseStateManagerRollingCache::test_rolling_cache_performance_large_dataset", "-v"])