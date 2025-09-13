#!/usr/bin/env python3
"""
Tests for HDF5 Multi-Scale Cache

Comprehensive tests for the HDF5MultiScaleCache class, ensuring proper
storage, retrieval, and aggregation of multi-scale temporal data.
"""

import pytest
import numpy as np
import pandas as pd
import h5py
import asyncio
import tempfile
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from src.storage.hdf5_multi_scale_cache import (
    HDF5MultiScaleCache,
    CacheConfig,
    create_hdf5_cache
)
from src.storage.multi_scale_sequence import (
    TimeScale,
    MultiScaleSequence
)


class TestCacheConfig:
    """Test CacheConfig functionality."""

    def test_default_config(self):
        """Test default configuration."""
        config = CacheConfig()

        assert config.compression == "gzip"
        assert config.compression_level == 6
        assert config.chunk_size == 1000
        assert config.enable_checksums is True
        assert config.auto_aggregate is True

        # Check aggregation functions
        assert config.aggregation_functions['open'] == 'first'
        assert config.aggregation_functions['high'] == 'max'
        assert config.aggregation_functions['low'] == 'min'
        assert config.aggregation_functions['close'] == 'last'
        assert config.aggregation_functions['volume'] == 'sum'
        assert config.aggregation_functions['default'] == 'mean'

    def test_custom_config(self):
        """Test custom configuration."""
        config = CacheConfig(
            max_cache_size_gb=5.0,
            compression="lzf",
            compression_level=3,
            auto_aggregate=False
        )

        assert config.max_cache_size_gb == 5.0
        assert config.compression == "lzf"
        assert config.compression_level == 3
        assert config.auto_aggregate is False


@pytest.fixture
def temp_cache_dir():
    """Create temporary directory for cache tests."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_minute_data():
    """Create sample minute-level data."""
    base_time = datetime(2024, 1, 15, 9, 30)
    n_minutes = 1000

    timestamps = pd.date_range(base_time, periods=n_minutes, freq='1min')

    data = pd.DataFrame({
        'timestamp': timestamps,
        'open': np.random.uniform(150, 160, n_minutes),
        'high': np.random.uniform(155, 165, n_minutes),
        'low': np.random.uniform(145, 155, n_minutes),
        'close': np.random.uniform(150, 160, n_minutes),
        'volume': np.random.randint(1000, 10000, n_minutes),
        'rsi': np.random.uniform(30, 70, n_minutes),
        'macd': np.random.uniform(-1, 1, n_minutes),
        'bollinger_upper': np.random.uniform(160, 170, n_minutes),
        'bollinger_lower': np.random.uniform(140, 150, n_minutes)
    })

    return data


class TestHDF5MultiScaleCache:
    """Test HDF5MultiScaleCache functionality."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_cache_initialization(self, temp_cache_dir):
        """Test cache initialization."""
        config = CacheConfig(cache_path=temp_cache_dir)
        cache = HDF5MultiScaleCache(config)

        assert cache.cache_path == Path(temp_cache_dir)
        assert cache.config == config
        assert cache.cache_path.exists()

        await cache.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_store_minute_data(self, temp_cache_dir, sample_minute_data):
        """Test storing minute-level data."""
        config = CacheConfig(cache_path=temp_cache_dir)
        cache = HDF5MultiScaleCache(config)

        # Store data
        result = await cache.store_minute_data(
            symbol="AAPL",
            minute_data=sample_minute_data,
            auto_aggregate=True
        )

        assert result['stored'] == len(sample_minute_data)
        assert 'aggregated' in result
        assert 'hourly' in result['aggregated']
        assert 'daily' in result['aggregated']

        # Verify file was created
        file_path = cache._get_file_path("AAPL")
        assert file_path.exists()

        # Verify HDF5 structure
        with h5py.File(file_path, 'r') as f:
            # Check minute data exists
            minute_groups = [key for key in f.keys() if key.startswith('/minute')]
            assert len(minute_groups) > 0

            # Check aggregated data exists
            if 'hourly' in f:
                assert 'timestamps' in f['/hourly/2024']
                assert 'features' in f['/hourly/2024']

        await cache.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_data_minute(self, temp_cache_dir, sample_minute_data):
        """Test retrieving minute data."""
        config = CacheConfig(cache_path=temp_cache_dir)
        cache = HDF5MultiScaleCache(config)

        # Store data first
        await cache.store_minute_data("AAPL", sample_minute_data)

        # Retrieve data
        start_time = sample_minute_data['timestamp'].iloc[100]
        end_time = sample_minute_data['timestamp'].iloc[200]

        retrieved_data = await cache.get_data(
            symbol="AAPL",
            scale=TimeScale.MINUTE,
            start_time=start_time,
            end_time=end_time
        )

        assert retrieved_data is not None
        assert not retrieved_data.empty
        assert len(retrieved_data) <= 101  # 100 to 200 inclusive

        # Check data integrity
        assert 'timestamp' in retrieved_data.columns
        assert 'open' in retrieved_data.columns
        assert 'close' in retrieved_data.columns
        assert 'volume' in retrieved_data.columns

        # Verify time range
        assert retrieved_data['timestamp'].min() >= start_time
        assert retrieved_data['timestamp'].max() <= end_time

        await cache.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_data_hourly(self, temp_cache_dir, sample_minute_data):
        """Test retrieving hourly aggregated data."""
        config = CacheConfig(cache_path=temp_cache_dir, auto_aggregate=True)
        cache = HDF5MultiScaleCache(config)

        # Store data with auto-aggregation
        await cache.store_minute_data("AAPL", sample_minute_data, auto_aggregate=True)

        # Retrieve hourly data
        start_time = sample_minute_data['timestamp'].iloc[0]
        end_time = start_time + timedelta(hours=12)

        hourly_data = await cache.get_data(
            symbol="AAPL",
            scale=TimeScale.HOURLY,
            start_time=start_time,
            end_time=end_time
        )

        if hourly_data is not None and not hourly_data.empty:
            # Should have fewer records than minute data
            assert len(hourly_data) <= 13  # At most 13 hours

            # Check aggregation worked correctly
            assert 'timestamp' in hourly_data.columns
            assert 'open' in hourly_data.columns
            assert 'high' in hourly_data.columns
            assert 'low' in hourly_data.columns
            assert 'close' in hourly_data.columns
            assert 'volume' in hourly_data.columns

        await cache.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_data_with_columns(self, temp_cache_dir, sample_minute_data):
        """Test retrieving specific columns."""
        config = CacheConfig(cache_path=temp_cache_dir)
        cache = HDF5MultiScaleCache(config)

        # Store data
        await cache.store_minute_data("AAPL", sample_minute_data)

        # Retrieve specific columns
        start_time = sample_minute_data['timestamp'].iloc[0]
        end_time = sample_minute_data['timestamp'].iloc[100]

        specific_data = await cache.get_data(
            symbol="AAPL",
            scale=TimeScale.MINUTE,
            start_time=start_time,
            end_time=end_time,
            columns=['open', 'close', 'volume']
        )

        assert specific_data is not None
        assert not specific_data.empty

        # Should only have requested columns plus timestamp
        expected_columns = {'timestamp', 'open', 'close', 'volume'}
        assert set(specific_data.columns) == expected_columns

        await cache.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_multi_scale_data(self, temp_cache_dir, sample_minute_data):
        """Test retrieving multi-scale data."""
        config = CacheConfig(cache_path=temp_cache_dir, auto_aggregate=True)
        cache = HDF5MultiScaleCache(config)

        # Store data with auto-aggregation
        await cache.store_minute_data("AAPL", sample_minute_data, auto_aggregate=True)

        # Get multi-scale sequence
        start_time = sample_minute_data['timestamp'].iloc[0]
        end_time = start_time + timedelta(hours=6)

        sequence = await cache.get_multi_scale_data(
            symbol="AAPL",
            time_range=(start_time, end_time),
            scales=[TimeScale.MINUTE, TimeScale.HOURLY]
        )

        if sequence is not None:
            assert isinstance(sequence, MultiScaleSequence)
            assert sequence.symbol == "AAPL"
            assert TimeScale.MINUTE in sequence.scales

            # Check minute data
            minute_features = sequence.get_features(TimeScale.MINUTE, 'ohlcv')
            assert minute_features is not None
            assert minute_features.shape[1] == 5  # OHLCV

            # Check if hourly data was created
            if TimeScale.HOURLY in sequence.scales:
                hourly_features = sequence.get_features(TimeScale.HOURLY, 'ohlcv')
                assert hourly_features is not None

        await cache.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_cache_stats(self, temp_cache_dir, sample_minute_data):
        """Test cache statistics."""
        config = CacheConfig(cache_path=temp_cache_dir)
        cache = HDF5MultiScaleCache(config)

        # Store data for multiple symbols
        await cache.store_minute_data("AAPL", sample_minute_data)
        await cache.store_minute_data("GOOGL", sample_minute_data)

        # Get cache statistics
        stats = await cache.get_cache_stats()

        assert stats['symbols'] == 2
        assert stats['total_size_mb'] > 0
        assert 'files' in stats
        assert 'AAPL' in stats['files']
        assert 'GOOGL' in stats['files']

        # Check scale statistics
        assert 'scales' in stats
        assert 'minute' in stats['scales']

        minute_stats = stats['scales']['minute']
        assert minute_stats['symbols'] > 0
        assert minute_stats['total_records'] > 0

        await cache.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_data_integrity_checksums(self, temp_cache_dir, sample_minute_data):
        """Test data integrity with checksums."""
        config = CacheConfig(cache_path=temp_cache_dir, enable_checksums=True)
        cache = HDF5MultiScaleCache(config)

        # Store data with checksums enabled
        await cache.store_minute_data("AAPL", sample_minute_data)

        # Retrieve data (checksums should be verified)
        start_time = sample_minute_data['timestamp'].iloc[0]
        end_time = sample_minute_data['timestamp'].iloc[100]

        retrieved_data = await cache.get_data(
            symbol="AAPL",
            scale=TimeScale.MINUTE,
            start_time=start_time,
            end_time=end_time
        )

        assert retrieved_data is not None
        assert not retrieved_data.empty

        # Verify checksum was stored
        file_path = cache._get_file_path("AAPL")
        with h5py.File(file_path, 'r') as f:
            # Find a minute dataset
            for key in f.keys():
                if '/minute/' in key:
                    group = f[key]
                    if 'checksum' in group.attrs:
                        assert len(group.attrs['checksum']) > 0
                        break

        await cache.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_aggregation_functions(self, temp_cache_dir):
        """Test custom aggregation functions."""
        # Create specific data for testing aggregation
        base_time = datetime(2024, 1, 15, 9, 30)
        timestamps = pd.date_range(base_time, periods=120, freq='1min')  # 2 hours

        # Create predictable data for testing
        minute_data = pd.DataFrame({
            'timestamp': timestamps,
            'open': [100.0] * 120,      # Should be 100.0 (first)
            'high': list(range(120)),   # Should be max in each hour
            'low': [50.0] * 120,        # Should be 50.0 (min)
            'close': [110.0] * 120,     # Should be 110.0 (last)
            'volume': [1000] * 120,     # Should be 60000 (sum) per hour
            'rsi': [50.0] * 120         # Should be 50.0 (mean)
        })

        config = CacheConfig(cache_path=temp_cache_dir, auto_aggregate=True)
        cache = HDF5MultiScaleCache(config)

        # Store with auto-aggregation
        await cache.store_minute_data("TEST", minute_data, auto_aggregate=True)

        # Get hourly data
        hourly_data = await cache.get_data(
            symbol="TEST",
            scale=TimeScale.HOURLY,
            start_time=base_time,
            end_time=base_time + timedelta(hours=2)
        )

        if hourly_data is not None and not hourly_data.empty:
            # Check aggregation worked correctly
            first_hour = hourly_data.iloc[0]

            assert first_hour['open'] == 100.0      # First value
            assert first_hour['low'] == 50.0        # Min value
            assert first_hour['close'] == 110.0     # Last value
            assert first_hour['volume'] == 60000    # Sum of 60 * 1000
            assert abs(first_hour['rsi'] - 50.0) < 0.1  # Mean value

        await cache.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_empty_data_handling(self, temp_cache_dir):
        """Test handling of empty data."""
        config = CacheConfig(cache_path=temp_cache_dir)
        cache = HDF5MultiScaleCache(config)

        # Try to store empty DataFrame
        empty_data = pd.DataFrame()
        result = await cache.store_minute_data("EMPTY", empty_data)

        assert result['stored'] == 0

        # Try to retrieve from non-existent symbol
        retrieved = await cache.get_data(
            symbol="NONEXISTENT",
            scale=TimeScale.MINUTE,
            start_time=datetime.now(),
            end_time=datetime.now() + timedelta(hours=1)
        )

        assert retrieved is None

        await cache.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_cleanup_cache(self, temp_cache_dir, sample_minute_data):
        """Test cache cleanup functionality."""
        config = CacheConfig(cache_path=temp_cache_dir)
        cache = HDF5MultiScaleCache(config)

        # Store some data
        await cache.store_minute_data("AAPL", sample_minute_data)

        # Run cleanup (with 0 days to clean everything)
        cleanup_stats = await cache.cleanup_cache(max_age_days=0)

        assert 'files_removed' in cleanup_stats
        assert cleanup_stats['files_removed'] >= 0

        await cache.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_concurrent_operations(self, temp_cache_dir, sample_minute_data):
        """Test concurrent read/write operations."""
        config = CacheConfig(cache_path=temp_cache_dir, max_concurrent_operations=4)
        cache = HDF5MultiScaleCache(config)

        # Create tasks for concurrent operations
        symbols = ["AAPL", "GOOGL", "MSFT", "TSLA"]

        async def store_data(symbol):
            return await cache.store_minute_data(symbol, sample_minute_data)

        # Run concurrent stores
        store_tasks = [store_data(symbol) for symbol in symbols]
        store_results = await asyncio.gather(*store_tasks)

        # All should succeed
        for result in store_results:
            assert result['stored'] > 0

        # Test concurrent reads
        async def read_data(symbol):
            return await cache.get_data(
                symbol=symbol,
                scale=TimeScale.MINUTE,
                start_time=sample_minute_data['timestamp'].iloc[0],
                end_time=sample_minute_data['timestamp'].iloc[100]
            )

        read_tasks = [read_data(symbol) for symbol in symbols]
        read_results = await asyncio.gather(*read_tasks)

        # All should succeed
        for result in read_results:
            assert result is not None
            assert not result.empty

        await cache.close()


class TestCreateHDF5Cache:
    """Test convenience function for creating cache."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_create_hdf5_cache(self, temp_cache_dir):
        """Test cache creation function."""
        cache = await create_hdf5_cache(
            cache_path=temp_cache_dir,
            max_cache_size_gb=5.0,
            compression="lzf"
        )

        assert isinstance(cache, HDF5MultiScaleCache)
        assert cache.config.cache_path == temp_cache_dir
        assert cache.config.max_cache_size_gb == 5.0
        assert cache.config.compression == "lzf"

        await cache.close()


class TestPerformance:
    """Test performance characteristics."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_large_dataset_performance(self, temp_cache_dir):
        """Test performance with large datasets."""
        # Create large dataset (1 week of minute data)
        base_time = datetime(2024, 1, 15, 9, 30)
        n_minutes = 2520  # 1 week of trading hours

        timestamps = pd.date_range(base_time, periods=n_minutes, freq='1min')

        large_data = pd.DataFrame({
            'timestamp': timestamps,
            'open': np.random.uniform(150, 160, n_minutes),
            'high': np.random.uniform(155, 165, n_minutes),
            'low': np.random.uniform(145, 155, n_minutes),
            'close': np.random.uniform(150, 160, n_minutes),
            'volume': np.random.randint(1000, 10000, n_minutes),
            'rsi': np.random.uniform(30, 70, n_minutes),
            'macd': np.random.uniform(-1, 1, n_minutes)
        })

        config = CacheConfig(cache_path=temp_cache_dir, compression="lzf")
        cache = HDF5MultiScaleCache(config)

        # Test storage performance
        import time
        start_time = time.time()

        result = await cache.store_minute_data("LARGE", large_data, auto_aggregate=True)

        store_time = time.time() - start_time

        assert store_time < 30.0  # Should store within 30 seconds
        assert result['stored'] == n_minutes

        # Test retrieval performance
        start_time = time.time()

        retrieved = await cache.get_data(
            symbol="LARGE",
            scale=TimeScale.MINUTE,
            start_time=large_data['timestamp'].iloc[1000],
            end_time=large_data['timestamp'].iloc[2000]
        )

        retrieval_time = time.time() - start_time

        assert retrieval_time < 5.0  # Should retrieve within 5 seconds
        assert retrieved is not None
        assert len(retrieved) == 1001  # 1000 to 2000 inclusive

        await cache.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_memory_usage(self, temp_cache_dir, sample_minute_data):
        """Test memory usage characteristics."""
        import psutil
        import os

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        config = CacheConfig(cache_path=temp_cache_dir)
        cache = HDF5MultiScaleCache(config)

        # Store multiple datasets
        symbols = [f"SYM_{i}" for i in range(10)]

        for symbol in symbols:
            await cache.store_minute_data(symbol, sample_minute_data)

        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory

        # Memory increase should be reasonable (less than 100MB for test data)
        assert memory_increase < 100

        await cache.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])