"""
Comprehensive tests for Hybrid Minute Data Manager.

Tests storage tiers, data lifecycle management, query performance,
and archival strategies for 1-minute financial data.
"""

import pytest
import asyncio
import tempfile
import shutil
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import asyncpg
import json

from storage.hybrid_minute_data_manager import (
    HybridMinuteDataManager,
    StorageConfig,
    create_hybrid_manager,
    migrate_existing_data
)


class TestStorageConfig:
    """Test StorageConfig settings."""
    
    def test_default_config(self):
        """Test default configuration."""
        config = StorageConfig()
        
        assert config.base_data_path == "/home/jianjun/ats/data/STK/1min"
        assert config.hot_data_days == 30
        assert config.warm_data_days == 90
        assert config.cold_data_days == 365
        assert config.db_pool_size == 20
        assert config.partition_by == "year_month"
        assert config.compression == "snappy"
        assert config.batch_size == 10000
    
    def test_custom_config(self):
        """Test custom configuration."""
        config = StorageConfig(
            base_data_path="/tmp/test_data",
            hot_data_days=7,
            warm_data_days=30,
            partition_by="year",
            compression="gzip",
            batch_size=5000
        )
        
        assert config.base_data_path == "/tmp/test_data"
        assert config.hot_data_days == 7
        assert config.warm_data_days == 30
        assert config.partition_by == "year"
        assert config.compression == "gzip"
        assert config.batch_size == 5000


class TestHybridMinuteDataManager:
    """Test HybridMinuteDataManager functionality."""
    
    @pytest.fixture
    def temp_storage_path(self):
        """Create temporary storage directory."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.fixture
    def storage_config(self, temp_storage_path):
        """Create test storage configuration."""
        return StorageConfig(
            base_data_path=temp_storage_path,
            hot_data_days=30,
            warm_data_days=90,
            batch_size=100
        )
    
    @pytest.fixture
    def mock_pool(self):
        """Create mock database pool."""
        pool = AsyncMock(spec=asyncpg.Pool)
        conn = AsyncMock()
        pool.acquire.return_value.__aenter__.return_value = conn
        pool.acquire.return_value.__aexit__.return_value = None
        return pool
    
    @pytest.fixture
    def manager(self, mock_pool, storage_config):
        """Create test manager instance."""
        return HybridMinuteDataManager(mock_pool, storage_config)
    
    def test_manager_initialization(self, manager, storage_config, temp_storage_path):
        """Test manager initialization."""
        assert manager.config == storage_config
        assert manager.pool is not None
        assert manager.executor is not None
        
        # Check that storage directories were created
        base_path = Path(temp_storage_path)
        assert (base_path / "hot").exists()
        assert (base_path / "warm").exists()
        assert (base_path / "cold").exists()
        assert (base_path / "archive").exists()
    
    def test_get_file_path_year_month(self, manager):
        """Test file path generation with year_month partitioning."""
        timestamp = datetime(2024, 3, 15, 10, 30)
        file_path = manager._get_file_path('AAPL', timestamp, 'cold')
        
        expected_dir = Path(manager.config.base_data_path) / "cold" / "AAPL" / "2024" / "03"
        expected_file = expected_dir / "AAPL_2024_03.parquet"
        
        assert file_path == expected_file
        assert expected_dir.exists()  # Directory should be created
    
    def test_get_file_path_year(self, temp_storage_path):
        """Test file path generation with year partitioning."""
        config = StorageConfig(
            base_data_path=temp_storage_path,
            partition_by="year"
        )
        pool = AsyncMock()
        manager = HybridMinuteDataManager(pool, config)
        
        timestamp = datetime(2024, 3, 15, 10, 30)
        file_path = manager._get_file_path('AAPL', timestamp, 'warm')
        
        expected_dir = Path(temp_storage_path) / "warm" / "AAPL" / "2024"
        expected_file = expected_dir / "AAPL_2024.parquet"
        
        assert file_path == expected_file
    
    def test_get_file_path_year_month_day(self, temp_storage_path):
        """Test file path generation with year_month_day partitioning."""
        config = StorageConfig(
            base_data_path=temp_storage_path,
            partition_by="year_month_day"
        )
        pool = AsyncMock()
        manager = HybridMinuteDataManager(pool, config)
        
        timestamp = datetime(2024, 3, 15, 10, 30)
        file_path = manager._get_file_path('AAPL', timestamp, 'cold')
        
        expected_dir = Path(temp_storage_path) / "cold" / "AAPL" / "2024" / "03"
        expected_file = expected_dir / "AAPL_2024_03_15.parquet"
        
        assert file_path == expected_file
    
    def test_get_partition_key(self, manager):
        """Test partition key generation."""
        timestamp = datetime(2024, 3, 15, 10, 30)
        
        # Default is year_month
        key = manager._get_partition_key(timestamp)
        assert key == "2024_03"
        
        # Test year partitioning
        manager.config.partition_by = "year"
        key = manager._get_partition_key(timestamp)
        assert key == "2024"
        
        # Test year_month_day partitioning
        manager.config.partition_by = "year_month_day"
        key = manager._get_partition_key(timestamp)
        assert key == "2024_03_15"
    
    @pytest.mark.asyncio
    async def test_store_minute_data_empty(self, manager):
        """Test storing empty data."""
        result = await manager.store_minute_data('AAPL', [])
        
        assert result == {'stored_hot': 0, 'stored_cold': 0, 'errors': 0}
    
    @pytest.mark.asyncio
    async def test_store_minute_data_hot_tier(self, manager):
        """Test storing recent data in hot tier."""
        now = datetime.now()
        data = [
            {
                'symbol': 'AAPL',
                'timestamp': now,
                'open': 180.0,
                'high': 181.0,
                'low': 179.0,
                'close': 180.5,
                'volume': 1000000,
                'vendor': 'polygon'
            }
        ]
        
        # Mock the hot data storage
        with patch.object(manager, '_store_hot_data', return_value=1) as mock_hot:
            result = await manager.store_minute_data('AAPL', data)
        
        assert result['stored_hot'] == 1
        assert result['stored_cold'] == 0
        assert result['stored_warm'] == 0
        mock_hot.assert_called_once_with('AAPL', data)
    
    @pytest.mark.asyncio
    async def test_store_minute_data_cold_tier(self, manager):
        """Test storing old data in cold tier."""
        old_date = datetime.now() - timedelta(days=200)
        data = [
            {
                'symbol': 'AAPL',
                'timestamp': old_date,
                'open': 180.0,
                'high': 181.0,
                'low': 179.0,
                'close': 180.5,
                'volume': 1000000,
                'vendor': 'polygon'
            }
        ]
        
        # Mock the cold data storage
        with patch.object(manager, '_store_cold_data', return_value=1) as mock_cold:
            result = await manager.store_minute_data('AAPL', data)
        
        assert result['stored_hot'] == 0
        assert result['stored_cold'] == 1
        assert result['stored_warm'] == 0
        mock_cold.assert_called_once_with('AAPL', data)
    
    @pytest.mark.asyncio
    async def test_store_minute_data_force_tier(self, manager):
        """Test forcing data to specific tier."""
        now = datetime.now()
        data = [
            {
                'symbol': 'AAPL',
                'timestamp': now,
                'open': 180.0,
                'high': 181.0,
                'low': 179.0,
                'close': 180.5,
                'volume': 1000000,
                'vendor': 'polygon'
            }
        ]
        
        # Force recent data to cold tier
        with patch.object(manager, '_store_cold_data', return_value=1) as mock_cold:
            result = await manager.store_minute_data('AAPL', data, force_tier='cold')
        
        assert result['stored_cold'] == 1
        assert result['stored_hot'] == 0
        mock_cold.assert_called_once_with('AAPL', data)
    
    @pytest.mark.asyncio
    async def test_store_minute_data_mixed_tiers(self, manager):
        """Test storing data across multiple tiers."""
        now = datetime.now()
        data = [
            # Hot data (recent)
            {
                'symbol': 'AAPL',
                'timestamp': now,
                'open': 180.0, 'high': 181.0, 'low': 179.0, 'close': 180.5,
                'volume': 1000000, 'vendor': 'polygon'
            },
            # Warm data (1-3 months old)
            {
                'symbol': 'AAPL',
                'timestamp': now - timedelta(days=60),
                'open': 175.0, 'high': 176.0, 'low': 174.0, 'close': 175.5,
                'volume': 800000, 'vendor': 'polygon'
            },
            # Cold data (>3 months old)
            {
                'symbol': 'AAPL',
                'timestamp': now - timedelta(days=200),
                'open': 170.0, 'high': 171.0, 'low': 169.0, 'close': 170.5,
                'volume': 600000, 'vendor': 'polygon'
            }
        ]
        
        with patch.object(manager, '_store_hot_data', return_value=1) as mock_hot, \
             patch.object(manager, '_store_warm_data', return_value=1) as mock_warm, \
             patch.object(manager, '_store_cold_data', return_value=1) as mock_cold:
            
            result = await manager.store_minute_data('AAPL', data)
        
        assert result['stored_hot'] == 1
        assert result['stored_warm'] == 1
        assert result['stored_cold'] == 1
        assert result['errors'] == 0
    
    @pytest.mark.asyncio
    async def test_store_hot_data_database_insert(self, manager):
        """Test hot data database insertion."""
        data = [
            {
                'symbol': 'AAPL',
                'timestamp': datetime.now(),
                'open': 180.0, 'high': 181.0, 'low': 179.0, 'close': 180.5,
                'volume': 1000000, 'vendor': 'polygon', 'quality_score': 0.9
            }
        ]
        
        # Mock database connection
        mock_conn = AsyncMock()
        manager.pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        with patch('storage.hybrid_minute_data_manager.env.get_table_name', return_value='test_minute_bars'):
            stored_count = await manager._store_hot_data('AAPL', data)
        
        assert stored_count == 1
        mock_conn.executemany.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_store_parquet_data_new_file(self, manager, temp_storage_path):
        """Test storing data to new Parquet file."""
        data = [
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2024, 1, 15, 10, 30),
                'open': 180.0, 'high': 181.0, 'low': 179.0, 'close': 180.5,
                'volume': 1000000, 'vendor': 'polygon'
            }
        ]
        
        stored_count = await manager._store_parquet_data('AAPL', data, 'cold', compress=True)
        
        assert stored_count == 1
        
        # Check file was created
        expected_file = Path(temp_storage_path) / "cold" / "AAPL" / "2024" / "01" / "AAPL_2024_01.parquet"
        assert expected_file.exists()
        
        # Verify file contents
        df = pd.read_parquet(expected_file)
        assert len(df) == 1
        assert df.iloc[0]['symbol'] == 'AAPL'
        assert df.iloc[0]['open'] == 180.0
    
    @pytest.mark.asyncio
    async def test_store_parquet_data_append_to_existing(self, manager, temp_storage_path):
        """Test appending data to existing Parquet file."""
        # Create initial file
        initial_data = [
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2024, 1, 15, 10, 30),
                'open': 180.0, 'high': 181.0, 'low': 179.0, 'close': 180.5,
                'volume': 1000000, 'vendor': 'polygon'
            }
        ]
        
        await manager._store_parquet_data('AAPL', initial_data, 'cold')
        
        # Add more data
        additional_data = [
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2024, 1, 15, 10, 31),
                'open': 180.5, 'high': 181.5, 'low': 180.0, 'close': 181.0,
                'volume': 800000, 'vendor': 'polygon'
            }
        ]
        
        await manager._store_parquet_data('AAPL', additional_data, 'cold')
        
        # Check combined file
        expected_file = Path(temp_storage_path) / "cold" / "AAPL" / "2024" / "01" / "AAPL_2024_01.parquet"
        df = pd.read_parquet(expected_file)
        assert len(df) == 2
        assert df.iloc[0]['timestamp'] < df.iloc[1]['timestamp']  # Should be sorted
    
    @pytest.mark.asyncio
    async def test_query_minute_data_hot_only(self, manager):
        """Test querying data from hot storage only."""
        now = datetime.now()
        start_date = now - timedelta(days=1)
        end_date = now
        
        # Mock hot data query
        mock_df = pd.DataFrame([
            {
                'symbol': 'AAPL',
                'timestamp': now,
                'open': 180.0, 'high': 181.0, 'low': 179.0, 'close': 180.5,
                'volume': 1000000
            }
        ])
        
        with patch.object(manager, '_query_hot_data', return_value=mock_df) as mock_hot:
            result = await manager.query_minute_data('AAPL', start_date, end_date)
        
        mock_hot.assert_called_once()
        assert len(result) == 1
        assert result.iloc[0]['symbol'] == 'AAPL'
    
    @pytest.mark.asyncio
    async def test_query_minute_data_cold_only(self, manager):
        """Test querying data from cold storage only."""
        old_start = datetime.now() - timedelta(days=200)
        old_end = datetime.now() - timedelta(days=100)
        
        # Mock cold data query
        mock_df = pd.DataFrame([
            {
                'symbol': 'AAPL',
                'timestamp': old_start + timedelta(days=1),
                'open': 170.0, 'high': 171.0, 'low': 169.0, 'close': 170.5,
                'volume': 600000
            }
        ])
        
        with patch.object(manager, '_query_cold_data', return_value=mock_df) as mock_cold:
            result = await manager.query_minute_data('AAPL', old_start, old_end)
        
        mock_cold.assert_called_once()
        assert len(result) == 1
        assert result.iloc[0]['symbol'] == 'AAPL'
    
    @pytest.mark.asyncio
    async def test_query_minute_data_mixed_storage(self, manager):
        """Test querying data across hot and cold storage."""
        now = datetime.now()
        start_date = now - timedelta(days=60)  # Spans both hot and cold
        end_date = now
        
        # Mock both hot and cold data
        hot_df = pd.DataFrame([
            {
                'symbol': 'AAPL',
                'timestamp': now,
                'open': 180.0, 'high': 181.0, 'low': 179.0, 'close': 180.5,
                'volume': 1000000
            }
        ])
        
        cold_df = pd.DataFrame([
            {
                'symbol': 'AAPL',
                'timestamp': start_date + timedelta(days=1),
                'open': 175.0, 'high': 176.0, 'low': 174.0, 'close': 175.5,
                'volume': 800000
            }
        ])
        
        with patch.object(manager, '_query_hot_data', return_value=hot_df), \
             patch.object(manager, '_query_cold_data', return_value=cold_df):
            
            result = await manager.query_minute_data('AAPL', start_date, end_date)
        
        assert len(result) == 2
        # Should be sorted by timestamp
        assert result.iloc[0]['timestamp'] < result.iloc[1]['timestamp']
    
    @pytest.mark.asyncio
    async def test_query_hot_data_database(self, manager):
        """Test querying hot data from database."""
        start_date = datetime(2024, 1, 1, 9, 30)
        end_date = datetime(2024, 1, 1, 10, 30)
        
        # Mock database response
        mock_rows = [
            {
                'symbol': 'AAPL',
                'timestamp': start_date,
                'open': 180.0, 'high': 181.0, 'low': 179.0, 'close': 180.5,
                'volume': 1000000
            }
        ]
        
        mock_conn = AsyncMock()
        mock_conn.fetch.return_value = mock_rows
        manager.pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        with patch('storage.hybrid_minute_data_manager.env.get_table_name', return_value='test_minute_bars'):
            result = await manager._query_hot_data('AAPL', start_date, end_date)
        
        assert len(result) == 1
        assert result.iloc[0]['symbol'] == 'AAPL'
        mock_conn.fetch.assert_called_once()
    
    def test_find_relevant_files_year_month(self, manager, temp_storage_path):
        """Test finding relevant files with year_month partitioning."""
        # Create test files
        base_path = Path(temp_storage_path) / "cold" / "AAPL"
        (base_path / "2024" / "01").mkdir(parents=True)
        (base_path / "2024" / "02").mkdir(parents=True)
        (base_path / "2024" / "03").mkdir(parents=True)
        
        # Create parquet files
        (base_path / "2024" / "01" / "AAPL_2024_01.parquet").touch()
        (base_path / "2024" / "02" / "AAPL_2024_02.parquet").touch()
        (base_path / "2024" / "03" / "AAPL_2024_03.parquet").touch()
        
        start_date = datetime(2024, 1, 15)
        end_date = datetime(2024, 2, 15)
        
        files = manager._find_relevant_files('AAPL', start_date, end_date)
        
        assert len(files) >= 2  # Should find Jan and Feb files
        file_names = [f.name for f in files]
        assert "AAPL_2024_01.parquet" in file_names
        assert "AAPL_2024_02.parquet" in file_names
    
    @pytest.mark.asyncio
    async def test_archive_old_data(self, manager):
        """Test archiving old data."""
        with patch.object(manager, '_archive_hot_data', return_value=100) as mock_archive_hot, \
             patch.object(manager, '_compress_warm_data', return_value=5) as mock_compress:
            
            result = await manager.archive_old_data(days_old=365)
        
        assert 'cutoff_date' in result
        assert result['hot_records_archived'] == 100
        assert result['warm_files_compressed'] == 5
        mock_archive_hot.assert_called_once()
        mock_compress.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_get_storage_stats(self, manager):
        """Test getting storage statistics."""
        with patch.object(manager, '_get_hot_storage_stats', return_value={'total_records': 1000}) as mock_hot_stats, \
             patch.object(manager, '_get_cold_storage_stats', return_value={'total_files': 50}) as mock_cold_stats:
            
            result = await manager.get_storage_stats()
        
        assert 'hot_storage' in result
        assert 'cold_storage' in result
        assert result['hot_storage']['total_records'] == 1000
        assert result['cold_storage']['total_files'] == 50
    
    @pytest.mark.asyncio
    async def test_get_hot_storage_stats_database(self, manager):
        """Test getting hot storage statistics from database."""
        mock_conn = AsyncMock()
        mock_conn.fetchval.side_effect = [1000, 50, ['2024-01-01', '2024-02-01'], 1048576]
        manager.pool.acquire.return_value.__aenter__.return_value = mock_conn
        
        with patch('storage.hybrid_minute_data_manager.env.get_table_name', return_value='test_minute_bars'):
            result = await manager._get_hot_storage_stats()
        
        assert result['total_records'] == 1000
        assert result['unique_symbols'] == 50
        assert result['table_size'] == 1048576
    
    @pytest.mark.asyncio
    async def test_get_cold_storage_stats_filesystem(self, manager, temp_storage_path):
        """Test getting cold storage statistics from filesystem."""
        # Create test files
        cold_path = Path(temp_storage_path) / "cold" / "AAPL" / "2024" / "01"
        cold_path.mkdir(parents=True)
        
        warm_path = Path(temp_storage_path) / "warm" / "MSFT" / "2024" / "02"
        warm_path.mkdir(parents=True)
        
        # Create test parquet files
        test_file1 = cold_path / "AAPL_2024_01.parquet"
        test_file2 = warm_path / "MSFT_2024_02.parquet"
        
        test_file1.write_text("test data 1")
        test_file2.write_text("test data 2")
        
        result = await manager._get_cold_storage_stats()
        
        assert result['total_files'] == 2
        assert result['total_size_bytes'] > 0
        assert result['total_symbols'] == 2
        assert 'tiers' in result
        assert 'cold' in result['tiers']
        assert 'warm' in result['tiers']
    
    @pytest.mark.asyncio
    async def test_close(self, manager):
        """Test manager cleanup."""
        await manager.close()
        # Should not raise any exceptions


class TestConvenienceFunctions:
    """Test convenience functions."""
    
    @pytest.mark.asyncio
    async def test_create_hybrid_manager(self):
        """Test creating hybrid manager with connection."""
        db_url = "postgresql://test:test@localhost:5432/test"
        
        with patch('asyncpg.create_pool') as mock_create_pool:
            mock_pool = AsyncMock()
            mock_create_pool.return_value = mock_pool
            
            manager = await create_hybrid_manager(db_url)
        
        mock_create_pool.assert_called_once_with(db_url, min_size=5, max_size=20)
        assert isinstance(manager, HybridMinuteDataManager)
    
    @pytest.mark.asyncio
    async def test_migrate_existing_data_no_source(self):
        """Test migration with non-existent source path."""
        manager = MagicMock()
        
        result = await migrate_existing_data(manager, "/nonexistent/path")
        
        assert result['symbols'] == 0
        assert result['files'] == 0
        assert result['records'] == 0


class TestDataLifecycleManagement:
    """Test data lifecycle management scenarios."""
    
    @pytest.fixture
    def temp_storage_path(self):
        """Create temporary storage directory."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.mark.asyncio
    async def test_data_aging_workflow(self, temp_storage_path):
        """Test complete data aging workflow."""
        config = StorageConfig(
            base_data_path=temp_storage_path,
            hot_data_days=1,  # Very short for testing
            warm_data_days=2,
            batch_size=10
        )
        
        mock_pool = AsyncMock()
        manager = HybridMinuteDataManager(mock_pool, config)
        
        now = datetime.now()
        
        # Create data for different time periods
        recent_data = [  # Should go to hot
            {
                'symbol': 'AAPL',
                'timestamp': now,
                'open': 180.0, 'high': 181.0, 'low': 179.0, 'close': 180.5,
                'volume': 1000000, 'vendor': 'polygon'
            }
        ]
        
        warm_data = [  # Should go to warm
            {
                'symbol': 'AAPL',
                'timestamp': now - timedelta(days=1, hours=12),
                'open': 175.0, 'high': 176.0, 'low': 174.0, 'close': 175.5,
                'volume': 800000, 'vendor': 'polygon'
            }
        ]
        
        cold_data = [  # Should go to cold
            {
                'symbol': 'AAPL',
                'timestamp': now - timedelta(days=5),
                'open': 170.0, 'high': 171.0, 'low': 169.0, 'close': 170.5,
                'volume': 600000, 'vendor': 'polygon'
            }
        ]
        
        all_data = recent_data + warm_data + cold_data
        
        # Mock database operations
        with patch.object(manager, '_store_hot_data', return_value=len(recent_data)) as mock_hot:
            result = await manager.store_minute_data('AAPL', all_data)
        
        # Verify data was distributed correctly
        assert result['stored_hot'] == 1
        assert result['stored_warm'] == 1
        assert result['stored_cold'] == 1
        
        # Verify files were created for warm and cold data
        warm_files = list(Path(temp_storage_path).rglob("*warm*/*.parquet"))
        cold_files = list(Path(temp_storage_path).rglob("*cold*/*.parquet"))
        
        assert len(warm_files) >= 1
        assert len(cold_files) >= 1
    
    @pytest.mark.asyncio
    async def test_data_deduplication(self, temp_storage_path):
        """Test data deduplication in Parquet files."""
        config = StorageConfig(base_data_path=temp_storage_path)
        mock_pool = AsyncMock()
        manager = HybridMinuteDataManager(mock_pool, config)
        
        # Create duplicate data
        timestamp = datetime(2024, 1, 15, 10, 30)
        data1 = [
            {
                'symbol': 'AAPL',
                'timestamp': timestamp,
                'open': 180.0, 'high': 181.0, 'low': 179.0, 'close': 180.5,
                'volume': 1000000, 'vendor': 'polygon'
            }
        ]
        
        data2 = [
            {
                'symbol': 'AAPL',
                'timestamp': timestamp,  # Same timestamp
                'open': 180.1, 'high': 181.1, 'low': 179.1, 'close': 180.6,  # Different values
                'volume': 1100000, 'vendor': 'tiingo'
            }
        ]
        
        # Store first batch
        await manager._store_parquet_data('AAPL', data1, 'cold')
        
        # Store second batch (should replace first due to deduplication)
        await manager._store_parquet_data('AAPL', data2, 'cold')
        
        # Check that only one record exists (latest one)
        parquet_files = list(Path(temp_storage_path).rglob("*.parquet"))
        assert len(parquet_files) == 1
        
        df = pd.read_parquet(parquet_files[0])
        assert len(df) == 1
        assert df.iloc[0]['vendor'] == 'tiingo'  # Should be the latest one


class TestPerformanceAndEdgeCases:
    """Test performance scenarios and edge cases."""
    
    @pytest.fixture
    def temp_storage_path(self):
        """Create temporary storage directory."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    @pytest.mark.asyncio
    async def test_large_batch_processing(self, temp_storage_path):
        """Test processing large batches of data."""
        config = StorageConfig(
            base_data_path=temp_storage_path,
            batch_size=100  # Small batch for testing
        )
        mock_pool = AsyncMock()
        manager = HybridMinuteDataManager(mock_pool, config)
        
        # Create large dataset
        base_time = datetime(2024, 1, 1, 9, 30)
        large_data = []
        
        for i in range(500):  # 500 records
            large_data.append({
                'symbol': 'AAPL',
                'timestamp': base_time + timedelta(minutes=i),
                'open': 180.0 + i * 0.01,
                'high': 181.0 + i * 0.01,
                'low': 179.0 + i * 0.01,
                'close': 180.5 + i * 0.01,
                'volume': 1000000 - i * 1000,
                'vendor': 'polygon'
            })
        
        # Store data (should be batched automatically)
        stored_count = await manager._store_parquet_data('AAPL', large_data, 'cold')
        
        assert stored_count == 500
        
        # Verify all data was stored
        parquet_files = list(Path(temp_storage_path).rglob("*.parquet"))
        total_records = 0
        for file in parquet_files:
            df = pd.read_parquet(file)
            total_records += len(df)
        
        assert total_records == 500
    
    @pytest.mark.asyncio
    async def test_concurrent_file_operations(self, temp_storage_path):
        """Test concurrent file operations."""
        config = StorageConfig(
            base_data_path=temp_storage_path,
            max_concurrent_files=2
        )
        mock_pool = AsyncMock()
        manager = HybridMinuteDataManager(mock_pool, config)
        
        # Create data for different symbols
        symbols_data = {}
        base_time = datetime(2024, 1, 1, 9, 30)
        
        for symbol in ['AAPL', 'MSFT', 'GOOGL']:
            symbols_data[symbol] = [
                {
                    'symbol': symbol,
                    'timestamp': base_time + timedelta(minutes=i),
                    'open': 100.0 + i,
                    'high': 101.0 + i,
                    'low': 99.0 + i,
                    'close': 100.5 + i,
                    'volume': 1000000,
                    'vendor': 'polygon'
                }
                for i in range(10)
            ]
        
        # Store data concurrently
        tasks = []
        for symbol, data in symbols_data.items():
            task = manager._store_parquet_data(symbol, data, 'cold')
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        
        # Verify all data was stored
        assert all(result == 10 for result in results)
        
        # Check files were created for each symbol
        for symbol in symbols_data.keys():
            symbol_files = list(Path(temp_storage_path).rglob(f"*{symbol}*.parquet"))
            assert len(symbol_files) >= 1
    
    @pytest.mark.asyncio
    async def test_error_handling_storage_failure(self, temp_storage_path):
        """Test error handling in storage operations."""
        config = StorageConfig(base_data_path="/invalid/path/that/cannot/be/created")
        mock_pool = AsyncMock()
        
        # Should handle invalid path gracefully
        with pytest.raises(Exception):
            manager = HybridMinuteDataManager(mock_pool, config)
    
    @pytest.mark.asyncio
    async def test_query_with_missing_files(self, temp_storage_path):
        """Test querying when expected files are missing."""
        config = StorageConfig(base_data_path=temp_storage_path)
        mock_pool = AsyncMock()
        manager = HybridMinuteDataManager(mock_pool, config)
        
        # Query for data that doesn't exist
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 2)
        
        result = await manager._query_cold_data('NONEXISTENT', start_date, end_date)
        
        assert len(result) == 0
    
    @pytest.mark.asyncio
    async def test_corrupted_parquet_file_handling(self, temp_storage_path):
        """Test handling of corrupted Parquet files."""
        config = StorageConfig(base_data_path=temp_storage_path)
        mock_pool = AsyncMock()
        manager = HybridMinuteDataManager(mock_pool, config)
        
        # Create a corrupted file
        corrupted_path = Path(temp_storage_path) / "cold" / "AAPL" / "2024" / "01"
        corrupted_path.mkdir(parents=True)
        corrupted_file = corrupted_path / "AAPL_2024_01.parquet"
        corrupted_file.write_text("This is not a valid Parquet file")
        
        # Try to read the corrupted file
        result = await manager._read_parquet_file(corrupted_file)
        
        assert result is None  # Should return None for corrupted files


if __name__ == '__main__':
    pytest.main([__file__, '-v'])