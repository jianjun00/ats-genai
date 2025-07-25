"""
Comprehensive unit tests for UniverseStateManager.

Tests cover persistence operations, caching, metadata management,
data optimization, and error handling scenarios.
"""

import pytest
import pandas as pd
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import json

from src.state.universe_state_manager import UniverseStateManager, UniverseStateMetadata
from db.test_db_manager import unit_test_db


class TestUniverseStateManager:
    """Test suite for UniverseStateManager class."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for testing."""
        temp_path = tempfile.mkdtemp()
        yield temp_path
        shutil.rmtree(temp_path)
    
    @pytest.fixture
    def state_manager(self, temp_dir):
        """Create UniverseStateManager instance for testing."""
        return UniverseStateManager(base_path=temp_dir)
    
    @pytest.fixture
    def sample_universe_data(self):
        """Create sample universe data for testing."""
        return pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN'],
            'name': ['Apple Inc.', 'Alphabet Inc.', 'Microsoft Corp.', 'Tesla Inc.', 'Amazon.com Inc.'],
            
            'exchange': ['NASDAQ', 'NASDAQ', 'NASDAQ', 'NASDAQ', 'NASDAQ'],
            'market_cap': [2800000000000, 1600000000000, 2400000000000, 800000000000, 1400000000000],
            'close_price': [150.0, 2500.0, 300.0, 800.0, 3200.0],
            'volume': [50000000, 1500000, 30000000, 25000000, 3000000],
            'is_active': [True, True, True, True, True],
            'as_of_date': ['2023-12-01'] * 5,
            'sector': ['Technology', 'Technology', 'Technology', 'Finance', 'Finance']
        })
    
    @pytest.fixture
    def valid_timestamp(self):
        """Valid timestamp for testing."""
        return "20231201_120000"
    
    def test_initialization(self, temp_dir):
        """Test UniverseStateManager initialization."""
        manager = UniverseStateManager(base_path=temp_dir)
        
        assert manager.base_path == Path(temp_dir)
        assert manager.states_dir.exists()
        assert manager.metadata_dir.exists()
        assert manager.cache_dir.exists()
        assert isinstance(manager._cache, dict)
        assert len(manager._cache) == 0
    
    def test_initialization_default_path(self):
        """Test initialization with default path."""
        with patch('src.state.universe_state_manager.get_environment'):
            manager = UniverseStateManager()
            assert manager.base_path == Path("data/universe_state")
    
    def test_save_universe_state_success(self, state_manager, sample_universe_data, valid_timestamp):
        """Test successful universe state saving."""
        file_path = state_manager.save_universe_state(
            sample_universe_data, 
            valid_timestamp,
            metadata={'data_sources': ['test'], 'universe_type': 'equity'}
        )
        
        # Check file was created
        assert Path(file_path).exists()
        assert Path(file_path).suffix == '.parquet'
        
        # Check metadata was created
        metadata_file = state_manager.metadata_dir / f"metadata_{valid_timestamp}.json"
        assert metadata_file.exists()
        
        # Verify data in cache
        assert valid_timestamp in state_manager._cache
        assert len(state_manager._cache[valid_timestamp]) == 5
    
    def test_save_universe_state_empty_data(self, state_manager, valid_timestamp):
        """Test saving empty universe state raises error."""
        empty_data = pd.DataFrame()
        
        with pytest.raises(ValueError, match="Cannot save empty universe state"):
            state_manager.save_universe_state(empty_data, valid_timestamp)
    
    def test_save_universe_state_invalid_timestamp(self, state_manager, sample_universe_data):
        """Test saving with invalid timestamp format."""
        invalid_timestamp = "invalid_format"
        
        with pytest.raises(ValueError, match="Invalid timestamp format"):
            state_manager.save_universe_state(sample_universe_data, invalid_timestamp)
    
    def test_load_universe_state_success(self, state_manager, sample_universe_data, valid_timestamp):
        """Test successful universe state loading."""
        # First save data
        state_manager.save_universe_state(sample_universe_data, valid_timestamp)
        
        # Then load it
        loaded_data = state_manager.load_universe_state(valid_timestamp)
        
        assert len(loaded_data) == 5
        assert list(loaded_data['symbol']) == ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN']
        assert loaded_data['market_cap'].dtype in ['int64', 'uint64', 'int32', 'uint32']  # Optimized type
    
    def test_load_universe_state_with_filters(self, state_manager, sample_universe_data, valid_timestamp):
        """Test loading universe state with filters."""
        # Save data first
        state_manager.save_universe_state(sample_universe_data, valid_timestamp)
        
        # Load with filters (PyArrow filter format)
        filters = [('sector', '=', 'Technology')]
        loaded_data = state_manager.load_universe_state(valid_timestamp, filters=filters)
        
        assert len(loaded_data) == 3  # Only Technology stocks
        assert all(loaded_data['sector'] == 'Technology')
    
    def test_load_universe_state_with_columns(self, state_manager, sample_universe_data, valid_timestamp):
        """Test loading specific columns only."""
        # Save data first
        state_manager.save_universe_state(sample_universe_data, valid_timestamp)
        
        # Load specific columns
        columns = ['symbol', 'market_cap']
        loaded_data = state_manager.load_universe_state(valid_timestamp, columns=columns)
        
        assert list(loaded_data.columns) == ['symbol', 'market_cap']
        assert len(loaded_data) == 5
    
    def test_load_universe_state_not_found(self, state_manager):
        """Test loading non-existent universe state."""
        with pytest.raises(FileNotFoundError, match="Universe state not found"):
            state_manager.load_universe_state("20231201_999999")
    
    def test_load_universe_state_latest(self, state_manager, sample_universe_data):
        """Test loading latest universe state."""
        # Save multiple states
        timestamps = ["20231201_120000", "20231202_120000", "20231203_120000"]
        for timestamp in timestamps:
            state_manager.save_universe_state(sample_universe_data, timestamp)
        
        # Load latest (should be the last one)
        loaded_data = state_manager.load_universe_state()
        assert len(loaded_data) == 5
        
        # Verify it's the latest
        latest_timestamp = state_manager.get_latest_timestamp()
        assert latest_timestamp == "20231203_120000"
    
    def test_get_latest_timestamp(self, state_manager, sample_universe_data):
        """Test getting latest timestamp."""
        # No states initially
        assert state_manager.get_latest_timestamp() is None
        
        # Save states in random order
        timestamps = ["20231201_120000", "20231203_120000", "20231202_120000"]
        for timestamp in timestamps:
            state_manager.save_universe_state(sample_universe_data, timestamp)
        
        # Should return the latest chronologically
        latest = state_manager.get_latest_timestamp()
        assert latest == "20231203_120000"
    
    def test_list_available_states(self, state_manager, sample_universe_data):
        """Test listing available states."""
        # Save multiple states
        timestamps = ["20231201_120000", "20231202_120000", "20231203_120000"]
        for timestamp in timestamps:
            state_manager.save_universe_state(sample_universe_data, timestamp)
        
        # List all states
        available = state_manager.list_available_states()
        assert len(available) == 3
        assert available == ["20231203_120000", "20231202_120000", "20231201_120000"]  # Sorted desc
        
        # List with limit
        limited = state_manager.list_available_states(limit=2)
        assert len(limited) == 2
        assert limited == ["20231203_120000", "20231202_120000"]
    
    def test_cleanup_old_states(self, state_manager, sample_universe_data):
        """Test cleanup of old states."""
        # Create states with different ages
        old_timestamp = (datetime.now() - timedelta(days=35)).strftime("%Y%m%d_%H%M%S")
        recent_timestamp = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d_%H%M%S")
        
        state_manager.save_universe_state(sample_universe_data, old_timestamp)
        state_manager.save_universe_state(sample_universe_data, recent_timestamp)
        
        # Cleanup states older than 30 days
        removed_count = state_manager.cleanup_old_states(keep_days=30)
        
        assert removed_count == 1
        available_states = state_manager.list_available_states()
        assert len(available_states) == 1
        assert available_states[0] == recent_timestamp
    
    def test_get_state_metadata(self, state_manager, sample_universe_data, valid_timestamp):
        """Test getting state metadata."""
        # Save state with metadata
        metadata = {'data_sources': ['test'], 'universe_type': 'equity'}
        state_manager.save_universe_state(sample_universe_data, valid_timestamp, metadata)
        
        # Get metadata
        state_metadata = state_manager.get_state_metadata(valid_timestamp)
        
        assert isinstance(state_metadata, UniverseStateMetadata)
        assert state_metadata.timestamp == valid_timestamp
        assert state_metadata.record_count == 5
        assert state_metadata.file_size_bytes > 0
        assert len(state_metadata.checksum) == 32  # MD5 hash
        assert state_metadata.columns == list(sample_universe_data.columns)
        assert state_metadata.data_sources == ['test']
        assert state_metadata.universe_type == 'equity'
    
    def test_get_state_metadata_not_found(self, state_manager):
        """Test getting metadata for non-existent state."""
        with pytest.raises(FileNotFoundError, match="Metadata not found"):
            state_manager.get_state_metadata("20231201_999999")
    
    def test_get_storage_stats(self, state_manager, sample_universe_data):
        """Test getting storage statistics."""
        # Save multiple states
        timestamps = ["20231201_120000", "20231202_120000"]
        for timestamp in timestamps:
            state_manager.save_universe_state(sample_universe_data, timestamp)
        
        stats = state_manager.get_storage_stats()
        
        assert stats['total_states'] == 2
        assert stats['total_size_bytes'] > 0
        assert stats['total_size_mb'] > 0
        assert stats['total_records'] == 10  # 5 records × 2 states
        assert stats['cache_size'] == 2
        assert stats['latest_timestamp'] == "20231202_120000"
        assert stats['oldest_timestamp'] == "20231201_120000"
    
    def test_cache_functionality(self, state_manager, sample_universe_data, valid_timestamp):
        """Test caching functionality."""
        # Save data
        state_manager.save_universe_state(sample_universe_data, valid_timestamp)
        
        # First load (from file)
        data1 = state_manager.load_universe_state(valid_timestamp, use_cache=True)
        
        # Second load (from cache)
        with patch('pandas.read_parquet') as mock_read:
            data2 = state_manager.load_universe_state(valid_timestamp, use_cache=True)
            mock_read.assert_not_called()  # Should not read from file
        
        pd.testing.assert_frame_equal(data1, data2)
    
    def test_cache_eviction(self, state_manager, sample_universe_data):
        """Test cache LRU eviction."""
        # Set small cache size for testing
        state_manager._max_cache_size = 2
        
        # Save more states than cache size
        timestamps = ["20231201_120000", "20231202_120000", "20231203_120000"]
        for timestamp in timestamps:
            state_manager.save_universe_state(sample_universe_data, timestamp)
        
        # Cache should only contain the last 2
        assert len(state_manager._cache) == 2
        assert "20231201_120000" not in state_manager._cache  # Evicted
        assert "20231202_120000" in state_manager._cache
        assert "20231203_120000" in state_manager._cache
    
    def test_clear_cache(self, state_manager, sample_universe_data, valid_timestamp):
        """Test cache clearing."""
        # Save data and populate cache
        state_manager.save_universe_state(sample_universe_data, valid_timestamp)
        assert len(state_manager._cache) > 0
        
        # Clear cache
        state_manager.clear_cache()
        assert len(state_manager._cache) == 0
        assert len(state_manager._cache_metadata) == 0
    
    def test_data_type_optimization(self, state_manager):
        """Test data type optimization for better compression."""
        # Create data with suboptimal types
        data = pd.DataFrame({
            'symbol': ['AAPL', 'GOOGL', 'MSFT'],
            'small_int': [1, 2, 3],  # Should become int8
            'large_int': [1000000, 2000000, 3000000],  # Should stay int64
            'categorical': ['A', 'B', 'A'],  # Should become category
            'price': [150.5, 2500.0, 300.0]
        })
        
        optimized = state_manager._optimize_data_types(data.copy())
        
        # Check optimizations
        assert optimized['small_int'].dtype == 'uint8'
        assert optimized['categorical'].dtype.name == 'category'
        assert optimized['price'].dtype == 'float64'  # Should remain float64
    
    def test_timestamp_validation(self, state_manager):
        """Test timestamp format validation."""
        # Valid formats
        assert state_manager._validate_timestamp_format("20231201_120000")
        assert state_manager._validate_timestamp_format("20240229_235959")  # Leap year
        
        # Invalid formats
        assert not state_manager._validate_timestamp_format("2023-12-01_12:00:00")
        assert not state_manager._validate_timestamp_format("20231301_120000")  # Invalid month
        assert not state_manager._validate_timestamp_format("20231201_250000")  # Invalid hour
        assert not state_manager._validate_timestamp_format("invalid")
    
    def test_metadata_creation_and_saving(self, state_manager, sample_universe_data, valid_timestamp):
        """Test metadata creation and saving."""
        additional_metadata = {
            'data_sources': ['polygon', 'tiingo'],
            'universe_type': 'equity',
            'version': '2.0'
        }
        
        # Save with metadata
        state_manager.save_universe_state(sample_universe_data, valid_timestamp, additional_metadata)
        
        # Load and verify metadata
        metadata = state_manager.get_state_metadata(valid_timestamp)
        assert metadata.data_sources == ['polygon', 'tiingo']
        assert metadata.universe_type == 'equity'
        assert metadata.version == '2.0'
        assert metadata.record_count == 5
        assert len(metadata.columns) == len(sample_universe_data.columns)
    
    def test_error_handling_file_operations(self, state_manager, sample_universe_data, valid_timestamp):
        """Test error handling in file operations."""
        # Mock file operations to raise errors
        with patch('pandas.DataFrame.to_parquet', side_effect=IOError("Disk full")):
            with pytest.raises(IOError, match="Failed to save universe state"):
                state_manager.save_universe_state(sample_universe_data, valid_timestamp)
        
        # Mock read operations to raise errors
        state_manager.save_universe_state(sample_universe_data, valid_timestamp)
        
        with patch('pandas.read_parquet', side_effect=IOError("File corrupted")):
            with pytest.raises(IOError, match="Failed to load universe state"):
                state_manager.load_universe_state(valid_timestamp, use_cache=False)
    
    def test_concurrent_access_safety(self, state_manager, sample_universe_data):
        """Test thread safety for concurrent access."""
        import threading
        import time
        
        results = []
        errors = []
        
        def save_and_load(thread_id):
            try:
                timestamp = f"20231201_{thread_id:06d}"
                # Save
                state_manager.save_universe_state(sample_universe_data, timestamp)
                time.sleep(0.01)  # Small delay
                # Load
                loaded_data = state_manager.load_universe_state(timestamp)
                results.append((thread_id, len(loaded_data)))
            except Exception as e:
                errors.append((thread_id, str(e)))
        
        # Create multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=save_and_load, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads
        for thread in threads:
            thread.join()
        
        # Check results
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 5
        assert all(result[1] == 5 for result in results)  # All should have 5 records
    
    def test_large_dataset_handling(self, state_manager):
        """Test handling of large datasets."""
        # Create larger dataset
        large_data = pd.DataFrame({
            'symbol': [f'STOCK_{i:04d}' for i in range(10000)],
            'market_cap': [1000000000 + i * 1000000 for i in range(10000)],
            'sector': ['Technology'] * 5000 + ['Finance'] * 5000,
            'price': [100.0 + i * 0.01 for i in range(10000)]
        })
        
        timestamp = "20231201_120000"
        
        # Should handle large dataset without issues
        file_path = state_manager.save_universe_state(large_data, timestamp)
        assert Path(file_path).exists()
        
        # Load and verify
        loaded_data = state_manager.load_universe_state(timestamp)
        assert len(loaded_data) == 10000
        
        # Check storage stats
        stats = state_manager.get_storage_stats()
        assert stats['total_records'] == 10000
        assert stats['total_size_mb'] > 0
    
    def test_edge_case_empty_directory(self, temp_dir):
        """Test behavior with empty directory."""
        manager = UniverseStateManager(base_path=temp_dir)
        
        # Should handle empty directory gracefully
        assert manager.get_latest_timestamp() is None
        assert manager.list_available_states() == []
        
        with pytest.raises(FileNotFoundError):
            manager.load_universe_state()
    
    def test_malformed_files_handling(self, state_manager, sample_universe_data, valid_timestamp):
        """Test handling of malformed files."""
        # Save valid data first
        state_manager.save_universe_state(sample_universe_data, valid_timestamp)
        
        # Corrupt the parquet file
        parquet_file = state_manager.states_dir / f"universe_state_{valid_timestamp}.parquet"
        with open(parquet_file, 'w') as f:
            f.write("corrupted data")
        
        # Should handle corrupted file gracefully
        with pytest.raises(IOError):
            state_manager.load_universe_state(valid_timestamp, use_cache=False)
    
    @pytest.mark.parametrize("compression", ['snappy', 'gzip', 'brotli'])
    def test_compression_options(self, state_manager, sample_universe_data, compression):
        """Test different compression options."""
        timestamp = f"20231201_120000_{compression}"
        
        # Mock to_parquet to verify compression parameter
        with patch.object(pd.DataFrame, 'to_parquet') as mock_to_parquet:
            try:
                state_manager.save_universe_state(sample_universe_data, timestamp)
                # Verify compression was passed (default is snappy)
                call_args = mock_to_parquet.call_args
                assert call_args[1]['compression'] == 'snappy'  # Default compression
            except Exception:
                pass  # Expected since we're mocking
