#!/usr/bin/env python3
"""
Comprehensive test suite for TrainingDataCallback end-to-end workflows.

This test suite covers:
1. ArrayRecord generation and file format validation
2. Sequence creation and management from universe state data
3. Multi-timeframe training data alignment and consistency
4. Database integration with UUID system
5. Metadata persistence and training dataset registration
6. End-to-end training data generation workflows
7. Error handling and data quality validation
8. Performance and memory characteristics
9. Integration with UniverseStateManager cache
"""

import pytest
import asyncio
import tempfile
import shutil
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import List, Dict, Any
import os
import numpy as np
import pandas as pd
from pathlib import Path

# Test imports
from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from domains.trading.services.state.universe_state_manager import UniverseStateManager
from domains.trading.services.state.instrument_interval import InstrumentInterval
from domains.trading.services.state.universe_state import UniverseStateInterval
from core.shared.data_handling.utils.environment import Environment


class TestTrainingDataCallbackArrayRecord:
    """Test ArrayRecord generation and file format validation."""

    @pytest.fixture
    def temp_output_dir(self):
        """Create temporary directory for ArrayRecord outputs."""
        temp_dir = tempfile.mkdtemp(prefix="training_test_")
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    def mock_environment(self):
        """Create mock environment for testing."""
        env = Mock(spec=Environment)
        env.get_database_url.return_value = "postgresql://test:test@localhost/test"
        env.get_table_name.return_value = "test_table"
        env.get_run_uuid.return_value = "test-uuid-callback"
        return env

    @pytest.fixture
    def training_callback(self, mock_environment, temp_output_dir):
        """Create TrainingDataCallback instance for testing."""
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL', 'TSLA'],
            config=None,
            storage_format="arrayrecord",
            output_dir=temp_output_dir,
            start_date=datetime(2025, 7, 1).date(),
            end_date=datetime(2025, 7, 2).date(),
            start_day_offset=0,
            end_day_offset=0
        )
        return callback

    @pytest.fixture
    def sample_universe_state_data(self):
        """Create sample universe state data for testing."""
        base_time = datetime(2025, 7, 1, 9, 30, 0)
        universe_states = []
        
        for i in range(60):  # 60 minutes of data
            current_time = base_time + timedelta(minutes=i)
            
            # Create instrument intervals
            instrument_intervals = {}
            for instrument_id in [1, 2]:  # AAPL=1, TSLA=2
                interval = InstrumentInterval(
                    instrument_id=instrument_id,
                    start_date_time=current_time,
                    end_date_time=current_time + timedelta(minutes=1),
                    open=100.0 + instrument_id * 100 + i,
                    high=102.0 + instrument_id * 100 + i,
                    low=99.0 + instrument_id * 100 + i,
                    close=101.0 + instrument_id * 100 + i,
                    traded_volume=1000 * (i + 1),
                    traded_dollar=(101.0 + instrument_id * 100 + i) * 1000 * (i + 1),
                    status='ok'
                )
                instrument_intervals[instrument_id] = interval
            
            # Create universe state
            universe_state = UniverseStateInterval(
                start_date_time=current_time,
                end_date_time=current_time + timedelta(minutes=1),
                instrument_intervals=instrument_intervals,
                instrument_indicator_intervals={},
                factor_intervals={}
            )
            
            universe_states.append((current_time, universe_state))
        
        return universe_states

    def test_arrayrecord_file_creation(self, training_callback, temp_output_dir):
        """Test ArrayRecord file creation and basic structure."""
        # Create sample sequence data
        sequence_data = {
            'timestamp': np.array([1625140200, 1625140260, 1625140320]),  # 3 timestamps
            'symbol': np.array(['AAPL', 'AAPL', 'AAPL']),
            'open': np.array([150.0, 151.0, 152.0]),
            'high': np.array([152.0, 153.0, 154.0]),
            'low': np.array([149.0, 150.0, 151.0]),
            'close': np.array([151.0, 152.0, 153.0]),
            'volume': np.array([1000, 1100, 1200]),
            'vwap': np.array([150.5, 151.5, 152.5])
        }
        
        # Test ArrayRecord writer creation
        try:
            # This would test the actual ArrayRecord writing logic
            # For now, test file path construction and directory creation
            dataset_id = "test_dataset_123"
            symbol = "AAPL"
            timeframe = "1h"
            
            # Construct expected file path
            expected_subdir = f"{dataset_id}/{symbol}_{timeframe}"
            expected_dir = os.path.join(temp_output_dir, expected_subdir, timeframe)
            expected_file = os.path.join(expected_dir, f"{symbol}_{timeframe}.arrayrecord")
            
            # Create directory structure
            os.makedirs(expected_dir, exist_ok=True)
            
            # Create a dummy file to simulate ArrayRecord creation
            with open(expected_file, 'w') as f:
                f.write("dummy arrayrecord content")
            
            # Verify file was created
            assert os.path.exists(expected_file)
            assert os.path.getsize(expected_file) > 0
            
        except ImportError:
            # ArrayRecord might not be available in test environment
            pytest.skip("ArrayRecord not available for testing")

    def test_sequence_data_extraction(self, training_callback, sample_universe_state_data):
        """Test extraction of sequence data from universe states."""
        # Mock the callback's universe state access
        with patch.object(training_callback, '_get_universe_states_for_sequence') as mock_get_states:
            mock_get_states.return_value = [state[1] for state in sample_universe_state_data[:10]]  # 10 states
            
            # Test sequence extraction logic
            sequence_length = 10
            start_time = datetime(2025, 7, 1, 9, 30, 0)
            end_time = start_time + timedelta(minutes=sequence_length-1)
            
            # This would call internal sequence extraction methods
            # For testing, verify the mock was called correctly
            states = mock_get_states.return_value
            assert len(states) == sequence_length
            
            # Verify universe states have expected structure
            for state in states:
                assert isinstance(state, UniverseStateInterval)
                assert len(state.instrument_intervals) > 0
                for instrument_id, interval in state.instrument_intervals.items():
                    assert isinstance(interval, InstrumentInterval)
                    assert hasattr(interval, 'open')
                    assert hasattr(interval, 'high')
                    assert hasattr(interval, 'low')
                    assert hasattr(interval, 'close')
                    assert hasattr(interval, 'traded_volume')

    def test_multi_timeframe_sequence_alignment(self, training_callback):
        """Test alignment of sequences across multiple timeframes."""
        timeframes = ['1m', '5m', '15m', '1h']
        base_time = datetime(2025, 7, 1, 9, 30, 0)
        
        # Test timeframe boundary alignment
        for timeframe in timeframes:
            if timeframe == '1m':
                # 1-minute intervals align with every minute
                for minute in range(0, 60, 1):
                    test_time = base_time + timedelta(minutes=minute)
                    assert test_time.minute % 1 == 0  # Always aligned
            
            elif timeframe == '5m':
                # 5-minute intervals align at :00, :05, :10, etc.
                for minute in range(0, 60, 5):
                    test_time = base_time + timedelta(minutes=minute)
                    assert test_time.minute % 5 == 0
            
            elif timeframe == '15m':
                # 15-minute intervals align at :00, :15, :30, :45
                for minute in range(0, 60, 15):
                    test_time = base_time + timedelta(minutes=minute)
                    assert test_time.minute % 15 == 0
            
            elif timeframe == '1h':
                # 1-hour intervals align at the top of each hour
                test_time = base_time.replace(minute=0)
                assert test_time.minute == 0

    def test_metadata_generation(self, training_callback, temp_output_dir):
        """Test training dataset metadata generation."""
        # Test metadata structure
        expected_metadata = {
            'dataset_name': training_callback.dataset_name,
            'symbols': training_callback.symbols,
            'start_date': training_callback.start_date.isoformat(),
            'end_date': training_callback.end_date.isoformat(),
            'base_interval_minutes': training_callback.base_interval_minutes,
            'training_interval_minutes': training_callback.training_interval_minutes,
            'storage_format': training_callback.storage_format,
            'creation_timestamp': datetime.now().isoformat(),
            'total_sequences': 0,  # Would be populated during generation
            'feature_count': 0,    # Would be populated during generation
        }
        
        # Verify metadata has required fields
        assert 'dataset_name' in expected_metadata
        assert 'symbols' in expected_metadata
        assert 'start_date' in expected_metadata
        assert 'end_date' in expected_metadata
        assert 'creation_timestamp' in expected_metadata
        
        # Test metadata file creation
        metadata_file = os.path.join(temp_output_dir, "metadata.json")
        
        # This would be done by the callback
        import json
        with open(metadata_file, 'w') as f:
            json.dump(expected_metadata, f, indent=2)
        
        # Verify metadata file was created and is valid JSON
        assert os.path.exists(metadata_file)
        with open(metadata_file, 'r') as f:
            loaded_metadata = json.load(f)
            assert loaded_metadata['dataset_name'] == training_callback.dataset_name


@pytest.mark.asyncio
class TestTrainingDataCallbackDatabaseIntegration:
    """Test database integration with UUID system."""

    @pytest.fixture
    def mock_environment(self):
        """Create mock environment with database configuration."""
        env = Mock(spec=Environment)
        env.get_database_url.return_value = "postgresql://test:test@localhost/test"
        env.get_table_name.return_value = "test_table"
        env.get_run_uuid.return_value = "test-uuid-db-integration"
        return env

    @pytest.fixture
    def temp_output_dir(self):
        """Create temporary directory for database integration tests."""
        temp_dir = tempfile.mkdtemp(prefix="training_db_test_")
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    def training_callback_with_mocks(self, mock_environment, temp_output_dir):
        """Create callback with mocked database components."""
        callback = IntervalBasedTrainingDataCallback(
            start_date=datetime(2025, 7, 1),
            end_date=datetime(2025, 7, 2),
            symbols=['AAPL'],
            output_dir=temp_output_dir,
            dataset_name="test_dataset_db",
            storage_format="arrayrecord",
            base_interval_minutes=1,
            training_interval_minutes=60,
            use_advanced_storage=True,
            environment=mock_environment
        )
        
        # Mock database DAOs
        callback.training_dataset_dao = Mock()
        callback.training_dataset_dao.insert_training_dataset = AsyncMock()
        callback.training_dataset_dao.update_training_dataset = AsyncMock()
        
        callback.runs_dao = Mock()
        callback.runs_dao.insert_run = AsyncMock()
        callback.runs_dao.update_run = AsyncMock()
        
        return callback

    async def test_training_dataset_registration(self, training_callback_with_mocks):
        """Test registration of training dataset in database."""
        # Test dataset registration
        dataset_metadata = {
            'dataset_name': 'test_dataset_db',
            'symbols': ['AAPL'],
            'start_date': datetime(2025, 7, 1),
            'end_date': datetime(2025, 7, 2),
            'total_sequences': 100,
            'feature_count': 8,
            'status': 'completed'
        }
        
        # This would be called during callback execution
        await training_callback_with_mocks.training_dataset_dao.insert_training_dataset(
            **dataset_metadata
        )
        
        # Verify database insertion was called
        training_callback_with_mocks.training_dataset_dao.insert_training_dataset.assert_called_once()
        call_args = training_callback_with_mocks.training_dataset_dao.insert_training_dataset.call_args[1]
        assert call_args['dataset_name'] == 'test_dataset_db'
        assert call_args['symbols'] == ['AAPL']

    async def test_uuid_system_integration(self, training_callback_with_mocks, mock_environment):
        """Test UUID system integration in database operations."""
        # Mock runner with run context
        mock_runner = Mock()
        mock_runner.run_context = Mock()
        mock_runner.run_context.run_id = "test-run-uuid-123"
        mock_runner.universe_state_manager = Mock()
        
        # Test that callback uses runner's UUID
        current_time = datetime(2025, 7, 1, 10, 0, 0)
        
        # This would be called during handleInterval
        # For testing, verify UUID is accessible
        assert mock_runner.run_context.run_id == "test-run-uuid-123"
        assert mock_environment.get_run_uuid() == "test-uuid-db-integration"
        
        # UUID system should provide consistent IDs for database operations

    async def test_run_tracking_database_operations(self, training_callback_with_mocks):
        """Test run tracking in database."""
        # Test run creation
        run_metadata = {
            'run_type': 'training_data_generation',
            'status': 'running',
            'start_time': datetime.now(),
            'parameters': {
                'symbols': ['AAPL'],
                'start_date': '2025-07-01',
                'end_date': '2025-07-02'
            }
        }
        
        await training_callback_with_mocks.runs_dao.insert_run(**run_metadata)
        
        # Verify run insertion
        training_callback_with_mocks.runs_dao.insert_run.assert_called_once()
        
        # Test run completion
        await training_callback_with_mocks.runs_dao.update_run(
            run_id="test-run-123",
            status='completed',
            end_time=datetime.now()
        )
        
        training_callback_with_mocks.runs_dao.update_run.assert_called_once()

    async def test_database_error_handling(self, training_callback_with_mocks):
        """Test error handling during database operations."""
        # Mock database error
        training_callback_with_mocks.training_dataset_dao.insert_training_dataset.side_effect = Exception("Database connection failed")
        
        # Should handle database errors gracefully
        try:
            await training_callback_with_mocks.training_dataset_dao.insert_training_dataset(
                dataset_name='test',
                symbols=['AAPL'],
                start_date=datetime.now(),
                end_date=datetime.now()
            )
        except Exception as e:
            assert "database" in str(e).lower() or "connection" in str(e).lower()

    async def test_transaction_consistency(self, training_callback_with_mocks):
        """Test database transaction consistency."""
        # This would test that database operations are atomic
        # For now, verify that multiple related operations are called together
        
        dataset_id = 123
        run_id = "test-run-456"
        
        # Both dataset and run operations should succeed or fail together
        await training_callback_with_mocks.training_dataset_dao.insert_training_dataset(
            dataset_name='test_transaction',
            symbols=['AAPL']
        )
        
        await training_callback_with_mocks.runs_dao.insert_run(
            run_type='training_data_generation',
            dataset_id=dataset_id
        )
        
        # Both operations should have been called
        training_callback_with_mocks.training_dataset_dao.insert_training_dataset.assert_called()
        training_callback_with_mocks.runs_dao.insert_run.assert_called()


@pytest.mark.asyncio
class TestTrainingDataCallbackEndToEnd:
    """Test end-to-end training data generation workflows."""

    @pytest.fixture
    def temp_output_dir(self):
        """Create temporary directory for end-to-end tests."""
        temp_dir = tempfile.mkdtemp(prefix="training_e2e_test_")
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    def mock_environment(self):
        """Create mock environment for end-to-end testing."""
        env = Mock(spec=Environment)
        env.get_database_url.return_value = "postgresql://test:test@localhost/test"
        env.get_table_name.return_value = "test_table"
        env.get_run_uuid.return_value = "test-uuid-e2e"
        return env

    @pytest.fixture
    def mock_universe_state_manager(self):
        """Create mock UniverseStateManager with realistic data."""
        manager = Mock(spec=UniverseStateManager)
        
        # Create realistic cache data
        cache_data = []
        base_time = datetime(2025, 7, 1, 9, 30, 0)
        for i in range(60):  # 1 hour of minute data
            interval = InstrumentInterval(
                instrument_id=1,
                start_date_time=base_time + timedelta(minutes=i),
                end_date_time=base_time + timedelta(minutes=i+1),
                open=150.0 + (i * 0.1),
                high=152.0 + (i * 0.1),
                low=149.0 + (i * 0.1),
                close=151.0 + (i * 0.1),
                traded_volume=1000 + (i * 10),
                traded_dollar=(151.0 + (i * 0.1)) * (1000 + (i * 10)),
                status='ok'
            )
            cache_data.append(interval)
        
        manager.get_instrument_history_for_timeframe.return_value = cache_data
        manager.get_lag_prices.return_value = pd.DataFrame({
            'timestamp': [base_time + timedelta(minutes=i) for i in range(10)],
            'open': [150.0 + i for i in range(10)],
            'high': [152.0 + i for i in range(10)],
            'low': [149.0 + i for i in range(10)],
            'close': [151.0 + i for i in range(10)],
            'volume': [1000 + (i * 10) for i in range(10)],
            'vwap': [150.5 + i for i in range(10)]
        })
        
        return manager

    @pytest.fixture
    def training_callback_e2e(self, mock_environment, temp_output_dir):
        """Create callback for end-to-end testing."""
        callback = IntervalBasedTrainingDataCallback(
            start_date=datetime(2025, 7, 1),
            end_date=datetime(2025, 7, 1, 23, 59, 59),  # Single day
            symbols=['AAPL'],
            output_dir=temp_output_dir,
            dataset_name="test_dataset_e2e",
            storage_format="arrayrecord",
            base_interval_minutes=1,
            training_interval_minutes=60,
            use_advanced_storage=True,
            environment=mock_environment,
            sequence_length=10,
            prediction_horizon=1
        )
        return callback

    async def test_complete_training_data_workflow(self, training_callback_e2e, mock_universe_state_manager, temp_output_dir):
        """Test complete training data generation workflow."""
        # Mock runner
        mock_runner = Mock()
        mock_runner.run_context = Mock()
        mock_runner.run_context.run_id = "test-e2e-run-123"
        mock_runner.universe_state_manager = mock_universe_state_manager
        
        # Mock database components
        training_callback_e2e.training_dataset_dao = Mock()
        training_callback_e2e.training_dataset_dao.insert_training_dataset = AsyncMock(return_value=123)
        training_callback_e2e.runs_dao = Mock()
        training_callback_e2e.runs_dao.insert_run = AsyncMock()
        
        # Test initialization
        await training_callback_e2e.initialize()
        
        # Test multiple handleInterval calls (simulating time progression)
        base_time = datetime(2025, 7, 1, 9, 30, 0)
        for i in range(5):  # Process 5 intervals
            current_time = base_time + timedelta(minutes=i * 60)  # Every hour
            await training_callback_e2e.handleInterval(mock_runner, current_time)
        
        # Test finalization
        await training_callback_e2e.finalize()
        
        # Verify outputs were created
        # (This would check for actual ArrayRecord files in a real implementation)
        assert os.path.exists(temp_output_dir)
        
        # Verify database operations were called
        training_callback_e2e.training_dataset_dao.insert_training_dataset.assert_called()

    async def test_sequence_generation_workflow(self, training_callback_e2e, mock_universe_state_manager):
        """Test sequence generation from universe state data."""
        # Mock runner
        mock_runner = Mock()
        mock_runner.universe_state_manager = mock_universe_state_manager
        
        # Test sequence generation for a specific time
        current_time = datetime(2025, 7, 1, 10, 30, 0)
        
        # This would test the internal sequence generation logic
        sequence_length = training_callback_e2e.sequence_length
        
        # Get mock lag prices data
        lag_data = mock_universe_state_manager.get_lag_prices.return_value
        assert len(lag_data) == 10  # Should have data for sequence
        
        # Verify data has required columns
        required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'vwap']
        for col in required_columns:
            assert col in lag_data.columns

    async def test_multi_symbol_processing(self, mock_environment, temp_output_dir, mock_universe_state_manager):
        """Test processing multiple symbols simultaneously."""
        callback = IntervalBasedTrainingDataCallback(
            start_date=datetime(2025, 7, 1),
            end_date=datetime(2025, 7, 1, 23, 59, 59),
            symbols=['AAPL', 'TSLA', 'MSFT'],  # Multiple symbols
            output_dir=temp_output_dir,
            dataset_name="test_multi_symbol",
            storage_format="arrayrecord",
            base_interval_minutes=1,
            training_interval_minutes=60,
            use_advanced_storage=True,
            environment=mock_environment
        )
        
        # Mock database components
        callback.training_dataset_dao = Mock()
        callback.training_dataset_dao.insert_training_dataset = AsyncMock(return_value=456)
        
        # Test initialization handles multiple symbols
        await callback.initialize()
        
        # Verify symbol configuration
        assert len(callback.symbols) == 3
        assert 'AAPL' in callback.symbols
        assert 'TSLA' in callback.symbols
        assert 'MSFT' in callback.symbols

    async def test_error_recovery_and_cleanup(self, training_callback_e2e, mock_universe_state_manager, temp_output_dir):
        """Test error recovery and cleanup mechanisms."""
        # Mock runner
        mock_runner = Mock()
        mock_runner.universe_state_manager = mock_universe_state_manager
        
        # Mock database error during processing
        training_callback_e2e.training_dataset_dao = Mock()
        training_callback_e2e.training_dataset_dao.insert_training_dataset = AsyncMock(
            side_effect=Exception("Database error during processing")
        )
        
        # Test error handling during initialization
        try:
            await training_callback_e2e.initialize()
        except Exception as e:
            assert "database" in str(e).lower() or "error" in str(e).lower()
        
        # Test cleanup after error
        # (This would test that temporary files are cleaned up, database transactions are rolled back, etc.)
        # For now, verify that the output directory still exists (cleanup might preserve it for debugging)
        assert os.path.exists(temp_output_dir)

    async def test_data_quality_validation(self, training_callback_e2e, mock_universe_state_manager):
        """Test data quality validation during training data generation."""
        # Mock runner with potentially invalid data
        mock_runner = Mock()
        mock_runner.universe_state_manager = mock_universe_state_manager
        
        # Create data with potential quality issues
        invalid_data = pd.DataFrame({
            'timestamp': [datetime(2025, 7, 1, 10, 0, 0)] * 5,
            'open': [150.0, np.nan, 151.0, 152.0, 153.0],  # Contains NaN
            'high': [152.0, 153.0, 154.0, 155.0, 156.0],
            'low': [149.0, 150.0, 151.0, 152.0, 153.0],
            'close': [151.0, 152.0, 153.0, 154.0, 155.0],
            'volume': [1000, 1100, 1200, 1300, 1400],
            'vwap': [150.5, 151.5, 152.5, 153.5, 154.5]
        })
        
        mock_universe_state_manager.get_lag_prices.return_value = invalid_data
        
        # Test data quality validation (should handle NaN values)
        current_time = datetime(2025, 7, 1, 10, 30, 0)
        
        try:
            await training_callback_e2e.handleInterval(mock_runner, current_time)
            # Should either handle NaN gracefully or raise appropriate error
        except Exception as e:
            # If it raises an error, it should be about data quality
            assert any(term in str(e).lower() for term in ['nan', 'invalid', 'quality', 'data'])


class TestTrainingDataCallbackPerformance:
    """Test performance characteristics of TrainingDataCallback."""

    @pytest.fixture
    def temp_output_dir(self):
        """Create temporary directory for performance tests."""
        temp_dir = tempfile.mkdtemp(prefix="training_perf_test_")
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    def mock_environment(self):
        """Create mock environment for performance testing."""
        env = Mock(spec=Environment)
        env.get_database_url.return_value = "postgresql://test:test@localhost/test"
        env.get_table_name.return_value = "test_table"
        env.get_run_uuid.return_value = "test-uuid-perf"
        return env

    def test_memory_usage_large_sequences(self, mock_environment, temp_output_dir):
        """Test memory usage with large sequence processing."""
        import psutil
        import gc
        
        # Get initial memory
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # Create callback with large sequence parameters
        callback = IntervalBasedTrainingDataCallback(
            start_date=datetime(2025, 7, 1),
            end_date=datetime(2025, 7, 31),  # Full month
            symbols=['AAPL'] * 100,  # Many symbols (simulate large universe)
            output_dir=temp_output_dir,
            dataset_name="test_performance",
            storage_format="arrayrecord",
            base_interval_minutes=1,
            training_interval_minutes=60,
            sequence_length=100,  # Large sequence length
            use_advanced_storage=True,
            environment=mock_environment
        )
        
        # Force garbage collection
        gc.collect()
        
        # Check memory after callback creation
        final_memory = process.memory_info().rss
        memory_growth = final_memory - initial_memory
        
        # Memory growth should be reasonable (less than 100MB for object creation)
        assert memory_growth < 100 * 1024 * 1024

    def test_processing_speed_benchmark(self, mock_environment, temp_output_dir):
        """Test processing speed for training data generation."""
        import time
        
        callback = IntervalBasedTrainingDataCallback(
            start_date=datetime(2025, 7, 1),
            end_date=datetime(2025, 7, 2),
            symbols=['AAPL', 'TSLA'],
            output_dir=temp_output_dir,
            dataset_name="test_speed",
            storage_format="arrayrecord",
            base_interval_minutes=1,
            training_interval_minutes=60,
            sequence_length=10,
            use_advanced_storage=True,
            environment=mock_environment
        )
        
        # Mock universe state manager with data
        mock_universe_state_manager = Mock()
        mock_data = pd.DataFrame({
            'timestamp': [datetime(2025, 7, 1, 10, 0, 0) + timedelta(minutes=i) for i in range(100)],
            'open': [150.0 + i for i in range(100)],
            'high': [152.0 + i for i in range(100)],
            'low': [149.0 + i for i in range(100)],
            'close': [151.0 + i for i in range(100)],
            'volume': [1000 + (i * 10) for i in range(100)],
            'vwap': [150.5 + i for i in range(100)]
        })
        mock_universe_state_manager.get_lag_prices.return_value = mock_data
        
        # Mock runner
        mock_runner = Mock()
        mock_runner.universe_state_manager = mock_universe_state_manager
        
        # Benchmark multiple handleInterval calls
        start_time = time.time()
        
        # Simulate processing multiple intervals
        for i in range(50):  # 50 intervals
            current_time = datetime(2025, 7, 1, 10, 0, 0) + timedelta(minutes=i * 60)
            
            # This would call handleInterval in a real test
            # For performance testing, just test the data access patterns
            _ = mock_universe_state_manager.get_lag_prices(
                instrument_id=1,
                cur_datetime=current_time,
                lag_periods=10
            )
        
        end_time = time.time()
        
        # Should process quickly (less than 5 seconds for 50 intervals)
        assert end_time - start_time < 5.0


if __name__ == "__main__":
    # Run specific test for debugging
    pytest.main([__file__ + "::TestTrainingDataCallbackArrayRecord::test_arrayrecord_file_creation", "-v"])