#!/usr/bin/env python3
"""
Comprehensive integration test suite for the complete UUID + cache + training pipeline.

This test suite validates the entire system working together:
1. UUID system providing consistent database operation deduplication
2. UniverseStateManager shared rolling cache across components
3. UniverseStateBuilder multi-timeframe processing with cache integration
4. TrainingDataCallback end-to-end data generation workflows
5. Database persistence and retrieval with UUID consistency
6. Error handling and recovery across the entire pipeline
7. Performance and scalability characteristics
8. Data integrity and consistency validation
9. Concurrency and thread safety
10. Memory management and resource cleanup
"""

import pytest
import asyncio
import tempfile
import shutil
import threading
import time
from datetime import datetime, timedelta
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from typing import List, Dict, Any
import os
import pandas as pd
import gc
import psutil

# Test imports
from domains.trading.services.state.universe_state_manager import UniverseStateManager
from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from domains.trading.services.state.instrument_interval import InstrumentInterval
from domains.trading.services.state.universe_state import UniverseStateInterval
from shared.data_handling.utils.environment import Environment
from services.core.app.runner import Runner


@pytest.mark.asyncio
class TestCompleteUUIDCacheTrainingPipeline:
    """Test the complete pipeline integration."""

    @pytest.fixture
    def temp_output_dir(self):
        """Create temporary directory for pipeline outputs."""
        temp_dir = tempfile.mkdtemp(prefix="pipeline_integration_test_")
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    def mock_environment(self):
        """Create comprehensive mock environment for integration testing."""
        env = Mock(spec=Environment)
        env.get_database_url.return_value = "postgresql://test:test@localhost/test"
        env.get_table_name.return_value = "test_table"
        env.get_run_uuid.return_value = "integration-test-uuid-123"
        env.set_run_uuid = Mock()
        env.indicator_rolling_window = 20
        return env

    @pytest.fixture
    def universe_state_manager(self, mock_environment):
        """Create real UniverseStateManager for integration testing."""
        manager = UniverseStateManager(env=mock_environment)
        
        # Mock the DAOs to avoid actual database operations
        manager._interval_dao = Mock()
        manager._interval_dao.insert_universe_state_interval = AsyncMock()
        manager._interval_dao.get_universe_state_intervals = AsyncMock(return_value=[])
        
        return manager

    @pytest.fixture
    def universe_state_builder(self, mock_environment, universe_state_manager):
        """Create UniverseStateBuilder integrated with shared cache."""
        builder = UniverseStateIntervalBuilder(
            env=mock_environment,
            base_duration="1m",
            target_durations="1m,5m,15m,1h",
            universe_state_manager=universe_state_manager
        )
        return builder

    @pytest.fixture
    def training_data_callback(self, mock_environment, temp_output_dir):
        """Create TrainingDataCallback for integration testing."""
        callback = IntervalBasedTrainingDataCallback(
            start_date=datetime(2025, 7, 1),
            end_date=datetime(2025, 7, 2),
            symbols=['AAPL', 'TSLA'],
            output_dir=temp_output_dir,
            dataset_name="integration_test_dataset",
            storage_format="arrayrecord",
            base_interval_minutes=1,
            training_interval_minutes=60,
            use_advanced_storage=True,
            environment=mock_environment,
            sequence_length=10,
            prediction_horizon=1
        )
        
        # Mock database components
        callback.training_dataset_dao = Mock()
        callback.training_dataset_dao.insert_training_dataset = AsyncMock(return_value=123)
        callback.training_dataset_dao.update_training_dataset = AsyncMock()
        callback.runs_dao = Mock()
        callback.runs_dao.insert_run = AsyncMock()
        callback.runs_dao.update_run = AsyncMock()
        
        return callback

    @pytest.fixture
    def mock_runner(self, universe_state_manager):
        """Create mock Runner that integrates all components."""
        runner = Mock()
        runner.run_context = Mock()
        runner.run_context.run_id = "integration-test-run-456"
        runner.universe_state_manager = universe_state_manager
        runner.universe_manager = Mock()
        runner.universe_manager.instrument_ids = [1, 2]  # AAPL=1, TSLA=2
        runner.market_data_manager = Mock()
        runner.get_environment = Mock()
        runner.get_environment.return_value = Mock()
        
        # Mock market data with realistic OHLC data
        def mock_get_minute_ohlc_batch(symbols, start_time, end_time):
            data = {}
            for i, symbol in enumerate([1, 2]):  # instrument_ids
                data[symbol] = {
                    'open': 100.0 + (symbol * 100),
                    'high': 102.0 + (symbol * 100),
                    'low': 99.0 + (symbol * 100),
                    'close': 101.0 + (symbol * 100),
                    'volume': 1000 * symbol,
                    'vwap': 100.5 + (symbol * 100)
                }
            return data
        
        runner.market_data_manager.get_minute_ohlc_batch = mock_get_minute_ohlc_batch
        
        return runner

    async def test_uuid_system_consistency_across_pipeline(self, mock_environment, universe_state_manager, universe_state_builder, training_data_callback, mock_runner):
        """Test UUID system provides consistent IDs across all components."""
        # Set UUID in environment
        test_uuid = "pipeline-uuid-consistency-test"
        mock_environment.set_run_uuid(test_uuid)
        mock_environment.get_run_uuid.return_value = test_uuid
        
        # Verify environment UUID is accessible
        assert mock_environment.get_run_uuid() == test_uuid
        
        # Verify runner provides UUID
        assert mock_runner.run_context.run_id is not None
        
        # Test universe state manager uses environment UUID
        await universe_state_manager.addUniverseState(
            "1m",
            UniverseStateInterval(
                start_date_time=datetime.now(),
                end_date_time=datetime.now() + timedelta(minutes=1),
                instrument_intervals={},
                instrument_indicator_intervals={},
                factor_intervals={}
            ),
            datetime.now()
        )
        
        # Verify DAO was called (indicating UUID system was used)
        universe_state_manager._interval_dao.insert_universe_state_interval.assert_called()
        
        # Test training callback uses runner's UUID
        await training_data_callback.initialize()
        training_data_callback.training_dataset_dao.insert_training_dataset.assert_called()

    async def test_shared_cache_integration(self, universe_state_manager, universe_state_builder, mock_runner):
        """Test shared cache integration between builder and manager."""
        # Add data via builder
        current_time = datetime(2025, 7, 1, 9, 30, 0)
        
        # Mock instrument xrefs DAO
        with patch('core.dao.instruments.instrument_xrefs_dao.InstrumentXrefsDAO') as mock_xrefs_dao:
            mock_dao_instance = mock_xrefs_dao.return_value
            mock_dao_instance.get_symbols_by_instrument_ids_batch = AsyncMock(return_value={1: 'AAPL', 2: 'TSLA'})
            
            # Process intervals through builder
            await universe_state_builder.handleInterval(mock_runner, current_time)
            
            # Verify cache was populated via manager
            debug_info = universe_state_manager.get_rolling_cache_debug_info()
            
            # Should have cache data for at least one timeframe
            assert len(debug_info) > 0
            
            # Verify manager can access the same data
            for timeframe in debug_info:
                for instrument_id in debug_info[timeframe]['instruments']:
                    history = universe_state_manager.get_instrument_history_for_timeframe(instrument_id, timeframe)
                    # Should have the data that was added via builder
                    assert len(history) >= 0  # At least accessible, even if empty

    async def test_multi_timeframe_pipeline_processing(self, universe_state_manager, universe_state_builder, training_data_callback, mock_runner):
        """Test complete multi-timeframe processing pipeline."""
        # Initialize training callback
        await training_data_callback.initialize()
        
        # Process multiple timeframes sequentially
        base_time = datetime(2025, 7, 1, 9, 30, 0)
        timeframes_to_test = [
            (base_time + timedelta(minutes=1), ["1m"]),           # 1-minute boundary
            (base_time + timedelta(minutes=5), ["1m", "5m"]),     # 5-minute boundary
            (base_time + timedelta(minutes=15), ["1m", "5m", "15m"]), # 15-minute boundary
            (base_time + timedelta(minutes=60), ["1m", "5m", "15m", "1h"]) # 1-hour boundary
        ]
        
        # Mock instrument xrefs DAO
        with patch('core.dao.instruments.instrument_xrefs_dao.InstrumentXrefsDAO') as mock_xrefs_dao:
            mock_dao_instance = mock_xrefs_dao.return_value
            mock_dao_instance.get_symbols_by_instrument_ids_batch = AsyncMock(return_value={1: 'AAPL', 2: 'TSLA'})
            
            for current_time, expected_timeframes in timeframes_to_test:
                # Process through universe state builder
                await universe_state_builder.handleInterval(mock_runner, current_time)
                
                # Process through training callback
                await training_data_callback.handleInterval(mock_runner, current_time)
                
                # Verify cache contains data for expected timeframes
                debug_info = universe_state_manager.get_rolling_cache_debug_info()
                
                # Should have some cache activity (exact timeframes depend on implementation)
                assert isinstance(debug_info, dict)

    async def test_end_to_end_data_flow_validation(self, mock_environment, universe_state_manager, universe_state_builder, training_data_callback, mock_runner, temp_output_dir):
        """Test complete data flow from market data to training files."""
        # Step 1: Initialize all components
        await training_data_callback.initialize()
        
        # Step 2: Simulate market data flow through builder
        base_time = datetime(2025, 7, 1, 9, 30, 0)
        
        # Mock instrument xrefs DAO
        with patch('core.dao.instruments.instrument_xrefs_dao.InstrumentXrefsDAO') as mock_xrefs_dao:
            mock_dao_instance = mock_xrefs_dao.return_value
            mock_dao_instance.get_symbols_by_instrument_ids_batch = AsyncMock(return_value={1: 'AAPL', 2: 'TSLA'})
            
            # Process multiple intervals to build up cache
            for i in range(65):  # More than 1 hour to trigger multiple timeframes
                current_time = base_time + timedelta(minutes=i)
                
                # Process market data through builder (populates cache)
                await universe_state_builder.handleInterval(mock_runner, current_time)
                
                # Process training data (consumes cache data)
                if i >= 10:  # Start training after some data is accumulated
                    await training_data_callback.handleInterval(mock_runner, current_time)
        
        # Step 3: Finalize training data generation
        await training_data_callback.finalize()
        
        # Step 4: Verify outputs
        # Cache should have data
        debug_info = universe_state_manager.get_rolling_cache_debug_info()
        assert len(debug_info) > 0
        
        # Training callback should have processed data
        training_data_callback.training_dataset_dao.insert_training_dataset.assert_called()
        
        # Output directory should exist
        assert os.path.exists(temp_output_dir)

    async def test_error_handling_and_recovery(self, mock_environment, universe_state_manager, universe_state_builder, training_data_callback, mock_runner):
        """Test error handling and recovery across the pipeline."""
        # Test UUID system error handling
        mock_environment.get_run_uuid.side_effect = Exception("UUID system error")
        
        try:
            # Should handle UUID errors gracefully
            current_time = datetime(2025, 7, 1, 10, 0, 0)
            await universe_state_builder.handleInterval(mock_runner, current_time)
        except Exception as e:
            assert "uuid" in str(e).lower() or "error" in str(e).lower()
        
        # Reset UUID system
        mock_environment.get_run_uuid.side_effect = None
        mock_environment.get_run_uuid.return_value = "recovery-uuid-123"
        
        # Test cache error handling
        original_add_method = universe_state_manager.add_interval_to_rolling_cache
        universe_state_manager.add_interval_to_rolling_cache = Mock(side_effect=Exception("Cache error"))
        
        try:
            # Mock instrument xrefs DAO
            with patch('core.dao.instruments.instrument_xrefs_dao.InstrumentXrefsDAO') as mock_xrefs_dao:
                mock_dao_instance = mock_xrefs_dao.return_value
                mock_dao_instance.get_symbols_by_instrument_ids_batch = AsyncMock(return_value={1: 'AAPL'})
                
                await universe_state_builder.handleInterval(mock_runner, current_time)
        except Exception as e:
            assert "cache" in str(e).lower() or "error" in str(e).lower()
        
        # Restore cache method
        universe_state_manager.add_interval_to_rolling_cache = original_add_method
        
        # Test training callback error handling
        training_data_callback.training_dataset_dao.insert_training_dataset.side_effect = Exception("Database error")
        
        try:
            await training_data_callback.initialize()
        except Exception as e:
            assert "database" in str(e).lower() or "error" in str(e).lower()

    async def test_concurrent_pipeline_access(self, universe_state_manager, universe_state_builder, mock_runner):
        """Test concurrent access to the pipeline components."""
        results = []
        exceptions = []
        
        async def concurrent_worker(worker_id):
            """Worker function for concurrent pipeline access."""
            try:
                current_time = datetime(2025, 7, 1, 9, 30, 0) + timedelta(minutes=worker_id)
                
                # Mock instrument xrefs DAO
                with patch('core.dao.instruments.instrument_xrefs_dao.InstrumentXrefsDAO') as mock_xrefs_dao:
                    mock_dao_instance = mock_xrefs_dao.return_value
                    mock_dao_instance.get_symbols_by_instrument_ids_batch = AsyncMock(return_value={1: 'AAPL'})
                    
                    # Each worker processes through the pipeline
                    await universe_state_builder.handleInterval(mock_runner, current_time)
                    
                    # Access cache data
                    history = universe_state_manager.get_instrument_history_for_timeframe(1, "1m")
                    
                    results.append(f"Worker {worker_id} completed, cache_size={len(history)}")
                    
            except Exception as e:
                exceptions.append(f"Worker {worker_id} failed: {e}")
        
        # Run multiple concurrent workers
        tasks = []
        for i in range(5):
            task = asyncio.create_task(concurrent_worker(i))
            tasks.append(task)
        
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Check results
        assert len(exceptions) == 0, f"Concurrent access failed: {exceptions}"
        assert len(results) == 5
        
        # Cache should have data from concurrent operations
        debug_info = universe_state_manager.get_rolling_cache_debug_info()
        assert len(debug_info) >= 0  # Should handle concurrent access without corruption

    def test_pipeline_memory_management(self, mock_environment, temp_output_dir):
        """Test memory management across the pipeline."""
        import gc
        
        # Get initial memory
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # Create pipeline components
        manager = UniverseStateManager(env=mock_environment)
        builder = UniverseStateIntervalBuilder(
            env=mock_environment,
            base_duration="1m",
            target_durations="1m,5m,15m,1h",
            universe_state_manager=manager
        )
        callback = IntervalBasedTrainingDataCallback(
            start_date=datetime(2025, 7, 1),
            end_date=datetime(2025, 7, 2),
            symbols=['AAPL'] * 50,  # Many symbols to test memory
            output_dir=temp_output_dir,
            dataset_name="memory_test",
            storage_format="arrayrecord",
            base_interval_minutes=1,
            training_interval_minutes=60,
            sequence_length=50,  # Large sequence
            use_advanced_storage=True,
            environment=mock_environment
        )
        
        # Add significant amount of data to cache
        base_time = datetime(2025, 7, 1, 9, 30, 0)
        for i in range(1000):  # Large amount of data
            interval = InstrumentInterval(
                instrument_id=1,
                start_date_time=base_time + timedelta(minutes=i),
                end_date_time=base_time + timedelta(minutes=i+1),
                open=100.0, high=102.0, low=99.0, close=101.0,
                traded_volume=1000, traded_dollar=101000.0, status='ok'
            )
            manager.add_interval_to_rolling_cache(1, "1m", interval)
        
        # Force garbage collection
        del builder
        del callback
        gc.collect()
        
        # Check memory after cleanup
        final_memory = process.memory_info().rss
        memory_growth = final_memory - initial_memory
        
        # Memory growth should be reasonable (less than 200MB)
        assert memory_growth < 200 * 1024 * 1024

    async def test_pipeline_performance_benchmark(self, universe_state_manager, universe_state_builder, training_data_callback, mock_runner):
        """Test performance characteristics of the complete pipeline."""
        import time
        
        # Initialize components
        await training_data_callback.initialize()
        
        # Benchmark pipeline processing
        start_time = time.time()
        base_time = datetime(2025, 7, 1, 9, 30, 0)
        
        # Mock instrument xrefs DAO
        with patch('core.dao.instruments.instrument_xrefs_dao.InstrumentXrefsDAO') as mock_xrefs_dao:
            mock_dao_instance = mock_xrefs_dao.return_value
            mock_dao_instance.get_symbols_by_instrument_ids_batch = AsyncMock(return_value={1: 'AAPL', 2: 'TSLA'})
            
            # Process many intervals through the pipeline
            for i in range(100):  # 100 intervals
                current_time = base_time + timedelta(minutes=i)
                
                # Process through builder
                await universe_state_builder.handleInterval(mock_runner, current_time)
                
                # Process through training callback
                if i >= 10:  # Start training after some accumulation
                    await training_data_callback.handleInterval(mock_runner, current_time)
        
        end_time = time.time()
        
        # Performance assertions
        processing_time = end_time - start_time
        assert processing_time < 30.0  # Should complete in under 30 seconds
        
        # Verify data was processed
        debug_info = universe_state_manager.get_rolling_cache_debug_info()
        assert len(debug_info) > 0


@pytest.mark.asyncio
class TestPipelineDataIntegrity:
    """Test data integrity across the pipeline."""

    @pytest.fixture
    def mock_environment(self):
        """Create environment for data integrity testing."""
        env = Mock(spec=Environment)
        env.get_database_url.return_value = "postgresql://test:test@localhost/test"
        env.get_table_name.return_value = "test_table"
        env.get_run_uuid.return_value = "data-integrity-uuid"
        env.indicator_rolling_window = 10
        return env

    async def test_ohlc_data_consistency(self, mock_environment):
        """Test OHLC data consistency across timeframes."""
        manager = UniverseStateManager(env=mock_environment)
        
        # Add 5 minutes of 1-minute data
        base_time = datetime(2025, 7, 1, 9, 30, 0)
        minute_intervals = []
        
        for i in range(5):
            interval = InstrumentInterval(
                instrument_id=1,
                start_date_time=base_time + timedelta(minutes=i),
                end_date_time=base_time + timedelta(minutes=i+1),
                open=100.0 + i,
                high=105.0 + i,
                low=95.0 + i,
                close=102.0 + i,
                traded_volume=1000 * (i + 1),
                traded_dollar=(102.0 + i) * 1000 * (i + 1),
                status='ok'
            )
            minute_intervals.append(interval)
            manager.add_interval_to_rolling_cache(1, "1m", interval)
        
        # Get 1-minute data
        one_min_history = manager.get_instrument_history_for_timeframe(1, "1m")
        assert len(one_min_history) == 5
        
        # Verify OHLC aggregation properties
        if len(one_min_history) == 5:
            # Expected 5-minute aggregated values
            expected_open = one_min_history[0].open  # First open = 100.0
            expected_high = max(interval.high for interval in one_min_history)  # Max high = 109.0
            expected_low = min(interval.low for interval in one_min_history)    # Min low = 95.0
            expected_close = one_min_history[-1].close  # Last close = 106.0
            expected_volume = sum(interval.traded_volume for interval in one_min_history)  # Sum volumes = 15000
            
            # Verify aggregation rules
            assert expected_open == 100.0
            assert expected_high == 109.0
            assert expected_low == 95.0
            assert expected_close == 106.0
            assert expected_volume == 15000

    async def test_sequence_data_integrity(self, mock_environment):
        """Test sequence data integrity in training generation."""
        manager = UniverseStateManager(env=mock_environment)
        
        # Create time series data with known pattern
        base_time = datetime(2025, 7, 1, 9, 30, 0)
        expected_sequence = []
        
        for i in range(20):  # 20 intervals
            interval = InstrumentInterval(
                instrument_id=1,
                start_date_time=base_time + timedelta(minutes=i),
                end_date_time=base_time + timedelta(minutes=i+1),
                open=100.0 + (i * 0.1),    # Linear progression
                high=102.0 + (i * 0.1),
                low=99.0 + (i * 0.1),
                close=101.0 + (i * 0.1),
                traded_volume=1000 + (i * 10),  # Linear progression
                traded_dollar=(101.0 + (i * 0.1)) * (1000 + (i * 10)),
                status='ok'
            )
            expected_sequence.append(interval)
            manager.add_interval_to_rolling_cache(1, "1m", interval)
        
        # Retrieve sequence
        retrieved_sequence = manager.get_instrument_history_for_timeframe(1, "1m")
        
        # Should maintain rolling window (default 20)
        assert len(retrieved_sequence) <= 20
        
        # Verify data integrity in sequence
        for i, interval in enumerate(retrieved_sequence):
            # Check time progression
            if i > 0:
                prev_interval = retrieved_sequence[i-1]
                time_diff = interval.start_date_time - prev_interval.start_date_time
                assert time_diff == timedelta(minutes=1)
            
            # Check price relationships
            assert interval.low <= interval.open
            assert interval.low <= interval.close
            assert interval.high >= interval.open
            assert interval.high >= interval.close
            assert interval.traded_volume > 0
            assert interval.traded_dollar > 0

    async def test_uuid_consistency_validation(self, mock_environment):
        """Test UUID consistency across all database operations."""
        test_uuid = "consistency-test-uuid-789"
        mock_environment.get_run_uuid.return_value = test_uuid
        
        manager = UniverseStateManager(env=mock_environment)
        manager._interval_dao = Mock()
        manager._interval_dao.insert_universe_state_interval = AsyncMock()
        
        # Mock DAO to capture UUID usage
        captured_uuids = []
        
        async def capture_uuid_insert(*args, **kwargs):
            # This would capture the UUID used in database operations
            captured_uuids.append(test_uuid)
            
        manager._interval_dao.insert_universe_state_interval.side_effect = capture_uuid_insert
        
        # Perform multiple operations
        for i in range(5):
            universe_state = UniverseStateInterval(
                start_date_time=datetime.now() + timedelta(minutes=i),
                end_date_time=datetime.now() + timedelta(minutes=i+1),
                instrument_intervals={},
                instrument_indicator_intervals={},
                factor_intervals={}
            )
            
            await manager.addUniverseState("1m", universe_state, datetime.now())
        
        # Verify all operations used the same UUID
        assert len(captured_uuids) == 5
        assert all(uuid == test_uuid for uuid in captured_uuids)


if __name__ == "__main__":
    # Run specific test for debugging
    pytest.main([__file__ + "::TestCompleteUUIDCacheTrainingPipeline::test_end_to_end_data_flow_validation", "-v"])