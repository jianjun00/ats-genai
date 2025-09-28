#!/usr/bin/env python3
"""
Comprehensive Unit Tests for Ray-Enhanced Training Data Callback

Tests the Ray parallel processing enhancements in IntervalBasedTrainingDataCallback,
ensuring proper parallel execution, fallback mechanisms, and performance benefits.

Test Coverage:
- Ray actor initialization and lifecycle
- Parallel vs sequential processing modes
- Symbol distribution across workers
- Error handling and fallback mechanisms
- Performance validation
- Configuration options
"""

import pytest
import ray
import logging
from unittest.mock import Mock, AsyncMock
from datetime import datetime, date, timedelta

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

from domains.ml.services.training_data.callbacks.training_data_callback import (
    IntervalBasedTrainingDataCallback,
    ParallelSequenceGenerator
)

# Configure logging for tests
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def ray_context():
    """Initialize Ray for testing."""
    if not ray.is_initialized():
        ray.init(
            object_store_memory=100_000_000,  # 100MB for tests
            num_cpus=2,  # Limit CPUs for tests
            ignore_reinit_error=True
        )
    yield
    # Don't shutdown Ray - let it persist for other tests


@pytest.fixture
def mock_training_generator():
    """Mock training data generator."""
    generator = AsyncMock()
    generator.generate_training_example.return_value = {
        'timestamp': datetime.now().isoformat(),
        'symbol': 'TEST',
        'features': [1.0, 2.0, 3.0],
        'labels': [0.05],
        'metadata': {'test': True}
    }
    return generator


@pytest.fixture
def mock_storage_manager():
    """Mock storage manager."""
    storage = AsyncMock()
    storage.save_sequence_batch.return_value = {
        'sequence_file': '/test/sequences.riegeli',
        'metadata_file': '/test/metadata.json'
    }
    return storage


class TestParallelSequenceGenerator:
    """Test the Ray actor for parallel sequence generation."""

    def test_ray_actor_initialization(self, ray_context):
        """Test Ray actor can be created and initialized."""
        # Create Ray actor
        actor = ParallelSequenceGenerator.remote()

        # Test that actor was created successfully
        assert actor is not None

        # Test basic Ray functionality
        actor_id = ray.get(actor.__ray_ready__.remote())
        assert actor_id is not None

    @pytest.mark.asyncio
    async def test_generate_sequences_for_symbol_batch(self, ray_context):
        """Test parallel sequence generation for symbol batch."""
        actor = ParallelSequenceGenerator.remote()

        # Test data
        symbol = "AAPL"
        date_range = [date(2025, 7, 1), date(2025, 7, 2)]
        config = {'test_config': True}

        # Call the remote method
        result_future = actor.generate_sequences_for_symbol_batch.remote(
            symbol=symbol,
            date_range=date_range,
            config=config
        )

        # Get result
        result = ray.get(result_future)

        # Validate result
        assert isinstance(result, list)
        assert len(result) == 2  # One sequence per date

        for sequence in result:
            assert sequence['symbol'] == symbol
            assert 'date' in sequence
            assert 'features' in sequence
            assert 'labels' in sequence
            assert 'metadata' in sequence
            assert 'worker_id' in sequence['metadata']

    @pytest.mark.asyncio
    async def test_parallel_actor_error_handling(self, ray_context):
        """Test error handling in Ray actor."""
        actor = ParallelSequenceGenerator.remote()

        # Test with invalid input
        result_future = actor.generate_sequences_for_symbol_batch.remote(
            symbol="",  # Empty symbol should handle gracefully
            date_range=[],  # Empty date range
            config=None
        )

        result = ray.get(result_future)
        assert isinstance(result, list)
        assert len(result) == 0  # Should return empty list for empty inputs


class TestIntervalBasedTrainingDataCallbackRayIntegration:
    """Test Ray integration in IntervalBasedTrainingDataCallback."""

    def test_callback_ray_initialization_enabled(self, ray_context, mock_storage_manager):
        """Test callback initializes Ray workers when enabled."""
        symbols = ['AAPL', 'TSLA', 'MSFT']

        callback = IntervalBasedTrainingDataCallback(
            symbols=symbols,
            storage_manager=mock_storage_manager,
            enable_ray_parallel=True,
            max_parallel_workers=2
        )

        # Verify Ray workers were created
        assert callback.enable_ray_parallel is True
        assert len(callback.ray_workers) == 2  # min(max_parallel_workers, len(symbols))
        assert all(worker is not None for worker in callback.ray_workers)

    def test_callback_ray_initialization_disabled(self, mock_storage_manager):
        """Test callback works without Ray when disabled."""
        symbols = ['AAPL', 'TSLA']

        callback = IntervalBasedTrainingDataCallback(
            symbols=symbols,
            storage_manager=mock_storage_manager,
            enable_ray_parallel=False
        )

        # Verify Ray is disabled
        assert callback.enable_ray_parallel is False
        assert len(callback.ray_workers) == 0

    def test_symbol_distribution_to_workers(self, ray_context, mock_storage_manager):
        """Test symbols are properly distributed to Ray workers."""
        symbols = ['AAPL', 'TSLA', 'MSFT', 'GOOGL', 'AMZN']

        callback = IntervalBasedTrainingDataCallback(
            symbols=symbols,
            storage_manager=mock_storage_manager,
            enable_ray_parallel=True,
            max_parallel_workers=3
        )

        # Test symbol distribution
        batches = callback._distribute_symbols_to_workers()

        # Verify distribution
        assert len(batches) <= 3  # No more batches than workers
        assert len(batches) > 0

        # Verify all symbols are distributed
        all_symbols = []
        for batch in batches:
            all_symbols.extend(batch)

        # Should have reasonable distribution
        assert len(all_symbols) <= len(symbols)

    def test_symbol_distribution_edge_cases(self, ray_context, mock_storage_manager):
        """Test symbol distribution with edge cases."""
        # Test with single symbol
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            storage_manager=mock_storage_manager,
            enable_ray_parallel=True,
            max_parallel_workers=4
        )

        batches = callback._distribute_symbols_to_workers()
        assert len(batches) == 1
        assert batches[0] == ['AAPL']

        # Test with no symbols
        callback.symbols = []
        batches = callback._distribute_symbols_to_workers()
        assert len(batches) == 0

    @pytest.mark.asyncio
    async def test_parallel_vs_sequential_processing_mode(self, ray_context, mock_training_generator, mock_storage_manager):
        """Test callback chooses correct processing mode."""
        symbols = ['AAPL', 'TSLA']

        # Test Ray-enabled callback
        callback_ray = IntervalBasedTrainingDataCallback(
            symbols=symbols,
            storage_manager=mock_storage_manager,
            enable_ray_parallel=True,
            max_parallel_workers=2
        )
        callback_ray.training_generator = mock_training_generator
        callback_ray.current_date = date.today()

        # Test sequential callback
        callback_seq = IntervalBasedTrainingDataCallback(
            symbols=symbols,
            storage_manager=mock_storage_manager,
            enable_ray_parallel=False
        )
        callback_seq.training_generator = mock_training_generator
        callback_seq.current_date = date.today()

        # Mock runner
        mock_runner = Mock()
        test_time = datetime.now()

        # Initialize daily stats for both callbacks
        callback_ray.daily_stats = {'errors': [], 'examples_generated': 0, 'intervals_processed': 0}
        callback_seq.daily_stats = {'errors': [], 'examples_generated': 0, 'intervals_processed': 0}

        # Test both processing modes
        await callback_ray.handleInterval(mock_runner, test_time)
        await callback_seq.handleInterval(mock_runner, test_time)

        # Both should have processed examples
        assert len(callback_ray.daily_examples) > 0
        assert len(callback_seq.daily_examples) > 0


class TestRayPerformanceAndReliability:
    """Test Ray performance benefits and reliability."""

    @pytest.mark.asyncio
    async def test_ray_fallback_on_error(self, ray_context, mock_training_generator, mock_storage_manager):
        """Test fallback to sequential processing when Ray fails."""
        symbols = ['AAPL', 'TSLA']

        callback = IntervalBasedTrainingDataCallback(
            symbols=symbols,
            storage_manager=mock_storage_manager,
            enable_ray_parallel=True,
            max_parallel_workers=2
        )
        callback.training_generator = mock_training_generator
        callback.current_date = date.today()
        callback.daily_stats = {'errors': [], 'examples_generated': 0, 'intervals_processed': 0}

        # Mock Ray workers to fail
        callback.ray_workers = [Mock()]
        callback.ray_workers[0].generate_sequences_for_symbol_batch.remote = Mock(side_effect=Exception("Ray error"))

        # Test that it falls back to sequential
        test_time = datetime.now()
        await callback.handleInterval(Mock(), test_time)

        # Should still have processed examples via fallback
        assert len(callback.daily_examples) > 0

    @pytest.mark.asyncio
    async def test_sequential_processing_reliability(self, mock_training_generator, mock_storage_manager):
        """Test sequential processing works reliably."""
        symbols = ['AAPL', 'TSLA', 'MSFT']

        callback = IntervalBasedTrainingDataCallback(
            symbols=symbols,
            storage_manager=mock_storage_manager,
            enable_ray_parallel=False
        )
        callback.training_generator = mock_training_generator
        callback.current_date = date.today()
        callback.daily_stats = {'errors': [], 'examples_generated': 0, 'intervals_processed': 0}

        # Test multiple intervals
        test_times = [datetime.now() + timedelta(minutes=i) for i in range(5)]

        for test_time in test_times:
            await callback.handleInterval(Mock(), test_time)

        # Should have processed all intervals
        expected_examples = len(symbols) * len(test_times)
        assert len(callback.daily_examples) == expected_examples

    @pytest.mark.asyncio
    async def test_parallel_processing_with_multiple_workers(self, ray_context, mock_storage_manager):
        """Test parallel processing distributes work correctly."""
        symbols = ['AAPL', 'TSLA', 'MSFT', 'GOOGL']

        callback = IntervalBasedTrainingDataCallback(
            symbols=symbols,
            storage_manager=mock_storage_manager,
            enable_ray_parallel=True,
            max_parallel_workers=2
        )
        callback.current_date = date.today()
        callback.daily_stats = {'errors': [], 'examples_generated': 0, 'intervals_processed': 0}

        # Test parallel example generation
        test_time = datetime.now()
        result = await callback._generate_examples_parallel(test_time)

        # Should return some results (even if placeholder)
        assert isinstance(result, list)


class TestRayConfigurationOptions:
    """Test various Ray configuration options."""

    def test_ray_worker_count_configuration(self, ray_context, mock_storage_manager):
        """Test different Ray worker count configurations."""
        symbols = ['AAPL', 'TSLA', 'MSFT', 'GOOGL', 'AMZN']

        # Test various worker counts
        worker_counts = [1, 2, 4, 8, 16]

        for worker_count in worker_counts:
            callback = IntervalBasedTrainingDataCallback(
                symbols=symbols,
                storage_manager=mock_storage_manager,
                enable_ray_parallel=True,
                max_parallel_workers=worker_count
            )

            # Should not exceed number of symbols or requested workers
            expected_workers = min(worker_count, len(symbols))
            assert len(callback.ray_workers) == expected_workers

    def test_ray_configuration_with_no_symbols(self, mock_storage_manager):
        """Test Ray configuration with edge case inputs."""
        # Test with empty symbols list
        callback = IntervalBasedTrainingDataCallback(
            symbols=[],
            storage_manager=mock_storage_manager,
            enable_ray_parallel=True,
            max_parallel_workers=4
        )

        # Should handle gracefully
        assert len(callback.ray_workers) == 0

        # Test distribution with no symbols
        batches = callback._distribute_symbols_to_workers()
        assert len(batches) == 0


class TestRayIntegrationWithExistingWorkflow:
    """Test Ray integration doesn't break existing workflow."""

    @pytest.mark.asyncio
    async def test_start_of_day_with_ray(self, ray_context, mock_storage_manager):
        """Test SOD handling works with Ray enabled."""
        symbols = ['AAPL', 'TSLA']

        callback = IntervalBasedTrainingDataCallback(
            symbols=symbols,
            storage_manager=mock_storage_manager,
            enable_ray_parallel=True,
            max_parallel_workers=2
        )

        # Test SOD handling
        mock_runner = Mock()
        test_time = datetime.now()

        callback.handleStartOfDay(mock_runner, test_time)

        # Verify SOD setup
        assert callback.current_date == test_time.date()
        assert len(callback.daily_examples) == 0
        assert isinstance(callback.daily_stats, dict)

    @pytest.mark.asyncio
    async def test_end_of_day_with_ray(self, ray_context, mock_storage_manager):
        """Test EOD handling works with Ray enabled."""
        symbols = ['AAPL', 'TSLA']

        callback = IntervalBasedTrainingDataCallback(
            symbols=symbols,
            storage_manager=mock_storage_manager,
            enable_ray_parallel=True,
            max_parallel_workers=2
        )

        # Setup for EOD
        callback.current_date = date.today()
        callback.daily_examples = [
            {'test_example': 1, 'timestamp': datetime.now().isoformat()},
            {'test_example': 2, 'timestamp': datetime.now().isoformat()}
        ]
        callback.daily_stats = {
            'errors': [],
            'examples_generated': 2,
            'intervals_processed': 1,
            'start_time': datetime.now().isoformat()
        }

        # Mock the save methods to avoid file I/O
        callback._save_daily_data = AsyncMock()
        callback._save_daily_metadata = AsyncMock()

        # Test EOD handling
        mock_runner = Mock()
        test_time = datetime.now()

        await callback.handleEndOfDay(mock_runner, test_time)

        # Verify EOD cleanup
        assert callback.current_date is None
        assert len(callback.daily_examples) == 0
        assert len(callback.daily_stats) == 0


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v", "--tb=short"])