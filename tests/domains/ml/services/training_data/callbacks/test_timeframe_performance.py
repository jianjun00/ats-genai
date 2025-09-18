#!/usr/bin/env python3
"""
Performance and efficiency tests for single timeframe generation.

Validates that the new single timeframe approach is more efficient than
the old generate-all-then-filter approach in terms of:
- Reduced unnecessary data generation
- Optimized memory usage
- Improved call efficiency
- Better resource utilization
"""

import pytest
import asyncpg
from datetime import datetime
import time
import gc
import psutil
import os
from unittest.mock import AsyncMock, MagicMock

from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig
from core.shared.data_handling.utils.environment import Environment, EnvironmentType
from tests.utils.test_data_setup import setup_single_symbol_test


class TestTimeframePerformance:
    """Performance and efficiency tests for single timeframe generation."""

    @pytest.mark.asyncio
    async def test_no_unused_timeframe_data_generated(self, unit_test_db):
        """Test that no data is generated for timeframes we don't need."""
        
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await conn.close()
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA'],
            config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi'])
        )
        
        # Track exactly what timeframes are requested
        mock_generator = AsyncMock()
        requested_timeframes = []
        
        def track_requests(symbol, prediction_timestamp, target_timeframes):
            requested_timeframes.extend(target_timeframes)
            timeframe = target_timeframes[0]
            return {
                'symbol': symbol,
                'prediction_timestamp': prediction_timestamp,
                'timeframe_features': {
                    timeframe: {'open': 250.0, 'high': 252.0, 'low': 248.0, 'close': 251.0, 'volume': 1000000}
                }
            }
        
        mock_generator.generate_training_example.side_effect = track_requests
        callback.training_generator = mock_generator
        
        # Test time that should only generate 5m (35 minutes)
        test_time = datetime(2025, 7, 1, 14, 35, 0)
        
        await callback.handleInterval(None, test_time)
        
        # Verify ONLY 5m was requested, no other timeframes
        assert requested_timeframes == ['5m']
        assert '15m' not in requested_timeframes
        assert '1h' not in requested_timeframes
        assert '1d' not in requested_timeframes
        assert '1w' not in requested_timeframes

    @pytest.mark.asyncio
    async def test_call_efficiency_single_vs_multiple_symbols(self, unit_test_db):
        """Test call efficiency with different symbol counts."""
        
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        for i, symbol in enumerate(['TSLA', 'AAPL', 'GOOGL', 'MSFT', 'AMZN']):
            await setup_single_symbol_test(environment, conn, symbol, 999999-i, 1)
        await conn.close()
        
        # Test with increasing symbol counts
        symbol_counts = [1, 2, 3, 5]
        all_symbols = ['TSLA', 'AAPL', 'GOOGL', 'MSFT', 'AMZN']
        
        for symbol_count in symbol_counts:
            test_symbols = all_symbols[:symbol_count]
            
            callback = IntervalBasedTrainingDataCallback(
                symbols=test_symbols,
                config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi'])
            )
            
            mock_generator = AsyncMock()
            call_count = 0
            
            def count_calls(*args, **kwargs):
                nonlocal call_count
                call_count += 1
                return {
                    'symbol': kwargs.get('symbol', 'TEST'),
                    'prediction_timestamp': kwargs.get('prediction_timestamp'),
                    'timeframe_features': {
                        kwargs['target_timeframes'][0]: {'open': 250.0, 'close': 251.0, 'volume': 1000000}
                    }
                }
            
            mock_generator.generate_training_example.side_effect = count_calls
            callback.training_generator = mock_generator
            
            # Test time that generates 2 timeframes (45 minutes = 5m, 15m)
            test_time = datetime(2025, 7, 1, 14, 45, 0)
            
            start_time = time.time()
            await callback.handleInterval(None, test_time)
            end_time = time.time()
            
            execution_time = end_time - start_time
            
            # Verify linear call scaling: symbols × timeframes
            expected_calls = symbol_count * 2  # 2 timeframes at 45 minutes
            assert call_count == expected_calls, (
                f"Expected {expected_calls} calls for {symbol_count} symbols, got {call_count}"
            )
            
            # Verify reasonable execution time scaling
            assert execution_time < 1.0, f"Execution too slow: {execution_time}s for {symbol_count} symbols"

    @pytest.mark.asyncio
    async def test_memory_efficiency_no_accumulation(self, unit_test_db):
        """Test that memory doesn't accumulate with single timeframe approach."""
        
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await conn.close()
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA'],
            config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi'])
        )
        
        # Create mock that returns increasingly large data (simulating memory test)
        mock_generator = AsyncMock()
        
        def memory_test_return(*args, **kwargs):
            timeframe = kwargs['target_timeframes'][0]
            # Return data structure (not actually large, but representative)
            return {
                'symbol': kwargs.get('symbol', 'TEST'),
                'prediction_timestamp': kwargs.get('prediction_timestamp'),
                'timeframe_features': {
                    timeframe: {
                        'open': 250.0, 'high': 252.0, 'low': 248.0, 'close': 251.0, 'volume': 1000000,
                        'data_id': time.time()  # Unique per call
                    }
                }
            }
        
        mock_generator.generate_training_example.side_effect = memory_test_return
        callback.training_generator = mock_generator
        
        # Get initial memory usage
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # Run multiple intervals  
        test_times = [
            datetime(2025, 7, 1, 14, 35, 0),  # 5m only
            datetime(2025, 7, 1, 14, 40, 0),  # 5m only  
            datetime(2025, 7, 1, 14, 45, 0),  # 5m, 15m
            datetime(2025, 7, 1, 14, 50, 0),  # 5m only
            datetime(2025, 7, 1, 15, 0, 0),   # 5m, 15m, 1h
        ]
        
        for test_time in test_times:
            await callback.handleInterval(None, test_time)
            
            # Force garbage collection
            gc.collect()
        
        # Check final memory usage
        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory
        
        # Memory increase should be minimal (< 10MB for this test)
        max_allowed_increase = 10 * 1024 * 1024  # 10MB
        assert memory_increase < max_allowed_increase, (
            f"Memory increased too much: {memory_increase} bytes (limit: {max_allowed_increase})"
        )

    def test_call_pattern_efficiency(self):
        """Test that call patterns are optimal for different scenarios."""
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA', 'AAPL'],
            config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi'])
        )
        
        # Test different time scenarios and expected call counts
        efficiency_test_cases = [
            # (time, expected_timeframes, symbols, expected_total_calls)
            (datetime(2025, 7, 1, 14, 37, 0), [], 2, 0),           # No generation
            (datetime(2025, 7, 1, 14, 35, 0), ['5m'], 2, 2),      # 2 symbols × 1 timeframe = 2
            (datetime(2025, 7, 1, 14, 45, 0), ['5m', '15m'], 2, 4), # 2 symbols × 2 timeframes = 4
            (datetime(2025, 7, 1, 15, 0, 0), ['5m', '15m', '1h'], 2, 6), # 2 symbols × 3 timeframes = 6
            (datetime(2025, 7, 7, 0, 0, 0), ['5m', '15m', '1h', '1d', '1w'], 2, 10), # 2 symbols × 5 timeframes = 10
        ]
        
        for test_time, expected_timeframes, symbol_count, expected_calls in efficiency_test_cases:
            target_timeframes = callback._get_target_timeframes_for_interval(test_time)
            
            # Verify timeframe detection is correct
            assert target_timeframes == expected_timeframes, (
                f"Wrong timeframes for {test_time}: expected {expected_timeframes}, got {target_timeframes}"
            )
            
            # Verify call count calculation is optimal
            actual_call_count = len(target_timeframes) * symbol_count
            assert actual_call_count == expected_calls, (
                f"Inefficient call count for {test_time}: expected {expected_calls}, would be {actual_call_count}"
            )

    @pytest.mark.asyncio
    async def test_error_overhead_minimal(self, unit_test_db):
        """Test that error handling doesn't add significant overhead."""
        
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await conn.close()
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA'],
            config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi'])
        )
        
        # Test with errors vs without errors
        mock_generator = AsyncMock()
        
        # Success case timing
        mock_generator.generate_training_example.return_value = {
            'symbol': 'TSLA',
            'prediction_timestamp': datetime.now(),
            'timeframe_features': {
                '5m': {'open': 250.0, 'close': 251.0, 'volume': 1000000}
            }
        }
        
        callback.training_generator = mock_generator
        test_time = datetime(2025, 7, 1, 14, 35, 0)
        
        # Measure success case
        start_time = time.time()
        await callback.handleInterval(None, test_time)
        success_time = time.time() - start_time
        
        # Error case timing
        mock_generator.generate_training_example.side_effect = Exception("Test error")
        
        start_time = time.time()
        await callback.handleInterval(None, test_time)  # Should handle error gracefully
        error_time = time.time() - start_time
        
        # Error handling should not add significant overhead (< 2x success time)
        assert error_time < success_time * 2, (
            f"Error handling overhead too high: success={success_time}s, error={error_time}s"
        )

    def test_timeframe_generation_selectivity(self):
        """Test that we only generate exactly what we need, no more, no less."""
        
        callback = IntervalBasedTrainingDataCallback(symbols=['TSLA'])
        
        # Test various time scenarios and verify precise timeframe selection
        selectivity_tests = [
            # (time, minute, expected_timeframes, unexpected_timeframes)
            (datetime(2025, 7, 1, 14, 5, 0), 5, ['5m'], ['15m', '1h', '1d', '1w']),
            (datetime(2025, 7, 1, 14, 10, 0), 10, ['5m'], ['15m', '1h', '1d', '1w']),
            (datetime(2025, 7, 1, 14, 15, 0), 15, ['5m', '15m'], ['1h', '1d', '1w']),
            (datetime(2025, 7, 1, 14, 30, 0), 30, ['5m', '15m'], ['1h', '1d', '1w']),
            (datetime(2025, 7, 1, 14, 0, 0), 0, ['5m', '15m', '1h'], ['1d', '1w']),
            (datetime(2025, 7, 1, 0, 0, 0), 0, ['5m', '15m', '1h', '1d'], ['1w']),
            (datetime(2025, 7, 7, 0, 0, 0), 0, ['5m', '15m', '1h', '1d', '1w'], []),  # Monday
        ]
        
        for test_time, minute, expected, unexpected in selectivity_tests:
            target_timeframes = callback._get_target_timeframes_for_interval(test_time)
            
            # Verify we get exactly what we expect
            assert set(target_timeframes) == set(expected), (
                f"At {minute}m: expected {expected}, got {target_timeframes}"
            )
            
            # Verify we DON'T get what we shouldn't
            for unwanted_tf in unexpected:
                assert unwanted_tf not in target_timeframes, (
                    f"At {minute}m: unexpected timeframe {unwanted_tf} in {target_timeframes}"
                )

    @pytest.mark.asyncio
    async def test_batch_processing_efficiency(self, unit_test_db):
        """Test efficiency when processing multiple intervals in sequence."""
        
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await conn.close()
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA'],
            config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi'])
        )
        
        mock_generator = AsyncMock()
        total_calls = 0
        
        def count_total_calls(*args, **kwargs):
            nonlocal total_calls
            total_calls += 1
            timeframe = kwargs['target_timeframes'][0]
            return {
                'symbol': kwargs.get('symbol', 'TEST'),
                'prediction_timestamp': kwargs.get('prediction_timestamp'),
                'timeframe_features': {
                    timeframe: {'open': 250.0, 'close': 251.0, 'volume': 1000000}
                }
            }
        
        mock_generator.generate_training_example.side_effect = count_total_calls
        callback.training_generator = mock_generator
        
        # Process a sequence of intervals (1 hour = 12 five-minute intervals)
        start_time = time.time()
        
        for minute in range(0, 60, 5):  # Every 5 minutes for 1 hour
            test_time = datetime(2025, 7, 1, 14, minute, 0)
            await callback.handleInterval(None, test_time)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Verify efficient batch processing
        expected_calls = 12  # 12 five-minute intervals, each generates 5m timeframe
        # Plus extra calls for 15m (every 15 minutes = 4 times) and 1h (once at 00)
        expected_calls += 4  # 15m at 00, 15, 30, 45
        expected_calls += 1  # 1h at 00
        
        assert total_calls == expected_calls, (
            f"Expected {expected_calls} total calls for 1-hour batch, got {total_calls}"
        )
        
        # Verify reasonable processing time (< 2 seconds for 1 hour simulation)
        assert processing_time < 2.0, f"Batch processing too slow: {processing_time}s"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])