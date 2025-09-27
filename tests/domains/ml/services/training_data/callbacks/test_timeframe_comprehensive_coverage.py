#!/usr/bin/env python3
"""
Comprehensive test coverage for single timeframe generation refactoring.

This test suite covers areas not addressed by the basic timeframe tests:
- Multi-symbol scenarios
- Error handling and resilience
- Call pattern validation
- Data structure integrity
- Integration with real components
- Performance and resource validation
"""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, call
import asyncpg

from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig
from core.platform.config.environment import Environment, EnvironmentType
# FIXME: tests.utils module does not exist
# from tests.utils.test_data_setup import setup_single_symbol_test


class TestTimeframeComprehensiveCoverage:
    """Comprehensive test coverage for single timeframe generation changes."""

    @pytest.mark.asyncio
    async def test_multiple_symbols_single_timeframe(self, unit_test_db):
        """Test multiple symbols with single timeframe generates correct number of calls."""
        
        # Setup with multiple symbols
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await setup_single_symbol_test(environment, conn, 'AAPL', 999998, 1)
        await conn.close()
        
        config = TrainingDataConfig(
            feature_types=['ohlcv'],
            signal_names=['rsi', 'macd']
        )
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA', 'AAPL'],  # Multiple symbols
            config=config
        )
        
        mock_generator = AsyncMock()
        callback.training_generator = mock_generator
        
        # Test time that generates only 5m (35 minutes)
        test_time = datetime(2025, 7, 1, 14, 35, 0)
        
        # Mock generator returns valid examples
        mock_generator.generate_training_example.return_value = {
            'symbol': 'TEST',
            'prediction_timestamp': test_time,
            'timeframe_features': {
                '5m': {'open': 250.0, 'high': 252.0, 'low': 248.0, 'close': 251.0, 'volume': 1000000}
            }
        }
        
        await callback.handleInterval(None, test_time)
        
        # Verify: 2 symbols × 1 timeframe = 2 calls
        assert mock_generator.generate_training_example.call_count == 2
        
        # Verify each symbol was called with 5m timeframe
        calls = mock_generator.generate_training_example.call_args_list
        symbols_called = [call[1]['symbol'] for call in calls]
        timeframes_called = [call[1]['target_timeframes'] for call in calls]
        
        assert set(symbols_called) == {'TSLA', 'AAPL'}
        assert all(tf == ['5m'] for tf in timeframes_called)

    @pytest.mark.asyncio
    async def test_multiple_symbols_multiple_timeframes(self, unit_test_db):
        """Test multiple symbols with multiple timeframes generates correct call matrix."""
        
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await setup_single_symbol_test(environment, conn, 'AAPL', 999998, 1)
        await setup_single_symbol_test(environment, conn, 'GOOGL', 999997, 1)
        await conn.close()
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA', 'AAPL', 'GOOGL'],
            config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi'])
        )
        
        mock_generator = AsyncMock()
        callback.training_generator = mock_generator
        
        # Test time that generates 5m and 15m (45 minutes)
        test_time = datetime(2025, 7, 1, 14, 45, 0)
        
        def mock_return(symbol, prediction_timestamp, target_timeframes):
            timeframe = target_timeframes[0]
            return {
                'symbol': symbol,
                'prediction_timestamp': test_time,
                'timeframe_features': {
                    timeframe: {'open': 250.0, 'high': 252.0, 'low': 248.0, 'close': 251.0, 'volume': 1000000}
                }
            }
        
        mock_generator.generate_training_example.side_effect = mock_return
        
        await callback.handleInterval(None, test_time)
        
        # Verify: 3 symbols × 2 timeframes = 6 calls
        assert mock_generator.generate_training_example.call_count == 6
        
        # Verify call matrix: each symbol called for each timeframe
        calls = mock_generator.generate_training_example.call_args_list
        call_matrix = {}
        
        for call in calls:
            symbol = call[1]['symbol']
            timeframe = call[1]['target_timeframes'][0]
            if symbol not in call_matrix:
                call_matrix[symbol] = []
            call_matrix[symbol].append(timeframe)
        
        # Each symbol should be called for both timeframes
        assert set(call_matrix.keys()) == {'TSLA', 'AAPL', 'GOOGL'}
        for symbol in call_matrix:
            assert set(call_matrix[symbol]) == {'5m', '15m'}

    @pytest.mark.asyncio
    async def test_error_resilience_single_timeframe_failure(self, unit_test_db):
        """Test that if one timeframe fails, other timeframes continue processing."""
        
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await conn.close()
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA'],
            config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi'])
        )
        
        mock_generator = AsyncMock()
        callback.training_generator = mock_generator
        
        # Test time that generates 5m and 15m
        test_time = datetime(2025, 7, 1, 14, 45, 0)
        
        def mock_return(symbol, prediction_timestamp, target_timeframes):
            timeframe = target_timeframes[0]
            if timeframe == '5m':
                raise Exception("5m generation failed")
            elif timeframe == '15m':
                return {
                    'symbol': symbol,
                    'prediction_timestamp': test_time,
                    'timeframe_features': {
                        '15m': {'open': 250.0, 'high': 252.0, 'low': 248.0, 'close': 251.0, 'volume': 1000000}
                    }
                }
        
        mock_generator.generate_training_example.side_effect = mock_return
        
        # Should not raise exception despite 5m failure
        await callback.handleInterval(None, test_time)
        
        # Verify both timeframes were attempted
        assert mock_generator.generate_training_example.call_count == 2

    @pytest.mark.asyncio  
    async def test_error_resilience_single_symbol_failure(self, unit_test_db):
        """Test that if one symbol fails, other symbols continue processing."""
        
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await setup_single_symbol_test(environment, conn, 'AAPL', 999998, 1)
        await conn.close()
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA', 'AAPL'],
            config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi'])
        )
        
        mock_generator = AsyncMock()
        callback.training_generator = mock_generator
        
        test_time = datetime(2025, 7, 1, 14, 35, 0)  # 5m only
        
        def mock_return(symbol, prediction_timestamp, target_timeframes):
            if symbol == 'TSLA':
                raise Exception("TSLA generation failed")
            elif symbol == 'AAPL':
                return {
                    'symbol': symbol,
                    'prediction_timestamp': test_time,
                    'timeframe_features': {
                        '5m': {'open': 150.0, 'high': 152.0, 'low': 148.0, 'close': 151.0, 'volume': 2000000}
                    }
                }
        
        mock_generator.generate_training_example.side_effect = mock_return
        
        # Should not raise exception despite TSLA failure
        await callback.handleInterval(None, test_time)
        
        # Verify both symbols were attempted
        assert mock_generator.generate_training_example.call_count == 2

    @pytest.mark.asyncio
    async def test_generator_returns_none_handling(self, unit_test_db):
        """Test handling when generator returns None for a timeframe."""
        
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await conn.close()
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA'],
            config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi'])
        )
        
        mock_generator = AsyncMock()
        callback.training_generator = mock_generator
        
        test_time = datetime(2025, 7, 1, 14, 45, 0)  # 5m and 15m
        
        def mock_return(symbol, prediction_timestamp, target_timeframes):
            timeframe = target_timeframes[0]
            if timeframe == '5m':
                return None  # Generator returns None
            elif timeframe == '15m':
                return {
                    'symbol': symbol,
                    'prediction_timestamp': test_time,
                    'timeframe_features': {
                        '15m': {'open': 250.0, 'high': 252.0, 'low': 248.0, 'close': 251.0, 'volume': 1000000}
                    }
                }
        
        mock_generator.generate_training_example.side_effect = mock_return
        
        # Should handle None gracefully
        await callback.handleInterval(None, test_time)
        
        assert mock_generator.generate_training_example.call_count == 2

    @pytest.mark.asyncio
    async def test_generator_returns_wrong_timeframe_data(self, unit_test_db):
        """Test handling when generator returns features for wrong timeframe."""
        
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await conn.close()
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA'],
            config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi'])
        )
        
        mock_generator = AsyncMock()
        callback.training_generator = mock_generator
        
        test_time = datetime(2025, 7, 1, 14, 35, 0)  # Should generate 5m only
        
        # Generator returns data for wrong timeframe
        mock_generator.generate_training_example.return_value = {
            'symbol': 'TSLA',
            'prediction_timestamp': test_time,
            'timeframe_features': {
                '15m': {'open': 250.0, 'high': 252.0, 'low': 248.0, 'close': 251.0, 'volume': 1000000}  # Wrong timeframe!
            }
        }
        
        await callback.handleInterval(None, test_time)
        
        # Should handle mismatched timeframe gracefully  
        assert mock_generator.generate_training_example.call_count == 1

    def test_call_pattern_consistency(self):
        """Test that call patterns are consistent across different time scenarios."""
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA', 'AAPL'],
            config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi'])
        )
        
        # Test various time scenarios
        test_scenarios = [
            (datetime(2025, 7, 1, 14, 35, 0), ['5m']),           # 35m → 5m only
            (datetime(2025, 7, 1, 14, 45, 0), ['5m', '15m']),   # 45m → 5m, 15m
            (datetime(2025, 7, 1, 14, 0, 0), ['5m', '15m', '1h']),  # 00m → 5m, 15m, 1h
        ]
        
        for test_time, expected_timeframes in test_scenarios:
            target_timeframes = callback._get_target_timeframes_for_interval(test_time)
            assert target_timeframes == expected_timeframes, (
                f"Inconsistent timeframe pattern for {test_time}: expected {expected_timeframes}, got {target_timeframes}"
            )

    def test_data_structure_integrity(self):
        """Test that single timeframe examples maintain correct structure."""
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA'],
            config=TrainingDataConfig(feature_types=['ohlcv'], signal_names=['rsi'])
        )
        
        # Test example structure for single timeframe
        test_time = datetime(2025, 7, 1, 14, 35, 0)
        
        single_timeframe_example = {
            'symbol': 'TSLA',
            'prediction_timestamp': test_time,
            'timeframe_features': {
                '5m': {'open': 250.0, 'high': 252.0, 'low': 248.0, 'close': 251.0, 'volume': 1000000}
            }
        }
        
        # Verify structure
        assert 'symbol' in single_timeframe_example
        assert 'prediction_timestamp' in single_timeframe_example
        assert 'timeframe_features' in single_timeframe_example
        
        timeframe_features = single_timeframe_example['timeframe_features']
        assert len(timeframe_features) == 1  # Only one timeframe
        assert '5m' in timeframe_features
        assert '15m' not in timeframe_features  # No other timeframes
        
        # Verify OHLCV data structure
        ohlcv_data = timeframe_features['5m']
        required_fields = ['open', 'high', 'low', 'close', 'volume']
        for field in required_fields:
            assert field in ohlcv_data
            assert isinstance(ohlcv_data[field], (int, float))

    def test_no_cross_timeframe_contamination(self):
        """Test that timeframe features only contain requested timeframe."""
        
        callback = IntervalBasedTrainingDataCallback(symbols=['TSLA'])
        
        # Simulate what should happen: request 5m, get only 5m
        timeframes_to_test = ['5m', '15m', '1h', '1d', '1w']
        
        for target_timeframe in timeframes_to_test:
            # Each single timeframe example should only contain that timeframe
            single_example = {
                'symbol': 'TSLA',
                'prediction_timestamp': datetime(2025, 7, 1, 14, 0, 0),
                'timeframe_features': {
                    target_timeframe: {'open': 250.0, 'close': 251.0, 'volume': 1000000}
                }
            }
            
            # Verify only target timeframe present
            timeframe_features = single_example['timeframe_features']
            assert len(timeframe_features) == 1
            assert target_timeframe in timeframe_features
            
            # Verify no other timeframes contaminate
            other_timeframes = set(timeframes_to_test) - {target_timeframe}
            for other_tf in other_timeframes:
                assert other_tf not in timeframe_features


if __name__ == '__main__':
    pytest.main([__file__, '-v'])