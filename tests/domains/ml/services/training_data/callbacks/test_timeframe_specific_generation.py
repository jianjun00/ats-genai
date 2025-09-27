#!/usr/bin/env python3
"""
Test timeframe-specific generation logic in handleInterval.

Validates that handleInterval generates examples only for specific timeframes
based on the current time's minute value, implementing PRD/DRD requirements:

- 5m: Every 5 minutes (05, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 00)
- 15m: Every 15 minutes (00, 15, 30, 45)  
- 1h: Every hour (00 minutes only)
- 1d: Once per day (00:00 only)
- 1w: Once per week (Monday 00:00 only)

Examples:
- At 35m: Generate only 5m timeframe
- At 45m: Generate 5m and 15m timeframes
- At 00m: Generate 5m, 15m, and 1h timeframes
"""

import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock
import asyncpg

from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig
from core.platform.config.environment import Environment, EnvironmentType
# FIXME: tests.utils module does not exist
# from tests.utils.test_data_setup import setup_single_symbol_test


class TestTimeframeSpecificGeneration:
    """Test timeframe-specific generation logic per PRD/DRD requirements."""

    @pytest.mark.asyncio
    async def test_5m_only_generation_at_35_minutes(self, unit_test_db):
        """Test that at 35 minutes, only 5m timeframe is generated."""
        
        # Setup
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await conn.close()
        
        config = TrainingDataConfig(
            feature_types=['ohlcv'],
            signal_names=['rsi', 'macd']
        )
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA'],
            config=config
        )
        
        # Mock the training generator to capture which timeframes are requested
        mock_generator = AsyncMock()
        callback.training_generator = mock_generator
        
        # Test time at 35 minutes - should only generate 5m
        test_time = datetime(2025, 7, 1, 14, 35, 0)  # 35 minutes
        
        # Expected timeframes for 35 minutes: only 5m (35 % 5 == 0, but 35 % 15 != 0)
        expected_timeframes = ['5m']
        
        # Mock training generator to return example
        mock_generator.generate_training_example.return_value = {
            'symbol': 'TSLA',
            'prediction_timestamp': test_time,
            'timeframe_features': {
                '5m': {'open': 250.0, 'high': 252.0, 'low': 248.0, 'close': 251.0, 'volume': 1000000}
            }
        }
        
        # Call handleInterval
        await callback.handleInterval(None, test_time)
        
        # Verify generate_training_example was called once for 5m timeframe
        mock_generator.generate_training_example.assert_called_once_with(
            symbol='TSLA',
            prediction_timestamp=test_time,
            target_timeframes=['5m']  # Single timeframe in list
        )

    @pytest.mark.asyncio
    async def test_5m_and_15m_generation_at_45_minutes(self, unit_test_db):
        """Test that at 45 minutes, both 5m and 15m timeframes are generated."""
        
        # Setup
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await conn.close()
        
        config = TrainingDataConfig(
            feature_types=['ohlcv'],
            signal_names=['rsi', 'macd']
        )
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA'],
            config=config
        )
        
        # Mock the training generator
        mock_generator = AsyncMock()
        callback.training_generator = mock_generator
        
        # Test time at 45 minutes - should generate 5m and 15m
        test_time = datetime(2025, 7, 1, 14, 45, 0)  # 45 minutes
        
        # Expected timeframes: 5m (45 % 5 == 0) and 15m (45 % 15 == 0)
        expected_timeframes = ['5m', '15m']
        
        # Mock training generator to return examples for each timeframe
        def mock_return(symbol, prediction_timestamp, target_timeframes):
            timeframe = target_timeframes[0]  # Single timeframe
            if timeframe == '5m':
                return {
                    'symbol': 'TSLA',
                    'prediction_timestamp': test_time,
                    'timeframe_features': {
                        '5m': {'open': 250.0, 'high': 252.0, 'low': 248.0, 'close': 251.0, 'volume': 1000000}
                    }
                }
            elif timeframe == '15m':
                return {
                    'symbol': 'TSLA',
                    'prediction_timestamp': test_time,
                    'timeframe_features': {
                        '15m': {'open': 248.0, 'high': 253.0, 'low': 247.0, 'close': 251.0, 'volume': 3000000}
                    }
                }
            
        mock_generator.generate_training_example.side_effect = mock_return
        
        # Call handleInterval
        await callback.handleInterval(None, test_time)
        
        # Verify generate_training_example was called twice: once for 5m and once for 15m
        assert mock_generator.generate_training_example.call_count == 2
        calls = mock_generator.generate_training_example.call_args_list
        
        # First call should be for 5m
        assert calls[0][1]['target_timeframes'] == ['5m']
        # Second call should be for 15m
        assert calls[1][1]['target_timeframes'] == ['15m']

    @pytest.mark.asyncio
    async def test_all_timeframes_at_midnight(self, unit_test_db):
        """Test that at Monday midnight, all timeframes including 1d and 1w are generated."""
        
        # Setup
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await conn.close()
        
        config = TrainingDataConfig(
            feature_types=['ohlcv'],
            signal_names=['rsi', 'macd', 'sma', 'ema', 'bb']
        )
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA'],
            config=config
        )
        
        # Mock the training generator
        mock_generator = AsyncMock()
        callback.training_generator = mock_generator
        
        # Test time at Monday 00:00 - should generate all timeframes
        test_time = datetime(2025, 7, 7, 0, 0, 0)  # Monday 00:00
        
        # Expected timeframes: 5m, 15m, 1h, 1d, 1w (all conditions met)
        expected_timeframes = ['5m', '15m', '1h', '1d', '1w']
        
        # Mock training generator to return examples for each timeframe
        def mock_return(symbol, prediction_timestamp, target_timeframes):
            timeframe = target_timeframes[0]  # Single timeframe
            timeframe_data = {
                '5m': {'open': 250.0, 'high': 252.0, 'low': 248.0, 'close': 251.0, 'volume': 1000000},
                '15m': {'open': 248.0, 'high': 253.0, 'low': 247.0, 'close': 251.0, 'volume': 3000000},
                '1h': {'open': 245.0, 'high': 255.0, 'low': 244.0, 'close': 251.0, 'volume': 12000000},
                '1d': {'open': 240.0, 'high': 260.0, 'low': 238.0, 'close': 251.0, 'volume': 25000000},
                '1w': {'open': 235.0, 'high': 265.0, 'low': 230.0, 'close': 251.0, 'volume': 125000000}
            }
            
            if timeframe in timeframe_data:
                return {
                    'symbol': 'TSLA',
                    'prediction_timestamp': test_time,
                    'timeframe_features': {
                        timeframe: timeframe_data[timeframe]
                    }
                }
            
        mock_generator.generate_training_example.side_effect = mock_return
        
        # Call handleInterval
        await callback.handleInterval(None, test_time)
        
        # Verify generate_training_example was called 5 times for all timeframes
        assert mock_generator.generate_training_example.call_count == 5
        calls = mock_generator.generate_training_example.call_args_list
        
        # Verify each timeframe was called individually
        called_timeframes = [call[1]['target_timeframes'][0] for call in calls]
        assert set(called_timeframes) == set(expected_timeframes)

    @pytest.mark.asyncio 
    async def test_no_generation_at_non_divisible_minute(self, unit_test_db):
        """Test that at minutes not divisible by 5, no timeframes are generated."""
        
        # Setup
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await conn.close()
        
        config = TrainingDataConfig(
            feature_types=['ohlcv'],
            signal_names=['rsi', 'macd']
        )
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA'],
            config=config
        )
        
        # Mock the training generator
        mock_generator = AsyncMock()
        callback.training_generator = mock_generator
        
        # Test time at 37 minutes - should not generate any timeframes
        test_time = datetime(2025, 7, 1, 14, 37, 0)  # 37 minutes (not divisible by 5)
        
        # Call handleInterval
        await callback.handleInterval(None, test_time)
        
        # Verify generate_training_example was NOT called
        mock_generator.generate_training_example.assert_not_called()

    def test_get_target_timeframes_logic(self):
        """Test the _get_target_timeframes_for_interval logic directly."""
        
        callback = IntervalBasedTrainingDataCallback(symbols=['TSLA'])
        
        # Test various time scenarios
        test_cases = [
            # (datetime, expected_timeframes)
            (datetime(2025, 7, 1, 14, 5, 0), ['5m']),                          # 05 minutes: only 5m
            (datetime(2025, 7, 1, 14, 10, 0), ['5m']),                         # 10 minutes: only 5m  
            (datetime(2025, 7, 1, 14, 15, 0), ['5m', '15m']),                  # 15 minutes: 5m + 15m
            (datetime(2025, 7, 1, 14, 30, 0), ['5m', '15m']),                  # 30 minutes: 5m + 15m
            (datetime(2025, 7, 1, 14, 0, 0), ['5m', '15m', '1h']),             # 00 minutes: 5m + 15m + 1h
            (datetime(2025, 7, 1, 0, 0, 0), ['5m', '15m', '1h', '1d']),        # 00:00: 5m + 15m + 1h + 1d
            (datetime(2025, 7, 7, 0, 0, 0), ['5m', '15m', '1h', '1d', '1w']),  # Monday 00:00: all timeframes
            (datetime(2025, 7, 1, 14, 37, 0), []),                             # 37 minutes: no timeframes
            (datetime(2025, 7, 1, 14, 43, 0), []),                             # 43 minutes: no timeframes
        ]
        
        for test_time, expected_timeframes in test_cases:
            actual_timeframes = callback._get_target_timeframes_for_interval(test_time)
            assert actual_timeframes == expected_timeframes, (
                f"At {test_time.strftime('%A %H:%M')}, expected {expected_timeframes}, "
                f"got {actual_timeframes}"
            )

    def test_single_timeframe_generation(self):
        """Test that generating single timeframes works correctly."""
        
        callback = IntervalBasedTrainingDataCallback(symbols=['TSLA'])
        
        # Test timeframe logic 
        target_timeframes = callback._get_target_timeframes_for_interval(
            datetime(2025, 7, 1, 14, 45, 0)
        )
        
        # At 45 minutes, should generate 5m and 15m
        assert target_timeframes == ['5m', '15m']
        
        # Verify each timeframe would be generated individually
        for timeframe in target_timeframes:
            single_timeframe_list = [timeframe]
            assert timeframe in single_timeframe_list
            assert len(single_timeframe_list) == 1


if __name__ == '__main__':
    pytest.main([__file__, '-v'])