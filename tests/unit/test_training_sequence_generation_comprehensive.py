"""
Comprehensive unit tests for training sequence generation.

Tests the full training sequence generation pipeline using the same setup
as the actual training data generation to validate that sequence generation
works correctly and that "Sequences generated: 0" issues are properly diagnosed.
"""

import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import MagicMock, AsyncMock
import pandas as pd
import numpy as np

from domains.ml.services.training_data.timeseries_sequence_training_generator import TimeSeriesSequenceTrainingGenerator
from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from domains.trading.services.state.universe_state_manager import UniverseStateManager
from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
from domains.trading.services.state.universe_state import UniverseStateInterval, InstrumentInterval
from core.business.calendars.time_duration import TimeDuration
from shared.data_handling.utils.environment import Environment


@pytest.fixture
def mock_environment():
    """Create a mock environment for testing."""
    env = MagicMock(spec=Environment)
    env.get_database_url.return_value = "postgresql://test:test@localhost:5432/test_db"
    return env


@pytest.fixture
def sample_ohlcv_data():
    """Create sample OHLCV data for testing."""
    dates = pd.date_range('2025-07-01 09:30:00', periods=100, freq='1min', tz='America/New_York')
    np.random.seed(42)  # For reproducible tests
    
    data = pd.DataFrame({
        'timestamp': dates,
        'open': 200 + np.random.randn(100) * 0.5,
        'high': 200 + np.random.randn(100) * 0.5 + 0.5,
        'low': 200 + np.random.randn(100) * 0.5 - 0.5,
        'close': 200 + np.random.randn(100) * 0.5,
        'volume': 1000 + np.random.randint(-100, 100, 100),
        'date': dates.date
    })
    
    # Ensure high >= open,close and low <= open,close
    data['high'] = data[['open', 'close']].max(axis=1) + abs(np.random.randn(100) * 0.1)
    data['low'] = data[['open', 'close']].min(axis=1) - abs(np.random.randn(100) * 0.1)
    
    return data


@pytest.fixture
def mock_universe_state_interval():
    """Create a mock UniverseStateInterval with realistic instrument data."""
    def _create_interval(timeframe: str, timestamp: datetime, instrument_id: int = 31):
        # Create instrument interval with realistic OHLCV data
        instrument_interval = InstrumentInterval(
            instrument_id=instrument_id,
            open=200.0 + np.random.randn() * 0.5,
            high=200.5 + np.random.randn() * 0.5,  
            low=199.5 + np.random.randn() * 0.5,
            close=200.0 + np.random.randn() * 0.5,
            volume=1000 + np.random.randint(-100, 100),
            timestamp=timestamp,
            vwap=200.0 + np.random.randn() * 0.5
        )
        
        # Create universe state interval
        universe_interval = UniverseStateInterval(
            timeframe=timeframe,
            timestamp=timestamp,
            instrument_intervals={instrument_id: instrument_interval}
        )
        
        return universe_interval
    
    return _create_interval


@pytest.fixture
def mock_universe_manager(mock_universe_state_interval):
    """Create a mock UniverseStateManager that returns realistic data."""
    manager = MagicMock(spec=UniverseStateManager)
    
    def get_universe_state_interval(timeframe: str, current_time: datetime):
        """Return universe state interval for current time."""
        return mock_universe_state_interval(timeframe, current_time)
    
    def get_future_universe_state_interval(timeframe: str, current_time: datetime, lead_periods: int = 1):
        """Return future universe state interval.""" 
        future_time = current_time + timedelta(minutes=5 * lead_periods)  # 5min lead
        return mock_universe_state_interval(timeframe, future_time)
    
    manager.get_universe_state_interval.side_effect = get_universe_state_interval
    manager.get_future_universe_state_interval.side_effect = get_future_universe_state_interval
    
    return manager


class TestTrainingSequenceGenerationComprehensive:
    """Comprehensive tests for training sequence generation pipeline."""
    
    @pytest.mark.asyncio
    async def test_training_generator_with_all_timeframes(self, mock_environment, mock_universe_manager):
        """Test that training generator can generate sequences when all timeframes have data."""
        # Create training generator with same config as production
        class MockConfig:
            def __init__(self):
                self.feature_types = ['ohlcv', 'returns', 'volatility', 'volume_profile', 'technical', 'indicators', 'support_resistance', 'market_structure'] 
                self.signal_names = ['etop', 'ebot', 'pldot', 'envelope_top', 'envelope_bot', 'z1b', 'z2b', 'z5t', 'z6t', 'sma_20', 'ema_12', 'rsi_14', 'macd_line', 'macd_signal', 'bb_upper', 'bb_lower', 'bb_middle']
                self.base_interval_minutes = 1
                self.training_interval_minutes = 60
        
        config = MockConfig()
        
        generator = TimeSeriesSequenceTrainingGenerator(config=config)
        generator.universe_manager = mock_universe_manager
        
        # Test prediction timestamp that should have all timeframes
        prediction_timestamp = datetime(2025, 7, 1, 14, 0, 0)  # 14:00 - hour boundary, should process all timeframes
        
        # Generate training example
        example = await generator.generate_training_example(
            symbol='AAPL',
            prediction_timestamp=prediction_timestamp,
            target_timeframes=['5m', '15m', '60m', '1d']
        )
        
        # Validate results
        assert example is not None, "Training example should not be None when universe data exists"
        assert 'timeframe_features' in example
        assert 'prediction_targets' in example
        assert example['symbol'] == 'AAPL'
        assert example['prediction_timestamp'] == prediction_timestamp
        
        # Check that timeframe features were generated
        timeframe_features = example['timeframe_features']
        assert isinstance(timeframe_features, dict)
        
        # At least some timeframes should have features (even if not all due to timing constraints)
        assert len(timeframe_features) > 0, "Should have generated features for at least one timeframe"
        
        # Check prediction targets
        prediction_targets = example['prediction_targets']
        assert isinstance(prediction_targets, dict)
        assert len(prediction_targets) > 0, "Should have generated prediction targets for at least one timeframe"
    
    @pytest.mark.asyncio  
    async def test_training_generator_with_partial_timeframes(self, mock_environment, mock_universe_manager):
        """Test training generator when only some timeframes have data (realistic scenario).""" 
        class MockConfig:
            def __init__(self):
                self.feature_types = ['ohlcv', 'support_resistance'] 
                self.signal_names = ['sma_20', 'ema_12']
                self.base_interval_minutes = 1
                self.training_interval_minutes = 60
        
        config = MockConfig()
        
        generator = TimeSeriesSequenceTrainingGenerator(config=config)
        generator.universe_manager = mock_universe_manager
        
        # Test prediction timestamp that should only have 5m data (like 13:35 scenario)
        prediction_timestamp = datetime(2025, 7, 1, 13, 35, 0)  # 13:35 - only 5m should be available
        
        # Generate training example for only 5m timeframe
        example = await generator.generate_training_example(
            symbol='AAPL',
            prediction_timestamp=prediction_timestamp,
            target_timeframes=['5m']  # Only request the timeframe that should be available
        )
        
        # Validate results  
        assert example is not None, "Training example should not be None even with limited timeframes"
        assert 'timeframe_features' in example
        assert example['symbol'] == 'AAPL'
        
        timeframe_features = example['timeframe_features']
        assert '5m' in timeframe_features, "5m timeframe should be present"
        assert len(timeframe_features['5m']) > 0, "5m timeframe should have features"
    
    def test_universe_state_builder_timeframe_filtering(self):
        """Test that UniverseStateBuilder correctly filters timeframes based on time."""
        env = MagicMock(spec=Environment)
        
        builder = UniverseStateIntervalBuilder(
            env=env,
            base_duration='5m',
            target_durations='5m,15m,60m,1d'
        )
        
        # Test different timestamps to verify timeframe filtering logic
        test_cases = [
            # (timestamp, expected_timeframes)
            (datetime(2025, 7, 1, 13, 35, 0), ['5m']),  # Only 5m at 35 minutes
            (datetime(2025, 7, 1, 13, 30, 0), ['5m', '15m']),  # 5m + 15m at 30 minutes  
            (datetime(2025, 7, 1, 14, 0, 0), ['5m', '15m', '60m']),  # 5m + 15m + 60m at hour boundary
            (datetime(2025, 7, 1, 16, 0, 0), ['5m', '15m', '60m', '1d']),  # All at market close (4 PM)
        ]
        
        for timestamp, expected_timeframes in test_cases:
            processed_timeframes = []
            
            for duration in builder.target_durations:
                if builder._should_process_timeframe(duration, timestamp):
                    processed_timeframes.append(duration.get_duration_string())
            
            assert processed_timeframes == expected_timeframes, f"At {timestamp}, expected {expected_timeframes}, got {processed_timeframes}"
    
    @pytest.mark.asyncio
    async def test_interval_based_callback_single_timeframe_generation(self, mock_environment, mock_universe_manager, sample_ohlcv_data):
        """Test IntervalBasedTrainingDataCallback generates sequences for single timeframes correctly."""
        class MockConfig:
            feature_types = ['ohlcv', 'support_resistance']
            signal_names = ['sma_20']
            
        # Create callback with realistic configuration
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            output_dir='/tmp/test_training_data',
            storage_format='arrayrecord'
        )
        
        # Mock the training generator
        callback.training_generator = MagicMock()
        callback.training_generator.generate_training_example = AsyncMock(return_value={
            'instrument_id': 31,
            'symbol': 'AAPL', 
            'prediction_timestamp': datetime(2025, 7, 1, 13, 35, 0),
            'base_features': {'base_feature_1': 0.5},
            'timeframe_features': {'5m': {'ohlc_feature_1': 200.0, 'support_resistance_1': 0.8}},
            'prediction_targets': {'5m': {'target_return_1': 0.01}}
        })
        
        # Mock the file operations
        callback._save_simple_arrayrecord = MagicMock()
        
        # Mock runner
        mock_runner = MagicMock()
        mock_runner.universe_manager = mock_universe_manager
        
        # Test handleInterval for 13:35 (should generate 5m timeframe only)
        current_time = datetime(2025, 7, 1, 13, 35, 0)
        
        await callback.handleInterval(mock_runner, current_time)
        
        # Verify training example was generated
        callback.training_generator.generate_training_example.assert_called_once()
        call_args = callback.training_generator.generate_training_example.call_args
        
        assert call_args[1]['symbol'] == 'AAPL'
        assert call_args[1]['prediction_timestamp'] == current_time
        assert call_args[1]['target_timeframes'] == ['5m']  # Only 5m at 13:35
        
        # Verify arrayrecord was saved
        callback._save_simple_arrayrecord.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_training_sequence_generation_end_to_end(self, mock_environment, sample_ohlcv_data):
        """End-to-end test of training sequence generation pipeline."""
        # Create all components as they would be in production
        env = mock_environment
        
        # Create UniverseStateBuilder with production config
        builder = UniverseStateIntervalBuilder(
            env=env,
            base_duration='5m', 
            target_durations='5m,15m,60m,1d'
        )
        
        # Create UniverseStateManager
        universe_manager = MagicMock(spec=UniverseStateManager)
        
        # Create realistic UniverseStateInterval data
        def create_mock_interval(timeframe: str, timestamp: datetime):
            instrument_interval = InstrumentInterval(
                instrument_id=31,
                open=200.0,
                high=200.5,
                low=199.5,
                close=200.2,
                volume=1000,
                timestamp=timestamp,
                vwap=200.1
            )
            
            return UniverseStateInterval(
                timeframe=timeframe,
                timestamp=timestamp,
                instrument_intervals={31: instrument_interval}
            )
        
        universe_manager.get_universe_state_interval.side_effect = lambda tf, ts: create_mock_interval(tf, ts)
        universe_manager.get_future_universe_state_interval.side_effect = lambda tf, ts, lp: create_mock_interval(tf, ts + timedelta(minutes=5))
        
        # Create TrainingGenerator
        class MockConfig:
            feature_types = ['ohlcv', 'support_resistance']
            signal_names = ['sma_20', 'ema_12']
            base_interval_minutes = 1
            training_interval_minutes = 60
            
        generator = TimeSeriesSequenceTrainingGenerator(
            config=MockConfig(),
            universe_manager=universe_manager,
            timeframes=['5m', '15m', '60m', '1d']
        )
        
        # Create Callback
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            output_dir='/tmp/test_training_data',
            storage_format='arrayrecord'
        )
        callback.training_generator = generator
        
        # Mock file operations
        callback._save_simple_arrayrecord = MagicMock()
        
        # Test the full pipeline
        mock_runner = MagicMock()
        mock_runner.universe_manager = MagicMock()
        mock_runner.universe_manager.instrument_ids = [31]
        
        # Test at 13:35 - should generate 5m sequences
        current_time = datetime(2025, 7, 1, 13, 35, 0)
        
        await callback.handleInterval(mock_runner, current_time)
        
        # Verify sequences were generated and saved
        assert callback._save_simple_arrayrecord.call_count > 0, "Should have saved training sequences"
        
        # Get the saved example
        save_call = callback._save_simple_arrayrecord.call_args
        example = save_call[0][0]  # First argument to _save_simple_arrayrecord
        
        # Validate the generated training sequence
        assert example is not None, "Training example should not be None"
        assert example['symbol'] == 'AAPL'
        assert example['prediction_timestamp'] == current_time
        assert 'timeframe_features' in example
        assert 'prediction_targets' in example
        
        # Verify timeframe features are present for available timeframes
        timeframe_features = example['timeframe_features']
        assert len(timeframe_features) > 0, "Should have timeframe features"
        
        # At 13:35, only 5m should be available, but the sequence should still be valid
        print(f"✅ End-to-end test successful: Generated {len(timeframe_features)} timeframe features")
        print(f"   Available timeframes: {list(timeframe_features.keys())}")
        print(f"   This demonstrates sequences CAN be generated when universe data is available")


if __name__ == '__main__':
    pytest.main([__file__, '-v'])