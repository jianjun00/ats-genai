"""
Comprehensive tests for generate_base_features logic.

Tests all the critical issues:
1. Historical data amount and timing
2. Future leakage prevention  
3. Timeframe awareness
4. Configurable lookback periods
5. Error handling
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
import numpy as np

from domains.ml.services.training_data.timeseries_sequence_training_generator import TimeSeriesSequenceTrainingGenerator, TrainingDataConfig
from core.platform.config.environment import Environment


class TestGenerateBaseFeaturesComprehensive:
    """Comprehensive tests for generate_base_features method."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock config with realistic feature requirements."""
        config = MagicMock(spec=TrainingDataConfig)
        config.feature_types = ['ohlcv', 'technical', 'indicators', 'support_resistance']
        config.signal_names = ['sma_20', 'ema_12', 'rsi_14', 'macd_line']
        config.base_interval_minutes = 5
        config.training_interval_minutes = 60
        return config

    @pytest.fixture
    def mock_universe_manager(self):
        """Create a mock universe manager."""
        return MagicMock()

    @pytest.fixture
    def generator(self, mock_config, mock_universe_manager):
        """Create generator instance for testing."""
        env = MagicMock(spec=Environment)
        generator = TimeSeriesSequenceTrainingGenerator(
            env=env,
            config=mock_config,
            universe_manager=mock_universe_manager
        )
        return generator

    @pytest.fixture
    def sample_historical_data(self):
        """Create realistic historical OHLCV data for testing."""
        # Generate 50 periods of 5-minute data (4+ hours of history)
        start_time = datetime(2025, 7, 1, 9, 30)  # Market open
        periods = 50
        
        dates = []
        for i in range(periods):
            dates.append(start_time + timedelta(minutes=5 * i))
            
        np.random.seed(42)  # Reproducible data
        base_price = 200.0
        
        data = []
        for i, dt in enumerate(dates):
            # Create realistic OHLCV with some trend
            price_drift = i * 0.01  # Small upward trend
            noise = np.random.normal(0, 0.5)
            
            open_price = base_price + price_drift + noise
            close_price = open_price + np.random.normal(0, 0.3)
            high_price = max(open_price, close_price) + abs(np.random.normal(0, 0.2))
            low_price = min(open_price, close_price) - abs(np.random.normal(0, 0.2))
            volume = 1000000 + np.random.randint(-100000, 100000)
            
            data.append({
                'timestamp': dt,
                'open': open_price,
                'high': high_price, 
                'low': low_price,
                'close': close_price,
                'volume': volume,
                'date': dt.date()
            })
            
        return pd.DataFrame(data)

    def test_historical_data_amount_is_configurable(self, generator, sample_historical_data):
        """Test that historical data amount is based on feature requirements, not hardcoded."""
        instrument_id = 31
        prediction_timestamp = datetime(2025, 7, 1, 13, 30)  # 1:30 PM
        
        # Mock universe manager to return our sample data
        generator.universe_manager.get_lag_prices.return_value = sample_historical_data
        
        # Mock feature extractor
        generator.feature_extractor = MagicMock()
        generator.feature_extractor.extract_all_features.return_value = {'test_feature': 1.0}
        
        # Call generate_base_features
        result = generator.generate_base_features(instrument_id, prediction_timestamp)
        
        # Verify get_lag_prices was called with proper parameters
        generator.universe_manager.get_lag_prices.assert_called_once()
        call_args = generator.universe_manager.get_lag_prices.call_args
        
        # Check that lookback period is reasonable for technical indicators
        lookback_periods = call_args[0][2]  # Third argument is lookback count
        assert lookback_periods >= 20, f"Lookback periods ({lookback_periods}) should be >= 20 for technical indicators like SMA(20)"
        assert lookback_periods <= 100, f"Lookback periods ({lookback_periods}) should be reasonable (<=100)"

    def test_no_future_leakage_strict_timing(self, generator, sample_historical_data):
        """Test that historical data strictly ends BEFORE prediction timestamp."""
        instrument_id = 31
        prediction_timestamp = datetime(2025, 7, 1, 13, 30)  # 1:30 PM
        
        # Create data that includes the prediction timestamp (should be filtered out)
        future_data = sample_historical_data.copy()
        future_row = {
            'timestamp': prediction_timestamp,  # This should NOT be included
            'open': 210.0, 'high': 211.0, 'low': 209.0, 'close': 210.5,
            'volume': 1000000, 'date': prediction_timestamp.date()
        }
        future_data = pd.concat([future_data, pd.DataFrame([future_row])], ignore_index=True)
        
        generator.universe_manager.get_lag_prices.return_value = future_data
        generator.feature_extractor = MagicMock()
        generator.feature_extractor.extract_all_features.return_value = {'test_feature': 1.0}
        
        # Call generate_base_features
        result = generator.generate_base_features(instrument_id, prediction_timestamp)
        
        # Verify that feature extractor received data without future leakage
        feature_call_args = generator.feature_extractor.extract_all_features.call_args
        historical_data = feature_call_args[0][0]  # First argument is the DataFrame
        
        # All timestamps should be BEFORE prediction_timestamp
        if not historical_data.empty:
            latest_timestamp = historical_data['timestamp'].max()
            assert latest_timestamp < prediction_timestamp, \
                f"Historical data includes future timestamp {latest_timestamp} >= {prediction_timestamp}"

    def test_uses_prediction_timestamp_not_date(self, generator, sample_historical_data):
        """Test that it uses precise prediction_timestamp, not just the date."""
        from datetime import datetime as dt, date
        
        instrument_id = 31
        prediction_timestamp = datetime(2025, 7, 1, 13, 30, 0)  # 1:30 PM exactly
        
        generator.universe_manager.get_lag_prices.return_value = sample_historical_data
        generator.feature_extractor = MagicMock()
        generator.feature_extractor.extract_all_features.return_value = {'test_feature': 1.0}
        
        # Call generate_base_features
        result = generator.generate_base_features(instrument_id, prediction_timestamp)
        
        # Verify get_lag_prices was called with timestamp-aware parameters
        call_args = generator.universe_manager.get_lag_prices.call_args
        
        # Should use timestamp with time precision, not just date
        # The exact parameter depends on implementation, but it should include time information
        timestamp_param = call_args[0][1]  # Second argument
        
        # Verify it's a datetime object (not just date) and has time precision
        assert isinstance(timestamp_param, dt), \
            f"Should pass datetime object, got {type(timestamp_param)}"
        assert timestamp_param == prediction_timestamp, \
            f"Should pass exact prediction_timestamp {prediction_timestamp}, got {timestamp_param}"

    def test_handles_insufficient_historical_data(self, generator):
        """Test graceful handling when insufficient historical data is available."""
        instrument_id = 31
        prediction_timestamp = datetime(2025, 7, 1, 13, 30)
        
        # Return minimal data (less than required for technical indicators)
        minimal_data = pd.DataFrame([
            {'timestamp': datetime(2025, 7, 1, 13, 25), 'open': 200, 'high': 201, 'low': 199, 'close': 200.5, 'volume': 1000, 'date': datetime(2025, 7, 1).date()}
        ])
        
        generator.universe_manager.get_lag_prices.return_value = minimal_data
        generator.feature_extractor = MagicMock()
        generator.feature_extractor.extract_all_features.return_value = {}  # Empty features due to insufficient data
        
        # Should not crash and should return empty or default features
        result = generator.generate_base_features(instrument_id, prediction_timestamp)
        
        assert isinstance(result, dict), "Should return dict even with insufficient data"
        # Could be empty dict or dict with default values, but should not crash

    def test_handles_no_historical_data(self, generator):
        """Test graceful handling when no historical data is available."""
        instrument_id = 31
        prediction_timestamp = datetime(2025, 7, 1, 13, 30)
        
        # Return empty DataFrame
        generator.universe_manager.get_lag_prices.return_value = pd.DataFrame()
        generator.feature_extractor = MagicMock()
        generator.feature_extractor.extract_all_features.return_value = {}
        
        # Should not crash
        result = generator.generate_base_features(instrument_id, prediction_timestamp)
        
        assert isinstance(result, dict), "Should return dict even with no data"

    def test_feature_extractor_called_with_base_timeframe(self, generator, sample_historical_data):
        """Test that feature extractor is called with 'base' timeframe identifier."""
        instrument_id = 31
        prediction_timestamp = datetime(2025, 7, 1, 13, 30)
        
        generator.universe_manager.get_lag_prices.return_value = sample_historical_data
        generator.feature_extractor = MagicMock()
        generator.feature_extractor.extract_all_features.return_value = {'base_feature': 1.0}
        
        result = generator.generate_base_features(instrument_id, prediction_timestamp)
        
        # Verify feature extractor called with 'base' timeframe
        generator.feature_extractor.extract_all_features.assert_called_once()
        call_args = generator.feature_extractor.extract_all_features.call_args
        timeframe_param = call_args[0][1]  # Second argument should be timeframe
        assert timeframe_param == 'base', f"Should call feature extractor with 'base' timeframe, got '{timeframe_param}'"

    def test_returns_feature_dictionary(self, generator, sample_historical_data):
        """Test that it returns a proper feature dictionary."""
        instrument_id = 31
        prediction_timestamp = datetime(2025, 7, 1, 13, 30)
        
        generator.universe_manager.get_lag_prices.return_value = sample_historical_data
        generator.feature_extractor = MagicMock()
        expected_features = {
            'base_sma_20': 200.5,
            'base_ema_12': 201.0,
            'base_rsi_14': 0.6,
            'base_volume_avg': 1000000.0
        }
        generator.feature_extractor.extract_all_features.return_value = expected_features
        
        result = generator.generate_base_features(instrument_id, prediction_timestamp)
        
        assert isinstance(result, dict), "Should return dictionary"
        assert result == expected_features, "Should return features from feature extractor"

    def test_lookback_period_calculation_logic(self, generator, sample_historical_data):
        """Test that lookback period is calculated based on actual feature requirements."""
        instrument_id = 31
        prediction_timestamp = datetime(2025, 7, 1, 13, 30)
        
        # Mock config with specific signal requirements
        generator.config.signal_names = ['sma_20', 'sma_50', 'ema_12', 'rsi_14']
        
        generator.universe_manager.get_lag_prices.return_value = sample_historical_data
        generator.feature_extractor = MagicMock()
        generator.feature_extractor.extract_all_features.return_value = {'test': 1.0}
        
        result = generator.generate_base_features(instrument_id, prediction_timestamp)
        
        call_args = generator.universe_manager.get_lag_prices.call_args
        lookback_periods = call_args[0][2]
        
        # Should be at least as much as the largest indicator period (SMA_50 = 50)
        # Plus some buffer for calculation
        assert lookback_periods >= 50, f"Should request at least 50 periods for SMA_50, got {lookback_periods}"

    def test_error_handling_universe_manager_failure(self, generator):
        """Test graceful error handling when universe manager fails."""
        instrument_id = 31
        prediction_timestamp = datetime(2025, 7, 1, 13, 30)
        
        # Make universe manager raise an exception
        generator.universe_manager.get_lag_prices.side_effect = Exception("Database connection failed")
        
        # Should handle the exception gracefully
        result = generator.generate_base_features(instrument_id, prediction_timestamp)
        
        # Should return empty dict or some default, not crash
        assert isinstance(result, dict), "Should return dict even when universe manager fails"

    def test_error_handling_feature_extractor_failure(self, generator, sample_historical_data):
        """Test graceful error handling when feature extractor fails."""
        instrument_id = 31
        prediction_timestamp = datetime(2025, 7, 1, 13, 30)
        
        generator.universe_manager.get_lag_prices.return_value = sample_historical_data
        generator.feature_extractor = MagicMock()
        generator.feature_extractor.extract_all_features.side_effect = Exception("Feature calculation failed")
        
        # Should handle the exception gracefully
        result = generator.generate_base_features(instrument_id, prediction_timestamp)
        
        # Should return empty dict or some default, not crash
        assert isinstance(result, dict), "Should return dict even when feature extractor fails"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])