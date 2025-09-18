"""
Unit tests for refactored training data callback methods.

These tests validate the QR4-compliant feature extraction and ArrayRecord generation.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

# Import the callback class we're testing
from domains.ml.legacy.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from domains.ml.legacy.training_data.timeseries_sequence_training_generator import TrainingDataConfig


class TestTrainingDataCallbackRefactored:
    """Test suite for refactored training data callback methods."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock TrainingDataConfig for testing."""
        config = MagicMock(spec=TrainingDataConfig)
        config.timeframes = {'5m': 5, '15m': 15, '1h': 60, '1d': 1440}
        # No sequence_lengths - single-step approach
        return config

    @pytest.fixture
    def callback_instance(self, mock_config):
        """Create an IntervalBasedTrainingDataCallback instance for testing."""
        with patch('ml.training_data.callbacks.training_data_callback.ray'):
            callback = IntervalBasedTrainingDataCallback(
                symbols=['AAPL', 'TSLA'],
                config=mock_config,
                output_dir='/tmp/test_training'
            )
        return callback

    def test_timeframe_to_minutes_conversion(self, callback_instance):
        """Test _timeframe_to_minutes method converts correctly."""
        # Test standard timeframes
        assert callback_instance._timeframe_to_minutes('1m') == 1
        assert callback_instance._timeframe_to_minutes('5m') == 5
        assert callback_instance._timeframe_to_minutes('15m') == 15
        assert callback_instance._timeframe_to_minutes('1h') == 60
        assert callback_instance._timeframe_to_minutes('1d') == 1440
        assert callback_instance._timeframe_to_minutes('1w') == 10080

        # Test unknown timeframe defaults to 60
        assert callback_instance._timeframe_to_minutes('unknown') == 60

    def test_extract_timeframe_features_success(self, callback_instance):
        """Test _extract_timeframe_features extracts scalar features correctly."""
        # Create mock DataFrame with OHLCV data
        test_data = pd.DataFrame({
            'timestamp': pd.date_range('2025-01-01', periods=25, freq='5min'),
            'open': range(100, 125),
            'high': range(105, 130),
            'low': range(95, 120),
            'close': range(102, 127),
            'volume': range(1000, 1025),
            'vwap': range(101, 126),
            'sma_20': range(103, 128),
            'rsi_14': [50.0] * 25
        })

        signals = ['sma_20', 'rsi_14', 'vwap']

        # Test extraction for 5m timeframe (latest single bar)
        features = callback_instance._extract_timeframe_features('5m', test_data, signals, 'AAPL')

        # Verify base features are extracted as scalars
        assert 'open' in features
        assert 'high' in features
        assert 'low' in features
        assert 'close' in features
        assert 'volume' in features
        assert 'vwap' in features

        # Verify values are scalars (not lists)
        assert isinstance(features['open'], float)
        assert features['open'] == 124.0  # Latest value
        assert features['close'] == 126.0  # Latest value
        assert features['volume'] == 1024.0  # Latest value

        # Verify signals are included as scalars
        assert 'sma_20' in features
        assert 'rsi_14' in features
        assert isinstance(features['sma_20'], float)
        assert features['sma_20'] == 127.0

    def test_extract_timeframe_features_insufficient_data(self, callback_instance):
        """Test _extract_timeframe_features handles insufficient data."""
        # Create empty DataFrame
        test_data = pd.DataFrame({
            'timestamp': pd.DatetimeIndex([]),
            'open': [],
            'high': [],
            'low': [],
            'close': [],
            'volume': [],
        })

        # Should return empty dict for no data
        features = callback_instance._extract_timeframe_features('5m', test_data, [], 'AAPL')
        assert features == {}

    def test_extract_timeframe_features_missing_columns(self, callback_instance):
        """Test _extract_timeframe_features handles missing columns gracefully."""
        # Create DataFrame missing some base features
        test_data = pd.DataFrame({
            'timestamp': pd.date_range('2025-01-01', periods=25, freq='5min'),
            'open': range(100, 125),
            'high': range(105, 130),
            # Missing 'low' column
            'close': range(102, 127),
            'volume': range(1000, 1025),
        })

        features = callback_instance._extract_timeframe_features('5m', test_data, [], 'AAPL')

        # Should have features that exist as scalars
        assert 'open' in features
        assert 'high' in features
        assert 'close' in features
        assert 'volume' in features

        # Should have missing feature as 0.0
        assert 'low' in features
        assert features['low'] == 0.0

        # Should have vwap as close (fallback)
        assert 'vwap' in features
        assert features['vwap'] == features['close']

    def test_convert_scalar_to_qr4_row_success(self, callback_instance):
        """Test _convert_scalar_to_qr4_row converts scalar features correctly."""
        # Create example with scalar features
        example = {
            'symbol': 'AAPL',
            'timestamp': '2025-01-01T10:00:00',
            'features': {
                'open': 100.0,
                'high': 105.0,
                'low': 95.0,
                'close': 103.0,
                'volume': 1000.0,
                'vwap': 102.0,
                # Non-QR4 feature should be ignored
                'some_signal': 50.0
            }
        }

        qr4_row = callback_instance._convert_scalar_to_qr4_row(example, 'AAPL', '5m')

        # Should create single row (scalar approach)
        assert isinstance(qr4_row, dict)

        # Verify row structure
        assert qr4_row['timestamp'] == '2025-01-01T10:00:00'
        assert qr4_row['symbol'] == 'AAPL'
        assert qr4_row['open'] == 100.0
        assert qr4_row['high'] == 105.0
        assert qr4_row['low'] == 95.0
        assert qr4_row['close'] == 103.0
        assert qr4_row['volume'] == 1000.0
        assert qr4_row['vwap'] == 102.0

        # Verify non-QR4 features are not included
        assert 'some_signal' not in qr4_row

    def test_convert_scalar_to_qr4_row_missing_features(self, callback_instance):
        """Test _convert_scalar_to_qr4_row handles missing QR4 features."""
        # Create example missing some QR4 features
        example = {
            'symbol': 'AAPL',
            'timestamp': '2025-01-01T10:00:00',
            'features': {
                'open': 100.0,
                'high': 105.0,
                # Missing 'low', 'close', 'volume', 'vwap'
            }
        }

        qr4_row = callback_instance._convert_scalar_to_qr4_row(example, 'AAPL', '5m')

        # Should create single row
        assert isinstance(qr4_row, dict)

        # Missing features should be filled with 0.0
        assert qr4_row['open'] == 100.0
        assert qr4_row['high'] == 105.0
        assert qr4_row['low'] == 0.0  # Missing, filled with 0
        assert qr4_row['close'] == 0.0  # Missing, filled with 0
        assert qr4_row['volume'] == 0.0  # Missing, filled with 0
        assert qr4_row['vwap'] == 0.0  # Missing, filled with 0

    def test_convert_scalar_to_qr4_row_empty_features(self, callback_instance):
        """Test _convert_scalar_to_qr4_row handles empty features."""
        example = {
            'symbol': 'AAPL',
            'timestamp': '2025-01-01T10:00:00',
            'features': {}
        }

        qr4_row = callback_instance._convert_scalar_to_qr4_row(example, 'AAPL', '5m')

        # Should create row with all zeros for missing features
        assert isinstance(qr4_row, dict)
        assert qr4_row['symbol'] == 'AAPL'
        assert qr4_row['timestamp'] == '2025-01-01T10:00:00'
        assert qr4_row['open'] == 0.0
        assert qr4_row['high'] == 0.0
        assert qr4_row['low'] == 0.0
        assert qr4_row['close'] == 0.0
        assert qr4_row['volume'] == 0.0
        assert qr4_row['vwap'] == 0.0

    def test_convert_sequence_to_qr4_rows_uneven_sequences(self, callback_instance):
        """Test _convert_sequence_to_qr4_rows handles uneven sequence lengths."""
        # Create example with list values (legacy format support)
        example = {
            'symbol': 'AAPL',
            'timestamp': '2025-01-01T10:00:00',
            'features': {
                'open': [100.0, 101.0, 102.0],  # Should take last value
                'high': 105.0,                  # Already scalar
                'low': [95.0, 96.0],           # Should take last value
                'close': 103.0,                # Already scalar
                'volume': [1000.0, 1100.0, 1200.0],  # Should take last value
                'vwap': []                     # Empty list should be 0.0
            }
        }

        qr4_row = callback_instance._convert_scalar_to_qr4_row(example, 'AAPL', '5m')

        # Should create single row with last values from lists
        assert isinstance(qr4_row, dict)
        assert qr4_row['open'] == 102.0      # Last value from list
        assert qr4_row['high'] == 105.0     # Scalar value
        assert qr4_row['low'] == 96.0       # Last value from list
        assert qr4_row['close'] == 103.0    # Scalar value
        assert qr4_row['volume'] == 1200.0  # Last value from list
        assert qr4_row['vwap'] == 0.0       # Empty list becomes 0.0


class TestQR4ComplianceValidation:
    """Test QR4 compliance requirements are met."""

    def test_qr4_base_feature_names_only(self):
        """Test that only QR4 base feature names are used."""
        qr4_base_features = ['open', 'high', 'low', 'close', 'volume', 'vwap']

        # This should match the list in _convert_scalar_to_qr4_row
        with patch('ml.training_data.callbacks.training_data_callback.ray'):
            callback = IntervalBasedTrainingDataCallback([], None, '/tmp')

        # Test a mock example
        example = {
            'features': {
                'open': 100.0,
                'high': 105.0,
                'low': 95.0,
                'close': 103.0,
                'volume': 1000.0,
                'vwap': 102.0,
                'non_qr4_feature': 50.0  # Should be ignored
            }
        }

        qr4_row = callback._convert_scalar_to_qr4_row(example, 'TEST', '5m')

        row_keys = set(qr4_row.keys())
        expected_keys = set(['timestamp', 'symbol'] + qr4_base_features)

        # Should only contain QR4 compliant keys
        assert row_keys <= expected_keys  # row_keys is subset of expected_keys
        assert 'non_qr4_feature' not in row_keys

    def test_qr4_no_indexed_features(self):
        """Test that QR4 row doesn't contain indexed features like open_000."""
        with patch('ml.training_data.callbacks.training_data_callback.ray'):
            callback = IntervalBasedTrainingDataCallback([], None, '/tmp')

        example = {
            'features': {
                'open': 100.0,
                'close': 103.0
            }
        }

        qr4_row = callback._convert_scalar_to_qr4_row(example, 'TEST', '5m')

        # Check that row doesn't contain indexed features
        for key in qr4_row.keys():
            assert '_000' not in key
            assert '_001' not in key
            assert '_002' not in key
            # Only base names should be present
            if key not in ['timestamp', 'symbol']:
                assert key in ['open', 'high', 'low', 'close', 'volume', 'vwap']

    def test_qr4_scalar_values_only(self):
        """Test that QR4 row contains only scalar values, not lists."""
        with patch('ml.training_data.callbacks.training_data_callback.ray'):
            callback = IntervalBasedTrainingDataCallback([], None, '/tmp')

        example = {
            'features': {
                'open': 100.0,
                'close': 103.0
            }
        }

        qr4_row = callback._convert_scalar_to_qr4_row(example, 'TEST', '5m')

        # Row should contain only scalar values
        for key, value in qr4_row.items():
            assert not isinstance(value, list), f"Feature {key} should be scalar, got {type(value)}"
            if key not in ['timestamp', 'symbol']:
                assert isinstance(value, (int, float)), f"Numeric feature {key} should be int/float, got {type(value)}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])