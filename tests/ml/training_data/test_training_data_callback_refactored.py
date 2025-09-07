"""
Unit tests for refactored training data callback methods.

These tests validate the QR4-compliant feature extraction and ArrayRecord generation.
"""

import pytest
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List
from unittest.mock import MagicMock, patch

# Import the callback class we're testing
from ml.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from ml.training_data.timeseries_sequence_training_generator import TrainingDataConfig


class TestTrainingDataCallbackRefactored:
    """Test suite for refactored training data callback methods."""
    
    @pytest.fixture
    def mock_config(self):
        """Create a mock TrainingDataConfig for testing."""
        config = MagicMock(spec=TrainingDataConfig)
        config.timeframes = {'5m': 5, '15m': 15, '1h': 60, '1d': 1440}
        config.sequence_lengths = {'5m': 12, '15m': 16, '1h': 24, '1d': 20}
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
        """Test _extract_timeframe_features extracts features correctly."""
        # Create mock DataFrame with OHLCV data
        test_data = pd.DataFrame({
            'timestamp': pd.date_range('2025-01-01', periods=25, freq='5T'),
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
        
        # Test extraction for 5m timeframe (sequence_length = 12)
        features = callback_instance._extract_timeframe_features('5m', test_data, signals, 'AAPL')
        
        # Verify base features are extracted
        assert 'open' in features
        assert 'high' in features
        assert 'low' in features
        assert 'close' in features
        assert 'volume' in features
        assert 'vwap' in features
        
        # Verify sequence length is correct (last 12 bars)
        assert len(features['open']) == 12
        assert features['open'] == list(range(113, 125))  # Last 12 values
        assert features['close'] == list(range(115, 127))  # Last 12 values
        
        # Verify signals are included
        assert 'sma_20' in features
        assert 'rsi_14' in features
        assert len(features['sma_20']) == 12
        
    def test_extract_timeframe_features_insufficient_data(self, callback_instance):
        """Test _extract_timeframe_features handles insufficient data."""
        # Create DataFrame with insufficient data (only 5 rows, need 12)
        test_data = pd.DataFrame({
            'timestamp': pd.date_range('2025-01-01', periods=5, freq='5T'),
            'open': range(100, 105),
            'high': range(105, 110),
            'low': range(95, 100),
            'close': range(102, 107),
            'volume': range(1000, 1005),
        })
        
        # Should return empty dict for insufficient data
        features = callback_instance._extract_timeframe_features('5m', test_data, [], 'AAPL')
        assert features == {}
    
    def test_extract_timeframe_features_missing_columns(self, callback_instance):
        """Test _extract_timeframe_features handles missing columns gracefully."""
        # Create DataFrame missing some base features
        test_data = pd.DataFrame({
            'timestamp': pd.date_range('2025-01-01', periods=25, freq='5T'),
            'open': range(100, 125),
            'high': range(105, 130),
            # Missing 'low' column
            'close': range(102, 127),
            'volume': range(1000, 1025),
        })
        
        features = callback_instance._extract_timeframe_features('5m', test_data, [], 'AAPL')
        
        # Should have features that exist
        assert 'open' in features
        assert 'high' in features  
        assert 'close' in features
        assert 'volume' in features
        
        # Should not have missing feature
        assert 'low' not in features
        
        # Should have vwap as close (fallback)
        assert 'vwap' in features
        assert features['vwap'] == features['close']
    
    def test_convert_sequence_to_qr4_rows_success(self, callback_instance):
        """Test _convert_sequence_to_qr4_rows converts sequences correctly."""
        # Create example with sequence features
        example = {
            'symbol': 'AAPL',
            'timestamp': '2025-01-01T10:00:00',
            'features': {
                'open': [100.0, 101.0, 102.0],
                'high': [105.0, 106.0, 107.0],
                'low': [95.0, 96.0, 97.0],
                'close': [103.0, 104.0, 105.0],
                'volume': [1000.0, 1100.0, 1200.0],
                'vwap': [102.0, 103.0, 104.0],
                # Non-QR4 feature should be ignored
                'some_signal': [50.0, 51.0, 52.0]
            }
        }
        
        qr4_rows = callback_instance._convert_sequence_to_qr4_rows(example, 'AAPL', '5m')
        
        # Should create 3 rows (sequence length)
        assert len(qr4_rows) == 3
        
        # Verify first row structure
        first_row = qr4_rows[0]
        assert first_row['timestamp'] == '2025-01-01T10:00:00'
        assert first_row['symbol'] == 'AAPL'
        assert first_row['open'] == 100.0
        assert first_row['high'] == 105.0
        assert first_row['low'] == 95.0
        assert first_row['close'] == 103.0
        assert first_row['volume'] == 1000.0
        assert first_row['vwap'] == 102.0
        
        # Verify second row has next values
        second_row = qr4_rows[1]
        assert second_row['open'] == 101.0
        assert second_row['high'] == 106.0
        assert second_row['close'] == 104.0
        
        # Verify third row has final values
        third_row = qr4_rows[2]
        assert third_row['open'] == 102.0
        assert third_row['high'] == 107.0
        assert third_row['close'] == 105.0
        
        # Verify non-QR4 features are not included
        assert 'some_signal' not in first_row
    
    def test_convert_sequence_to_qr4_rows_missing_features(self, callback_instance):
        """Test _convert_sequence_to_qr4_rows handles missing QR4 features."""
        # Create example missing some QR4 features
        example = {
            'symbol': 'AAPL',
            'timestamp': '2025-01-01T10:00:00',
            'features': {
                'open': [100.0, 101.0],
                'high': [105.0, 106.0],
                # Missing 'low', 'close', 'volume', 'vwap'
            }
        }
        
        qr4_rows = callback_instance._convert_sequence_to_qr4_rows(example, 'AAPL', '5m')
        
        # Should create 2 rows
        assert len(qr4_rows) == 2
        
        # Missing features should be filled with 0.0
        first_row = qr4_rows[0]
        assert first_row['open'] == 100.0
        assert first_row['high'] == 105.0
        assert first_row['low'] == 0.0  # Missing, filled with 0
        assert first_row['close'] == 0.0  # Missing, filled with 0
        assert first_row['volume'] == 0.0  # Missing, filled with 0
        assert first_row['vwap'] == 0.0  # Missing, filled with 0
    
    def test_convert_sequence_to_qr4_rows_empty_features(self, callback_instance):
        """Test _convert_sequence_to_qr4_rows handles empty features."""
        example = {
            'symbol': 'AAPL',
            'timestamp': '2025-01-01T10:00:00',
            'features': {}
        }
        
        qr4_rows = callback_instance._convert_sequence_to_qr4_rows(example, 'AAPL', '5m')
        
        # Should return empty list for no features
        assert qr4_rows == []
    
    def test_convert_sequence_to_qr4_rows_uneven_sequences(self, callback_instance):
        """Test _convert_sequence_to_qr4_rows handles uneven sequence lengths."""
        # Create example with uneven sequence lengths
        example = {
            'symbol': 'AAPL',
            'timestamp': '2025-01-01T10:00:00',
            'features': {
                'open': [100.0, 101.0, 102.0],  # Length 3
                'high': [105.0, 106.0],         # Length 2
                'close': [103.0]                # Length 1
            }
        }
        
        qr4_rows = callback_instance._convert_sequence_to_qr4_rows(example, 'AAPL', '5m')
        
        # Should create 3 rows (longest sequence)
        assert len(qr4_rows) == 3
        
        # First row should have all values
        assert qr4_rows[0]['open'] == 100.0
        assert qr4_rows[0]['high'] == 105.0
        assert qr4_rows[0]['close'] == 103.0
        
        # Second row should fill missing values with 0
        assert qr4_rows[1]['open'] == 101.0
        assert qr4_rows[1]['high'] == 106.0
        assert qr4_rows[1]['close'] == 0.0  # Missing, filled with 0
        
        # Third row should fill missing values with 0
        assert qr4_rows[2]['open'] == 102.0
        assert qr4_rows[2]['high'] == 0.0   # Missing, filled with 0
        assert qr4_rows[2]['close'] == 0.0  # Missing, filled with 0


class TestQR4ComplianceValidation:
    """Test QR4 compliance requirements are met."""
    
    def test_qr4_base_feature_names_only(self):
        """Test that only QR4 base feature names are used."""
        qr4_base_features = ['open', 'high', 'low', 'close', 'volume', 'vwap']
        
        # This should match the list in _convert_sequence_to_qr4_rows
        callback = IntervalBasedTrainingDataCallback([], None, '/tmp')
        
        # Test a mock example
        example = {
            'features': {
                'open': [100.0],
                'high': [105.0],
                'low': [95.0],
                'close': [103.0],
                'volume': [1000.0],
                'vwap': [102.0],
                'non_qr4_feature': [50.0]  # Should be ignored
            }
        }
        
        qr4_rows = callback._convert_sequence_to_qr4_rows(example, 'TEST', '5m')
        
        if qr4_rows:
            row_keys = set(qr4_rows[0].keys())
            expected_keys = set(['timestamp', 'symbol'] + qr4_base_features)
            
            # Should only contain QR4 compliant keys
            assert row_keys <= expected_keys  # row_keys is subset of expected_keys
            assert 'non_qr4_feature' not in row_keys
    
    def test_qr4_no_indexed_features(self):
        """Test that QR4 rows don't contain indexed features like open_000."""
        callback = IntervalBasedTrainingDataCallback([], None, '/tmp')
        
        example = {
            'features': {
                'open': [100.0, 101.0, 102.0],
                'close': [103.0, 104.0, 105.0]
            }
        }
        
        qr4_rows = callback._convert_sequence_to_qr4_rows(example, 'TEST', '5m')
        
        # Check that no row contains indexed features
        for row in qr4_rows:
            for key in row.keys():
                assert '_000' not in key
                assert '_001' not in key
                assert '_002' not in key
                # Only base names should be present
                if key not in ['timestamp', 'symbol']:
                    assert key in ['open', 'high', 'low', 'close', 'volume', 'vwap']
    
    def test_qr4_scalar_values_only(self):
        """Test that QR4 rows contain only scalar values, not lists."""
        callback = IntervalBasedTrainingDataCallback([], None, '/tmp')
        
        example = {
            'features': {
                'open': [100.0, 101.0],
                'close': [103.0, 104.0]
            }
        }
        
        qr4_rows = callback._convert_sequence_to_qr4_rows(example, 'TEST', '5m')
        
        # Each row should contain only scalar values
        for row in qr4_rows:
            for key, value in row.items():
                assert not isinstance(value, list), f"Feature {key} should be scalar, got {type(value)}"
                if key not in ['timestamp', 'symbol']:
                    assert isinstance(value, (int, float)), f"Numeric feature {key} should be int/float, got {type(value)}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])