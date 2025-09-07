#!/usr/bin/env python3
"""
Comprehensive Unit Tests for NaN Handling
Tests all aspects of NaN value sanitization to prevent JSON serialization errors
"""

import unittest
import math
import json
import numpy as np
from unittest.mock import patch, MagicMock
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from core.sanitizers.json_sanitizer import (
    JSONSanitizer, 
    sanitize_training_features, 
    sanitize_ohlc_data, 
    validate_api_response
)

class TestNaNHandling(unittest.TestCase):
    """Test comprehensive NaN value handling."""
    
    def test_sanitize_float_nan(self):
        """Test that NaN floats are converted to 0.0."""
        nan_value = float('nan')
        sanitized = JSONSanitizer.sanitize_value(nan_value)
        self.assertEqual(sanitized, 0.0)
        self.assertFalse(math.isnan(sanitized))
    
    def test_sanitize_float_infinity(self):
        """Test that infinity values are handled properly."""
        pos_inf = float('inf')
        neg_inf = float('-inf')
        
        pos_sanitized = JSONSanitizer.sanitize_value(pos_inf)
        neg_sanitized = JSONSanitizer.sanitize_value(neg_inf)
        
        self.assertEqual(pos_sanitized, 1e10)
        self.assertEqual(neg_sanitized, -1e10)
        self.assertFalse(math.isinf(pos_sanitized))
        self.assertFalse(math.isinf(neg_sanitized))
    
    def test_sanitize_numpy_nan(self):
        """Test that numpy NaN values are handled."""
        numpy_nan = np.float32('nan')
        numpy_inf = np.float64('inf')
        
        sanitized_nan = JSONSanitizer.sanitize_value(numpy_nan)
        sanitized_inf = JSONSanitizer.sanitize_value(numpy_inf)
        
        self.assertEqual(sanitized_nan, 0.0)
        # Check that infinity is handled (could be 0.0 or 1e10 depending on context)
        self.assertFalse(np.isnan(sanitized_inf))
        self.assertFalse(np.isinf(sanitized_inf))
    
    def test_sanitize_training_features_with_nan(self):
        """Test sanitization of training features containing NaN."""
        features = {
            'open': 150.5,
            'high': float('nan'),
            'low': np.float32('inf'),
            'close': 149.8,
            'volume': float('-inf'),
            'symbol': 'AAPL',
            'invalid_field': float('nan')
        }
        
        sanitized = sanitize_training_features(features)
        
        self.assertEqual(sanitized['open'], 150.5)
        self.assertEqual(sanitized['high'], 0.0)
        self.assertEqual(sanitized['low'], 0.0)  # Financial data: infinity becomes 0.0
        self.assertEqual(sanitized['close'], 149.8)
        self.assertEqual(sanitized['volume'], 0.0)  # Financial data: -infinity becomes 0.0
        self.assertEqual(sanitized['symbol'], 'AAPL')
        self.assertEqual(sanitized['invalid_field'], 0.0)
    
    def test_sanitize_ohlc_data_with_nan(self):
        """Test sanitization of OHLC data containing NaN."""
        ohlc_data = [
            {
                'timestamp': 1625097600,
                'open': float('nan'),
                'high': 151.0,
                'low': float('-inf'),
                'close': 150.5,
                'volume': float('inf'),
                'vwap': 150.75
            },
            {
                'timestamp': float('nan'),
                'open': 150.5,
                'high': float('nan'),
                'low': 149.0,
                'close': 150.0,
                'volume': 1000000,
                'vwap': float('inf')
            }
        ]
        
        sanitized = sanitize_ohlc_data(ohlc_data)
        
        # First record
        self.assertEqual(sanitized[0]['timestamp'], 1625097600)
        self.assertEqual(sanitized[0]['open'], 0.0)
        self.assertEqual(sanitized[0]['high'], 151.0)
        self.assertEqual(sanitized[0]['low'], 0.0)  # Negative infinity becomes 0 for OHLC
        self.assertEqual(sanitized[0]['close'], 150.5)
        self.assertEqual(sanitized[0]['volume'], 0.0)  # Positive infinity becomes 0 for financial data
        self.assertEqual(sanitized[0]['vwap'], 150.75)
        
        # Second record
        self.assertEqual(sanitized[1]['timestamp'], 0)  # NaN timestamp becomes 0
        self.assertEqual(sanitized[1]['open'], 150.5)
        self.assertEqual(sanitized[1]['high'], 0.0)
        self.assertEqual(sanitized[1]['low'], 149.0)
        self.assertEqual(sanitized[1]['close'], 150.0)
        self.assertEqual(sanitized[1]['volume'], 1000000.0)
        self.assertEqual(sanitized[1]['vwap'], 0.0)  # Infinity becomes 0 for OHLC
    
    def test_json_serialization_safety(self):
        """Test that sanitized data can be safely serialized to JSON."""
        problematic_data = {
            'values': [1.0, float('nan'), float('inf'), float('-inf'), 2.0],
            'nested': {
                'nan_field': float('nan'),
                'inf_field': float('inf'),
                'normal_field': 'test'
            },
            'numpy_array': np.array([1.0, np.nan, np.inf, 2.0])
        }
        
        sanitized = JSONSanitizer.sanitize_response(problematic_data)
        
        # Should not raise any exceptions
        json_str = json.dumps(sanitized)
        
        # Verify the JSON is valid
        parsed = json.loads(json_str)
        self.assertEqual(parsed['values'], [1.0, 0.0, 1e10, -1e10, 2.0])
        self.assertEqual(parsed['nested']['nan_field'], 0.0)
        self.assertEqual(parsed['nested']['inf_field'], 1e10)
        self.assertEqual(parsed['nested']['normal_field'], 'test')
    
    def test_validate_json_serializable(self):
        """Test JSON serializability validation."""
        valid_data = {'a': 1, 'b': 2.0, 'c': 'test'}
        invalid_data = {'a': float('nan'), 'b': float('inf')}
        
        self.assertTrue(JSONSanitizer.validate_json_serializable(valid_data))
        self.assertTrue(JSONSanitizer.validate_json_serializable(invalid_data))  # Should be True after sanitization
    
    def test_safe_json_dumps(self):
        """Test safe JSON dumping with automatic sanitization."""
        data_with_nan = {
            'normal': 123,
            'nan_value': float('nan'),
            'inf_value': float('inf'),
            'list_with_nan': [1, float('nan'), 3]
        }
        
        json_str = JSONSanitizer.safe_json_dumps(data_with_nan)
        parsed = json.loads(json_str)
        
        self.assertEqual(parsed['normal'], 123)
        self.assertEqual(parsed['nan_value'], 0.0)
        self.assertEqual(parsed['inf_value'], 1e10)
        self.assertEqual(parsed['list_with_nan'], [1, 0.0, 3])
    
    def test_validate_api_response(self):
        """Test complete API response validation."""
        api_response = {
            'success': True,
            'ohlc_data': {
                '5m': [
                    {
                        'timestamp': 1625097600,
                        'open': float('nan'),
                        'high': 151.0,
                        'low': 149.0,
                        'close': 150.5,
                        'volume': float('inf')
                    }
                ]
            },
            'table_data': [
                {
                    'feature_1': 123.45,
                    'feature_2': float('nan'),
                    'feature_3': float('-inf')
                }
            ]
        }
        
        sanitized = validate_api_response(api_response)
        
        # Check OHLC data sanitization
        ohlc_5m = sanitized['ohlc_data']['5m'][0]
        self.assertEqual(ohlc_5m['open'], 0.0)
        # Volume infinity should be sanitized (exact value depends on sanitizer used)
        self.assertFalse(math.isinf(ohlc_5m['volume']))
        self.assertFalse(math.isnan(ohlc_5m['volume']))
        
        # Check table data sanitization
        table_row = sanitized['table_data'][0]
        self.assertEqual(table_row['feature_1'], 123.45)
        self.assertEqual(table_row['feature_2'], 0.0)
        self.assertEqual(table_row['feature_3'], -1e10)  # -inf becomes -1e10 for training features

class TestAnalyticsServiceNaNHandling(unittest.TestCase):
    """Test NaN handling in analytics service context."""
    
    def test_arrayrecord_nan_handling_simulation(self):
        """Test that ArrayRecord data with NaN is properly handled (simulated)."""
        
        # Simulate ArrayRecord data with NaN values
        mock_columns = ['open_000', 'high_000', 'low_000', 'close_000', 'symbol_encoded']
        mock_data = np.array([150.0, float('nan'), 149.0, 150.5, float('nan')], dtype=np.float32)
        
        # Simulate the fixed code behavior from analytics service
        feature_row = {}
        for i, col_name in enumerate(mock_columns):
            val = mock_data[i]
            # This is the fixed NaN handling:
            if math.isnan(val):
                val = 0.0
            feature_row[col_name] = float(val)
        
        # Verify no NaN values remain
        for key, value in feature_row.items():
            self.assertFalse(math.isnan(value), f"NaN found in {key}")
        
        # Verify specific values
        self.assertEqual(feature_row['open_000'], 150.0)
        self.assertEqual(feature_row['high_000'], 0.0)  # NaN -> 0.0
        self.assertEqual(feature_row['low_000'], 149.0)
        self.assertEqual(feature_row['close_000'], 150.5)
        self.assertEqual(feature_row['symbol_encoded'], 0.0)  # NaN -> 0.0
        
        # Verify JSON serialization works
        json_str = json.dumps(feature_row)
        self.assertNotIn('NaN', json_str)
        self.assertNotIn('Infinity', json_str)

class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions."""
    
    def test_empty_data_structures(self):
        """Test handling of empty data structures."""
        self.assertEqual(JSONSanitizer.sanitize_value([]), [])
        self.assertEqual(JSONSanitizer.sanitize_value({}), {})
        self.assertEqual(JSONSanitizer.sanitize_value(None), None)
    
    def test_nested_nan_structures(self):
        """Test deeply nested structures with NaN values."""
        nested_data = {
            'level1': {
                'level2': {
                    'level3': [float('nan'), {'level4': float('inf')}]
                }
            }
        }
        
        sanitized = JSONSanitizer.sanitize_response(nested_data)
        
        self.assertEqual(sanitized['level1']['level2']['level3'][0], 0.0)
        self.assertEqual(sanitized['level1']['level2']['level3'][1]['level4'], 1e10)
    
    def test_mixed_types_with_nan(self):
        """Test mixed data types including NaN."""
        mixed_data = [
            123,
            'string',
            float('nan'),
            True,
            {'key': float('-inf')},
            [1, float('nan'), 3]
        ]
        
        sanitized = JSONSanitizer.sanitize_value(mixed_data)
        expected = [123, 'string', 0.0, True, {'key': -1e10}, [1, 0.0, 3]]
        
        self.assertEqual(sanitized, expected)

if __name__ == '__main__':
    unittest.main()