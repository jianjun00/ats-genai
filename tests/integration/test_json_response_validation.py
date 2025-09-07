#!/usr/bin/env python3
"""
Integration Tests for JSON Response Validation
Tests API endpoints to ensure they never return invalid JSON with NaN values
"""

import unittest
import requests
import json
import math
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from core.sanitizers.json_sanitizer import JSONSanitizer, validate_api_response

class TestJSONResponseValidation(unittest.TestCase):
    """Test that API responses are always valid JSON without NaN values."""

    BASE_URL = "http://localhost:3000"

    @classmethod
    def setUpClass(cls):
        """Check if analytics service is running."""
        try:
            response = requests.get(f"{cls.BASE_URL}/health", timeout=5)
            if response.status_code != 200:
                cls.skipTest(cls, "Analytics service not running")
        except requests.exceptions.RequestException:
            cls.skipTest(cls, "Analytics service not accessible")

    def validate_json_response(self, response):
        """Helper method to validate JSON responses."""
        # Check that response is valid JSON
        self.assertEqual(response.status_code, 200)

        # Parse JSON
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            self.fail(f"Invalid JSON response: {e}")

        # Check for NaN values in the raw response text
        response_text = response.text
        self.assertNotIn('NaN', response_text, "Found NaN in JSON response")
        self.assertNotIn('Infinity', response_text, "Found Infinity in JSON response")
        self.assertNotIn('-Infinity', response_text, "Found -Infinity in JSON response")

        # Recursively check for NaN values in parsed data
        self.check_no_nan_values(data, "response")

        return data

    def check_no_nan_values(self, obj, path=""):
        """Recursively check for NaN values in data structures."""
        if isinstance(obj, dict):
            for key, value in obj.items():
                self.check_no_nan_values(value, f"{path}.{key}")
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                self.check_no_nan_values(item, f"{path}[{i}]")
        elif isinstance(obj, float):
            self.assertFalse(math.isnan(obj), f"NaN found at {path}")
            self.assertFalse(math.isinf(obj), f"Infinity found at {path}")

    def test_training_datasets_list_json_valid(self):
        """Test that training datasets list returns valid JSON."""
        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets")
        data = self.validate_json_response(response)

        # Verify structure
        self.assertIn('datasets', data)
        self.assertIsInstance(data['datasets'], list)

    def test_training_dataset_sequences_json_valid(self):
        """Test that training dataset sequences return valid JSON."""
        # First get available datasets
        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets")
        datasets = response.json()['datasets']

        if not datasets:
            self.skipTest("No training datasets available")

        dataset_id = datasets[0]['id']

        # Test sequences endpoint
        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/sequences")
        data = self.validate_json_response(response)

        # Verify structure
        self.assertIn('datasets', data)
        self.assertIn('sequences', data)

    def test_multi_timeframe_data_json_valid(self):
        """Test that multi-timeframe data returns valid JSON without NaN."""
        # Get a dataset with sequences
        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets")
        datasets = response.json()['datasets']

        if not datasets:
            self.skipTest("No training datasets available")

        # Find a dataset with sequences
        dataset_id = None
        sequence_id = None

        for dataset in datasets:
            dataset_id = dataset['id']
            seq_response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/sequences")
            if seq_response.status_code == 200:
                seq_data = seq_response.json()
                if seq_data.get('sequences'):
                    sequence_id = seq_data['sequences'][0].get('sequence_id')
                    break

        if not dataset_id or not sequence_id:
            self.skipTest("No datasets with sequences available")

        # Test multi-timeframe endpoint - this is where NaN errors were occurring
        response = requests.get(
            f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/multi-timeframe",
            params={'row_index': 10}
        )

        data = self.validate_json_response(response)

        # Verify structure and content
        self.assertIn('success', data)
        self.assertIn('ohlc_data', data)
        self.assertIn('table_data', data)

        # Specifically check OHLC data for NaN
        ohlc_data = data.get('ohlc_data', {})
        for timeframe, bars in ohlc_data.items():
            self.assertIsInstance(bars, list, f"OHLC data for {timeframe} should be a list")
            for i, bar in enumerate(bars):
                for field in ['open', 'high', 'low', 'close', 'volume']:
                    if field in bar:
                        value = bar[field]
                        self.assertIsInstance(value, (int, float),
                                           f"OHLC {field} in {timeframe}[{i}] should be numeric")
                        if isinstance(value, float):
                            self.assertFalse(math.isnan(value),
                                           f"NaN found in OHLC {field} for {timeframe}[{i}]")
                            self.assertFalse(math.isinf(value),
                                           f"Infinity found in OHLC {field} for {timeframe}[{i}]")

        # Specifically check table data for NaN (comprehensive features)
        table_data = data.get('table_data', [])
        for i, row in enumerate(table_data):
            if isinstance(row, dict):
                for feature_name, feature_value in row.items():
                    if isinstance(feature_value, float):
                        self.assertFalse(math.isnan(feature_value),
                                       f"NaN found in feature '{feature_name}' in table row {i}")
                        self.assertFalse(math.isinf(feature_value),
                                       f"Infinity found in feature '{feature_name}' in table row {i}")

    def test_visualization_data_json_valid(self):
        """Test that visualization data returns valid JSON."""
        # Get a dataset with sequences
        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets")
        datasets = response.json()['datasets']

        if not datasets:
            self.skipTest("No training datasets available")

        dataset_id = datasets[0]['id']

        # Test visualization endpoint
        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/visualization")

        if response.status_code == 200:
            data = self.validate_json_response(response)

            # Check for chart data
            if 'chart_data' in data:
                chart_data = data['chart_data']
                self.check_no_nan_values(chart_data, "chart_data")

    def test_edge_case_responses(self):
        """Test edge cases that might produce NaN values."""
        # Test with invalid dataset ID
        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets/99999/sequences")

        # Even error responses should be valid JSON
        if response.status_code != 404:  # Allow 404s
            try:
                data = response.json()
                self.check_no_nan_values(data, "error_response")
            except json.JSONDecodeError:
                pass  # Error responses might not be JSON

        # Test with invalid sequence parameters
        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets/1/sequences/invalid_sequence/multi-timeframe")

        if response.status_code == 200:
            data = self.validate_json_response(response)

class TestResponseSanitization(unittest.TestCase):
    """Test that response sanitization works correctly."""

    def test_sanitize_api_response_with_nan(self):
        """Test that API response sanitization removes NaN values."""
        mock_api_response = {
            'success': True,
            'ohlc_data': {
                '5m': [
                    {
                        'timestamp': 1625097600,
                        'open': 150.0,
                        'high': float('nan'),
                        'low': 149.0,
                        'close': 150.5,
                        'volume': float('inf')
                    }
                ],
                '15m': [
                    {
                        'timestamp': 1625097900,
                        'open': float('-inf'),
                        'high': 151.0,
                        'low': 149.5,
                        'close': float('nan'),
                        'volume': 50000
                    }
                ]
            },
            'table_data': [
                {
                    'feature_1': 123.45,
                    'feature_2': float('nan'),
                    'feature_3': float('inf'),
                    'symbol': 'AAPL'
                }
            ]
        }

        sanitized = validate_api_response(mock_api_response)

        # Verify JSON serialization works
        json_str = json.dumps(sanitized)

        # Verify no NaN in the JSON string
        self.assertNotIn('NaN', json_str)
        self.assertNotIn('Infinity', json_str)

        # Verify data integrity
        self.assertEqual(sanitized['success'], True)
        self.assertEqual(sanitized['ohlc_data']['5m'][0]['open'], 150.0)
        self.assertEqual(sanitized['ohlc_data']['5m'][0]['high'], 0.0)  # NaN -> 0.0
        self.assertEqual(sanitized['ohlc_data']['15m'][0]['open'], 0.0)  # -inf -> 0.0 for OHLC
        self.assertEqual(sanitized['table_data'][0]['feature_2'], 0.0)  # NaN -> 0.0

class TestJSONMiddleware(unittest.TestCase):
    """Test JSON sanitization middleware functionality."""

    def test_safe_json_response(self):
        """Test that safe JSON responses handle all problematic values."""
        problematic_data = {
            'float_nan': float('nan'),
            'float_inf': float('inf'),
            'float_neg_inf': float('-inf'),
            'nested': {
                'array_with_nan': [1.0, float('nan'), 3.0],
                'mixed_types': [
                    {'value': float('inf')},
                    {'value': 42},
                    {'value': float('nan')}
                ]
            }
        }

        sanitized_json = JSONSanitizer.safe_json_dumps(problematic_data, indent=2)

        # Should be parseable
        parsed = json.loads(sanitized_json)

        # Verify sanitization
        self.assertEqual(parsed['float_nan'], 0.0)
        self.assertEqual(parsed['float_inf'], 1e10)
        self.assertEqual(parsed['float_neg_inf'], -1e10)
        self.assertEqual(parsed['nested']['array_with_nan'], [1.0, 0.0, 3.0])

if __name__ == '__main__':
    # Run with verbosity to see detailed test results
    unittest.main(verbosity=2)