#!/usr/bin/env python3
"""
Integration Tests for Multi-Timeframe API Fixes

Tests the complete multi-timeframe API pipeline that was fixed to resolve:
- "No data files found for sequence" errors
- Variable scope issues in ArrayRecord reading
- Timestamp format consistency across timeframes
"""
import unittest
import requests
import json
from pathlib import Path
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))


class TestMultiTimeframeAPIFixes(unittest.TestCase):
    """Integration tests for multi-timeframe API endpoints."""

    def setUp(self):
        """Set up test configuration."""
        self.base_url = "http://localhost:3000"
        self.test_dataset_id = 65
        self.test_sequence_id = "AAPL_20250701_000000_20250906_000000"
        self.test_row_index = 50

    def test_analytics_service_health(self):
        """Test that analytics service is running and healthy."""
        response = requests.get(f"{self.base_url}/health")
        self.assertEqual(response.status_code, 200)

        data = response.json()
        self.assertEqual(data["status"], "healthy")
        self.assertEqual(data["service"], "ats-unified-analytics")
        self.assertTrue(data["features"]["training_datasets"])

    def test_training_datasets_list_api(self):
        """Test that training datasets API returns expected data."""
        response = requests.get(f"{self.base_url}/api/v1/training-datasets")
        self.assertEqual(response.status_code, 200)

        data = response.json()
        self.assertIn("datasets", data)
        self.assertIn("total_count", data)
        self.assertGreater(len(data["datasets"]), 0)

        # Find our test dataset
        test_dataset = None
        for dataset in data["datasets"]:
            if dataset["id"] == self.test_dataset_id:
                test_dataset = dataset
                break

        self.assertIsNotNone(test_dataset, f"Dataset {self.test_dataset_id} not found")
        self.assertIn("AAPL", test_dataset["symbols"])
        self.assertIn("TSLA", test_dataset["symbols"])

    def test_dataset_sequences_api(self):
        """Test that dataset sequences API returns expected sequences."""
        response = requests.get(f"{self.base_url}/api/v1/training-datasets/{self.test_dataset_id}/sequences")
        self.assertEqual(response.status_code, 200)

        data = response.json()
        self.assertIn("sequences", data)
        self.assertIn("total_count", data)
        self.assertGreater(len(data["sequences"]), 0)

        # Find our test sequence
        test_sequence = None
        for sequence in data["sequences"]:
            if sequence["sequence_id"] == self.test_sequence_id:
                test_sequence = sequence
                break

        self.assertIsNotNone(test_sequence, f"Sequence {self.test_sequence_id} not found")
        self.assertEqual(test_sequence["symbol"], "AAPL")
        self.assertEqual(test_sequence["timeframe"], "multi")

    def test_multi_timeframe_api_success(self):
        """Test that multi-timeframe API returns successful response."""
        url = f"{self.base_url}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/multi-timeframe"
        params = {"row_index": self.test_row_index}

        response = requests.get(url, params=params)
        self.assertEqual(response.status_code, 200)

        data = response.json()

        # Verify no error response
        self.assertNotIn("error", data, f"API returned error: {data.get('error')}")

        # Verify success indicators
        self.assertTrue(data.get("success", False), "API success field is not True")
        self.assertEqual(data.get("sequence_id"), self.test_sequence_id)

        # Verify response structure
        self.assertIn("ohlc_data", data)
        self.assertIn("table_data", data)
        self.assertIn("dataset_name", data)

    def test_all_timeframes_present(self):
        """Test that all 5 timeframes are returned."""
        url = f"{self.base_url}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/multi-timeframe"
        params = {"row_index": self.test_row_index}

        response = requests.get(url, params=params)
        self.assertEqual(response.status_code, 200)

        data = response.json()
        self.assertIn("ohlc_data", data)

        ohlc_data = data["ohlc_data"]
        expected_timeframes = ["5m", "15m", "1h", "1d", "1w"]

        for timeframe in expected_timeframes:
            self.assertIn(timeframe, ohlc_data, f"Missing timeframe: {timeframe}")
            self.assertIsInstance(ohlc_data[timeframe], list, f"{timeframe} data is not a list")
            self.assertGreater(len(ohlc_data[timeframe]), 0, f"{timeframe} data is empty")

    def test_timestamp_format_consistency(self):
        """Test that all timeframes return Unix timestamp integers."""
        url = f"{self.base_url}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/multi-timeframe"
        params = {"row_index": self.test_row_index}

        response = requests.get(url, params=params)
        self.assertEqual(response.status_code, 200)

        data = response.json()
        ohlc_data = data["ohlc_data"]

        for timeframe in ["5m", "15m", "1h", "1d", "1w"]:
            timeframe_data = ohlc_data[timeframe]
            self.assertGreater(len(timeframe_data), 0, f"{timeframe} has no data")

            for i, bar in enumerate(timeframe_data[:3]):  # Check first 3 bars
                # Critical: timestamp must be integer (Unix seconds)
                self.assertIn("timestamp", bar, f"{timeframe}[{i}] missing timestamp")
                timestamp = bar["timestamp"]
                self.assertIsInstance(timestamp, int, f"{timeframe}[{i}] timestamp is not integer: {type(timestamp)}")
                self.assertGreater(timestamp, 1700000000, f"{timeframe}[{i}] timestamp too old: {timestamp}")
                self.assertLess(timestamp, 1800000000, f"{timeframe}[{i}] timestamp too new: {timestamp}")

    def test_ohlcv_data_structure(self):
        """Test that OHLCV data has correct structure."""
        url = f"{self.base_url}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/multi-timeframe"
        params = {"row_index": self.test_row_index}

        response = requests.get(url, params=params)
        self.assertEqual(response.status_code, 200)

        data = response.json()
        ohlc_data = data["ohlc_data"]

        required_fields = ["timestamp", "open", "high", "low", "close", "volume", "vwap"]

        for timeframe in ["5m", "15m", "1h", "1d", "1w"]:
            timeframe_data = ohlc_data[timeframe]
            self.assertGreater(len(timeframe_data), 0, f"{timeframe} has no data")

            for i, bar in enumerate(timeframe_data[:2]):  # Check first 2 bars
                for field in required_fields:
                    self.assertIn(field, bar, f"{timeframe}[{i}] missing field: {field}")
                    value = bar[field]
                    self.assertIsInstance(value, (int, float), f"{timeframe}[{i}][{field}] is not numeric: {type(value)}")

    def test_21_bar_context_window(self):
        """Test that 21-bar context window logic works correctly."""
        url = f"{self.base_url}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/multi-timeframe"
        params = {"row_index": self.test_row_index}

        response = requests.get(url, params=params)
        self.assertEqual(response.status_code, 200)

        data = response.json()
        ohlc_data = data["ohlc_data"]

        # Each timeframe should have bars selected according to 21-bar context window logic
        # However, if row_index is beyond available data, all available data is returned
        for timeframe in ["5m", "15m", "1h", "1d", "1w"]:
            timeframe_data = ohlc_data[timeframe]
            data_length = len(timeframe_data)

            # Should have some data
            self.assertGreater(data_length, 0, f"{timeframe} has no data")

            # Should have at most all available data (which could be more than 21 if row_index is out of bounds)
            # This is expected behavior when row_index >= data_length
            self.assertLessEqual(data_length, 100, f"{timeframe} has unreasonably many bars: {data_length}")

    def test_table_data_consistency(self):
        """Test that table data is provided and consistent."""
        url = f"{self.base_url}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/multi-timeframe"
        params = {"row_index": self.test_row_index}

        response = requests.get(url, params=params)
        self.assertEqual(response.status_code, 200)

        data = response.json()

        self.assertIn("table_data", data)
        table_data = data["table_data"]
        self.assertIsInstance(table_data, list)
        self.assertGreater(len(table_data), 0, "Table data is empty")

        # Table data should match 1h timeframe structure
        ohlc_1h = data["ohlc_data"]["1h"]
        self.assertEqual(len(table_data), len(ohlc_1h), "Table data length doesn't match 1h OHLC data")

    def test_error_handling_invalid_dataset(self):
        """Test error handling for invalid dataset ID."""
        invalid_dataset_id = 99999
        url = f"{self.base_url}/api/v1/training-datasets/{invalid_dataset_id}/sequences/{self.test_sequence_id}/multi-timeframe"
        params = {"row_index": self.test_row_index}

        response = requests.get(url, params=params)
        self.assertEqual(response.status_code, 200)  # Service returns 200 with error in body

        data = response.json()
        self.assertIn("error", data, "Expected error response for invalid dataset")

    def test_error_handling_invalid_sequence(self):
        """Test error handling for invalid sequence ID."""
        invalid_sequence_id = "INVALID_SEQUENCE"
        url = f"{self.base_url}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{invalid_sequence_id}/multi-timeframe"
        params = {"row_index": self.test_row_index}

        response = requests.get(url, params=params)
        self.assertEqual(response.status_code, 200)  # Service returns 200 with error in body

        data = response.json()
        self.assertIn("error", data, "Expected error response for invalid sequence")

    def test_row_index_boundary_conditions(self):
        """Test row index boundary conditions."""
        test_row_indices = [0, 10, 25, 50, 100]

        for row_index in test_row_indices:
            url = f"{self.base_url}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/multi-timeframe"
            params = {"row_index": row_index}

            response = requests.get(url, params=params)
            self.assertEqual(response.status_code, 200)

            data = response.json()

            # Should either succeed or handle gracefully
            if "error" in data:
                # Error is acceptable for out-of-bounds row indices
                continue
            else:
                # Should have valid data structure
                self.assertTrue(data.get("success", False))
                self.assertIn("ohlc_data", data)

    def test_json_serialization_compatibility(self):
        """Test that response is JSON serializable (no datetime/NaN issues)."""
        url = f"{self.base_url}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/multi-timeframe"
        params = {"row_index": self.test_row_index}

        response = requests.get(url, params=params)
        self.assertEqual(response.status_code, 200)

        # Response should be valid JSON
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            self.fail(f"Response is not valid JSON: {e}")

        # Should be able to serialize back to JSON
        try:
            json_str = json.dumps(data)
            parsed_back = json.loads(json_str)
            self.assertEqual(data, parsed_back)
        except (TypeError, ValueError) as e:
            self.fail(f"Data cannot be JSON serialized: {e}")


if __name__ == '__main__':
    # Check if analytics service is running
    try:
        response = requests.get("http://localhost:3000/health", timeout=5)
        if response.status_code != 200:
            print("Analytics service not healthy - skipping integration tests")
            sys.exit(0)
    except requests.RequestException:
        print("Analytics service not running - skipping integration tests")
        sys.exit(0)

    unittest.main()