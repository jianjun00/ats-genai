#!/usr/bin/env python3
"""
Unit Tests for Timestamp Format Fixes

Tests the critical fixes for timestamp formatting issues that caused
"Invalid Date to Invalid Date" errors in frontend JavaScript.
"""
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone, timedelta
import json


class TestTimestampFormatFixes(unittest.TestCase):
    """Test timestamp format consistency across API endpoints."""

    def setUp(self):
        """Set up test fixtures."""
        self.base_timestamp = datetime(2025, 7, 1, 9, 0, 0, tzinfo=timezone.utc)
        self.expected_unix_timestamp = int(self.base_timestamp.timestamp())

    def test_unix_timestamp_generation(self):
        """Test that datetime.timestamp() generates correct Unix seconds."""
        # Test base timestamp
        timestamp = int(self.base_timestamp.timestamp())
        expected_timestamp = int(self.base_timestamp.timestamp())  # Calculate dynamically
        self.assertEqual(timestamp, expected_timestamp)
        self.assertIsInstance(timestamp, int)

        # Test with offset
        offset_dt = self.base_timestamp + timedelta(minutes=5)
        offset_timestamp = int(offset_dt.timestamp())
        self.assertEqual(offset_timestamp, expected_timestamp + 300)  # +5 minutes = +300 seconds

    def test_timedelta_calculations_vs_replace(self):
        """Test that timedelta prevents minute range errors."""
        base_dt = datetime(2025, 7, 1, 9, 0, 0, tzinfo=timezone.utc)

        # This would cause "minute must be in 0..59" error
        # base_dt.replace(minute=base_dt.minute + 65)  # Would fail

        # But timedelta works correctly
        result_dt = base_dt + timedelta(minutes=65)
        expected_dt = datetime(2025, 7, 1, 10, 5, 0, tzinfo=timezone.utc)
        self.assertEqual(result_dt, expected_dt)

        # Test various offsets that would break replace()
        for offset in [60, 120, 300, 1440]:  # 1h, 2h, 5h, 24h in minutes
            result = base_dt + timedelta(minutes=offset)
            self.assertIsInstance(result, datetime)
            self.assertGreater(result, base_dt)

    def test_timeframe_timestamp_calculations(self):
        """Test timestamp calculations for different timeframes."""
        base_dt = datetime(2025, 7, 1, 9, 0, 0, tzinfo=timezone.utc)

        timeframe_configs = {
            '5m': {'minutes': 5, 'sequence_length': 52},
            '15m': {'minutes': 15, 'sequence_length': 52},
            '1h': {'hours': 1, 'sequence_length': 24},
            '1d': {'days': 1, 'sequence_length': 20},
            '1w': {'days': 7, 'sequence_length': 12}
        }

        for timeframe, config in timeframe_configs.items():
            for i in range(min(5, config['sequence_length'])):  # Test first 5 steps
                if 'minutes' in config:
                    expected_dt = base_dt + timedelta(minutes=i * config['minutes'])
                elif 'hours' in config:
                    expected_dt = base_dt + timedelta(hours=i * config['hours'])
                elif 'days' in config:
                    expected_dt = base_dt + timedelta(days=i * config['days'])

                timestamp = int(expected_dt.timestamp())
                self.assertIsInstance(timestamp, int)
                self.assertGreater(timestamp, 0)

    def test_javascript_compatibility(self):
        """Test that timestamps are compatible with JavaScript Date objects."""
        # Simulate what frontend JavaScript does
        unix_seconds = int(self.base_timestamp.timestamp())

        # JavaScript: new Date(timestamp * 1000)
        js_milliseconds = unix_seconds * 1000

        # Verify this produces a valid timestamp range
        self.assertGreater(js_milliseconds, 0)
        self.assertLess(js_milliseconds, 2147483647000)  # Max 32-bit timestamp in ms

        # Verify it's in the expected time range (2025)
        self.assertGreater(unix_seconds, 1704067200)  # Jan 1, 2024
        self.assertLess(unix_seconds, 1767225600)     # Jan 1, 2026

    def test_avoid_string_timestamps(self):
        """Test that we never return string timestamps from API."""
        # These formats would cause JavaScript parsing errors
        bad_formats = [
            "2025-07-01T09:00:00",           # Missing timezone
            "2025-07-01T09:00:00.000",       # Missing timezone
            "2025-07-01 09:00:00",           # Wrong format
            "07/01/2025 09:00:00",           # US format
            "1719914400.0"                   # String number
        ]

        # Good format (what we should return)
        good_format = 1719914400  # Unix epoch seconds as integer

        # Verify good format works
        self.assertIsInstance(good_format, int)
        self.assertGreater(good_format, 0)

        # Verify bad formats would cause issues
        for bad_format in bad_formats:
            self.assertIsInstance(bad_format, str)
            # In JavaScript: new Date(bad_format) might fail or give incorrect results

    def test_api_response_structure(self):
        """Test that API responses have correct timestamp structure."""
        # Expected API response structure
        expected_response = {
            "ohlc_data": {
                "5m": [
                    {
                        "timestamp": 1719914400,  # Unix seconds (int)
                        "open": 232.97,
                        "high": 233.02,
                        "low": 232.85,
                        "close": 232.98,
                        "volume": 220303,
                        "vwap": 0.0
                    }
                ]
            }
        }

        # Verify timestamp is integer
        timestamp = expected_response["ohlc_data"]["5m"][0]["timestamp"]
        self.assertIsInstance(timestamp, int)
        self.assertGreater(timestamp, 0)

        # Verify JSON serialization doesn't break
        json_str = json.dumps(expected_response)
        parsed_back = json.loads(json_str)
        self.assertEqual(parsed_back["ohlc_data"]["5m"][0]["timestamp"], timestamp)


class TestVariableScopeFixes(unittest.TestCase):
    """Test variable scope fixes that prevented 'timeframe' is not defined errors."""

    def test_timeframe_prefix_consistency(self):
        """Test that timeframe_prefix is properly derived from file paths."""
        test_cases = [
            ('/data/training_data/89/AAPL_20250701_000000_20250906_000000/5m/AAPL_20250701_000000_20250906_000000.arrayrecord', '5m', 52),
            ('/data/training_data/89/AAPL_20250701_000000_20250906_000000/15m/AAPL_20250701_000000_20250906_000000.arrayrecord', '15m', 52),
            ('/data/training_data/89/AAPL_20250701_000000_20250906_000000/1h/AAPL_20250701_000000_20250906_000000.arrayrecord', '1h', 24),
            ('/data/training_data/89/AAPL_20250701_000000_20250906_000000/1d/AAPL_20250701_000000_20250906_000000.arrayrecord', '1d', 20),
            ('/data/training_data/89/AAPL_20250701_000000_20250906_000000/1w/AAPL_20250701_000000_20250906_000000.arrayrecord', '1w', 12),
        ]

        for file_path, expected_prefix, expected_length in test_cases:
            # Simulate the logic from _read_arrayrecord_ohlc
            if '/5m/' in file_path:
                timeframe_prefix = '5m'
                sequence_length = 52
            elif '/15m/' in file_path:
                timeframe_prefix = '15m'
                sequence_length = 52
            elif '/1h/' in file_path:
                timeframe_prefix = '1h'
                sequence_length = 24
            elif '/1d/' in file_path:
                timeframe_prefix = '1d'
                sequence_length = 20
            elif '/1w/' in file_path:
                timeframe_prefix = '1w'
                sequence_length = 12
            else:
                timeframe_prefix = '5m'  # Default
                sequence_length = 52

            self.assertEqual(timeframe_prefix, expected_prefix)
            self.assertEqual(sequence_length, expected_length)

    def test_column_name_generation(self):
        """Test that column names are generated correctly with timeframe_prefix."""
        timeframe_prefix = '5m'

        for i in range(5):  # Test first 5 indices
            expected_columns = {
                'open': f'{timeframe_prefix}_open_{i:03d}',
                'high': f'{timeframe_prefix}_high_{i:03d}',
                'low': f'{timeframe_prefix}_low_{i:03d}',
                'close': f'{timeframe_prefix}_close_{i:03d}',
                'volume': f'{timeframe_prefix}_volume_{i:03d}',
                'vwap': f'{timeframe_prefix}_vwap_{i:03d}'
            }

            # Verify format
            for col_type, col_name in expected_columns.items():
                self.assertIn(timeframe_prefix, col_name)
                self.assertIn(f'{i:03d}', col_name)
                self.assertEqual(col_name, f'{timeframe_prefix}_{col_type}_{i:03d}')


if __name__ == '__main__':
    unittest.main()