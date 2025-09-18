#!/usr/bin/env python3
"""
Unit Tests for Timestamp-Based Multi-Timeframe Navigation

Tests the new API architecture:
1. 1-hour navigation endpoint for table display
2. Multi-timeframe endpoint using timestamps
3. Timestamp synchronization logic
4. 21-bar context window calculations
"""

import unittest
from unittest.mock import Mock, patch
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from domains.analytics.services.analytics_service import UnifiedAnalyticsService

class TestTimestampBasedNavigation(unittest.TestCase):
    """Test timestamp-based navigation API endpoints."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_db = Mock()
        self.analytics_service = UnifiedAnalyticsService(db_manager=self.mock_db)

        # Sample 1-hour data for testing
        self.sample_1h_data = [
            {
                "timestamp": 1751356800,  # 2025-06-30 22:00:00
                "datetime": "2025-06-30T22:00:00-04:00",
                "open": 230.50,
                "high": 231.20,
                "low": 230.10,
                "close": 230.80,
                "volume": 150000,
                "envelope_top": 235.5,
                "envelope_bot": 226.1,
                "pldot": 230.0
            },
            {
                "timestamp": 1751360400,  # 2025-06-30 23:00:00
                "datetime": "2025-06-30T23:00:00-04:00",
                "open": 230.80,
                "high": 231.50,
                "low": 230.30,
                "close": 231.10,
                "volume": 175000,
                "envelope_top": 235.8,
                "envelope_bot": 226.4,
                "pldot": 230.5
            },
            {
                "timestamp": 1751364000,  # 2025-07-01 00:00:00
                "datetime": "2025-07-01T00:00:00-04:00",
                "open": 231.10,
                "high": 231.80,
                "low": 230.60,
                "close": 231.40,
                "volume": 160000,
                "envelope_top": 236.1,
                "envelope_bot": 226.7,
                "pldot": 231.0
            }
        ]

        # Sample multi-timeframe data
        self.sample_multi_timeframe = {
            "5m": [{"timestamp": 1751360100, "open": 230.7, "high": 230.9, "low": 230.5, "close": 230.8}] * 21,
            "15m": [{"timestamp": 1751359500, "open": 230.6, "high": 231.1, "low": 230.4, "close": 230.9}] * 21,
            "1d": [{"timestamp": 1751270400, "open": 228.5, "high": 232.0, "low": 228.0, "close": 231.5}] * 21,
            "1w": [{"timestamp": 1750665600, "open": 225.0, "high": 235.0, "low": 224.5, "close": 232.0}] * 21
        }

    def test_1h_navigation_basic_functionality(self):
        """Test basic 1-hour navigation functionality."""
        # Mock the database and file operations
        with patch.object(self.analytics_service, '_get_training_dataset_info') as mock_info, \
             patch.object(self.analytics_service, '_read_arrayrecord_1h_sequence') as mock_read:

            mock_info.return_value = {
                'id': 65,
                'dataset_name': 'test_dataset',
                'run_id': 89,
                'symbols': 'AAPL'
            }
            mock_read.return_value = self.sample_1h_data

            # Test navigation to row index 1
            result = self.analytics_service.get_training_dataset_1h_navigation(
                dataset_id=65,
                sequence_id="AAPL_20250701_000000_20250906_000000",
                row_index=1
            )

            # Verify response structure
            self.assertIn('success', result)
            self.assertTrue(result['success'])
            self.assertIn('timestamp', result)
            self.assertIn('table_data', result)
            self.assertIn('current_position', result)

            # Verify timestamp is from selected row
            self.assertEqual(result['timestamp'], 1751360400)  # Row 1 timestamp

            # Verify table data is 21-bar context window
            self.assertIsInstance(result['table_data'], list)
            self.assertLessEqual(len(result['table_data']), 21)

    def test_1h_navigation_21_bar_context_window(self):
        """Test 21-bar context window calculation for 1-hour navigation."""
        # Create larger dataset for proper 21-bar testing
        large_1h_data = []
        base_timestamp = 1751320000
        for i in range(50):  # 50 hours of data
            large_1h_data.append({
                "timestamp": base_timestamp + (i * 3600),  # Hour intervals
                "datetime": f"2025-07-0{1 + i//24}T{i%24:02d}:00:00-04:00",
                "open": 230.0 + i * 0.1,
                "high": 230.5 + i * 0.1,
                "low": 229.5 + i * 0.1,
                "close": 230.2 + i * 0.1,
                "volume": 150000 + i * 1000
            })

        with patch.object(self.analytics_service, '_get_training_dataset_info') as mock_info, \
             patch.object(self.analytics_service, '_read_arrayrecord_1h_sequence') as mock_read:

            mock_info.return_value = {'run_id': 89}
            mock_read.return_value = large_1h_data

            # Test row index 25 (middle of dataset)
            result = self.analytics_service.get_training_dataset_1h_navigation(
                dataset_id=65,
                sequence_id="AAPL_20250701_000000_20250906_000000",
                row_index=25
            )

            # Should return exactly 21 bars (10 before + 1 current + 10 after)
            self.assertEqual(len(result['table_data']), 21)

            # Target timestamp should be from row 25
            expected_timestamp = base_timestamp + (25 * 3600)
            self.assertEqual(result['timestamp'], expected_timestamp)

            # Test edge case: row index 5 (near beginning)
            result_edge = self.analytics_service.get_training_dataset_1h_navigation(
                dataset_id=65,
                sequence_id="AAPL_20250701_000000_20250906_000000",
                row_index=5
            )

            # Should still try to return 21 bars, extending forward if needed
            self.assertLessEqual(len(result_edge['table_data']), 21)
            self.assertGreater(len(result_edge['table_data']), 0)

    def test_multi_timeframe_by_timestamp(self):
        """Test multi-timeframe data retrieval by timestamp."""
        target_timestamp = 1751360400  # 2025-06-30 23:00:00

        with patch.object(self.analytics_service, '_get_training_dataset_info') as mock_info, \
             patch.object(self.analytics_service, '_read_all_timeframes_by_timestamp') as mock_read:

            mock_info.return_value = {'run_id': 89}
            mock_read.return_value = self.sample_multi_timeframe

            result = self.analytics_service.get_training_dataset_multi_timeframe_by_timestamp(
                dataset_id=65,
                sequence_id="AAPL_20250701_000000_20250906_000000",
                timestamp=target_timestamp
            )

            # Verify response structure
            self.assertIn('success', result)
            self.assertTrue(result['success'])
            self.assertIn('timestamp', result)
            self.assertIn('ohlc_data', result)

            # Verify target timestamp
            self.assertEqual(result['timestamp'], target_timestamp)

            # Verify all expected timeframes present (excluding 1h)
            expected_timeframes = {'5m', '15m', '1d', '1w'}
            self.assertEqual(set(result['ohlc_data'].keys()), expected_timeframes)

            # Verify each timeframe has 21 bars
            for timeframe, data in result['ohlc_data'].items():
                self.assertEqual(len(data), 21, f"{timeframe} should have 21 bars")

    def test_timestamp_synchronization_logic(self):
        """Test timestamp-based synchronization across timeframes."""
        target_timestamp = 1751360400

        # Mock different timeframe data with varying timestamps
        mock_timeframe_data = {
            "5m": [
                {"timestamp": 1751360100, "open": 230.7},  # 5 min before
                {"timestamp": 1751360400, "open": 230.8},  # Exact match
                {"timestamp": 1751360700, "open": 230.9}   # 5 min after
            ],
            "15m": [
                {"timestamp": 1751359500, "open": 230.5},  # 15 min before
                {"timestamp": 1751360400, "open": 230.8},  # Exact match
                {"timestamp": 1751361300, "open": 231.0}   # 15 min after
            ]
        }

        # Test timestamp matching logic
        closest_5m_idx = self.analytics_service._find_closest_timestamp_index(
            mock_timeframe_data["5m"], target_timestamp
        )
        closest_15m_idx = self.analytics_service._find_closest_timestamp_index(
            mock_timeframe_data["15m"], target_timestamp
        )

        # Both should find index 1 (exact match)
        self.assertEqual(closest_5m_idx, 1)
        self.assertEqual(closest_15m_idx, 1)

    def test_edge_cases_and_error_handling(self):
        """Test edge cases and error handling for navigation APIs."""

        # Test invalid dataset ID
        with patch.object(self.analytics_service, '_get_training_dataset_info') as mock_info:
            mock_info.return_value = None

            result = self.analytics_service.get_training_dataset_1h_navigation(
                dataset_id=999,  # Non-existent
                sequence_id="INVALID_SEQUENCE",
                row_index=10
            )

            self.assertIn('error', result)
            self.assertFalse(result.get('success', False))

        # Test invalid row index (negative)
        with patch.object(self.analytics_service, '_get_training_dataset_info') as mock_info, \
             patch.object(self.analytics_service, '_read_arrayrecord_1h_sequence') as mock_read:

            mock_info.return_value = {'run_id': 89}
            mock_read.return_value = self.sample_1h_data

            result = self.analytics_service.get_training_dataset_1h_navigation(
                dataset_id=65,
                sequence_id="AAPL_20250701_000000_20250906_000000",
                row_index=-1  # Invalid negative index
            )

            # Should handle gracefully, default to 0
            self.assertTrue(result.get('success', False))
            self.assertGreaterEqual(result.get('current_position', -1), 0)

        # Test row index beyond data length
        with patch.object(self.analytics_service, '_get_training_dataset_info') as mock_info, \
             patch.object(self.analytics_service, '_read_arrayrecord_1h_sequence') as mock_read:

            mock_info.return_value = {'run_id': 89}
            mock_read.return_value = self.sample_1h_data  # Only 3 bars

            result = self.analytics_service.get_training_dataset_1h_navigation(
                dataset_id=65,
                sequence_id="AAPL_20250701_000000_20250906_000000",
                row_index=100  # Beyond data length
            )

            # Should handle gracefully, use last available index
            self.assertTrue(result.get('success', False))
            self.assertLess(result.get('current_position', 100), len(self.sample_1h_data))

    def test_api_response_format_compliance(self):
        """Test API responses comply with expected format."""

        with patch.object(self.analytics_service, '_get_training_dataset_info') as mock_info, \
             patch.object(self.analytics_service, '_read_arrayrecord_1h_sequence') as mock_read:

            mock_info.return_value = {
                'id': 65,
                'dataset_name': 'test_dataset',
                'run_id': 89,
                'symbols': 'AAPL'
            }
            mock_read.return_value = self.sample_1h_data

            # Test 1-hour navigation response format
            result = self.analytics_service.get_training_dataset_1h_navigation(65, "AAPL_TEST", 1)

            required_fields = ['success', 'timestamp', 'table_data', 'current_position',
                             'sequence_id', 'dataset_name']
            for field in required_fields:
                self.assertIn(field, result, f"Missing required field: {field}")

            # Test timestamp format (Unix epoch)
            self.assertIsInstance(result['timestamp'], int)
            self.assertGreater(result['timestamp'], 1700000000)  # Reasonable timestamp

            # Test table data format
            self.assertIsInstance(result['table_data'], list)
            if result['table_data']:
                bar = result['table_data'][0]
                required_bar_fields = ['timestamp', 'open', 'high', 'low', 'close']
                for field in required_bar_fields:
                    self.assertIn(field, bar, f"Missing bar field: {field}")

        # Test multi-timeframe response format
        with patch.object(self.analytics_service, '_get_training_dataset_info') as mock_info, \
             patch.object(self.analytics_service, '_read_all_timeframes_by_timestamp') as mock_read:

            mock_info.return_value = {'run_id': 89}
            mock_read.return_value = self.sample_multi_timeframe

            result = self.analytics_service.get_training_dataset_multi_timeframe_by_timestamp(
                65, "AAPL_TEST", 1751360400
            )

            required_fields = ['success', 'timestamp', 'ohlc_data', 'available_timeframes']
            for field in required_fields:
                self.assertIn(field, result, f"Missing required field: {field}")

            # Test timeframe data format
            self.assertIsInstance(result['ohlc_data'], dict)
            for timeframe, data in result['ohlc_data'].items():
                self.assertIsInstance(data, list)
                self.assertIn(timeframe, ['5m', '15m', '1d', '1w'])

if __name__ == '__main__':
    unittest.main()