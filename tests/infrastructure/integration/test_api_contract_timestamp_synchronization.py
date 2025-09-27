#!/usr/bin/env python3
"""
API Contract Tests for Timestamp Synchronization

Validates the timestamp-based multi-timeframe navigation contract:
1. 1-hour navigation endpoint contract compliance
2. Multi-timeframe endpoint contract compliance
3. Timestamp synchronization between endpoints
4. Data consistency across API calls
5. Performance and reliability requirements
"""

import pytest
import requests
import json
import time
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestAPIContractTimestampSync:
    """API contract tests for timestamp synchronization."""

    BASE_URL = "http://localhost:3001"

    @classmethod
    def setup_class(cls):
        """Set up API contract test environment."""
        print("🔧 Setting up API contract tests...")

        # Verify analytics service is running
        response = requests.get(f"{cls.BASE_URL}/health", timeout=5)
        if response.status_code != 200:
            pytest.skip("Analytics service not running")
        datasets_response = requests.get(f"{cls.BASE_URL}/api/v1/training-datasets")
        datasets = datasets_response.json()['datasets']

        if len(datasets) == 0:
            pytest.skip("No training datasets available")

        cls.test_dataset_id = datasets[0]['id']

        sequences_response = requests.get(f"{cls.BASE_URL}/api/v1/training-datasets/{cls.test_dataset_id}/sequences")
        sequences = sequences_response.json()['sequences']

        if len(sequences) == 0:
            pytest.skip("No sequences available for testing")

        cls.test_sequence_id = sequences[0]

        print(f"📊 Using test dataset {cls.test_dataset_id}, sequence {cls.test_sequence_id}")

    def test_1h_navigation_endpoint_contract(self):
        """Test 1-hour navigation endpoint contract compliance."""
        print("📋 Testing 1-hour navigation endpoint contract...")

        test_positions = [0, 10, 25, 50, 100]

        for row_index in test_positions:
            print(f"  🎯 Testing position {row_index}")

            url = f"{self.BASE_URL}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/1h"
            response = requests.get(f"{url}?row_index={row_index}")

            # Basic HTTP contract
            assert response.status_code in [200, 404], f"Expected 200 or 404, got {response.status_code}"
            assert response.headers.get('content-type') == 'application/json', "Content-Type should be application/json"

            if response.status_code == 200:
                data = response.json()

                # Required fields contract
                required_fields = {
                    'success': bool,
                    'timestamp': int,
                    'table_data': list,
                    'current_position': int,
                    'sequence_id': str,
                    'dataset_name': str
                }

                for field, expected_type in required_fields.items():
                    assert field in data, f"Missing required field: {field}"
                    assert isinstance(data[field], expected_type), f"Field {field} should be {expected_type.__name__}"

                # Timestamp contract validation
                timestamp = data['timestamp']
                assert timestamp > 1700000000, f"Timestamp {timestamp} seems too old"
                assert timestamp < 2000000000, f"Timestamp {timestamp} seems too far in future"

                # Table data contract validation
                table_data = data['table_data']
                assert len(table_data) <= 21, f"Table data should have max 21 bars, got {len(table_data)}"

                if table_data:
                    # Validate bar structure
                    bar = table_data[0]
                    required_bar_fields = {
                        'timestamp': int,
                        'open': (int, float),
                        'high': (int, float),
                        'low': (int, float),
                        'close': (int, float),
                        'volume': int
                    }

                    for field, expected_types in required_bar_fields.items():
                        assert field in bar, f"Missing bar field: {field}"
                        if isinstance(expected_types, tuple):
                            assert isinstance(bar[field], expected_types), f"Bar field {field} should be one of {expected_types}"
                        else:
                            assert isinstance(bar[field], expected_types), f"Bar field {field} should be {expected_types.__name__}"

                    # OHLC validation
                    assert bar['high'] >= bar['open'], "High should be >= Open"
                    assert bar['high'] >= bar['close'], "High should be >= Close"
                    assert bar['low'] <= bar['open'], "Low should be <= Open"
                    assert bar['low'] <= bar['close'], "Low should be <= Close"
                    assert bar['volume'] >= 0, "Volume should be non-negative"

                # Position validation
                current_position = data['current_position']
                assert current_position >= 0, "Current position should be non-negative"

                print(f"    ✅ Position {row_index}: {len(table_data)} bars, timestamp={timestamp}")

            else:
                print(f"    ⚠️ Position {row_index}: Not available (HTTP {response.status_code})")

    def test_multi_timeframe_endpoint_contract(self):
        """Test multi-timeframe endpoint contract compliance."""
        print("📊 Testing multi-timeframe endpoint contract...")

        # First get a timestamp from 1h navigation
        nav_response = requests.get(
            f"{self.BASE_URL}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/1h?row_index=25"
        )

        if nav_response.status_code != 200:
            pytest.skip("Cannot get timestamp from 1h navigation")

        nav_data = nav_response.json()
        test_timestamp = nav_data['timestamp']

        print(f"  📅 Using timestamp {test_timestamp} from 1h navigation")

        # Test multi-timeframe endpoint
        multi_url = f"{self.BASE_URL}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/multi-timeframe"
        response = requests.get(f"{multi_url}?timestamp={test_timestamp}")

        # Basic HTTP contract
        assert response.status_code in [200, 404], f"Expected 200 or 404, got {response.status_code}"
        assert response.headers.get('content-type') == 'application/json', "Content-Type should be application/json"

        if response.status_code == 200:
            data = response.json()

            # Required fields contract
            required_fields = {
                'success': bool,
                'timestamp': int,
                'ohlc_data': dict,
                'available_timeframes': list
            }

            for field, expected_type in required_fields.items():
                assert field in data, f"Missing required field: {field}"
                assert isinstance(data[field], expected_type), f"Field {field} should be {expected_type.__name__}"

            # Timestamp synchronization contract
            returned_timestamp = data['timestamp']
            assert returned_timestamp == test_timestamp, f"Timestamp mismatch: sent {test_timestamp}, got {returned_timestamp}"

            # OHLC data contract validation
            ohlc_data = data['ohlc_data']
            expected_timeframes = {'5m', '15m', '1d', '1w'}  # Excluding 1h

            for timeframe, timeframe_data in ohlc_data.items():
                assert timeframe in expected_timeframes, f"Unexpected timeframe: {timeframe}"
                assert isinstance(timeframe_data, list), f"{timeframe} data should be list"
                assert len(timeframe_data) <= 21, f"{timeframe} should have max 21 bars"

                if timeframe_data:
                    bar = timeframe_data[0]
                    required_bar_fields = {
                        'timestamp': int,
                        'open': (int, float),
                        'high': (int, float),
                        'low': (int, float),
                        'close': (int, float)
                    }

                    for field, expected_types in required_bar_fields.items():
                        assert field in bar, f"Missing bar field {field} in {timeframe}"
                        if isinstance(expected_types, tuple):
                            assert isinstance(bar[field], expected_types), f"{timeframe} bar field {field} wrong type"
                        else:
                            assert isinstance(bar[field], expected_types), f"{timeframe} bar field {field} wrong type"

                print(f"    ✅ {timeframe}: {len(timeframe_data)} bars")

            # Available timeframes should match ohlc_data keys
            available_timeframes = set(data['available_timeframes'])
            actual_timeframes = set(ohlc_data.keys())
            assert available_timeframes == actual_timeframes, f"Available timeframes mismatch: {available_timeframes} vs {actual_timeframes}"

            print(f"  🎉 Multi-timeframe contract validation successful")

        else:
            pytest.skip(f"Multi-timeframe endpoint not available: HTTP {response.status_code}")

    def test_timestamp_synchronization_consistency(self):
        """Test timestamp synchronization consistency between endpoints."""
        print("🔄 Testing timestamp synchronization consistency...")

        # Test multiple positions and verify timestamp consistency
        test_positions = [10, 25, 40]

        for position in test_positions:
            print(f"  🎯 Testing synchronization at position {position}")

            # Step 1: Get timestamp from 1h navigation
            nav_response = requests.get(
                f"{self.BASE_URL}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/1h?row_index={position}"
            )

            if nav_response.status_code != 200:
                continue

            nav_data = nav_response.json()
            if not nav_data.get('success'):
                continue

            source_timestamp = nav_data['timestamp']

            # Step 2: Use timestamp in multi-timeframe endpoint
            multi_response = requests.get(
                f"{self.BASE_URL}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/multi-timeframe?timestamp={source_timestamp}"
            )

            if multi_response.status_code == 200:
                multi_data = multi_response.json()

                if multi_data.get('success'):
                    returned_timestamp = multi_data['timestamp']

                    # Verify exact timestamp synchronization
                    assert source_timestamp == returned_timestamp, f"Timestamp sync failed: {source_timestamp} != {returned_timestamp}"

                    print(f"    ✅ Position {position}: timestamps synchronized ({source_timestamp})")
                else:
                    print(f"    ⚠️ Position {position}: multi-timeframe failed")
            else:
                print(f"    ⚠️ Position {position}: multi-timeframe HTTP {multi_response.status_code}")

    def test_navigation_workflow_integrity(self):
        """Test complete navigation workflow integrity."""
        print("🔄 Testing navigation workflow integrity...")

        # Test navigation sequence: position 20 → 30 → 25
        positions = [20, 30, 25]
        workflow_data = []

        for i, position in enumerate(positions):
            print(f"  📍 Step {i+1}: Navigate to position {position}")

            # Get 1h navigation data
            nav_response = requests.get(
                f"{self.BASE_URL}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/1h?row_index={position}"
            )

            if nav_response.status_code != 200:
                continue

            nav_data = nav_response.json()
            if not nav_data.get('success'):
                continue

            timestamp = nav_data['timestamp']

            # Get multi-timeframe data
            multi_response = requests.get(
                f"{self.BASE_URL}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/multi-timeframe?timestamp={timestamp}"
            )

            step_data = {
                'position': position,
                'timestamp': timestamp,
                'table_rows': len(nav_data.get('table_data', [])),
                'multi_success': multi_response.status_code == 200 and multi_response.json().get('success', False)
            }

            if step_data['multi_success']:
                multi_data = multi_response.json()
                step_data['timeframes'] = list(multi_data['ohlc_data'].keys())
                step_data['total_bars'] = sum(len(bars) for bars in multi_data['ohlc_data'].values())

            workflow_data.append(step_data)
            print(f"    ✅ Position {position}: timestamp={timestamp}, table_rows={step_data['table_rows']}")

        # Validate workflow consistency
        if len(workflow_data) >= 2:
            # Timestamps should be different for different positions
            timestamps = [step['timestamp'] for step in workflow_data]
            unique_timestamps = len(set(timestamps))
            assert unique_timestamps == len(timestamps), f"Expected unique timestamps, got {unique_timestamps}/{len(timestamps)} unique"

            # All steps should have table data
            table_rows = [step['table_rows'] for step in workflow_data]
            assert all(rows > 0 for rows in table_rows), f"All steps should have table data: {table_rows}"

            print(f"  🎉 Workflow integrity verified: {len(workflow_data)} steps, all unique timestamps")

        return workflow_data

    def test_api_response_time_contract(self):
        """Test API response time performance contract."""
        print("⏱️ Testing API response time contract...")

        # Performance thresholds (in seconds)
        NAVIGATION_THRESHOLD = 3.0  # 1h navigation should be < 3s
        MULTI_TIMEFRAME_THRESHOLD = 5.0  # Multi-timeframe should be < 5s
        TOTAL_WORKFLOW_THRESHOLD = 7.0  # Complete workflow should be < 7s

        # Test 1h navigation performance
        start_time = time.time()
        nav_response = requests.get(
            f"{self.BASE_URL}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/1h?row_index=25"
        )
        nav_time = time.time() - start_time

        print(f"  📋 1h navigation time: {nav_time:.3f}s (threshold: {NAVIGATION_THRESHOLD}s)")

        if nav_response.status_code == 200:
            nav_data = nav_response.json()

            if nav_data.get('success'):
                timestamp = nav_data['timestamp']

                # Test multi-timeframe performance
                start_time = time.time()
                multi_response = requests.get(
                    f"{self.BASE_URL}/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/multi-timeframe?timestamp={timestamp}"
                )
                multi_time = time.time() - start_time

                print(f"  📊 Multi-timeframe time: {multi_time:.3f}s (threshold: {MULTI_TIMEFRAME_THRESHOLD}s)")

                total_time = nav_time + multi_time
                print(f"  🔄 Total workflow time: {total_time:.3f}s (threshold: {TOTAL_WORKFLOW_THRESHOLD}s)")

                # Performance contract assertions
                assert nav_time < NAVIGATION_THRESHOLD, f"1h navigation too slow: {nav_time:.3f}s > {NAVIGATION_THRESHOLD}s"
                assert multi_time < MULTI_TIMEFRAME_THRESHOLD, f"Multi-timeframe too slow: {multi_time:.3f}s > {MULTI_TIMEFRAME_THRESHOLD}s"
                assert total_time < TOTAL_WORKFLOW_THRESHOLD, f"Total workflow too slow: {total_time:.3f}s > {TOTAL_WORKFLOW_THRESHOLD}s"

                print("  ✅ Performance contract satisfied")

                return {
                    'navigation_time': nav_time,
                    'multi_timeframe_time': multi_time,
                    'total_time': total_time
                }

        pytest.skip("Could not test performance due to API unavailability")

    def test_error_handling_contract(self):
        """Test error handling contract compliance."""
        print("🚨 Testing error handling contract...")

        error_test_cases = [
            ("Invalid dataset ID", f"/api/v1/training-datasets/99999/sequences/{self.test_sequence_id}/1h?row_index=10"),
            ("Invalid sequence ID", f"/api/v1/training-datasets/{self.test_dataset_id}/sequences/INVALID_SEQ/1h?row_index=10"),
            ("Missing row_index", f"/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/1h"),
            ("Invalid timestamp", f"/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/multi-timeframe?timestamp=invalid"),
            ("Missing timestamp", f"/api/v1/training-datasets/{self.test_dataset_id}/sequences/{self.test_sequence_id}/multi-timeframe")
        ]

        for case_name, url_path in error_test_cases:
            print(f"  🔍 Testing {case_name}")

            full_url = f"{self.BASE_URL}{url_path}"
            response = requests.get(full_url)

            # Error contract validation
            assert response.status_code in [400, 404, 422, 500], f"Expected error status, got {response.status_code}"
            assert response.headers.get('content-type') == 'application/json', "Error response should be JSON"

            error_data = response.json()
            # Error response should have error field
            assert 'error' in error_data, "Error response should have 'error' field"
            assert isinstance(error_data['error'], str), "Error field should be string"
            assert len(error_data['error']) > 0, "Error message should not be empty"

            print(f"    ✅ {case_name}: HTTP {response.status_code}, error: {error_data['error'][:50]}...")

        print("  🎉 Error handling contract validation successful")

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short', '-s'])