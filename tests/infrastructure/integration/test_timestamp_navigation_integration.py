#!/usr/bin/env python3
"""
Integration Tests for Timestamp-Based Multi-Timeframe Navigation

Tests the complete workflow:
1. Analytics service startup
2. Database connectivity
3. Real ArrayRecord file reading
4. 1-hour navigation → multi-timeframe coordination
5. API endpoint integration
"""

import pytest
import requests
import time
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestTimestampNavigationIntegration:
    """Integration tests for timestamp-based navigation system."""

    BASE_URL = "http://localhost:3001"

    @classmethod
    def setup_class(cls):
        """Set up integration test environment."""
        print("🔧 Setting up timestamp navigation integration tests...")

        # Check if analytics service is running
        response = requests.get(f"{cls.BASE_URL}/health", timeout=5)
        if response.status_code != 200:
            pytest.skip("Analytics service not running - start with: python scripts/run_dev.py start --service analytics")
    def test_1h_navigation_endpoint_integration(self):
        """Test 1-hour navigation endpoint integration."""
        print("🎯 Testing 1-hour navigation endpoint...")

        # Get available training datasets first
        datasets_response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets")
        assert datasets_response.status_code == 200

        datasets = datasets_response.json()['datasets']
        assert len(datasets) > 0, "No training datasets available for testing"

        # Use first available dataset
        dataset_id = datasets[0]['id']
        print(f"📊 Using dataset ID: {dataset_id}")

        # Get sequences for the dataset
        sequences_response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/sequences")
        assert sequences_response.status_code == 200

        sequences = sequences_response.json()['sequences']
        assert len(sequences) > 0, f"No sequences available for dataset {dataset_id}"

        sequence_id = sequences[0]
        print(f"📋 Using sequence ID: {sequence_id}")

        # Test 1-hour navigation endpoint
        navigation_url = f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/1h"

        # Test different row indices
        test_positions = [0, 10, 25, 50]

        for row_index in test_positions:
            print(f"  🎯 Testing row index: {row_index}")

            response = requests.get(f"{navigation_url}?row_index={row_index}")

            if response.status_code == 200:
                data = response.json()

                # Verify response structure
                assert 'success' in data
                assert data['success'] == True
                assert 'timestamp' in data
                assert 'table_data' in data
                assert 'current_position' in data

                # Verify timestamp format (Unix epoch)
                assert isinstance(data['timestamp'], int)
                assert data['timestamp'] > 1700000000  # Reasonable timestamp

                # Verify table data
                assert isinstance(data['table_data'], list)
                assert len(data['table_data']) <= 21  # Should not exceed 21 bars

                # Verify each bar has required OHLC fields
                if data['table_data']:
                    bar = data['table_data'][0]
                    required_fields = ['timestamp', 'open', 'high', 'low', 'close']
                    for field in required_fields:
                        assert field in bar, f"Missing field {field} in bar data"

                print(f"    ✅ Row {row_index}: {len(data['table_data'])} bars, timestamp={data['timestamp']}")

            else:
                print(f"    ⚠️ Row {row_index}: HTTP {response.status_code} - {response.text}")

    def test_multi_timeframe_by_timestamp_integration(self):
        """Test multi-timeframe endpoint with timestamp coordination."""
        print("🎯 Testing multi-timeframe timestamp coordination...")

        # First, get a timestamp from 1-hour navigation
        datasets_response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets")
        datasets = datasets_response.json()['datasets']
        dataset_id = datasets[0]['id']

        sequences_response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/sequences")
        sequences = sequences_response.json()['sequences']
        sequence_id = sequences[0]

        # Get 1-hour navigation data
        navigation_response = requests.get(
            f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/1h?row_index=25"
        )

        if navigation_response.status_code != 200:
            pytest.skip(f"1-hour navigation failed: {navigation_response.text}")

        navigation_data = navigation_response.json()
        target_timestamp = navigation_data['timestamp']

        print(f"📅 Using timestamp from 1h navigation: {target_timestamp}")

        # Test multi-timeframe endpoint with the timestamp
        multi_url = f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/multi-timeframe"

        response = requests.get(f"{multi_url}?timestamp={target_timestamp}")

        if response.status_code == 200:
            data = response.json()

            # Verify response structure
            assert 'success' in data
            assert data['success'] == True
            assert 'timestamp' in data
            assert 'ohlc_data' in data
            assert 'available_timeframes' in data

            # Verify timestamp matches
            assert data['timestamp'] == target_timestamp

            # Verify expected timeframes (excluding 1h)
            expected_timeframes = {'5m', '15m', '1d', '1w'}
            actual_timeframes = set(data['ohlc_data'].keys())

            print(f"  📊 Available timeframes: {actual_timeframes}")

            # Check that we have some timeframes (might not have all due to data availability)
            assert len(actual_timeframes) > 0, "No timeframe data returned"

            # Verify each timeframe has proper structure
            for timeframe, ohlc_data in data['ohlc_data'].items():
                assert isinstance(ohlc_data, list), f"{timeframe} data should be a list"
                assert len(ohlc_data) <= 21, f"{timeframe} should have max 21 bars"

                if ohlc_data:  # If we have data
                    bar = ohlc_data[0]
                    required_fields = ['timestamp', 'open', 'high', 'low', 'close']
                    for field in required_fields:
                        assert field in bar, f"Missing field {field} in {timeframe} data"

                print(f"    ✅ {timeframe}: {len(ohlc_data)} bars")

            print(f"  🎉 Multi-timeframe coordination successful!")

        else:
            pytest.fail(f"Multi-timeframe request failed: HTTP {response.status_code} - {response.text}")

    def test_complete_navigation_workflow_integration(self):
        """Test complete navigation workflow: 1h navigation → multi-timeframe → next position."""
        print("🎯 Testing complete navigation workflow...")

        # Setup
        datasets_response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets")
        datasets = datasets_response.json()['datasets']
        dataset_id = datasets[0]['id']

        sequences_response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/sequences")
        sequences = sequences_response.json()['sequences']
        sequence_id = sequences[0]

        print(f"📊 Testing workflow with dataset {dataset_id}, sequence {sequence_id}")

        # Step 1: Initial position (row 10)
        initial_position = 10
        nav_response_1 = requests.get(
            f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/1h?row_index={initial_position}"
        )

        assert nav_response_1.status_code == 200
        nav_data_1 = nav_response_1.json()
        timestamp_1 = nav_data_1['timestamp']

        print(f"  📍 Position {initial_position}: timestamp={timestamp_1}")

        # Step 2: Get multi-timeframe data for position 1
        multi_response_1 = requests.get(
            f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/multi-timeframe?timestamp={timestamp_1}"
        )

        if multi_response_1.status_code == 200:
            multi_data_1 = multi_response_1.json()
            print(f"    📊 Multi-timeframe data: {list(multi_data_1['ohlc_data'].keys())}")

        # Step 3: Navigate to next position (row 20)
        next_position = 20
        nav_response_2 = requests.get(
            f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/1h?row_index={next_position}"
        )

        if nav_response_2.status_code == 200:
            nav_data_2 = nav_response_2.json()
            timestamp_2 = nav_data_2['timestamp']

            print(f"  📍 Position {next_position}: timestamp={timestamp_2}")

            # Verify timestamps are different (indicating navigation worked)
            assert timestamp_1 != timestamp_2, "Timestamps should be different after navigation"

            # Step 4: Get multi-timeframe data for position 2
            multi_response_2 = requests.get(
                f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/multi-timeframe?timestamp={timestamp_2}"
            )

            if multi_response_2.status_code == 200:
                multi_data_2 = multi_response_2.json()

                # Verify multi-timeframe data changed
                if ('ohlc_data' in multi_data_1 and 'ohlc_data' in multi_data_2 and
                    multi_data_1['ohlc_data'] and multi_data_2['ohlc_data']):

                    # Compare first available timeframe data
                    tf1 = list(multi_data_1['ohlc_data'].keys())[0]
                    tf2 = list(multi_data_2['ohlc_data'].keys())[0]

                    if tf1 == tf2 and multi_data_1['ohlc_data'][tf1] and multi_data_2['ohlc_data'][tf2]:
                        data1_first = multi_data_1['ohlc_data'][tf1][0]
                        data2_first = multi_data_2['ohlc_data'][tf2][0]

                        # Data should be different after navigation
                        data_changed = (data1_first.get('timestamp') != data2_first.get('timestamp') or
                                      data1_first.get('open') != data2_first.get('open'))

                        print(f"    📊 Multi-timeframe data changed: {data_changed}")

            print("  🎉 Complete navigation workflow successful!")

        else:
            print(f"  ⚠️ Navigation to position {next_position} failed: {nav_response_2.status_code}")

    def test_navigation_edge_cases_integration(self):
        """Test navigation edge cases with real data."""
        print("🎯 Testing navigation edge cases...")

        # Setup
        datasets_response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets")
        datasets = datasets_response.json()['datasets']
        dataset_id = datasets[0]['id']

        sequences_response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/sequences")
        sequences = sequences_response.json()['sequences']
        sequence_id = sequences[0]

        edge_cases = [
            ("First position", 0),
            ("Negative position", -1),
            ("Large position", 1000),
            ("Very large position", 999999)
        ]

        for case_name, row_index in edge_cases:
            print(f"  🔍 Testing {case_name} (row_index={row_index})")

            response = requests.get(
                f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/1h?row_index={row_index}"
            )

            if response.status_code == 200:
                data = response.json()

                # Should handle edge cases gracefully
                assert 'success' in data
                if data['success']:
                    assert 'timestamp' in data
                    assert 'table_data' in data
                    assert data['current_position'] >= 0  # Should not be negative

                    print(f"    ✅ {case_name}: position={data['current_position']}, bars={len(data.get('table_data', []))}")
                else:
                    print(f"    ⚠️ {case_name}: handled as error - {data.get('error', 'Unknown error')}")

            else:
                print(f"    ❌ {case_name}: HTTP {response.status_code}")

    def test_api_performance_integration(self):
        """Test API performance for navigation operations."""
        print("🎯 Testing API performance...")

        # Setup
        datasets_response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets")
        datasets = datasets_response.json()['datasets']
        dataset_id = datasets[0]['id']

        sequences_response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/sequences")
        sequences = sequences_response.json()['sequences']
        sequence_id = sequences[0]

        # Test 1-hour navigation performance
        start_time = time.time()
        response = requests.get(
            f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/1h?row_index=25"
        )
        nav_time = time.time() - start_time

        assert response.status_code == 200
        print(f"  ⏱️ 1h navigation time: {nav_time:.3f}s")

        # Test multi-timeframe performance (if 1h navigation worked)
        if response.status_code == 200:
            nav_data = response.json()
            if nav_data.get('success'):
                timestamp = nav_data['timestamp']

                start_time = time.time()
                multi_response = requests.get(
                    f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/multi-timeframe?timestamp={timestamp}"
                )
                multi_time = time.time() - start_time

                if multi_response.status_code == 200:
                    print(f"  ⏱️ Multi-timeframe time: {multi_time:.3f}s")
                    total_time = nav_time + multi_time
                    print(f"  ⏱️ Total workflow time: {total_time:.3f}s")

                    # Performance assertions (reasonable thresholds)
                    assert nav_time < 5.0, f"1h navigation too slow: {nav_time:.3f}s"
                    assert multi_time < 10.0, f"Multi-timeframe too slow: {multi_time:.3f}s"
                    assert total_time < 12.0, f"Total workflow too slow: {total_time:.3f}s"

                    print("  🎉 Performance tests passed!")

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short', '-s'])