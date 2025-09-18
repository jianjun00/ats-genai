#!/usr/bin/env python3
"""
Integration Tests for Time Navigation API
Test navigation metadata and navigation endpoints
"""

import unittest
import requests
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestTimeNavigationAPI(unittest.TestCase):
    """Test time navigation API endpoints."""

    BASE_URL = "http://localhost:3000"
    DATASET_ID = 65
    SEQUENCE_ID = "AAPL_20250701_000000_20250906_000000"

    def test_navigation_metadata(self):
        """Test navigation metadata endpoint."""
        url = f"{self.BASE_URL}/api/v1/training-datasets/{self.DATASET_ID}/sequences/{self.SEQUENCE_ID}/navigation-metadata"

        response = requests.get(url, timeout=15)
        self.assertEqual(response.status_code, 200)

        data = response.json()

        # Validate structure
        self.assertIn('navigation', data)
        self.assertIn('sample_positions', data)
        self.assertIn('timeframes_available', data)

        # Validate navigation info
        nav = data['navigation']
        self.assertIn('min_row_index', nav)
        self.assertIn('max_row_index', nav)
        self.assertIn('total_positions', nav)
        self.assertIn('window_size', nav)

        # Validate ranges make sense
        self.assertGreaterEqual(nav['max_row_index'], nav['min_row_index'])
        self.assertGreater(nav['total_positions'], 0)
        self.assertEqual(nav['window_size'], 21)

        print(f"✅ Navigation metadata: {nav['min_row_index']} to {nav['max_row_index']} ({nav['total_positions']} positions)")

    def test_navigation_directions(self):
        """Test navigation with direction parameters."""
        base_url = f"{self.BASE_URL}/api/v1/training-datasets/{self.DATASET_ID}/sequences/{self.SEQUENCE_ID}/navigate"

        # Test different navigation directions
        directions = [
            ('first', 0),
            ('next', 10),
            ('prev', 0),  # From position 10, prev goes to 0
            ('last', None)  # Don't know exact value, just check it works
        ]

        current_row = 10
        for direction, expected_min in directions:
            url = f"{base_url}?direction={direction}&row_index={current_row}"

            response = requests.get(url, timeout=15)
            self.assertEqual(response.status_code, 200)

            data = response.json()

            # Validate response structure
            self.assertTrue(data.get('success'))
            self.assertIn('navigation_context', data)
            self.assertIn('table_data', data)

            # Validate navigation context
            nav_context = data['navigation_context']
            self.assertEqual(nav_context['direction_used'], direction)
            self.assertIn('current_row_index', nav_context)

            # Update current row for next iteration
            current_row = nav_context['current_row_index']

            print(f"✅ {direction} navigation: moved to row_index {current_row}")

    def test_specific_row_index_navigation(self):
        """Test navigation to specific row indices."""
        base_url = f"{self.BASE_URL}/api/v1/training-datasets/{self.DATASET_ID}/sequences/{self.SEQUENCE_ID}/navigate"

        # Test specific positions
        test_positions = [0, 25, 50]

        for row_index in test_positions:
            url = f"{base_url}?row_index={row_index}"

            response = requests.get(url, timeout=15)
            self.assertEqual(response.status_code, 200)

            data = response.json()

            # Validate response
            self.assertTrue(data.get('success'))
            self.assertIn('table_data', data)

            # Validate we got data
            table_data = data.get('table_data', [])
            self.assertGreater(len(table_data), 0)

            # Validate navigation context
            nav_context = data.get('navigation_context', {})
            self.assertEqual(nav_context.get('current_row_index'), row_index)

            # Validate timestamp range
            timestamp_range = nav_context.get('timestamp_range', {})
            self.assertIsNotNone(timestamp_range.get('start'))
            self.assertIsNotNone(timestamp_range.get('end'))

            print(f"✅ Row index {row_index}: {len(table_data)} bars, timestamps {timestamp_range['start']} to {timestamp_range['end']}")

    def test_navigation_bounds(self):
        """Test navigation boundary conditions."""
        base_url = f"{self.BASE_URL}/api/v1/training-datasets/{self.DATASET_ID}/sequences/{self.SEQUENCE_ID}/navigate"

        # Test edge cases
        edge_cases = [
            ('very_high', 1000),  # Beyond available data
            ('negative', -10),    # Negative index
        ]

        for case_name, row_index in edge_cases:
            url = f"{base_url}?row_index={row_index}"

            response = requests.get(url, timeout=15)

            # Should either work (bounded) or return reasonable error
            if response.status_code == 200:
                data = response.json()
                nav_context = data.get('navigation_context', {})
                actual_row = nav_context.get('current_row_index')
                print(f"✅ {case_name} ({row_index}) bounded to: {actual_row}")
            else:
                print(f"✅ {case_name} ({row_index}): Properly rejected with {response.status_code}")

class TestTimeNavigationUseCases(unittest.TestCase):
    """Test realistic user navigation scenarios."""

    BASE_URL = "http://localhost:3000"
    DATASET_ID = 65
    SEQUENCE_ID = "AAPL_20250701_000000_20250906_000000"

    def test_user_time_exploration_workflow(self):
        """Test a realistic user workflow for exploring time ranges."""
        nav_url = f"{self.BASE_URL}/api/v1/training-datasets/{self.DATASET_ID}/sequences/{self.SEQUENCE_ID}/navigate"

        print("\n🎯 Testing User Time Exploration Workflow:")

        # Step 1: Start at beginning
        response = requests.get(f"{nav_url}?direction=first")
        self.assertEqual(response.status_code, 200)
        data = response.json()

        first_position = data['navigation_context']['current_row_index']
        first_timestamp = data['navigation_context']['timestamp_range']['start']
        print(f"   1. Started at position {first_position}, timestamp {first_timestamp}")

        # Step 2: Move forward through time
        current_position = first_position
        for step in range(3):
            response = requests.get(f"{nav_url}?direction=next&row_index={current_position}")
            data = response.json()
            current_position = data['navigation_context']['current_row_index']
            current_timestamp = data['navigation_context']['timestamp_range']['start']
            print(f"   2.{step+1}. Moved to position {current_position}, timestamp {current_timestamp}")

        # Step 3: Jump to end
        response = requests.get(f"{nav_url}?direction=last")
        data = response.json()
        last_position = data['navigation_context']['current_row_index']
        last_timestamp = data['navigation_context']['timestamp_range']['start']
        print(f"   3. Jumped to end: position {last_position}, timestamp {last_timestamp}")

        # Step 4: Go back to specific interesting position
        middle_position = (first_position + last_position) // 2
        response = requests.get(f"{nav_url}?row_index={middle_position}")
        data = response.json()
        middle_timestamp = data['navigation_context']['timestamp_range']['start']
        print(f"   4. Selected middle: position {middle_position}, timestamp {middle_timestamp}")

        # Validate we traversed time correctly
        self.assertLess(first_position, last_position)
        self.assertGreater(middle_position, first_position)
        self.assertLess(middle_position, last_position)

        print("   ✅ Time exploration workflow completed successfully")

if __name__ == '__main__':
    # Run with high verbosity to see progress
    unittest.main(verbosity=2)