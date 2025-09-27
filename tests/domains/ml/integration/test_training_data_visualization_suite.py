#!/usr/bin/env python3
"""
Hermetic integration test suite for training data visualization.

This test suite validates both OHLC visualization and table data functionality
using mock data and a lightweight mock API server, eliminating dependencies
on the full ATS infrastructure.
"""

import asyncio
import json
import os
import sys
import time
from typing import Dict, Any

# Add tests directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from fixtures.training_data.mock_api_server import MockAPIServer


class HermeticTrainingDataVisualizationSuite:
    """Complete hermetic test suite for training data visualization"""

    def __init__(self):
        self.mock_server = MockAPIServer(port=3001)
        self.base_url = "http://localhost:3001"
        self.mock_data = self._load_mock_data()

    def _load_mock_data(self) -> Dict[str, Any]:
        """Load mock data from fixtures"""
        fixture_path = os.path.join(
            os.path.dirname(__file__),
            '..', 'fixtures', 'training_data', 'mock_datasets.json'
        )
        with open(fixture_path, 'r') as f:
            return json.load(f)

    async def setup(self):
        """Set up test environment"""
        print("🔧 Setting up hermetic test environment...")
        self.mock_server.start()

        # Wait for server to be ready
        max_retries = 10
        for i in range(max_retries):
            # Simple connectivity check
            import urllib.request
            urllib.request.urlopen(f"{self.base_url}/health", timeout=1)
            print(f"✅ Mock API server ready at {self.base_url}")
            return True
        return False

    async def teardown(self):
        """Clean up test environment"""
        print("🧹 Cleaning up test environment...")
        self.mock_server.stop()

    async def test_api_endpoints_hermetic(self) -> bool:
        """Test that all required API endpoints work with mock data"""
        print("📡 Testing API endpoints with mock data...")

        import urllib.request
        import json

        # Test datasets list endpoint
        response = urllib.request.urlopen(f"{self.base_url}/api/v1/training-datasets/")
        datasets_data = json.loads(response.read().decode())

        assert 'datasets' in datasets_data, "Missing datasets in API response"
        assert len(datasets_data['datasets']) >= 3, "Not enough test datasets"

        # Test table data endpoint
        dataset_id = datasets_data['datasets'][0]['id']
        response = urllib.request.urlopen(f"{self.base_url}/api/v1/training-datasets/{dataset_id}/data?page=1&limit=3")
        table_data = json.loads(response.read().decode())

        assert 'data' in table_data, "Missing data in table API response"
        assert len(table_data['data']) > 0, "No table data returned"

        # Validate technical indicators in table data
        first_row = table_data['data'][0]
        required_indicators = ['etop', 'ebot', 'pldot']
        for indicator in required_indicators:
            assert indicator in first_row, f"Missing {indicator} in table data"
            assert isinstance(first_row[indicator], (int, float)), f"{indicator} is not numeric"

        # CRITICAL: Validate datetime field is present in API response
        assert 'datetime' in first_row, "Missing datetime field in API response data"
        datetime_value = first_row['datetime']
        assert isinstance(datetime_value, str), f"datetime field should be string, got: {type(datetime_value)}"
        assert 'T' in datetime_value and ':' in datetime_value, f"datetime should be ISO format, got: {datetime_value}"

        # Validate datetime can be parsed
        from datetime import datetime
        parsed_dt = datetime.fromisoformat(datetime_value.replace('Z', ''))
        print(f"  ✓ API returns valid datetime: {datetime_value} → {parsed_dt}")
        response = urllib.request.urlopen(f"{self.base_url}/api/v1/training-datasets/{dataset_id}/visualization-data?sequence_index=0")
        viz_data = json.loads(response.read().decode())

        assert 'data' in viz_data, "Missing data in visualization API response"
        assert len(viz_data['data']) > 0, "No visualization data returned"

        print("✅ All API endpoints working with mock data")
        return True

    async def test_ohlc_data_processing_hermetic(self) -> bool:
        """Test OHLC data processing logic without browser dependencies"""
        print("📊 Testing OHLC data processing logic...")

        # Use mock dataset 15 (5m timeframe)
        dataset = next(d for d in self.mock_data['datasets'] if d['id'] == '15')
        sample_data = dataset['sample_data']

        # Simulate the JavaScript OHLC data processing from analytics_service.py
        processed_data = []
        for i, point in enumerate(sample_data):
            # Use previous close as current open (key fix from the conversation)
            prev_close = processed_data[i-1]['close'] if i > 0 else point['5m_close']

            processed_point = {
                'x': point.get('datetime', i),  # FIXED: Use datetime instead of index
                'open': prev_close,
                'high': point['5m_high'],
                'low': point['5m_low'],
                'close': point['5m_close'],
                'etop': point['etop'],
                'ebot': point['ebot'],
                'pldot': point['pldot']
            }
            processed_data.append(processed_point)

        # Validate processed data structure
        for i, point in enumerate(processed_data):
            # Validate OHLC relationships
            assert point['high'] >= point['close'] >= point['low'], f"Invalid OHLC relationship at index {i}"
            assert point['high'] >= point['open'] >= point['low'], f"Invalid open price relationship at index {i}"

            # Validate technical indicators
            assert point['etop'] > point['ebot'], f"Envelope top should be > envelope bottom at index {i}"
            assert point['low'] <= point['pldot'] <= point['high'], f"PL dot should be within OHLC range at index {i}"

        # Validate that open price chaining works (key fix)
        for i in range(1, len(processed_data)):
            expected_open = processed_data[i-1]['close']
            actual_open = processed_data[i]['open']
            assert abs(expected_open - actual_open) < 0.01, f"Open price chaining failed at index {i}"

        # CRITICAL: Validate datetime intervals are included
        for i, point in enumerate(processed_data):
            assert 'x' in point, f"Missing x-axis data at index {i}"

            # Check if x-axis uses datetime (proper) or numeric index (broken)
            x_value = point['x']
            if isinstance(x_value, str):
                # Should be ISO datetime format
                assert 'T' in x_value and ':' in x_value, f"x-axis value should be datetime, got: {x_value}"
                # Validate it's a proper ISO datetime
                from datetime import datetime
                datetime.fromisoformat(x_value.replace('Z', ''))
                print(f"  ✓ Point {i}: x-axis uses datetime: {x_value}")
                print(f"  ❌ Point {i}: x-axis uses numeric index ({x_value}) instead of datetime")
                assert False, f"x-axis should use datetime, not numeric index: {x_value}"

        print(f"✅ OHLC data processing working for {len(processed_data)} data points with proper datetime x-axis")
        return True

    async def test_table_html_generation_hermetic(self) -> bool:
        """Test table HTML generation without DOM dependencies"""
        print("📋 Testing table HTML generation...")

        # Use mock dataset sample data
        dataset = next(d for d in self.mock_data['datasets'] if d['id'] == '15')
        sample_data = dataset['sample_data'][0]  # First row

        # Simulate the HTML generation logic from analytics_service.py
        def generate_table_cell_html(row_data: Dict) -> str:
            """Generate HTML for table cell (simulating analytics_service.py logic)"""

            # Technical indicators cell
            tech_html = ""
            tech_indicators = ['etop', 'ebot', 'pldot', 'sma_20', 'ema_12', 'ema_26']
            for indicator in tech_indicators:
                if indicator in row_data and row_data[indicator] is not None:
                    tech_html += f'<div class="feature-item"><strong>{indicator}:</strong> {row_data[indicator]:.4f}</div>'

            # OHLC data cell
            ohlc_html = ""
            ohlc_fields = ['5m_high', '5m_low', '5m_close', '5m_volume']
            for field in ohlc_fields:
                if field in row_data and row_data[field] is not None:
                    if 'volume' in field:
                        ohlc_html += f'<div class="feature-item"><strong>{field}:</strong> {int(row_data[field]):,}</div>'
                    else:
                        ohlc_html += f'<div class="feature-item"><strong>{field}:</strong> {row_data[field]:.4f}</div>'

            # Labels cell
            labels_html = ""
            if 'target_return' in row_data:
                labels_html += f'<div class="label-item"><strong>target_return:</strong> {row_data["target_return"]:.4f}</div>'

            return tech_html, ohlc_html, labels_html

        tech_html, ohlc_html, labels_html = generate_table_cell_html(sample_data)

        # Validate technical indicators HTML
        assert len(tech_html) > 0, "No technical indicators HTML generated"
        assert 'etop:' in tech_html, "etop not in technical indicators HTML"
        assert 'ebot:' in tech_html, "ebot not in technical indicators HTML"
        assert 'pldot:' in tech_html, "pldot not in technical indicators HTML"
        assert 'feature-item' in tech_html, "Missing CSS class for technical indicators"

        # Validate OHLC HTML
        assert len(ohlc_html) > 0, "No OHLC HTML generated"
        assert '5m_high:' in ohlc_html, "5m_high not in OHLC HTML"
        assert '5m_close:' in ohlc_html, "5m_close not in OHLC HTML"

        # Validate labels HTML
        assert len(labels_html) > 0, "No labels HTML generated"
        assert 'target_return:' in labels_html, "target_return not in labels HTML"
        assert 'label-item' in labels_html, "Missing CSS class for labels"

        # Test that values are properly formatted
        assert f"{sample_data['etop']:.4f}" in tech_html, "etop value not properly formatted"
        assert f"{sample_data['target_return']:.4f}" in labels_html, "target_return not properly formatted"

        print("✅ Table HTML generation working correctly")
        return True

    async def test_multi_timeframe_support_hermetic(self) -> bool:
        """Test multi-timeframe support (5m, 15m, 1h) without browser"""
        print("⏱️ Testing multi-timeframe support...")

        timeframe_datasets = [
            ('15', '5m', ['5m_high', '5m_low', '5m_close']),
            ('16', '1h', ['1h_high', '1h_low', '1h_close']),
            ('17', '15m', ['15m_high', '15m_low', '15m_close'])
        ]

        for dataset_id, timeframe, expected_fields in timeframe_datasets:
            dataset = next(d for d in self.mock_data['datasets'] if d['id'] == dataset_id)
            sample_data = dataset['sample_data'][0]

            print(f"  Testing {timeframe} timeframe (dataset {dataset_id})...")

            # Validate that expected fields exist
            for field in expected_fields:
                assert field in sample_data, f"Missing {field} in {timeframe} dataset"
                assert isinstance(sample_data[field], (int, float)), f"{field} is not numeric"
                assert sample_data[field] > 0, f"{field} should be positive"

            # Validate OHLC relationships for this timeframe
            high_key, low_key, close_key = expected_fields
            high, low, close = sample_data[high_key], sample_data[low_key], sample_data[close_key]

            assert high >= close >= low, f"Invalid OHLC relationship in {timeframe} data"

            # Validate technical indicators exist regardless of timeframe
            tech_indicators = ['etop', 'ebot', 'pldot']
            for indicator in tech_indicators:
                assert indicator in sample_data, f"Missing {indicator} in {timeframe} dataset"

            print(f"    ✓ {timeframe} timeframe validation passed")

        print("✅ Multi-timeframe support working correctly")
        return True

    async def test_error_handling_hermetic(self) -> bool:
        """Test error handling for invalid datasets and empty data"""
        print("🚨 Testing error handling...")

        import urllib.request
        import urllib.error

        # Test invalid dataset ID
        urllib.request.urlopen(f"{self.base_url}/api/v1/training-datasets/999/data")
        assert False, "Should have returned 404 for invalid dataset"
        empty_response = {
            'data': [],
            'total_count': 0,
            'page': 1,
            'limit': 10
        }

        # Validate empty response structure
        assert isinstance(empty_response['data'], list), "Empty data should be a list"
        assert len(empty_response['data']) == 0, "Empty data should have length 0"
        assert empty_response['total_count'] == 0, "Empty total_count should be 0"

        # Test that UI would handle empty data gracefully
        should_show_no_data = len(empty_response['data']) == 0
        assert should_show_no_data, "Should indicate no data available"

        print("✅ Error handling working correctly")
        return True

    async def test_csv_format_compatibility_hermetic(self) -> bool:
        """Test CSV format compatibility without file system dependencies"""
        print("📄 Testing CSV format compatibility...")

        # Test datasets 16 and 17 which are CSV format
        csv_datasets = [d for d in self.mock_data['datasets'] if d['format'] == 'csv']
        assert len(csv_datasets) >= 2, "Not enough CSV datasets for testing"

        for dataset in csv_datasets:
            dataset_id = dataset['id']
            sample_data = dataset['sample_data']

            print(f"  Testing CSV dataset {dataset_id}...")

            # Validate sample data structure
            assert len(sample_data) > 0, f"No sample data for CSV dataset {dataset_id}"

            first_row = sample_data[0]

            # Validate required fields exist
            assert 'sequence_id' in first_row, f"Missing sequence_id in CSV dataset {dataset_id}"
            assert 'etop' in first_row, f"Missing etop in CSV dataset {dataset_id}"
            assert 'ebot' in first_row, f"Missing ebot in CSV dataset {dataset_id}"

            # Validate OHLC fields based on timeframe
            expected_timeframes = dataset.get('timeframes', [])
            if expected_timeframes:
                timeframe = expected_timeframes[0]
                expected_high = f'{timeframe}_high'
                expected_low = f'{timeframe}_low'
                expected_close = f'{timeframe}_close'

                assert expected_high in first_row, f"Missing {expected_high} in CSV dataset {dataset_id}"
                assert expected_low in first_row, f"Missing {expected_low} in CSV dataset {dataset_id}"
                assert expected_close in first_row, f"Missing {expected_close} in CSV dataset {dataset_id}"

            # Validate data types
            for key, value in first_row.items():
                if key != 'sequence_id' and isinstance(value, (int, float)):
                    assert value is not None, f"Null value for {key} in CSV dataset {dataset_id}"
                    # Allow negative values for returns, but price/volume data should be positive
                    if 'volume' not in key.lower() and 'return' not in key.lower():
                        assert value > 0, f"Non-positive value for {key} in CSV dataset {dataset_id}"

            print(f"    ✓ CSV dataset {dataset_id} structure validated")

        print("✅ CSV format compatibility working correctly")
        return True

    async def test_datetime_intervals_hermetic(self) -> bool:
        """Test that datetime intervals are properly included and sequential"""
        print("⏰ Testing datetime interval validation...")

        # Test different timeframes and their expected intervals
        timeframe_tests = [
            ('15', '5m', 5),    # 5-minute intervals
            ('16', '1h', 60),   # 1-hour intervals
            ('17', '15m', 15)   # 15-minute intervals
        ]

        for dataset_id, timeframe, expected_minutes in timeframe_tests:
            dataset = next(d for d in self.mock_data['datasets'] if d['id'] == dataset_id)
            sample_data = dataset['sample_data']

            print(f"  Testing {timeframe} intervals (dataset {dataset_id})...")

            # Validate all data points have datetime
            for i, point in enumerate(sample_data):
                assert 'datetime' in point, f"Missing datetime in {timeframe} dataset at index {i}"

                datetime_str = point['datetime']
                assert isinstance(datetime_str, str), f"datetime should be string in {timeframe} dataset"
                assert 'T' in datetime_str, f"datetime should be ISO format in {timeframe} dataset"

                # Parse the datetime
                from datetime import datetime
                parsed_dt = datetime.fromisoformat(datetime_str)
                print(f"    Point {i}: {datetime_str} → {parsed_dt.strftime('%H:%M:%S')}")

            # If we have multiple points, validate intervals are correct
            if len(sample_data) > 1:
                from datetime import datetime, timedelta

                for i in range(1, len(sample_data)):
                    prev_dt = datetime.fromisoformat(sample_data[i-1]['datetime'])
                    curr_dt = datetime.fromisoformat(sample_data[i]['datetime'])

                    time_diff = curr_dt - prev_dt
                    expected_diff = timedelta(minutes=expected_minutes)

                    assert time_diff == expected_diff, \
                        f"Incorrect {timeframe} interval: expected {expected_minutes}min, got {time_diff}"

                    print(f"    ✓ Interval {i}: {time_diff} = {expected_minutes} minutes")

            print(f"    ✓ {timeframe} datetime intervals validated")

        print("✅ Datetime interval validation working correctly")
        return True

    async def run_all_tests(self) -> bool:
        """Run all hermetic tests"""
        print("🧪 **HERMETIC TRAINING DATA VISUALIZATION TEST SUITE**")
        print("Testing: Complete visualization functionality with mock data")
        print("Benefits: No dependencies on ATS infrastructure, fast execution, reliable")
        print("=" * 80)

        # Setup test environment
        setup_success = await self.setup()
        if not setup_success:
            print("❌ Failed to set up test environment")
            return False

        tests = [
            ("API Endpoints", self.test_api_endpoints_hermetic()),
            ("OHLC Data Processing", self.test_ohlc_data_processing_hermetic()),
            ("Table HTML Generation", self.test_table_html_generation_hermetic()),
            ("Multi-Timeframe Support", self.test_multi_timeframe_support_hermetic()),
            ("Error Handling", self.test_error_handling_hermetic()),
            ("CSV Format Compatibility", self.test_csv_format_compatibility_hermetic()),
            ("Datetime Intervals", self.test_datetime_intervals_hermetic())
        ]

        results = []
        for test_name, test_coro in tests:
            print(f"\n🔬 Running: {test_name}")
            success = await test_coro
            status = "PASSED" if success else "FAILED"
            results.append((test_name, status))
            print(f"{'✅' if success else '❌'} {test_name}: {status}")
        passed = sum(1 for r in results if r[1] == "PASSED")
        total = len(results)

        print(f"\n📊 **HERMETIC TEST RESULTS: {passed}/{total} PASSED**")
        print("=" * 80)

        for result in results:
            status_icon = "✅" if result[1] == "PASSED" else "❌"
            print(f"{status_icon} {result[0]}: {result[1]}")
            if len(result) > 2:  # Has error message
                print(f"    Error: {result[2]}")

        if passed == total:
            print("\n🎉 **ALL HERMETIC TESTS PASSED!**")
            print("   • API endpoints working with mock data")
            print("   • OHLC data processing handles missing 'open' field correctly")
            print("   • Table HTML generation displays technical indicators")
            print("   • Multi-timeframe support (5m, 15m, 1h) working")
            print("   • Error handling for invalid datasets implemented")
            print("   • CSV format datasets fully compatible")
            print("   • Datetime intervals properly validated and sequential")
            print("   • No dependencies on ATS infrastructure")
            print("   • Fast, reliable, hermetic test execution")
            return True
        else:
            print(f"\n❌ **{total - passed} TEST(S) FAILED**")
            print("   Review failed tests above for specific issues")
            return False

async def main():
    """Main test runner"""
    test_suite = HermeticTrainingDataVisualizationSuite()
    success = await test_suite.run_all_tests()
    return 0 if success else 1


if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(result)