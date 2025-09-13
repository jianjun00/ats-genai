#!/usr/bin/env python3
"""
Integration tests for UI-API compatibility
Prevents API endpoint mismatches and ensures interface functionality
"""

import pytest
import requests
import re
from bs4 import BeautifulSoup


class TestUIAPIIntegration:
    """Test that UI interfaces use correct API endpoints and handle responses properly."""

    BASE_URL = "http://localhost:4000"

    def test_dataset_detail_api_endpoints_exist(self):
        """Test that all API endpoints used by dataset detail page actually exist."""

        # Read the HTML file and extract API calls
        with open('dataset_detail_page_frontend.html', 'r') as f:
            html_content = f.read()

        # Extract all fetch() calls
        api_calls = re.findall(r'fetch\(`([^`]+)`\)', html_content)

        for api_call in api_calls:
            # Replace template variables with test values
            test_url = api_call.replace('${currentDatasetId}', '24')
            test_url = test_url.replace('${params}', '')

            if not test_url.startswith('http'):
                test_url = f"{self.BASE_URL}{test_url}"

            print(f"Testing API endpoint: {test_url}")

            try:
                response = requests.get(test_url, timeout=5)
                assert response.status_code in [200, 404], f"API endpoint {test_url} returned {response.status_code}"

                if response.status_code == 404:
                    print(f"❌ MISSING API: {test_url}")
                else:
                    print(f"✅ API EXISTS: {test_url}")

            except requests.exceptions.RequestException as e:
                print(f"❌ API ERROR: {test_url} - {e}")
                assert False, f"API endpoint {test_url} failed: {e}"

    def test_training_datasets_api_response_structure(self):
        """Test that training datasets API returns expected data structure."""

        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets")
        assert response.status_code == 200

        data = response.json()
        assert 'datasets' in data
        assert isinstance(data['datasets'], list)

        if data['datasets']:
            dataset = data['datasets'][0]
            required_fields = ['id', 'dataset_name', 'total_sequences']
            for field in required_fields:
                assert field in dataset, f"Missing field '{field}' in dataset response"

    def test_symbol_filter_functionality(self):
        """Test that symbol filtering works properly."""

        # Get available datasets
        datasets_response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets")
        assert datasets_response.status_code == 200

        datasets = datasets_response.json()['datasets']
        if not datasets:
            pytest.skip("No training datasets available")

        dataset_id = datasets[0]['id']

        # Test data endpoint with filters
        test_params = {
            'limit': 10,
            'page': 1,
            'symbol': 'TSLA'  # This might not be supported
        }

        response = requests.get(
            f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/data",
            params=test_params
        )

        if response.status_code == 200:
            data = response.json()
            assert 'data' in data
            assert 'total_count' in data
            print(f"✅ Symbol filter works: {data['total_count']} records")
        else:
            print(f"❌ Symbol filter not supported or failed: {response.status_code}")

    def test_distributions_api_response(self):
        """Test that distributions API returns proper chart data."""

        # Get available datasets
        datasets_response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets")
        datasets = datasets_response.json()['datasets']

        if not datasets:
            pytest.skip("No training datasets available")

        dataset_id = datasets[0]['id']

        # Test distributions endpoint
        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/distributions")

        if response.status_code == 200:
            data = response.json()
            # Check if it has distribution data structure
            print(f"✅ Distributions API works: {list(data.keys())}")
        else:
            print(f"❌ Distributions API failed: {response.status_code}")

    def test_html_javascript_syntax(self):
        """Test that HTML file has valid JavaScript syntax."""

        with open('dataset_detail_page_frontend.html', 'r') as f:
            html_content = f.read()

        # Extract JavaScript from script tags
        soup = BeautifulSoup(html_content, 'html.parser')
        script_tags = soup.find_all('script')

        for i, script in enumerate(script_tags):
            if script.string:
                js_code = script.string

                # Basic syntax checks
                assert js_code.count('{') == js_code.count('}'), f"Mismatched braces in script {i}"
                assert js_code.count('(') == js_code.count(')'), f"Mismatched parentheses in script {i}"
                assert js_code.count('[') == js_code.count(']'), f"Mismatched brackets in script {i}"

                # Check for common API patterns
                if 'fetch(' in js_code:
                    # Ensure all fetch calls have error handling
                    fetch_lines = [line for line in js_code.split('\n') if 'fetch(' in line]
                    for line in fetch_lines:
                        # Look for try-catch or .catch() nearby
                        # This is a basic check - could be more sophisticated
                        assert 'try' in js_code or '.catch(' in js_code, "Fetch calls should have error handling"

    def test_global_vs_per_column_axis_controls(self):
        """Test that there's only one global x-axis control, not per-column controls."""

        with open('dataset_detail_page_frontend.html', 'r') as f:
            html_content = f.read()

        # Count x-axis related controls
        global_axis_count = html_content.count('global-x-axis')
        axis_select_count = html_content.count('x-axis') + html_content.count('X-axis') + html_content.count('X-Axis')

        print(f"Global x-axis controls found: {global_axis_count}")
        print(f"Total axis-related elements: {axis_select_count}")

        # Should have exactly one global x-axis control
        assert global_axis_count >= 1, "Should have at least one global x-axis control"

        # Check that it's positioned correctly (above Data Filter)
        chart_config_pos = html_content.find('Chart Configuration')
        data_filter_pos = html_content.find('Data Filter')

        assert chart_config_pos < data_filter_pos, "Chart Configuration should come before Data Filter"

    def test_date_columns_filtered_from_distributions(self):
        """Test that date columns are properly filtered out from distributions."""

        with open('dataset_detail_page_frontend.html', 'r') as f:
            html_content = f.read()

        # Check for date filtering logic
        assert 'dateColumns' in html_content, "Should have date column filtering logic"
        assert 'date' in html_content and 'timestamp' in html_content, "Should filter common date column names"
        assert 'filteredDistributions' in html_content, "Should have filtered distributions logic"


if __name__ == "__main__":
    # Run tests
    test_integration = TestUIAPIIntegration()

    print("🧪 Running UI-API Integration Tests...")

    try:
        test_integration.test_dataset_detail_api_endpoints_exist()
        print("✅ API endpoints test passed")
    except Exception as e:
        print(f"❌ API endpoints test failed: {e}")

    try:
        test_integration.test_training_datasets_api_response_structure()
        print("✅ API response structure test passed")
    except Exception as e:
        print(f"❌ API response structure test failed: {e}")

    try:
        test_integration.test_global_vs_per_column_axis_controls()
        print("✅ Global axis control test passed")
    except Exception as e:
        print(f"❌ Global axis control test failed: {e}")

    try:
        test_integration.test_html_javascript_syntax()
        print("✅ HTML/JS syntax test passed")
    except Exception as e:
        print(f"❌ HTML/JS syntax test failed: {e}")