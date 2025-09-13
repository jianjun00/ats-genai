#!/usr/bin/env python3
"""
Playwright Tests for Time Navigation UI
Validates time navigation through browser automation
"""

import pytest
from playwright.async_api import async_playwright
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestTimeNavigationPlaywright:
    """Test time navigation through browser automation."""

    DATASET_ID = 65
    SEQUENCE_ID = "AAPL_20250701_000000_20250906_000000"
    BASE_URL = "http://localhost:3000"

    @pytest.mark.asyncio
    async def test_sequence_page_loads_with_navigation_data(self):
        """Test that sequence page loads and displays navigation-ready data."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                # Navigate to sequence page
                url = f"{self.BASE_URL}/training-dataset/{self.DATASET_ID}/sequence/{self.SEQUENCE_ID}"
                await page.goto(url, wait_until='networkidle', timeout=30000)

                # Wait for page to load completely
                await page.wait_for_selector('table', timeout=15000)

                # Verify table data is loaded (should show OHLCV data, not N/A)
                table_rows = await page.locator('tbody tr').count()
                assert table_rows > 0, "No table rows found"

                # Verify we have actual price data, not N/A values
                first_cell_text = await page.locator('tbody tr:first-child td:nth-child(2)').text_content()
                assert first_cell_text and "N/A" not in first_cell_text, f"Price data shows N/A: {first_cell_text}"

                # Verify chart container exists (navigation target)
                chart_container = await page.locator('#chart-container').count()
                assert chart_container > 0, "Chart container not found"

                print("✅ Sequence page loads with valid navigation data")

            finally:
                await browser.close()

    @pytest.mark.asyncio
    async def test_navigation_api_responses_through_browser(self):
        """Test navigation API endpoints through browser network calls."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                # Capture network responses
                navigation_responses = []

                async def handle_response(response):
                    if 'navigation' in response.url or 'multi-timeframe' in response.url:
                        navigation_responses.append({
                            'url': response.url,
                            'status': response.status,
                            'content_type': response.headers.get('content-type', '')
                        })

                page.on('response', handle_response)

                # Navigate to sequence page (triggers API calls)
                url = f"{self.BASE_URL}/training-dataset/{self.DATASET_ID}/sequence/{self.SEQUENCE_ID}"
                await page.goto(url, wait_until='networkidle', timeout=30000)

                # Wait for navigation API calls
                await page.wait_for_timeout(2000)

                # Verify navigation API was called successfully
                nav_api_calls = [r for r in navigation_responses if 'multi-timeframe' in r['url']]
                assert len(nav_api_calls) > 0, "No navigation API calls detected"

                successful_calls = [r for r in nav_api_calls if r['status'] == 200]
                assert len(successful_calls) > 0, f"No successful API calls: {nav_api_calls}"

                # Verify JSON response format
                json_responses = [r for r in successful_calls if 'json' in r['content_type']]
                assert len(json_responses) > 0, "No JSON responses detected"

                print(f"✅ Navigation API calls successful: {len(successful_calls)} calls")

            finally:
                await browser.close()

    @pytest.mark.asyncio
    async def test_navigation_metadata_api_through_browser(self):
        """Test navigation metadata endpoint through browser."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                # Test navigation metadata endpoint directly
                metadata_url = f"{self.BASE_URL}/api/v1/training-datasets/{self.DATASET_ID}/sequences/{self.SEQUENCE_ID}/navigation-metadata"

                response = await page.request.get(metadata_url)
                assert response.status == 200, f"Metadata API failed: {response.status}"

                # Parse JSON response
                data = await response.json()

                # Validate metadata structure
                assert 'navigation' in data, "Navigation info missing from metadata"
                assert 'sample_positions' in data, "Sample positions missing from metadata"
                assert 'timeframes_available' in data, "Timeframes missing from metadata"

                # Validate navigation ranges
                nav_info = data['navigation']
                assert 'min_row_index' in nav_info, "min_row_index missing"
                assert 'max_row_index' in nav_info, "max_row_index missing"
                assert 'total_positions' in nav_info, "total_positions missing"

                # Validate ranges make sense
                assert nav_info['max_row_index'] >= nav_info['min_row_index'], "Invalid row_index range"
                assert nav_info['total_positions'] > 0, "No positions available"

                print(f"✅ Navigation metadata: {nav_info['min_row_index']} to {nav_info['max_row_index']} ({nav_info['total_positions']} positions)")

            finally:
                await browser.close()

    @pytest.mark.asyncio
    async def test_directional_navigation_through_browser(self):
        """Test directional navigation (first, next, prev, last) through browser."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                nav_base_url = f"{self.BASE_URL}/api/v1/training-datasets/{self.DATASET_ID}/sequences/{self.SEQUENCE_ID}/navigate"

                # Test navigation directions
                directions = [
                    ('first', {}),
                    ('next', {'row_index': 10}),
                    ('prev', {'row_index': 20}),
                    ('last', {})
                ]

                navigation_results = []

                for direction, params in directions:
                    # Build URL with parameters
                    url_params = f"direction={direction}"
                    if 'row_index' in params:
                        url_params += f"&row_index={params['row_index']}"

                    nav_url = f"{nav_base_url}?{url_params}"

                    response = await page.request.get(nav_url)
                    assert response.status == 200, f"Navigation {direction} failed: {response.status}"

                    data = await response.json()

                    # Validate response structure
                    assert data.get('success'), f"Navigation {direction} was not successful"
                    assert 'navigation_context' in data, f"Navigation context missing for {direction}"
                    assert 'table_data' in data, f"Table data missing for {direction}"

                    # Validate navigation context
                    nav_context = data['navigation_context']
                    assert 'current_row_index' in nav_context, f"current_row_index missing for {direction}"
                    assert nav_context.get('direction_used') == direction, f"Direction mismatch for {direction}"

                    # Validate we got data
                    table_data = data.get('table_data', [])
                    assert len(table_data) > 0, f"No table data for {direction}"

                    navigation_results.append({
                        'direction': direction,
                        'row_index': nav_context['current_row_index'],
                        'bars': len(table_data)
                    })

                print("✅ Directional navigation results:")
                for result in navigation_results:
                    print(f"   {result['direction']}: row_index={result['row_index']}, bars={result['bars']}")

            finally:
                await browser.close()

    @pytest.mark.asyncio
    async def test_time_range_navigation_workflow(self):
        """Test complete time navigation user workflow through browser."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                nav_base_url = f"{self.BASE_URL}/api/v1/training-datasets/{self.DATASET_ID}/sequences/{self.SEQUENCE_ID}/navigate"

                print("🎯 Testing Complete Time Navigation Workflow:")

                # Step 1: Start at beginning
                response = await page.request.get(f"{nav_base_url}?direction=first")
                data = await response.json()
                first_position = data['navigation_context']['current_row_index']
                first_timestamp = data['navigation_context']['timestamp_range']['start']
                print(f"   1. Started at position {first_position}, timestamp {first_timestamp}")

                # Step 2: Move forward through time
                current_position = first_position
                for step in range(3):
                    response = await page.request.get(f"{nav_base_url}?direction=next&row_index={current_position}")
                    data = await response.json()
                    current_position = data['navigation_context']['current_row_index']
                    current_timestamp = data['navigation_context']['timestamp_range']['start']
                    print(f"   2.{step+1}. Moved to position {current_position}, timestamp {current_timestamp}")

                # Step 3: Jump to end
                response = await page.request.get(f"{nav_base_url}?direction=last")
                data = await response.json()
                last_position = data['navigation_context']['current_row_index']
                last_timestamp = data['navigation_context']['timestamp_range']['start']
                print(f"   3. Jumped to end: position {last_position}, timestamp {last_timestamp}")

                # Step 4: Go to specific middle position
                middle_position = (first_position + last_position) // 2
                response = await page.request.get(f"{nav_base_url}?row_index={middle_position}")
                data = await response.json()
                actual_middle = data['navigation_context']['current_row_index']
                middle_timestamp = data['navigation_context']['timestamp_range']['start']
                print(f"   4. Selected middle: position {actual_middle}, timestamp {middle_timestamp}")

                # Validate we traversed time correctly
                assert first_position <= last_position, f"Time didn't progress: {first_position} >= {last_position}"
                assert actual_middle >= first_position, f"Middle position too early: {actual_middle} < {first_position}"
                assert actual_middle <= last_position, f"Middle position too late: {actual_middle} > {last_position}"

                print("   ✅ Time navigation workflow completed successfully")

            finally:
                await browser.close()

if __name__ == '__main__':
    # Run with high verbosity to see progress
    pytest.main([__file__, '-v', '--tb=short'])