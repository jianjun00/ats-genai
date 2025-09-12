#!/usr/bin/env python3
"""
Playwright integration tests for gap events functionality in the analytics dashboard.

This test verifies that:
1. The gap events button appears in the dashboard
2. Clicking the button loads gap events data
3. The gap events display properly with summary cards and table
4. The API endpoint returns proper data structure
5. Filters work correctly for symbol and date range filtering
6. Clear functionality works properly
"""

import pytest
import asyncio
import time
from playwright.async_api import async_playwright, expect
from playwright.sync_api import sync_playwright


class TestGapEventsIntegration:
    """Test gap events integration in the analytics dashboard."""

    @pytest.fixture(scope="class")
    def analytics_url(self):
        """Analytics service URL for testing."""
        return "http://localhost:3000"

    def test_gap_events_button_exists(self, analytics_url):
        """Test that the gap events button exists in the dashboard."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Navigate to analytics dashboard
            page.goto(analytics_url)
            page.wait_for_load_state("networkidle")

            # Check that the gap events button exists
            gap_button = page.locator('button:has-text("⚡ Gap Events")')
            assert gap_button.is_visible()

            browser.close()

    def test_gap_events_api_endpoint(self, analytics_url):
        """Test that the gap events API endpoint returns valid data."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Test API endpoint directly
            response = page.request.get(f"{analytics_url}/api/gap-events?limit=5")
            assert response.status == 200

            # Parse JSON response
            data = response.json()

            # Verify response structure
            assert "success" in data
            assert "events" in data
            assert "total_events" in data
            assert "unique_symbols" in data
            assert "summary" in data

            # Verify summary structure
            summary = data["summary"]
            assert "gap_ups" in summary
            assert "gap_downs" in summary
            assert "micro_gaps" in summary
            assert "small_gaps" in summary
            assert "medium_gaps" in summary
            assert "large_gaps" in summary
            assert "filled_gaps" in summary
            assert "unfilled_gaps" in summary
            assert "avg_significance_score" in summary

            browser.close()

    def test_gap_events_dashboard_interaction(self, analytics_url):
        """Test full gap events dashboard interaction."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Navigate to analytics dashboard
            page.goto(analytics_url)
            page.wait_for_load_state("networkidle")

            # Click the gap events button
            gap_button = page.locator('button:has-text("⚡ Gap Events")')
            gap_button.click()

            # Wait for content to load
            page.wait_for_timeout(3000)

            # Check for loading message initially
            content_div = page.locator('#analysis-content')

            # Wait for actual content to load (not loading message)
            page.wait_for_function(
                """() => {
                    const content = document.getElementById('analysis-content');
                    return content && content.innerHTML.includes('Gap Events Analysis');
                }""",
                timeout=10000
            )

            # Verify gap events content is displayed
            assert "⚡ Gap Events Analysis" in content_div.inner_text()

            # Check for summary cards
            assert page.locator('text=Total Gaps').is_visible()
            assert page.locator('text=Gap Ups').is_visible()
            assert page.locator('text=Gap Downs').is_visible()
            assert page.locator('text=Filled Gaps').is_visible()
            assert page.locator('text=Avg Score').is_visible()
            assert page.locator('text=Unique Symbols').is_visible()

            # Check for table headers
            assert page.locator('th:has-text("Symbol")').is_visible()
            assert page.locator('th:has-text("Date")').is_visible()
            assert page.locator('th:has-text("Gap %")').is_visible()
            assert page.locator('th:has-text("Direction")').is_visible()
            assert page.locator('th:has-text("Size")').is_visible()
            assert page.locator('th:has-text("Score")').is_visible()
            assert page.locator('th:has-text("Filled")').is_visible()

            browser.close()

    def test_gap_events_error_handling(self, analytics_url):
        """Test gap events error handling."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Test API endpoint with invalid parameters
            response = page.request.get(f"{analytics_url}/api/gap-events?limit=invalid")

            # Should still return 200 but handle the error gracefully
            assert response.status == 200

            data = response.json()
            # Should either succeed or return error structure
            assert "success" in data or "error" in data

            browser.close()

    def test_gap_events_symbol_filtering(self, analytics_url):
        """Test gap events symbol filtering."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Test API endpoint with symbol filter
            response = page.request.get(f"{analytics_url}/api/gap-events?symbol=AAPL&limit=10")
            assert response.status == 200

            data = response.json()
            assert "success" in data
            assert "filters" in data
            assert data["filters"]["symbol_filter"] == "AAPL"

            # If events exist, they should be for the specified symbol
            if data.get("events"):
                for event in data["events"]:
                    assert event.get("symbol") == "AAPL"

            browser.close()

    def test_gap_events_filters_ui_exists(self, analytics_url):
        """Test that filter controls exist in the gap events UI."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Navigate to analytics dashboard
            page.goto(analytics_url)
            page.wait_for_load_state("networkidle")

            # Click the gap events button
            gap_button = page.locator('button:has-text("⚡ Gap Events")')
            gap_button.click()

            # Wait for content to load
            page.wait_for_function(
                """() => {
                    const content = document.getElementById('analysis-content');
                    return content && content.innerHTML.includes('🔍 Filters');
                }""",
                timeout=10000
            )

            # Check for filter controls
            assert page.locator('text=🔍 Filters').is_visible()
            assert page.locator('#gap-symbol-filter').is_visible()
            assert page.locator('#gap-start-date-filter').is_visible()
            assert page.locator('#gap-end-date-filter').is_visible()
            assert page.locator('button:has-text("Apply Filters")').is_visible()
            assert page.locator('button:has-text("Clear")').is_visible()

            # Verify input field placeholders and types
            symbol_input = page.locator('#gap-symbol-filter')
            assert symbol_input.get_attribute('placeholder') == 'e.g. AAPL'
            assert symbol_input.get_attribute('type') == 'text'

            start_date_input = page.locator('#gap-start-date-filter')
            assert start_date_input.get_attribute('type') == 'date'

            end_date_input = page.locator('#gap-end-date-filter')
            assert end_date_input.get_attribute('type') == 'date'

            browser.close()

    def test_gap_events_symbol_filter_functionality(self, analytics_url):
        """Test symbol filter functionality."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Navigate to analytics dashboard
            page.goto(analytics_url)
            page.wait_for_load_state("networkidle")

            # Click the gap events button
            gap_button = page.locator('button:has-text("⚡ Gap Events")')
            gap_button.click()

            # Wait for initial content to load
            page.wait_for_function(
                """() => {
                    const content = document.getElementById('analysis-content');
                    return content && content.innerHTML.includes('🔍 Filters');
                }""",
                timeout=10000
            )

            # Enter a symbol filter
            symbol_input = page.locator('#gap-symbol-filter')
            symbol_input.fill('TSLA')

            # Click apply filters
            apply_button = page.locator('button:has-text("Apply Filters")')
            apply_button.click()

            # Wait for filtered results to load
            page.wait_for_timeout(3000)

            # Verify API was called with symbol parameter
            # Check that the filter value persists in the UI
            assert symbol_input.input_value() == 'TSLA'

            browser.close()

    def test_gap_events_date_range_filter_functionality(self, analytics_url):
        """Test date range filter functionality."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Navigate to analytics dashboard
            page.goto(analytics_url)
            page.wait_for_load_state("networkidle")

            # Click the gap events button
            gap_button = page.locator('button:has-text("⚡ Gap Events")')
            gap_button.click()

            # Wait for content to load
            page.wait_for_function(
                """() => {
                    const content = document.getElementById('analysis-content');
                    return content && content.innerHTML.includes('🔍 Filters');
                }""",
                timeout=10000
            )

            # Set date range filters
            start_date_input = page.locator('#gap-start-date-filter')
            end_date_input = page.locator('#gap-end-date-filter')

            start_date_input.fill('2024-01-01')
            end_date_input.fill('2024-12-31')

            # Click apply filters
            apply_button = page.locator('button:has-text("Apply Filters")')
            apply_button.click()

            # Wait for filtered results
            page.wait_for_timeout(3000)

            # Verify date values persist in the UI
            assert start_date_input.input_value() == '2024-01-01'
            assert end_date_input.input_value() == '2024-12-31'

            browser.close()

    def test_gap_events_clear_filters_functionality(self, analytics_url):
        """Test clear filters functionality."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Navigate to analytics dashboard
            page.goto(analytics_url)
            page.wait_for_load_state("networkidle")

            # Click the gap events button
            gap_button = page.locator('button:has-text("⚡ Gap Events")')
            gap_button.click()

            # Wait for content to load
            page.wait_for_function(
                """() => {
                    const content = document.getElementById('analysis-content');
                    return content && content.innerHTML.includes('🔍 Filters');
                }""",
                timeout=10000
            )

            # Fill in all filters
            symbol_input = page.locator('#gap-symbol-filter')
            start_date_input = page.locator('#gap-start-date-filter')
            end_date_input = page.locator('#gap-end-date-filter')

            symbol_input.fill('AMZN')
            start_date_input.fill('2025-01-01')
            end_date_input.fill('2025-12-31')

            # Verify filters are filled
            assert symbol_input.input_value() == 'AMZN'
            assert start_date_input.input_value() == '2025-01-01'
            assert end_date_input.input_value() == '2025-12-31'

            # Click clear button
            clear_button = page.locator('button:has-text("Clear")')
            clear_button.click()

            # Wait for filters to clear and data to reload
            page.wait_for_timeout(2000)

            # Verify all filters are cleared
            assert symbol_input.input_value() == ''
            assert start_date_input.input_value() == ''
            assert end_date_input.input_value() == ''

            browser.close()

    def test_gap_events_combined_filters_api_call(self, analytics_url):
        """Test that combined filters generate correct API call."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Intercept API requests to verify parameters
            api_requests = []

            def handle_request(request):
                if '/api/gap-events' in request.url:
                    api_requests.append(request.url)

            page.on('request', handle_request)

            # Navigate to analytics dashboard
            page.goto(analytics_url)
            page.wait_for_load_state("networkidle")

            # Click the gap events button
            gap_button = page.locator('button:has-text("⚡ Gap Events")')
            gap_button.click()

            # Wait for initial load
            page.wait_for_function(
                """() => {
                    const content = document.getElementById('analysis-content');
                    return content && content.innerHTML.includes('🔍 Filters');
                }""",
                timeout=10000
            )

            # Clear initial API requests
            api_requests.clear()

            # Set combined filters
            symbol_input = page.locator('#gap-symbol-filter')
            start_date_input = page.locator('#gap-start-date-filter')
            end_date_input = page.locator('#gap-end-date-filter')

            symbol_input.fill('TSLA')
            start_date_input.fill('2025-08-01')
            end_date_input.fill('2025-08-30')

            # Apply filters
            apply_button = page.locator('button:has-text("Apply Filters")')
            apply_button.click()

            # Wait for API call
            page.wait_for_timeout(3000)

            # Verify API call contains all parameters
            assert len(api_requests) > 0, "No API requests captured"
            last_request = api_requests[-1]

            # Check URL contains all expected parameters
            assert 'symbol=TSLA' in last_request
            assert 'start_date=2025-08-01' in last_request
            assert 'end_date=2025-08-30' in last_request
            assert 'limit=50' in last_request

            browser.close()

    def test_gap_events_gap_size_breakdown(self, analytics_url):
        """Test that gap events include size breakdown section."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Navigate to analytics dashboard
            page.goto(analytics_url)
            page.wait_for_load_state("networkidle")

            # Click the gap events button
            gap_button = page.locator('button:has-text("⚡ Gap Events")')
            gap_button.click()

            # Wait for content to load
            page.wait_for_function(
                """() => {
                    const content = document.getElementById('analysis-content');
                    return content && content.innerHTML.includes('Gap Size Breakdown');
                }""",
                timeout=10000
            )

            # Check for gap size breakdown section
            assert page.locator('text=📊 Gap Size Breakdown').is_visible()
            assert page.locator('text=Micro:').is_visible()
            assert page.locator('text=Small:').is_visible()
            assert page.locator('text=Medium:').is_visible()
            assert page.locator('text=Large:').is_visible()

            browser.close()


if __name__ == "__main__":
    # Run the tests directly for debugging
    import subprocess
    subprocess.run([
        "python", "-m", "pytest", __file__, "-v", "--tb=short"
    ])