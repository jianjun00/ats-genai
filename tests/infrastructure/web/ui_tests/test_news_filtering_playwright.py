#!/usr/bin/env python3
"""
Playwright Tests for News Events Filtering
Tests the actual browser behavior of symbol and date range filters
"""

import pytest
from playwright.async_api import async_playwright
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestNewsFilteringPlaywright:
    """Test news events filtering through browser automation."""

    BASE_URL = "http://localhost:3000"

    @pytest.mark.asyncio
    async def test_news_events_page_loads_with_filters(self):
        """Test that news events page loads and filter UI is present."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)  # Headless for CI/server
            page = await browser.new_page()

            try:
                print("🎭 Testing News Events Filter UI Loading")

                # Navigate to main page
                await page.goto(self.BASE_URL, wait_until='networkidle', timeout=30000)

                # Click on News Events button
                await page.wait_for_selector('button:has-text("📰 News Events")', timeout=15000)
                await page.click('button:has-text("📰 News Events")')
                print("✅ Clicked News Events button")

                # Wait for news events to load
                await page.wait_for_timeout(3000)

                # Check if filter UI elements are present
                await page.wait_for_selector('#symbol-filter', timeout=10000)
                await page.wait_for_selector('#start-date-filter', timeout=5000)
                await page.wait_for_selector('#end-date-filter', timeout=5000)
                await page.wait_for_selector('button:has-text("Apply Filters")', timeout=5000)
                await page.wait_for_selector('button:has-text("Clear")', timeout=5000)

                print("✅ All filter UI elements found")

                # Verify initial table has data
                await page.wait_for_timeout(1000)
                table_rows = await page.locator('tbody tr').count()
                print(f"✅ Initial table has {table_rows} rows")
                assert table_rows > 0, "Table should have initial data"

            finally:
                await browser.close()

    @pytest.mark.asyncio
    async def test_symbol_filter_functionality(self):
        """Test symbol filtering works correctly."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)  # Headless for CI
            page = await browser.new_page()

            # Capture API requests to verify correct calls are made
            api_requests = []
            page.on('request', lambda request: api_requests.append({
                'url': request.url,
                'method': request.method
            }) if 'news-events' in request.url else None)

            try:
                # Navigate and load news events
                await page.goto(self.BASE_URL, wait_until='networkidle', timeout=30000)
                await page.wait_for_selector('button:has-text("📰 News Events")', timeout=15000)
                await page.click('button:has-text("📰 News Events")')
                await page.wait_for_timeout(3000)

                # Wait for filter elements to be available
                await page.wait_for_selector('#symbol-filter', timeout=10000)

                # Clear previous API requests to focus on filter test
                api_requests.clear()

                # Fill in symbol filter and apply
                await page.fill('#symbol-filter', 'AAPL')
                symbol_value_before = await page.input_value('#symbol-filter')
                assert symbol_value_before == 'AAPL', f"Failed to enter AAPL: {symbol_value_before}"

                await page.click('button:has-text("Apply Filters")')
                await page.wait_for_timeout(3000)

                # Verify filter value preserved after apply
                symbol_value_after = await page.input_value('#symbol-filter')
                assert symbol_value_after == 'AAPL', f"Symbol filter cleared after apply: '{symbol_value_after}'"

                # Verify correct API call was made
                aapl_requests = [req for req in api_requests if 'symbol=AAPL' in req['url']]
                assert len(aapl_requests) > 0, f"No API request with AAPL symbol found. Requests: {api_requests}"

                print("✅ Symbol filter test passed")

            finally:
                await browser.close()

    @pytest.mark.asyncio
    async def test_date_filter_functionality(self):
        """Test date range filtering works correctly."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Capture API requests
            api_requests = []
            page.on('request', lambda request: api_requests.append({
                'url': request.url,
                'method': request.method
            }) if 'news-events' in request.url else None)

            try:
                # Navigate and load news events
                await page.goto(self.BASE_URL, wait_until='networkidle', timeout=30000)
                await page.wait_for_selector('button:has-text("📰 News Events")', timeout=15000)
                await page.click('button:has-text("📰 News Events")')
                await page.wait_for_timeout(3000)

                # Wait for filter elements
                await page.wait_for_selector('#start-date-filter', timeout=10000)
                await page.wait_for_selector('#end-date-filter', timeout=5000)

                api_requests.clear()  # Clear previous requests

                # Set date filters
                start_date = '2025-08-26'
                end_date = '2025-08-27'

                await page.fill('#start-date-filter', start_date)
                await page.fill('#end-date-filter', end_date)

                # Apply filters
                await page.click('button:has-text("Apply Filters")')
                await page.wait_for_timeout(3000)

                # Check values are preserved after apply
                start_value_after = await page.input_value('#start-date-filter')
                end_value_after = await page.input_value('#end-date-filter')

                assert start_value_after == start_date, f"Start date was cleared: {start_value_after}"
                assert end_value_after == end_date, f"End date was cleared: {end_value_after}"

                # Verify correct API call with date parameters
                date_requests = [req for req in api_requests if 'start_date=2025-08-26' in req['url'] and 'end_date=2025-08-27' in req['url']]
                assert len(date_requests) > 0, f"No API request with date filters found. Requests: {api_requests}"

                print("✅ Date filter test passed")

            finally:
                await browser.close()

    @pytest.mark.asyncio
    async def test_combined_filters_functionality(self):
        """Test combining symbol and date filters."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            page = await browser.new_page()

            try:
                print("🎭 Testing Combined Symbol + Date Filters")

                # Navigate and load news events
                await page.goto(self.BASE_URL, wait_until='networkidle', timeout=30000)
                await page.wait_for_selector('button:has-text("📰 News Events")', timeout=15000)
                await page.click('button:has-text("📰 News Events")')
                await page.wait_for_timeout(3000)

                # Wait for all filter elements
                await page.wait_for_selector('#symbol-filter', timeout=10000)
                await page.wait_for_selector('#start-date-filter', timeout=5000)
                await page.wait_for_selector('#end-date-filter', timeout=5000)

                # Set all filters
                symbol = 'TSLA'
                start_date = '2025-08-25'
                end_date = '2025-08-27'

                await page.fill('#symbol-filter', symbol)
                await page.fill('#start-date-filter', start_date)
                await page.fill('#end-date-filter', end_date)
                print(f"✅ Set combined filters: {symbol}, {start_date} to {end_date}")

                # Apply filters
                await page.click('button:has-text("Apply Filters")')
                await page.wait_for_timeout(3000)

                # Check all values are preserved
                symbol_after = await page.input_value('#symbol-filter')
                start_after = await page.input_value('#start-date-filter')
                end_after = await page.input_value('#end-date-filter')

                print(f"📝 Values after apply: symbol='{symbol_after}', start='{start_after}', end='{end_after}'")

                assert symbol_after == symbol, f"Symbol filter cleared: '{symbol_after}'"
                assert start_after == start_date, f"Start date cleared: '{start_after}'"
                assert end_after == end_date, f"End date cleared: '{end_after}'"

                print("✅ All combined filters preserved after apply")

            finally:
                await browser.close()

    @pytest.mark.asyncio
    async def test_clear_filters_functionality(self):
        """Test clear filters button works correctly."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            page = await browser.new_page()

            try:
                print("🎭 Testing Clear Filters Functionality")

                # Navigate and load news events
                await page.goto(self.BASE_URL, wait_until='networkidle', timeout=30000)
                await page.wait_for_selector('button:has-text("📰 News Events")', timeout=15000)
                await page.click('button:has-text("📰 News Events")')
                await page.wait_for_timeout(3000)

                # Set some filters
                await page.fill('#symbol-filter', 'AAPL')
                await page.fill('#start-date-filter', '2025-08-25')
                await page.fill('#end-date-filter', '2025-08-27')
                print("✅ Set filters before clearing")

                # Click Clear button
                await page.click('button:has-text("Clear")')
                await page.wait_for_timeout(2000)

                # Check all filters are cleared
                symbol_cleared = await page.input_value('#symbol-filter')
                start_cleared = await page.input_value('#start-date-filter')
                end_cleared = await page.input_value('#end-date-filter')

                print(f"📝 Values after clear: symbol='{symbol_cleared}', start='{start_cleared}', end='{end_cleared}'")

                assert symbol_cleared == '', f"Symbol not cleared: '{symbol_cleared}'"
                assert start_cleared == '', f"Start date not cleared: '{start_cleared}'"
                assert end_cleared == '', f"End date not cleared: '{end_cleared}'"

                print("✅ All filters successfully cleared")

            finally:
                await browser.close()

    @pytest.mark.asyncio
    async def test_api_call_inspection(self):
        """Test that correct API calls are made with filters."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            page = await browser.new_page()

            # Capture network requests
            requests = []
            page.on('request', lambda request: requests.append({
                'url': request.url,
                'method': request.method
            }))

            try:
                print("🎭 Testing API Call Inspection")

                # Navigate and load news events
                await page.goto(self.BASE_URL, wait_until='networkidle', timeout=30000)
                await page.wait_for_selector('button:has-text("📰 News Events")', timeout=15000)
                await page.click('button:has-text("📰 News Events")')
                await page.wait_for_timeout(3000)

                # Clear requests list to focus on filter requests
                requests.clear()

                # Set filters and apply
                await page.fill('#symbol-filter', 'AAPL')
                await page.fill('#start-date-filter', '2025-08-26')
                await page.click('button:has-text("Apply Filters")')
                await page.wait_for_timeout(3000)

                # Find API requests to news-events
                news_requests = [r for r in requests if 'news-events' in r['url']]
                print(f"📡 Found {len(news_requests)} news-events API calls")

                if news_requests:
                    latest_request = news_requests[-1]
                    print(f"📋 Latest API call: {latest_request['url']}")

                    # Check if parameters are in the URL
                    url = latest_request['url']
                    assert 'symbol=AAPL' in url, f"Symbol parameter not found in URL: {url}"
                    assert 'start_date=2025-08-26' in url, f"Start date parameter not found in URL: {url}"
                    print("✅ Correct API parameters found in request")
                else:
                    print("❌ No news-events API calls detected")
                    assert False, "No API calls were made with filters"

            finally:
                await browser.close()

if __name__ == '__main__':
    # Run with high verbosity to see progress
    pytest.main([__file__, '-v', '--tb=short'])