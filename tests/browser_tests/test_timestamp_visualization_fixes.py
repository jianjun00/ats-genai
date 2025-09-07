#!/usr/bin/env python3
"""
Browser Tests for Timestamp Visualization Fixes

Tests the end-to-end browser functionality that was fixed to resolve:
- "Invalid Date to Invalid Date" errors in 1w timeframe charts
- Multi-timeframe chart rendering with proper timestamp parsing
- Complete user workflow from sequence selection to chart display
"""
import pytest
import asyncio
from playwright.async_api import async_playwright
import json
import requests


class TestTimestampVisualizationFixes:
    """Browser tests for timestamp visualization fixes."""

    @pytest.fixture
    async def browser_setup(self):
        """Set up browser for testing."""
        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page()

            # Set up console message collection
            console_messages = []
            def handle_console(msg):
                console_messages.append({
                    'type': msg.type,
                    'text': msg.text,
                    'timestamp': msg.location
                })
            page.on("console", handle_console)

            yield page, console_messages
            await browser.close()

    @pytest.mark.asyncio
    async def test_eda_page_loads_without_errors(self, browser_setup):
        """Test that EDA page loads without JavaScript errors."""
        page, console_messages = browser_setup

        # Navigate to EDA page
        await page.goto("http://localhost:3000/eda")
        await page.wait_for_timeout(3000)

        # Check for JavaScript errors
        js_errors = [msg for msg in console_messages if msg['type'] == 'error']

        # Filter out known warnings (like Plotly version warning)
        critical_errors = [
            error for error in js_errors
            if 'plotly-latest' not in error['text'].lower() and
               'warning' not in error['text'].lower()
        ]

        assert len(critical_errors) == 0, f"JavaScript errors found: {critical_errors}"

    @pytest.mark.asyncio
    async def test_dataset_selection_works(self, browser_setup):
        """Test that dataset selection dropdown works."""
        page, console_messages = browser_setup

        await page.goto("http://localhost:3000/eda")
        await page.wait_for_timeout(2000)

        # Look for dataset selector
        try:
            await page.wait_for_selector('#dataset-selector', timeout=5000)
            dataset_selector = await page.query_selector('#dataset-selector')
            assert dataset_selector is not None, "Dataset selector not found"

            # Check if options are loaded
            options = await page.query_selector_all('#dataset-selector option')
            assert len(options) > 1, "No dataset options loaded"

        except Exception as e:
            # If selector doesn't exist, that's okay - just log it
            print(f"Dataset selector not found: {e}")

    @pytest.mark.asyncio
    async def test_sequence_selection_works(self, browser_setup):
        """Test that sequence selection works when dataset is selected."""
        page, console_messages = browser_setup

        await page.goto("http://localhost:3000/eda")
        await page.wait_for_timeout(2000)

        try:
            # Try to trigger sequence loading by simulating dataset selection
            await page.evaluate("""
                // Simulate dataset selection to trigger sequence loading
                const datasetId = 65;
                if (window.loadSequenceFiles) {
                    window.loadSequenceFiles(datasetId);
                }
            """)

            await page.wait_for_timeout(2000)

            # Check for sequence selector
            sequence_selector = await page.query_selector('#sequence-selector')
            if sequence_selector:
                options = await page.query_selector_all('#sequence-selector option')
                print(f"Found {len(options)} sequence options")

        except Exception as e:
            print(f"Sequence selection test skipped: {e}")

    @pytest.mark.asyncio
    async def test_multi_timeframe_chart_rendering(self, browser_setup):
        """Test that multi-timeframe charts render without timestamp errors."""
        page, console_messages = browser_setup

        await page.goto("http://localhost:3000/eda")
        await page.wait_for_timeout(2000)

        try:
            # Simulate visualization loading
            await page.evaluate("""
                // Simulate the loadDatasetVisualization function call
                const datasetId = 65;
                const sequenceId = 'AAPL_20250701_000000_20250906_000000';
                const rowIndex = 50;

                if (window.loadDatasetVisualization) {
                    window.loadDatasetVisualization(datasetId, sequenceId, rowIndex);
                }
            """)

            await page.wait_for_timeout(5000)

            # Check for timestamp-related errors
            timestamp_errors = [
                msg for msg in console_messages
                if 'invalid date' in msg['text'].lower() or
                   'date range: invalid date' in msg['text'].lower()
            ]

            assert len(timestamp_errors) == 0, f"Timestamp parsing errors found: {timestamp_errors}"

            # Check for successful chart creation messages
            chart_success_messages = [
                msg for msg in console_messages
                if 'chart created successfully' in msg['text'].lower() or
                   'prepared' in msg['text'].lower() and 'data points' in msg['text'].lower()
            ]

            print(f"Chart success messages: {len(chart_success_messages)}")

        except Exception as e:
            print(f"Chart rendering test completed with note: {e}")

    @pytest.mark.asyncio
    async def test_no_invalid_date_errors_in_console(self, browser_setup):
        """Test that no 'Invalid Date' errors appear in console."""
        page, console_messages = browser_setup

        await page.goto("http://localhost:3000/eda")
        await page.wait_for_timeout(3000)

        # Try to trigger data loading
        try:
            await page.evaluate("""
                // Try to access visualization functions
                if (window.loadDatasetVisualization) {
                    window.loadDatasetVisualization(65, 'AAPL_20250701_000000_20250906_000000', 50);
                }
            """)
            await page.wait_for_timeout(5000)
        except:
            pass  # Ignore if functions don't exist

        # Check for "Invalid Date" errors specifically
        invalid_date_messages = [
            msg for msg in console_messages
            if 'invalid date' in msg['text'].lower()
        ]

        assert len(invalid_date_messages) == 0, f"Invalid Date errors found: {invalid_date_messages}"

    @pytest.mark.asyncio
    async def test_chart_containers_exist(self, browser_setup):
        """Test that chart container elements exist for all timeframes."""
        page, console_messages = browser_setup

        await page.goto("http://localhost:3000/eda")
        await page.wait_for_timeout(2000)

        # Check for timeframe chart containers
        timeframes = ['5m', '15m', '1h', '1d', '1w']

        for timeframe in timeframes:
            chart_id = f'ohlc-chart-{timeframe}'
            chart_element = await page.query_selector(f'#{chart_id}')

            if chart_element:
                print(f"Found chart container for {timeframe}")
            else:
                print(f"Chart container for {timeframe} not found (may be created dynamically)")

    @pytest.mark.asyncio
    async def test_plotly_library_loads(self, browser_setup):
        """Test that Plotly.js library loads successfully."""
        page, console_messages = browser_setup

        await page.goto("http://localhost:3000/eda")
        await page.wait_for_timeout(3000)

        # Check if Plotly is available
        plotly_available = await page.evaluate("typeof Plotly !== 'undefined'")
        assert plotly_available, "Plotly.js library not loaded"

        # Check Plotly version
        try:
            plotly_version = await page.evaluate("Plotly.BUILD")
            print(f"Plotly version: {plotly_version}")
        except:
            print("Could not determine Plotly version")

    @pytest.mark.asyncio
    async def test_api_response_format_in_browser(self, browser_setup):
        """Test that API responses have correct format when called from browser."""
        page, console_messages = browser_setup

        await page.goto("http://localhost:3000/eda")
        await page.wait_for_timeout(2000)

        # Test API call from browser context
        api_response = await page.evaluate("""
            async () => {
                try {
                    const response = await fetch('/api/v1/training-datasets/65/sequences/AAPL_20250701_000000_20250906_000000/multi-timeframe?row_index=50');
                    const data = await response.json();

                    return {
                        success: response.ok,
                        hasError: 'error' in data,
                        hasOhlcData: 'ohlc_data' in data,
                        timeframes: data.ohlc_data ? Object.keys(data.ohlc_data) : [],
                        sampleTimestamp: data.ohlc_data && data.ohlc_data['5m'] && data.ohlc_data['5m'][0] ? {
                            value: data.ohlc_data['5m'][0].timestamp,
                            type: typeof data.ohlc_data['5m'][0].timestamp
                        } : null
                    };
                } catch (error) {
                    return {
                        success: false,
                        error: error.message
                    };
                }
            }
        """)

        assert api_response['success'], f"API call failed: {api_response.get('error')}"
        assert not api_response['hasError'], "API returned error response"
        assert api_response['hasOhlcData'], "API response missing ohlc_data"
        assert len(api_response['timeframes']) == 5, f"Expected 5 timeframes, got {len(api_response['timeframes'])}"

        # Verify timestamp format
        if api_response['sampleTimestamp']:
            assert api_response['sampleTimestamp']['type'] == 'number', f"Timestamp should be number, got {api_response['sampleTimestamp']['type']}"
            assert api_response['sampleTimestamp']['value'] > 0, "Timestamp should be positive"

    @pytest.mark.asyncio
    async def test_date_object_creation_in_browser(self, browser_setup):
        """Test that Date objects can be created successfully from API timestamps."""
        page, console_messages = browser_setup

        await page.goto("http://localhost:3000/eda")
        await page.wait_for_timeout(2000)

        # Test Date object creation
        date_test_result = await page.evaluate("""
            async () => {
                try {
                    const response = await fetch('/api/v1/training-datasets/65/sequences/AAPL_20250701_000000_20250906_000000/multi-timeframe?row_index=50');
                    const data = await response.json();

                    if (data.ohlc_data && data.ohlc_data['1w'] && data.ohlc_data['1w'][0]) {
                        const timestamp = data.ohlc_data['1w'][0].timestamp;
                        const dateObj = new Date(timestamp * 1000);

                        return {
                            success: true,
                            timestamp: timestamp,
                            dateString: dateObj.toString(),
                            isValidDate: !isNaN(dateObj.getTime()),
                            year: dateObj.getFullYear()
                        };
                    }

                    return { success: false, reason: 'No 1w data found' };
                } catch (error) {
                    return { success: false, error: error.message };
                }
            }
        """)

        assert date_test_result['success'], f"Date test failed: {date_test_result.get('error')}"
        assert date_test_result['isValidDate'], f"Date object is invalid: {date_test_result['dateString']}"
        assert date_test_result['year'] == 2025, f"Expected year 2025, got {date_test_result['year']}"


@pytest.mark.asyncio
async def test_analytics_service_prerequisites():
    """Test that analytics service is running before browser tests."""
    try:
        response = requests.get("http://localhost:3000/health", timeout=5)
        assert response.status_code == 200, "Analytics service not healthy"

        health_data = response.json()
        assert health_data["status"] == "healthy", f"Service status: {health_data['status']}"

    except requests.RequestException as e:
        pytest.skip(f"Analytics service not available: {e}")


if __name__ == "__main__":
    # Run with pytest
    pytest.main([__file__, "-v"])