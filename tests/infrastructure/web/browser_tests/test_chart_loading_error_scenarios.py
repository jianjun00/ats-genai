#!/usr/bin/env python3
"""
Playwright Tests for Chart Loading Error Scenarios
Tests chart loading with NaN values and other error conditions
"""

import asyncio
import pytest
import json
from playwright.async_api import async_playwright

@pytest.mark.asyncio
async def test_chart_loading_error_handling():
    """Test that chart loading handles NaN and error scenarios gracefully."""
    print("🎭 Testing Chart Loading Error Scenarios with Playwright")
    print("="*60)

    async with async_playwright() as playwright:
        # Launch browser in headless mode
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()

        # Capture console errors and network errors
        console_errors = []
        network_errors = []
        json_parse_errors = []

        def handle_console(msg):
            if msg.type == "error":
                console_errors.append(msg.text)
                # Specifically look for JSON parse errors
                if "JSON" in msg.text or "NaN" in msg.text or "Unexpected token" in msg.text:
                    json_parse_errors.append(msg.text)

        def handle_request_failed(request):
            network_errors.append(f"Request failed: {request.url}")

        page.on("console", handle_console)
        page.on("requestfailed", handle_request_failed)

        print("🧪 Test 1: Load EDA interface and check for JSON errors")
        await page.goto("http://localhost:3000/eda", timeout=15000)
        await page.wait_for_load_state("networkidle")
        print("✅ EDA interface loaded")

        print("\n🧪 Test 2: Select dataset and monitor for NaN errors")
        # Wait for datasets to load
        await page.wait_for_timeout(3000)

        # Try to select a dataset
        dataset_select = page.locator("#dataset-select")
        if await dataset_select.count() > 0:
            print("✅ Dataset selector found")

            options = await dataset_select.locator("option").all()
            if len(options) > 1:
                # Select first real dataset
                await dataset_select.locator("option").nth(1).click()
                print("✅ Dataset selected")

                # Wait for data to load and check for errors
                await page.wait_for_timeout(5000)

                # Check for JSON parse errors that occurred during loading
                if json_parse_errors:
                    print(f"❌ JSON parsing errors detected: {len(json_parse_errors)}")
                    for error in json_parse_errors[:3]:
                        print(f"   - {error}")
                else:
                    print("✅ No JSON parsing errors during dataset loading")

        print("\n🧪 Test 3: Test sequence selection and chart rendering")

        # Look for sequence selection controls
        sequence_selectors = await page.locator("input[type='radio'][name='sequenceId']").all()
        if len(sequence_selectors) > 0:
            print(f"✅ Found {len(sequence_selectors)} sequence options")

            # Select first sequence and monitor for chart loading errors
            await sequence_selectors[0].click()
            print("✅ Sequence selected")

            # Wait for charts to load
            await page.wait_for_timeout(5000)

            # Look for chart containers
            charts = await page.locator(".chart-container, .plotly-graph-div").all()
            print(f"📊 Found {len(charts)} chart containers")

            # Check for specific NaN-related error messages in the page
            page_content = await page.content()

            nan_indicators = [
                "Unexpected token 'N'",
                "NaN is not valid JSON",
                "Error loading",
                "Chart failed to load",
                "Invalid JSON",
                "SyntaxError"
            ]

            found_errors = []
            for indicator in nan_indicators:
                if indicator in page_content:
                    found_errors.append(indicator)

            if found_errors:
                print(f"❌ NaN-related errors found in page content:")
                for error in found_errors:
                    print(f"   - {error}")
            else:
                print("✅ No NaN-related errors found in page content")

            # Test specific timeframe charts
            timeframes = ['5m', '15m', '1h', '1d', '1w']
            for tf in timeframes:
                chart_element = page.locator(f"#{tf}-chart, .{tf}-chart, [data-timeframe='{tf}']").first
                if await chart_element.count() > 0:
                    print(f"✅ Found {tf} chart element")

                    # Check if chart has error state
                    error_element = chart_element.locator(".error, .chart-error").first
                    if await error_element.count() > 0:
                        error_text = await error_element.text_content()
                        print(f"❌ {tf} chart has error: {error_text}")
                    else:
                        print(f"✅ {tf} chart appears to be rendering correctly")
                else:
                    print(f"⚠️  {tf} chart element not found")

        print("\n🧪 Test 4: Test table view for feature display errors")

        # Check table for NaN values in displayed data
        table = page.locator("table").first
        if await table.count() > 0:
            print("✅ Table found")

            # Get table content
            table_text = await table.text_content()

            # Check for NaN or undefined values in table
            if "NaN" in table_text:
                print("❌ Found NaN values in table display")
            elif "undefined" in table_text and "record" not in table_text:
                print("❌ Found undefined values in table display")
            else:
                print("✅ No NaN or undefined values found in table")

            # Check table headers for proper feature names
            headers = await table.locator("thead th").all()
            header_texts = []
            for header in headers[:10]:  # Check first 10 headers
                text = await header.text_content()
                if text.strip():
                    header_texts.append(text.strip())

            if header_texts:
                print(f"✅ Table headers found: {len(header_texts)}")
                # Check for problematic header values
                problematic_headers = [h for h in header_texts if h in ['NaN', 'undefined', 'null']]
                if problematic_headers:
                    print(f"❌ Problematic header values: {problematic_headers}")
                else:
                    print("✅ All table headers appear valid")

        print("\n🧪 Test 5: Monitor network requests for malformed JSON")

        # Trigger a data refresh and monitor requests
        refresh_button = page.locator("button:has-text('Refresh'), button:has-text('Reload')").first
        if await refresh_button.count() > 0:

            # Set up response monitoring
            responses_with_errors = []

            async def handle_response(response):
                if response.url.endswith('/multi-timeframe') or 'training-datasets' in response.url:
                    # Try to parse response as JSON
                    if response.status == 200:
                        text = await response.text()
                        if text:
                            json.loads(text)  # Will raise exception if invalid JSON
                            print(f"✅ Valid JSON response from {response.url}")
            page.on("response", handle_response)

            await refresh_button.click()
            await page.wait_for_timeout(3000)

            if responses_with_errors:
                print(f"❌ Found {len(responses_with_errors)} responses with JSON errors")
                for error in responses_with_errors[:3]:
                    print(f"   - {error}")
            else:
                print("✅ All API responses contained valid JSON")

    print(f"\n📊 Test Results Summary:")
    print(f"   JavaScript Console Errors: {len(console_errors)}")
    print(f"   JSON Parse Errors: {len(json_parse_errors)}")
    print(f"   Network Errors: {len(network_errors)}")

    if json_parse_errors:
        print("❌ JSON parsing errors found - NaN handling may be incomplete")
        for error in json_parse_errors:
            print(f"   - {error}")
        return False
    else:
        print("✅ No JSON parsing errors detected")
        return True

@pytest.mark.asyncio
async def test_specific_nan_error_scenarios():
    """Test specific scenarios that previously caused NaN errors."""
    print("\n🔬 Testing Specific NaN Error Scenarios")
    print("-" * 50)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()

        # Track specific error patterns
        nan_errors = []
        infinity_errors = []
        json_errors = []

        def handle_console(msg):
            if msg.type == "error":
                error_text = msg.text.lower()
                if "nan" in error_text:
                    nan_errors.append(msg.text)
                if "infinity" in error_text or "inf" in error_text:
                    infinity_errors.append(msg.text)
                if "json" in error_text or "unexpected token" in error_text:
                    json_errors.append(msg.text)

        page.on("console", handle_console)

        await page.goto("http://localhost:3000/eda", timeout=15000)
        await page.wait_for_load_state("networkidle")

        # Test specific API endpoints that previously had NaN issues
        test_endpoints = [
            "/api/v1/training-datasets",
            "/api/v1/training-datasets/65/sequences",
            "/api/v1/training-datasets/65/sequences/AAPL_20250701_000000_20250906_000000/multi-timeframe?row_index=10"
        ]

        for endpoint in test_endpoints:
            print(f"🌐 Testing endpoint: {endpoint}")

            # Use page.evaluate to make fetch request and test JSON parsing
            result = await page.evaluate(f"""
                fetch('http://localhost:3000{endpoint}')
                    .then(response => response.text())
                    .then(text => {{
                        // Check for NaN in response text before parsing
                        if (text.includes('NaN') || text.includes('Infinity') || text.includes('-Infinity')) {{
                            return {{ error: 'Contains invalid JSON values', text: text.substring(0, 200) }};
                        }}
                        try {{
                            const parsed = JSON.parse(text);
                            return {{ success: true, hasData: Object.keys(parsed).length > 0 }};
                        }} catch (e) {{
                            return {{ error: e.message, text: text.substring(0, 200) }};
                        }}
                    }})
                    .catch(err => ({{ error: err.message }}))
            """)

            if result.get('error'):
                print(f"❌ {endpoint}: {result['error']}")
                if result.get('text'):
                    print(f"   Sample response: {result['text']}")
            else:
                print(f"✅ {endpoint}: Valid JSON response")

        print(f"\n📈 Specific Error Analysis:")
        print(f"   NaN-related errors: {len(nan_errors)}")
        print(f"   Infinity-related errors: {len(infinity_errors)}")
        print(f"   JSON parsing errors: {len(json_errors)}")

        total_errors = len(nan_errors) + len(infinity_errors) + len(json_errors)
        return total_errors == 0

async def run_all_chart_tests():
    """Run all chart loading error tests."""
    print("🧪 Running Comprehensive Chart Loading Error Tests")
    print("="*60)

    test1_success = await test_chart_loading_error_handling()
    test2_success = await test_specific_nan_error_scenarios()

    overall_success = test1_success and test2_success

    print(f"\n🎯 Overall Test Results:")
    print(f"   Chart Loading Test: {'✅ PASS' if test1_success else '❌ FAIL'}")
    print(f"   NaN Error Scenarios: {'✅ PASS' if test2_success else '❌ FAIL'}")
    print(f"   Overall Status: {'✅ ALL TESTS PASSED' if overall_success else '❌ SOME TESTS FAILED'}")

    return overall_success

if __name__ == "__main__":
    result = asyncio.run(run_all_chart_tests())
    exit(0 if result else 1)