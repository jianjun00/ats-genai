#!/usr/bin/env python3
"""
Playwright test to verify that 21 data points are properly rendered in OHLC chart
Tests the complete 21-row window visualization pipeline from API to chart display
"""

import asyncio
import pytest
from playwright.async_api import async_playwright

class Test21DataPointsChartRendering:

    @pytest.mark.asyncio

    async def test_21_datapoints_in_ohlc_chart(self):
        """Test that all 21 data points from API are rendered in the OHLC chart"""

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)  # Set to headless for server environment
            context = await browser.new_context()
            page = await context.new_page()

            # Enable console logging to debug
            console_messages = []
            page.on("console", lambda msg: console_messages.append(f"CONSOLE: {msg.text}"))

            # Navigate to EDA dashboard
            await page.goto("http://localhost:3000/eda")
            print("📊 Navigated to EDA dashboard")

            # Wait for the page to load and datasets to be available
            await page.wait_for_selector("#dataset-selector", timeout=10000)

            # Select dataset 17 (the AAPL hourly data we've been working with)
            await page.select_option("#dataset-selector", "17")
            print("🔍 Selected dataset 17")

            # Wait for dataset to load
            await page.wait_for_timeout(2000)

            # Set sequence to a known working value (90)
            await page.fill("#sequence-input", "90")
            print("⚙️ Set sequence to 90")

            # Click the OHLC visualization button to trigger chart rendering
            ohlc_button = page.locator("button:has-text('OHLC Visualization')")
            if await ohlc_button.count() > 0:
                await ohlc_button.click()
                print("📈 Clicked OHLC Visualization button")

            # Wait for the chart to render
            await page.wait_for_timeout(3000)

            # Check if the OHLC chart container exists
            chart_container = page.locator("#ohlc-chart")
            assert await chart_container.count() > 0, "OHLC chart container not found"
            print("✅ Found OHLC chart container")

            # Look for the actual chart content - check for Plotly chart
            plotly_chart = page.locator("#ohlc-chart .plotly-graph-div")
            chart_exists = await plotly_chart.count() > 0

            if chart_exists:
                print("📊 Found Plotly chart")

                # Get the chart data by evaluating JavaScript
                chart_data = await page.evaluate("""
                () => {
                    const chartDiv = document.querySelector('#ohlc-chart .plotly-graph-div');
                    if (chartDiv && chartDiv.data) {
                        const traces = chartDiv.data;
                        let totalDataPoints = 0;
                        let traceInfo = [];

                        traces.forEach((trace, index) => {
                            const pointCount = trace.x ? trace.x.length : 0;
                            totalDataPoints += pointCount;
                            traceInfo.push({
                                trace: index,
                                type: trace.type,
                                name: trace.name || 'unnamed',
                                points: pointCount,
                                x_sample: trace.x ? trace.x.slice(0, 3) : [],
                                y_sample: trace.y ? trace.y.slice(0, 3) : []
                            });
                        });

                        return {
                            total_traces: traces.length,
                            total_data_points: totalDataPoints,
                            traces: traceInfo
                        };
                    }
                    return null;
                }
                """)

                if chart_data:
                    print(f"📊 Chart Analysis Results:")
                    print(f"   Total traces: {chart_data['total_traces']}")
                    print(f"   Total data points: {chart_data['total_data_points']}")

                    for trace in chart_data['traces']:
                        print(f"   Trace {trace['trace']}: {trace['type']} '{trace['name']}' - {trace['points']} points")
                        if trace['x_sample']:
                            print(f"      X sample: {trace['x_sample'][:3]}")
                        if trace['y_sample']:
                            print(f"      Y sample: {trace['y_sample'][:3]}")

                    # Verify we have 21 data points total across all traces
                    expected_points = 21
                    actual_points = chart_data['total_data_points']

                    print(f"\n🎯 DATA POINTS VERIFICATION:")
                    print(f"   Expected: {expected_points} data points")
                    print(f"   Actual:   {actual_points} data points")

                    if actual_points == expected_points:
                        print("✅ SUCCESS: Chart correctly displays 21 data points!")
                        return True
                    elif actual_points == 1:
                        print("❌ ISSUE CONFIRMED: Chart only shows 1 data point instead of 21")
                        return False
                    else:
                        print(f"⚠️ UNEXPECTED: Chart shows {actual_points} data points (expected 21)")
                        return False
                else:
                    print("❌ Could not extract chart data")
                    return False

            else:
                # Check if there's an error message or other content
                chart_content = await chart_container.inner_html()
                print(f"❌ No Plotly chart found. Chart container content:")
                print(f"   {chart_content[:200]}...")

                # Check for error messages
                if "error" in chart_content.lower() or "bounds" in chart_content.lower():
                    print("⚠️ Found error message in chart container")

                return False

            # Print console messages for debugging
            if console_messages:
                print("\n🔍 Console Messages:")
                for msg in console_messages[-10:]:  # Last 10 messages
                    print(f"   {msg}")

            await browser.close()

    def test_api_returns_21_datapoints(self):
        """Verify that the API actually returns 21 data points for sequence 90"""

        import subprocess
        import json

        # Use curl to test the API
        cmd = [
            "curl", "-s",
            "http://localhost:3000/api/v1/training-datasets/17/visualization-data?start_idx=90&count=21"
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)

        if result.returncode != 0:
            print(f"❌ Curl command failed: {result.stderr}")
            return False

        data = json.loads(result.stdout)
        print(f"📡 API Response Analysis:")

        if 'data' in data and isinstance(data['data'], list):
            point_count = len(data['data'])
            print(f"   Data points returned: {point_count}")
            print(f"   Sequence info: idx={data.get('sequence_idx')}, total={data.get('total_sequences')}")

            # Check first and last data points
            if point_count > 0:
                first_point = data['data'][0]
                last_point = data['data'][-1] if point_count > 1 else first_point

                print(f"   First point OHLC: O={first_point.get('open')}, H={first_point.get('high')}, L={first_point.get('low')}, C={first_point.get('close')}")
                if point_count > 1:
                    print(f"   Last point OHLC:  O={last_point.get('open')}, H={last_point.get('high')}, L={last_point.get('low')}, C={last_point.get('close')}")

            if point_count == 21:
                print("✅ API correctly returns 21 data points")
                return True
            else:
                print(f"❌ Expected 21 data points, got {point_count}")
                return False

        elif 'error' in data:
            print(f"❌ API returned error: {data['error']}")
            if 'user_message' in data:
                print(f"   Message: {data['user_message']}")
            return False

        else:
            print(f"❌ Unexpected API response structure: {list(data.keys())}")
            return False

async def run_tests():
    """Run the 21 data points chart rendering tests"""

    print("🧪 Starting 21 Data Points Chart Rendering Tests")
    print("=" * 60)

    test_suite = Test21DataPointsChartRendering()

    # Test 1: Verify API returns 21 data points
    print("\n1️⃣ Testing API data points...")
    api_result = test_suite.test_api_returns_21_datapoints()

    if api_result:
        # Test 2: Verify chart renders all 21 points
        print("\n2️⃣ Testing chart rendering...")
        chart_result = await test_suite.test_21_datapoints_in_ohlc_chart()

        # Summary
        print("\n" + "=" * 60)
        print("📊 TEST SUMMARY:")
        print(f"   API Returns 21 Points: {'✅ PASS' if api_result else '❌ FAIL'}")
        print(f"   Chart Shows 21 Points: {'✅ PASS' if chart_result else '❌ FAIL'}")

        if api_result and chart_result:
            print("🎉 ALL TESTS PASSED: 21-row window working correctly!")
        elif api_result and not chart_result:
            print("⚠️ ISSUE IDENTIFIED: API works but chart rendering needs fixing")
        else:
            print("❌ ISSUES FOUND: Check API and chart rendering")

        return api_result and chart_result
    else:
        print("❌ API test failed, skipping chart test")
        return False

if __name__ == "__main__":
    result = asyncio.run(run_tests())