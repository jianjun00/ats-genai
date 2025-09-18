#!/usr/bin/env python3
"""
Playwright test to debug training dataset sequence visualization issues.

Specifically tests the "No sequence data available" problem where Plotly charts
and sequence tables don't show up properly.
"""

import pytest
from playwright.async_api import async_playwright
import json

class TestSequenceVisualizationDebug:
    """Debug training dataset sequence visualization issues."""

    @pytest.mark.asyncio
    async def test_training_dataset_sequence_data_availability(self):
        """Test that training dataset sequence data is available and displays correctly."""

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            # Capture console logs and errors
            console_logs = []
            page.on("console", lambda msg: console_logs.append(f"[{msg.type.upper()}] {msg.text}"))

            page_errors = []
            page.on("pageerror", lambda error: page_errors.append(str(error)))

            try:
                print("🔍 Testing training dataset sequence visualization...")

                # Step 1: Navigate to analytics service
                await page.goto("http://localhost:3000", timeout=15000)
                await page.wait_for_load_state("networkidle")

                # Step 2: Navigate to training datasets
                print("🔍 Navigating to training datasets...")
                await page.goto("http://localhost:3000/api/v1/training-datasets", timeout=15000)
                await page.wait_for_load_state("networkidle")

                # Step 3: Check if datasets table is present
                datasets_table = page.locator("table").first
                assert await datasets_table.count() > 0, "No datasets table found"

                # Wait for datasets to load
                await page.wait_for_timeout(2000)

                # Step 4: Check for dataset rows
                dataset_rows = page.locator("tbody tr")
                row_count = await dataset_rows.count()
                print(f"🔍 Found {row_count} training datasets")

                if row_count == 0:
                    # Check if we have any training datasets in the database
                    print("❌ No training datasets displayed in UI")
                    # Let's check the raw API response
                    api_response = await page.evaluate("""
                        fetch('/api/v1/datasets')
                            .then(r => r.json())
                            .then(data => data)
                            .catch(e => ({error: e.message}))
                    """)
                    print(f"🔍 Raw API response: {api_response}")

                    # Skip sequence testing if no datasets
                    pytest.skip("No training datasets available to test sequence visualization")

                # Step 5: Click on the first dataset
                print("🔍 Clicking on first dataset...")
                first_row = dataset_rows.first
                await first_row.click()
                await page.wait_for_timeout(3000)

                # Step 6: Check for sequence data section
                print("🔍 Looking for sequence data section...")

                # Look for sequence data related elements
                sequence_elements = await page.locator('[class*="sequence"], [id*="sequence"], text*="Training Sequence Data"').count()
                print(f"🔍 Found {sequence_elements} sequence-related elements")

                # Step 7: Check for "No sequence data available" message
                no_data_message = page.locator('text="No sequence data available"')
                no_data_count = await no_data_message.count()

                if no_data_count > 0:
                    print("❌ Found 'No sequence data available' message")

                    # Debug: Check what data is actually available
                    print("🔍 Debugging sequence data availability...")

                    # Look for any API calls that might be failing
                    sequence_api_data = await page.evaluate("""
                        // Try to find sequence data in the page
                        const elements = document.querySelectorAll('*');
                        const sequenceInfo = [];
                        for (let el of elements) {
                            if (el.textContent && el.textContent.includes('sequence')) {
                                sequenceInfo.push({
                                    tagName: el.tagName,
                                    className: el.className,
                                    textContent: el.textContent.substring(0, 100)
                                });
                            }
                        }
                        return sequenceInfo.slice(0, 10); // Limit to first 10
                    """)

                    print(f"🔍 Sequence-related elements: {json.dumps(sequence_api_data, indent=2)}")

                    # Check for Plotly containers
                    plotly_containers = await page.locator('[class*="plotly"], [id*="plotly"], .js-plotly-plot').count()
                    print(f"🔍 Found {plotly_containers} Plotly containers")

                    # Check for data tables
                    data_tables = await page.locator('table[class*="data"], .data-table, [class*="sequence-table"]').count()
                    print(f"🔍 Found {data_tables} data tables")

                else:
                    print("✅ No 'No sequence data available' message found")

                # Step 8: Test Plotly visualization loading
                print("🔍 Testing Plotly visualization...")

                # Wait for potential Plotly charts to load
                await page.wait_for_timeout(5000)

                # Check if Plotly is loaded
                plotly_loaded = await page.evaluate("typeof Plotly !== 'undefined'")
                print(f"🔍 Plotly loaded: {plotly_loaded}")

                if plotly_loaded:
                    # Check for actual plots
                    plotly_plots = await page.evaluate("""
                        document.querySelectorAll('.js-plotly-plot').length
                    """)
                    print(f"🔍 Found {plotly_plots} active Plotly plots")
                else:
                    print("❌ Plotly library not loaded")

                # Step 9: Check for specific sequence data elements
                sequence_data_found = False

                # Look for common sequence data indicators
                sequence_indicators = [
                    "OHLC", "Close", "Volume", "timestamp",
                    "Training Sequence", "bars", "±10 bars"
                ]

                for indicator in sequence_indicators:
                    count = await page.locator(f'text*="{indicator}"').count()
                    if count > 0:
                        print(f"✅ Found '{indicator}' indicator ({count} times)")
                        sequence_data_found = True
                    else:
                        print(f"❌ Missing '{indicator}' indicator")

                # Print console logs for debugging
                if console_logs:
                    print("🔍 Console logs:")
                    for log in console_logs[-10:]:  # Last 10 logs
                        print(f"  {log}")

                # Print any page errors
                if page_errors:
                    print("❌ Page errors:")
                    for error in page_errors:
                        print(f"  {error}")

                # Take a screenshot for debugging
                await page.screenshot(path="sequence_visualization_debug.png")
                print("📸 Screenshot saved as sequence_visualization_debug.png")

                # Summary
                if sequence_data_found:
                    print("✅ Some sequence data elements found")
                else:
                    print("❌ No sequence data elements found - this indicates the issue")

            except Exception as e:
                print(f"❌ Error during testing: {e}")
                await page.screenshot(path="sequence_visualization_error.png")
                raise

            finally:
                await browser.close()

    @pytest.mark.asyncio
    async def test_sequence_data_api_endpoints(self):
        """Test the API endpoints that provide sequence data."""

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            try:
                await page.goto("http://localhost:3000")

                # Test the datasets API endpoint
                datasets_response = await page.evaluate("""
                    fetch('/api/v1/datasets')
                        .then(r => r.json())
                        .then(data => data)
                        .catch(e => ({error: e.message}))
                """)

                print(f"🔍 Datasets API response: {json.dumps(datasets_response, indent=2)}")

                # If we have datasets, test sequence data endpoint
                if isinstance(datasets_response, list) and len(datasets_response) > 0:
                    dataset_id = datasets_response[0].get('id')
                    print(f"🔍 Testing sequence data for dataset ID: {dataset_id}")

                    # Test sequence data API endpoint
                    sequence_response = await page.evaluate(f"""
                        fetch('/api/v1/datasets/{dataset_id}/sequences')
                            .then(r => r.json())
                            .then(data => data)
                            .catch(e => ({{error: e.message}}))
                    """)

                    print(f"🔍 Sequence data API response: {json.dumps(sequence_response, indent=2)}")

                    if 'error' in sequence_response:
                        print(f"❌ Sequence API error: {sequence_response['error']}")
                    else:
                        print(f"✅ Sequence API returned data")
                else:
                    print("❌ No datasets found in API response")

            finally:
                await browser.close()

if __name__ == "__main__":
    # Run the tests directly
    import sys
    pytest.main([__file__, "-v", "--tb=short"] + sys.argv[1:])