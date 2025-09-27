#!/usr/bin/env python3
"""
Playwright test to diagnose and fix training dataset sequence visualization issues.
"""

import pytest
from playwright.async_api import async_playwright
import json

class TestSequenceVisualizationFix:
    """Test and diagnose sequence visualization issues."""

    @pytest.mark.asyncio
    async def test_training_dataset_page_loads_correctly(self):
        """Test that the training dataset page loads and shows datasets."""

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            # Capture console logs and errors
            console_logs = []
            page.on("console", lambda msg: console_logs.append(f"[{msg.type.upper()}] {msg.text}"))

            page_errors = []
            page.on("pageerror", lambda error: page_errors.append(str(error)))

            print("🔍 Testing training dataset page loading...")

            # Navigate to the correct training datasets endpoint
            await page.goto("http://localhost:3000/api/v1/training-datasets", timeout=15000)
            await page.wait_for_load_state("networkidle")

            # Check if we can see the page content
            page_content = await page.content()
            print(f"🔍 Page loaded, content length: {len(page_content)}")

            # Look for training datasets data
            datasets_api_data = await page.evaluate("""
                fetch('/api/v1/training-datasets')
                    .then(r => r.json())
                    .then(data => data)
                    .catch(e => ({error: e.message}))
            """)

            print(f"🔍 Training datasets API data: {json.dumps(datasets_api_data, indent=2)}")

            # Check if datasets are displayed in the UI
            if 'datasets' in datasets_api_data and len(datasets_api_data['datasets']) > 0:
                print(f"✅ Found {len(datasets_api_data['datasets'])} datasets in API")

                # Look for dataset display elements
                dataset_elements = await page.locator('table, .dataset, [class*="dataset"]').count()
                print(f"🔍 Found {dataset_elements} dataset display elements")

                # Check for specific dataset info
                first_dataset = datasets_api_data['datasets'][0]
                dataset_name = first_dataset.get('dataset_name', '')

                if dataset_name:
                    name_element = await page.locator(f'text*="{dataset_name}"').count()
                    print(f"🔍 Dataset name '{dataset_name}' found in UI: {name_element > 0}")

                    # Try to click on dataset if visible
                    if name_element > 0:
                        await page.locator(f'text*="{dataset_name}"').first.click()
                        await page.wait_for_timeout(3000)
                        print("🔍 Clicked on first dataset")

                        # Check for sequence data section after clicking
                        sequence_section = await page.locator('text*="Training Sequence Data"').count()
                        print(f"🔍 Found sequence data section: {sequence_section > 0}")

                        # Check for "No sequence data available" message
                        no_data_message = await page.locator('text="No sequence data available"').count()
                        if no_data_message > 0:
                            print("❌ Found 'No sequence data available' message")

                            # Debug the sequence data loading
                            await self.debug_sequence_data_loading(page, first_dataset['id'])
                        else:
                            print("✅ No 'No sequence data available' message found")
            else:
                print("❌ No datasets found in API response")

            # Print any errors for debugging
            if console_logs:
                print("🔍 Console logs:")
                for log in console_logs[-5:]:
                    print(f"  {log}")

            if page_errors:
                print("❌ Page errors:")
                for error in page_errors:
                    print(f"  {error}")

            # Take screenshot for debugging
            await page.screenshot(path="training_datasets_page.png")
            print("📸 Screenshot saved as training_datasets_page.png")

    async def debug_sequence_data_loading(self, page, dataset_id):
        """Debug why sequence data is not loading."""

        print(f"🔍 Debugging sequence data loading for dataset {dataset_id}...")

        # Check if there are any sequence-related API calls
        sequence_api_responses = await page.evaluate(f"""
            // Try different sequence data endpoints
            Promise.all([
                fetch('/api/v1/training-datasets/{dataset_id}/sequences').then(r => r.json()).catch(e => ({{endpoint: 'sequences', error: e.message}})),
                fetch('/api/v1/datasets/{dataset_id}/sequences').then(r => r.json()).catch(e => ({{endpoint: 'datasets/sequences', error: e.message}})),
                fetch('/api/ray-analytics/{dataset_id}').then(r => r.json()).catch(e => ({{endpoint: 'ray-analytics', error: e.message}})),
            ])
        """)

        print("🔍 Sequence API responses:")
        for i, response in enumerate(sequence_api_responses):
            print(f"  API {i+1}: {json.dumps(response, indent=4)}")

        # Check for Plotly loading
        plotly_status = await page.evaluate("""
            {
                plotlyLoaded: typeof Plotly !== 'undefined',
                plotlyVersion: typeof Plotly !== 'undefined' ? Plotly.version : null,
                plotlyContainers: document.querySelectorAll('[id*="plotly"], .js-plotly-plot').length
            }
        """)

        print(f"🔍 Plotly status: {json.dumps(plotly_status, indent=2)}")

        # Check for table elements that might hold sequence data
        table_info = await page.evaluate("""
            const tables = document.querySelectorAll('table');
            const tableInfo = Array.from(tables).map((table, i) => ({
                index: i,
                className: table.className,
                id: table.id,
                rowCount: table.rows ? table.rows.length : 0,
                hasData: table.textContent.includes('OHLC') || table.textContent.includes('Close') || table.textContent.includes('Volume')
            }));
            return tableInfo;
        """)

        print(f"🔍 Table information: {json.dumps(table_info, indent=2)}")

    @pytest.mark.asyncio
    async def test_sequence_data_api_directly(self):
        """Test sequence data APIs directly to understand the data structure."""

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            await page.goto("http://localhost:3000")

            # Get training datasets
            datasets = await page.evaluate("""
                fetch('/api/v1/training-datasets')
                    .then(r => r.json())
                    .catch(e => ({error: e.message}))
            """)

            print(f"🔍 Datasets response: {json.dumps(datasets, indent=2)}")

            if 'datasets' in datasets and len(datasets['datasets']) > 0:
                dataset_id = datasets['datasets'][0]['id']
                print(f"🔍 Testing sequence data for dataset ID: {dataset_id}")

                # Test various sequence data endpoints
                endpoints_to_test = [
                    f'/api/v1/training-datasets/{dataset_id}/sequences',
                    f'/api/v1/datasets/{dataset_id}/sequences',
                    f'/api/ray-analytics/{dataset_id}',
                    f'/api/v1/training-datasets/{dataset_id}',
                ]

                for endpoint in endpoints_to_test:
                    response = await page.evaluate(f"""
                        fetch('{endpoint}')
                            .then(r => r.json())
                            .catch(e => ({{error: e.message}}))
                    """)

                    print(f"🔍 Endpoint {endpoint}:")
                    print(f"  Response: {json.dumps(response, indent=4)}")
                    print()

if __name__ == "__main__":
    import sys
    pytest.main([__file__, "-v", "--tb=short"] + sys.argv[1:])