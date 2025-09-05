#!/usr/bin/env python3
"""
Playwright test for training dataset sequence loading functionality.
This test verifies the complete user workflow from dataset selection to sequence visualization.
"""

import pytest
from playwright.async_api import async_playwright, Page, expect
import asyncio
import time


@pytest.mark.asyncio
async def test_training_dataset_sequence_selection():
    """Test complete training dataset sequence selection workflow."""
    
    async with async_playwright() as p:
        # Launch browser
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        try:
            # Navigate to EDA dashboard
            await page.goto("http://localhost:3000/eda")
            await page.wait_for_load_state("networkidle")
            
            # Verify page loaded
            await expect(page.locator("h1")).to_contain_text("ATS Unified Analytics Dashboard")
            
            # Click on Training Datasets tab (use button selector to be specific)
            training_datasets_button = page.get_by_role("button", name="🤖 Training Datasets")
            await training_datasets_button.click()
            await page.wait_for_timeout(2000)  # Wait for datasets to load
            
            # Verify datasets loaded (should see dataset selector)
            dataset_selector = page.locator("#dataset-selector")
            await expect(dataset_selector).to_be_visible()
            
            # Get available datasets
            dataset_options = await dataset_selector.locator("option").count()
            print(f"Found {dataset_options} dataset options")
            
            if dataset_options > 1:  # More than just the "Choose a dataset..." option
                # Select the first real dataset (index 1, since 0 is "Choose a dataset...")
                await dataset_selector.select_option(index=1)
                await page.wait_for_timeout(3000)  # Wait for sequences to load
                
                # Check if sequence selector is now enabled and has options
                sequence_selector = page.locator("#sequence-selector")
                await expect(sequence_selector).to_be_visible()
                
                # Check if sequences loaded
                sequence_options = await sequence_selector.locator("option").count()
                print(f"Found {sequence_options} sequence options")
                
                if sequence_options > 1:  # More than just the "Choose a sequence..." option
                    print("✅ PASS: Sequences loaded successfully")
                    
                    # Try to select a sequence
                    await sequence_selector.select_option(index=1)
                    await page.wait_for_timeout(3000)
                    
                    # Check if Plotly visualization loaded
                    plotly_chart = page.locator(".plotly")
                    visualization_content = page.locator("#ohlc-chart")
                    
                    if await plotly_chart.count() > 0:
                        print("✅ PASS: Plotly visualization loaded")
                        
                        # Check if visualization has actual data (not empty)
                        plotly_data = page.locator(".plotly .plot-container")
                        if await plotly_data.count() > 0:
                            print("✅ PASS: Plotly chart contains data")
                        else:
                            print("❌ FAIL: Plotly chart is empty")
                            
                    elif await visualization_content.count() > 0:
                        print("⚠️  WARNING: Chart container exists but no Plotly content")
                    else:
                        print("❌ FAIL: No visualization loaded")
                        
                    # Check if Training Sequence Data table is populated
                    sequence_table = page.locator("#sequence-data-table")
                    if await sequence_table.count() > 0:
                        print("✅ PASS: Sequence data table found")
                        
                        # Check for table rows (should have data beyond headers)
                        table_rows = sequence_table.locator("tr")
                        row_count = await table_rows.count()
                        
                        if row_count > 1:  # More than just header row
                            print(f"✅ PASS: Sequence data table has {row_count - 1} data rows")
                            
                            # Check if table contains actual data (not "No sequence data available")
                            table_content = await sequence_table.inner_text()
                            if "No sequence data available" in table_content:
                                print("❌ FAIL: Table shows 'No sequence data available'")
                            else:
                                print("✅ PASS: Table contains actual sequence data")
                        else:
                            print("❌ FAIL: Sequence data table is empty")
                    else:
                        print("❌ FAIL: Sequence data table not found")
                        
                    # Additional check: Look for the specific "Training Sequence Data" heading
                    sequence_heading = page.locator("text=📋 Training Sequence Data")
                    if await sequence_heading.count() > 0:
                        print("✅ PASS: Training Sequence Data section found")
                    else:
                        print("❌ FAIL: Training Sequence Data section not found")
                        
                else:
                    print("❌ FAIL: No sequences found for selected dataset")
                    # This is the bug we're trying to fix
                    
                    # Let's check the API response directly
                    dataset_id = await dataset_selector.input_value()
                    if dataset_id:
                        sequences_response = await page.request.get(f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences")
                        sequences_data = await sequences_response.json()
                        print(f"API Response: {sequences_data}")
                        
                        if sequences_data.get('sequences', []):
                            print("❌ FAIL: API has sequences but UI doesn't show them")
                        else:
                            print("❌ FAIL: API returns no sequences - this is the root issue")
            else:
                print("❌ FAIL: No datasets found")
                
        except Exception as e:
            print(f"❌ ERROR: {e}")
            
        finally:
            await context.close()
            await browser.close()


@pytest.mark.asyncio  
async def test_training_dataset_api_endpoints():
    """Test training dataset API endpoints directly."""
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        try:
            # Test main datasets endpoint
            response = await page.request.get("http://localhost:3000/api/v1/training-datasets")
            data = await response.json()
            
            datasets = data.get('datasets', [])
            print(f"Found {len(datasets)} datasets")
            
            if datasets:
                dataset_id = datasets[0]['id']
                print(f"Testing sequences for dataset {dataset_id}")
                
                # Test sequences endpoint
                sequences_response = await page.request.get(f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences")
                sequences_data = await sequences_response.json()
                
                print(f"Sequences response: {sequences_data}")
                
                sequences = sequences_data.get('sequences', [])
                if sequences:
                    print(f"✅ PASS: Found {len(sequences)} sequences")
                else:
                    print("❌ FAIL: No sequences returned by API")
                    
                # Test visualization data endpoint
                viz_response = await page.request.get(f"http://localhost:3000/api/v1/training-datasets/{dataset_id}/visualization-data?start_idx=0")
                viz_data = await viz_response.json()
                
                if viz_data.get('error'):
                    print(f"❌ FAIL: Visualization API error: {viz_data['error']}")
                else:
                    print("✅ PASS: Visualization API working")
                    
            else:
                print("❌ FAIL: No datasets returned by main API")
                
        except Exception as e:
            print(f"❌ ERROR: {e}")
            
        finally:
            await context.close()
            await browser.close()


@pytest.mark.asyncio
async def test_plotly_visualization_and_sequence_data():
    """Test that Plotly visualization and sequence data table work correctly."""
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        try:
            # Navigate to EDA dashboard
            await page.goto("http://localhost:3000/eda")
            await page.wait_for_load_state("networkidle")
            
            # Switch to Training Datasets tab
            training_datasets_button = page.get_by_role("button", name="🤖 Training Datasets")
            await training_datasets_button.click()
            await page.wait_for_timeout(2000)
            
            # Select first dataset
            dataset_selector = page.locator("#dataset-selector")
            await dataset_selector.select_option(index=1)
            await page.wait_for_timeout(3000)
            
            # Select first sequence
            sequence_selector = page.locator("#sequence-selector")
            await sequence_selector.select_option(index=1)
            await page.wait_for_timeout(5000)  # Give more time for visualization to load
            
            print("\n=== TESTING PLOTLY VISUALIZATION ===")
            
            # Test Plotly chart loading
            plotly_elements = [
                ".plotly",
                ".js-plotly-plot", 
                ".plotly-graph-div",
                ".plot-container"
            ]
            
            plotly_found = False
            for selector in plotly_elements:
                element = page.locator(selector)
                if await element.count() > 0:
                    print(f"✅ PASS: Found Plotly element: {selector}")
                    plotly_found = True
                    
                    # Check if element is visible
                    if await element.is_visible():
                        print(f"✅ PASS: Plotly element {selector} is visible")
                    else:
                        print(f"❌ FAIL: Plotly element {selector} exists but not visible")
                        
            if not plotly_found:
                print("❌ FAIL: No Plotly visualization elements found")
                
            # Check for canvas element (Plotly renders to canvas)
            canvas = page.locator("canvas")
            if await canvas.count() > 0:
                print("✅ PASS: Canvas element found (Plotly likely rendered)")
            else:
                print("❌ FAIL: No canvas element found")
                
            # Check for SVG elements (alternative Plotly rendering)
            svg = page.locator("svg")
            if await svg.count() > 0:
                print("✅ PASS: SVG element found (Plotly likely rendered)")
            else:
                print("❌ FAIL: No SVG element found")
                
            print("\n=== TESTING SEQUENCE DATA TABLE ===")
            
            # Test sequence data table
            table_selectors = [
                "#sequence-data-table",
                "[data-testid='sequence-table']",
                "table:has(th:contains('Timestamp'))",
                "table:has(th:contains('Open'))"
            ]
            
            table_found = False
            for selector in table_selectors:
                try:
                    element = page.locator(selector)
                    if await element.count() > 0:
                        print(f"✅ PASS: Found table element: {selector}")
                        table_found = True
                        
                        # Check table content
                        table_text = await element.inner_text()
                        if "No sequence data available" in table_text:
                            print("❌ FAIL: Table shows 'No sequence data available'")
                        else:
                            print("✅ PASS: Table has content (not empty message)")
                            
                        # Check for data rows
                        rows = element.locator("tr")
                        row_count = await rows.count()
                        if row_count > 1:
                            print(f"✅ PASS: Table has {row_count - 1} data rows")
                        else:
                            print("❌ FAIL: Table has no data rows")
                        break
                except Exception as e:
                    continue
                    
            if not table_found:
                print("❌ FAIL: No sequence data table found")
                
            # Check for the section heading
            heading_selectors = [
                "text=📋 Training Sequence Data",
                ":has-text('Training Sequence Data')",
                "h3:has-text('Training Sequence Data')",
                "*:has-text('±10 bars from selected row')"
            ]
            
            for selector in heading_selectors:
                try:
                    element = page.locator(selector)
                    if await element.count() > 0:
                        print(f"✅ PASS: Found heading: {selector}")
                        break
                except Exception as e:
                    continue
            else:
                print("❌ FAIL: Training Sequence Data heading not found")
                
            # Take a screenshot for debugging
            await page.screenshot(path="/tmp/training_sequence_test.png", full_page=True)
            print("📸 Screenshot saved to /tmp/training_sequence_test.png")
            
            # Debug: Print page content to understand what's actually rendered
            page_content = await page.content()
            with open("/tmp/page_debug.html", "w") as f:
                f.write(page_content)
            print("🔍 Full page content saved to /tmp/page_debug.html")
            
            # Debug: Check for JavaScript errors
            console_messages = []
            page.on("console", lambda msg: console_messages.append(f"{msg.type}: {msg.text}"))
            
            if console_messages:
                print("\n🐛 Console messages:")
                for msg in console_messages[-10:]:  # Show last 10 messages
                    print(f"  {msg}")
                    
            # Debug: Check what APIs are being called
            network_requests = []
            page.on("request", lambda request: network_requests.append(f"{request.method} {request.url}"))
            
            if network_requests:
                print("\n🌐 Network requests:")
                for req in network_requests[-10:]:  # Show last 10 requests
                    print(f"  {req}")
                
        except Exception as e:
            print(f"❌ ERROR: {e}")
            await page.screenshot(path="/tmp/error_screenshot.png", full_page=True)
            
        finally:
            await context.close()
            await browser.close()


if __name__ == "__main__":
    # Run the test directly
    asyncio.run(test_training_dataset_sequence_selection())
    asyncio.run(test_training_dataset_api_endpoints())
    asyncio.run(test_plotly_visualization_and_sequence_data())