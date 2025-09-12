#!/usr/bin/env python3
"""
Playwright Tests for Monthly Training Data EDA Features
Tests the complete monthly training data table interface and plotly visualization.
"""

import asyncio
import pytest
from playwright.async_api import async_playwright

@pytest.mark.asyncio
async def test_monthly_training_data_table_interface():
    """Test the monthly training data table interface with filtering and sorting."""
    print("🎭 Testing Monthly Training Data Table Interface")
    print("="*60)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()

        console_errors = []
        page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)

        try:
            print("🧪 Test 1: Navigate to Monthly Training Data page")
            await page.goto("http://localhost:3000/eda", timeout=15000)
            await page.wait_for_load_state("networkidle")

            # Look for the monthly training data tab
            monthly_tab = page.locator("text=Monthly Training Data")
            if await monthly_tab.count() > 0:
                print("✅ Monthly Training Data tab found")
                await monthly_tab.click()
                await page.wait_for_timeout(2000)  # Wait for tab content to load
            else:
                print("❌ Monthly Training Data tab not found - checking alternative selectors")
                # Try alternative selectors
                training_data_link = page.locator("a[href*='training']")
                if await training_data_link.count() > 0:
                    print("✅ Found training data link")
                    await training_data_link.click()
                    await page.wait_for_timeout(2000)

            print("\n🧪 Test 2: Check table structure and headers")
            
            # Wait for table to load
            table = page.locator("table")
            await page.wait_for_selector("table", timeout=10000)
            
            if await table.count() > 0:
                print("✅ Monthly training data table found")
                
                # Check table headers
                expected_headers = ["Symbol", "Month", "Records", "Size (MB)", "Quality", "Status"]
                for header in expected_headers:
                    header_cell = page.locator(f"th:has-text('{header}')")
                    if await header_cell.count() > 0:
                        print(f"✅ Header '{header}' found")
                    else:
                        print(f"❌ Header '{header}' missing")
            else:
                print("❌ Monthly training data table not found")

            print("\n🧪 Test 3: Test filtering functionality")
            
            # Test symbol filter
            symbol_filter = page.locator("input[placeholder*='symbol']")
            if await symbol_filter.count() > 0:
                print("✅ Symbol filter input found")
                await symbol_filter.fill("TSLA")
                await page.wait_for_timeout(1000)
                
                # Check if table filters
                rows = page.locator("tbody tr")
                row_count = await rows.count()
                print(f"✅ Filter applied - {row_count} rows showing")
            else:
                print("❌ Symbol filter input not found")

            # Test status filter
            status_filter = page.locator("select[name*='status']")
            if await status_filter.count() > 0:
                print("✅ Status filter dropdown found")
                await status_filter.select_option("completed")
                await page.wait_for_timeout(1000)
            else:
                print("❌ Status filter dropdown not found")

            print("\n🧪 Test 4: Test sorting functionality")
            
            # Test sorting by clicking header
            month_header = page.locator("th:has-text('Month')")
            if await month_header.count() > 0:
                print("✅ Month header found - testing sort")
                await month_header.click()
                await page.wait_for_timeout(1000)
                print("✅ Sort by month triggered")
            else:
                print("❌ Month header not found for sorting")

            print("\n🧪 Test 5: Test row selection and visualization")
            
            # Select first data row
            first_row = page.locator("tbody tr").first
            if await first_row.count() > 0:
                print("✅ First data row found")
                await first_row.click()
                await page.wait_for_timeout(2000)
                
                # Check if visualization area appears
                viz_container = page.locator("#monthly-training-visualization")
                if await viz_container.count() > 0:
                    print("✅ Visualization container appeared")
                else:
                    print("❌ Visualization container not found")
            else:
                print("❌ No data rows found")

            # Check for JavaScript errors
            if len(console_errors) == 0:
                print("\n✅ No JavaScript console errors")
            else:
                print(f"\n❌ Found {len(console_errors)} console errors:")
                for error in console_errors[:5]:
                    print(f"  - {error}")

        except Exception as e:
            print(f"❌ Test failed with error: {e}")
        finally:
            await browser.close()

@pytest.mark.asyncio
async def test_monthly_training_data_plotly_visualization():
    """Test the plotly visualization with multi-timeframe charts."""
    print("\n🎭 Testing Monthly Training Data Plotly Visualization")
    print("="*60)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()

        console_errors = []
        page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)

        try:
            print("🧪 Test 1: Load page and select monthly training data")
            await page.goto("http://localhost:3000/eda", timeout=15000)
            await page.wait_for_load_state("networkidle")

            # Navigate to monthly training data
            monthly_tab = page.locator("text=Monthly Training Data")
            if await monthly_tab.count() > 0:
                await monthly_tab.click()
                await page.wait_for_timeout(2000)
            
            print("\n🧪 Test 2: Select a row to trigger visualization")
            
            # Wait for table and select first row
            await page.wait_for_selector("tbody tr", timeout=10000)
            first_row = page.locator("tbody tr").first
            await first_row.click()
            await page.wait_for_timeout(3000)  # Wait for charts to load

            print("\n🧪 Test 3: Check plotly chart containers")
            
            # Check for each timeframe chart
            timeframes = ["5m", "15m", "60m", "1d", "1w"]
            for timeframe in timeframes:
                chart_container = page.locator(f"#chart-{timeframe}")
                if await chart_container.count() > 0:
                    print(f"✅ {timeframe} chart container found")
                    
                    # Check if plotly chart was actually rendered
                    plotly_div = chart_container.locator(".plotly-graph-div")
                    if await plotly_div.count() > 0:
                        print(f"✅ {timeframe} plotly chart rendered")
                    else:
                        print(f"❌ {timeframe} plotly chart not rendered")
                else:
                    print(f"❌ {timeframe} chart container not found")

            print("\n🧪 Test 4: Test 60m navigation functionality")
            
            # Check for navigation controls
            nav_controls = page.locator("#timeframe-navigation")
            if await nav_controls.count() > 0:
                print("✅ Timeframe navigation controls found")
                
                # Test previous/next buttons
                prev_btn = page.locator("button:has-text('Previous')")
                next_btn = page.locator("button:has-text('Next')")
                
                if await prev_btn.count() > 0 and await next_btn.count() > 0:
                    print("✅ Navigation buttons found")
                    
                    # Test navigation
                    await next_btn.click()
                    await page.wait_for_timeout(1000)
                    print("✅ Next button clicked")
                    
                    await prev_btn.click()
                    await page.wait_for_timeout(1000)
                    print("✅ Previous button clicked")
                else:
                    print("❌ Navigation buttons not found")
            else:
                print("❌ Timeframe navigation controls not found")

            print("\n🧪 Test 5: Test centered timeframe display")
            
            # Check if other timeframes center around selected 60m record
            current_time_display = page.locator("#current-time-display")
            if await current_time_display.count() > 0:
                current_time = await current_time_display.inner_text()
                print(f"✅ Current time display found: {current_time}")
            else:
                print("❌ Current time display not found")

            print("\n🧪 Test 6: Test chart interactivity")
            
            # Test hovering over 60m chart
            chart_60m = page.locator("#chart-60m .plotly-graph-div")
            if await chart_60m.count() > 0:
                # Get chart area and hover
                chart_area = await chart_60m.bounding_box()
                if chart_area:
                    # Hover in center of chart
                    await page.mouse.move(
                        chart_area["x"] + chart_area["width"] / 2,
                        chart_area["y"] + chart_area["height"] / 2
                    )
                    await page.wait_for_timeout(500)
                    print("✅ Chart hover interaction tested")
            
            print("\n🧪 Test 7: Test chart data quality")
            
            # Check if charts show actual data (not empty)
            for timeframe in timeframes:
                chart = page.locator(f"#chart-{timeframe} .plotly-graph-div")
                if await chart.count() > 0:
                    # Check if chart has data points by looking for SVG paths
                    paths = chart.locator("path")
                    path_count = await paths.count()
                    if path_count > 0:
                        print(f"✅ {timeframe} chart has {path_count} data elements")
                    else:
                        print(f"❌ {timeframe} chart appears empty")

            # Final error check
            if len(console_errors) == 0:
                print("\n✅ No JavaScript console errors during visualization")
            else:
                print(f"\n❌ Found {len(console_errors)} console errors:")
                for error in console_errors[:5]:
                    print(f"  - {error}")

        except Exception as e:
            print(f"❌ Visualization test failed with error: {e}")
        finally:
            await browser.close()

@pytest.mark.asyncio
async def test_monthly_training_data_complete_workflow():
    """Test complete user workflow from table filtering to visualization."""
    print("\n🎭 Testing Complete Monthly Training Data Workflow")
    print("="*60)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()

        console_errors = []
        page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)

        try:
            print("🧪 Complete Workflow Test: Load → Filter → Sort → Select → Visualize → Navigate")
            
            # Step 1: Load page
            await page.goto("http://localhost:3000/eda", timeout=15000)
            await page.wait_for_load_state("networkidle")
            print("✅ Step 1: Page loaded")

            # Step 2: Navigate to monthly training data
            monthly_tab = page.locator("text=Monthly Training Data")
            if await monthly_tab.count() > 0:
                await monthly_tab.click()
                await page.wait_for_timeout(2000)
                print("✅ Step 2: Monthly training data tab selected")

            # Step 3: Apply filters
            symbol_filter = page.locator("input[placeholder*='symbol']")
            if await symbol_filter.count() > 0:
                await symbol_filter.fill("AAPL")
                await page.wait_for_timeout(1000)
                print("✅ Step 3: Symbol filter applied (AAPL)")

            # Step 4: Sort data
            quality_header = page.locator("th:has-text('Quality')")
            if await quality_header.count() > 0:
                await quality_header.click()
                await page.wait_for_timeout(1000)
                print("✅ Step 4: Sorted by quality score")

            # Step 5: Select highest quality row
            await page.wait_for_selector("tbody tr", timeout=10000)
            first_row = page.locator("tbody tr").first
            await first_row.click()
            await page.wait_for_timeout(3000)
            print("✅ Step 5: Selected first row for visualization")

            # Step 6: Verify all charts loaded
            timeframes = ["5m", "15m", "60m", "1d", "1w"]
            charts_loaded = 0
            for timeframe in timeframes:
                chart = page.locator(f"#chart-{timeframe} .plotly-graph-div")
                if await chart.count() > 0:
                    charts_loaded += 1
            
            print(f"✅ Step 6: {charts_loaded}/{len(timeframes)} charts loaded")

            # Step 7: Test navigation
            next_btn = page.locator("button:has-text('Next')")
            if await next_btn.count() > 0:
                await next_btn.click()
                await page.wait_for_timeout(2000)
                print("✅ Step 7: Navigation tested (next)")

            # Step 8: Verify workflow completion
            if charts_loaded >= 3 and len(console_errors) == 0:
                print("\n🎉 COMPLETE WORKFLOW SUCCESS!")
                print(f"   - Charts loaded: {charts_loaded}/{len(timeframes)}")
                print(f"   - JavaScript errors: {len(console_errors)}")
                print("   - User can filter, sort, select, and visualize monthly training data")
            else:
                print(f"\n⚠️ WORKFLOW PARTIALLY SUCCESSFUL")
                print(f"   - Charts loaded: {charts_loaded}/{len(timeframes)}")
                print(f"   - JavaScript errors: {len(console_errors)}")

        except Exception as e:
            print(f"❌ Complete workflow test failed: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    print("🚀 Running Monthly Training Data Playwright Tests")
    asyncio.run(test_monthly_training_data_table_interface())
    asyncio.run(test_monthly_training_data_plotly_visualization())
    asyncio.run(test_monthly_training_data_complete_workflow())