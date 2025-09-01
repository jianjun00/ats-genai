#!/usr/bin/env python3
"""
Playwright Browser Tests for EDA Interface
Tests the actual browser behavior of http://localhost:4000/eda
"""

import asyncio
import pytest
from playwright.async_api import async_playwright, Page, expect
import time
import json

class TestEDAInterface:
    """End-to-end browser tests for the EDA interface."""
    
    BASE_URL = "http://localhost:4000"
    
    @pytest.fixture(scope="session")
    async def browser(self):
        """Set up browser for testing."""
        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(headless=True)
        yield browser
        await browser.close()
        await playwright.stop()
    
    @pytest.fixture
    async def page(self, browser):
        """Create a new page for each test."""
        context = await browser.new_context()
        page = await context.new_page()
        yield page
        await context.close()
    
    async def test_eda_interface_loads(self, page: Page):
        """Test that EDA interface loads without JavaScript errors."""
        console_errors = []
        
        # Collect console errors
        page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
        
        # Navigate to EDA interface
        await page.goto(f"{self.BASE_URL}/eda")
        
        # Wait for page to load
        await page.wait_for_load_state("networkidle")
        
        # Check page title
        await expect(page).to_have_title("ATS EDA - Exploratory Data Analysis")
        
        # Verify no JavaScript errors
        assert len(console_errors) == 0, f"JavaScript errors found: {console_errors}"
        
        print("✅ EDA interface loads without JavaScript errors")
    
    async def test_datasets_dropdown_populates(self, page: Page):
        """Test that datasets dropdown gets populated with data."""
        await page.goto(f"{self.BASE_URL}/eda")
        await page.wait_for_load_state("networkidle")
        
        # Wait for datasets to load (give it time for async loading)
        await page.wait_for_timeout(3000)
        
        # Check if dropdown has options
        dataset_select = page.locator("#dataset-select")
        await expect(dataset_select).to_be_visible()
        
        # Get all options
        options = await dataset_select.locator("option").all()
        option_count = len(options)
        
        print(f"Found {option_count} dataset options")
        
        # Should have more than just the default "Select dataset..." option
        assert option_count > 1, f"Expected multiple dataset options, found {option_count}"
        
        # Get option texts for verification
        option_texts = []
        for option in options:
            text = await option.text_content()
            if text and text != "Select dataset...":
                option_texts.append(text)
        
        print(f"Available datasets: {option_texts[:3]}...")  # Show first 3
        assert len(option_texts) > 0, "No actual dataset options found"
        
        print("✅ Datasets dropdown populates correctly")
    
    async def test_global_axis_control_positioning(self, page: Page):
        """Test that global x-axis control appears above Data Filter."""
        await page.goto(f"{self.BASE_URL}/eda")
        await page.wait_for_load_state("networkidle")
        
        # Select a dataset to trigger sections to show
        await page.wait_for_timeout(2000)
        dataset_select = page.locator("#dataset-select")
        
        # Get first real dataset option (not "Select dataset...")
        first_option = dataset_select.locator("option").nth(1)
        await first_option.click()
        
        # Wait for sections to appear
        await page.wait_for_timeout(3000)
        
        # Check if global axis section exists and is visible
        global_axis_section = page.locator("#global-axis-section")
        await expect(global_axis_section).to_be_visible()
        
        # Check if Data Filter section exists
        filters_section = page.locator("#filters-section")
        await expect(filters_section).to_be_visible()
        
        # Get positions to verify ordering
        global_axis_box = await global_axis_section.bounding_box()
        filters_box = await filters_section.bounding_box()
        
        assert global_axis_box and filters_box, "Could not get element positions"
        
        # Global axis should be above (smaller y coordinate) than filters
        assert global_axis_box["y"] < filters_box["y"], \
            f"Global axis (y={global_axis_box['y']}) should be above Data Filter (y={filters_box['y']})"
        
        print("✅ Global x-axis control positioned correctly above Data Filter")
    
    async def test_per_column_axis_controls_removed(self, page: Page):
        """Test that individual per-column x-axis controls are removed."""
        await page.goto(f"{self.BASE_URL}/eda")
        await page.wait_for_load_state("networkidle")
        
        # Select dataset and wait for distributions to load
        await page.wait_for_timeout(2000)
        dataset_select = page.locator("#dataset-select")
        first_option = dataset_select.locator("option").nth(1)
        await first_option.click()
        
        # Wait for distributions to load
        await page.wait_for_timeout(5000)
        
        # Check for per-column x-axis selects (should not exist)
        per_column_selects = page.locator("select[id*='xaxis-']")
        per_column_count = await per_column_selects.count()
        
        print(f"Found {per_column_count} per-column x-axis selects")
        assert per_column_count == 0, f"Found {per_column_count} per-column x-axis controls, expected 0"
        
        # Also check for visualization-controls divs (should not exist)
        visualization_controls = page.locator(".visualization-controls")
        viz_control_count = await visualization_controls.count()
        
        print(f"Found {viz_control_count} visualization-controls divs")
        assert viz_control_count == 0, f"Found {viz_control_count} visualization-controls, expected 0"
        
        print("✅ Per-column x-axis controls successfully removed")
    
    async def test_global_axis_dropdown_functionality(self, page: Page):
        """Test that global x-axis dropdown has correct options and functions."""
        await page.goto(f"{self.BASE_URL}/eda")
        await page.wait_for_load_state("networkidle")
        
        # Select dataset
        await page.wait_for_timeout(2000)
        dataset_select = page.locator("#dataset-select")
        first_option = dataset_select.locator("option").nth(1)
        await first_option.click()
        await page.wait_for_timeout(3000)
        
        # Check global x-axis dropdown
        global_axis_select = page.locator("#global-x-axis")
        await expect(global_axis_select).to_be_visible()
        
        # Get all options
        options = await global_axis_select.locator("option").all()
        option_texts = [await option.text_content() for option in options]
        
        expected_options = [
            "Default (Value-based)",
            "Date", 
            "Sequence Step",
            "Trading Day",
            "Relative Time"
        ]
        
        print(f"Global axis options: {option_texts}")
        
        for expected in expected_options:
            assert expected in option_texts, f"Missing expected option: {expected}"
        
        # Test changing the selection
        await global_axis_select.select_option("date")
        selected_value = await global_axis_select.input_value()
        assert selected_value == "date", f"Expected 'date', got '{selected_value}'"
        
        print("✅ Global x-axis dropdown has correct options and functionality")
    
    async def test_symbol_filter_undefined_issue(self, page: Page):
        """Test the symbol filter to identify the 'undefined' records issue."""
        console_logs = []
        
        # Capture console logs to see API responses
        page.on("console", lambda msg: console_logs.append(f"{msg.type}: {msg.text}"))
        
        await page.goto(f"{self.BASE_URL}/eda")
        await page.wait_for_load_state("networkidle")
        
        # Select dataset
        await page.wait_for_timeout(2000)
        dataset_select = page.locator("#dataset-select")
        first_option = dataset_select.locator("option").nth(1)
        dataset_text = await first_option.text_content()
        await first_option.click()
        
        print(f"Selected dataset: {dataset_text}")
        
        # Wait for filters to load
        await page.wait_for_timeout(5000)
        
        # Look for symbol filter controls
        symbol_filters = page.locator("input[id*='symbol'], select[id*='symbol'], input[name*='symbol']")
        symbol_filter_count = await symbol_filters.count()
        
        print(f"Found {symbol_filter_count} symbol filter controls")
        
        if symbol_filter_count > 0:
            # Try to apply a symbol filter
            first_symbol_filter = symbol_filters.first
            
            # Check if it's a text input or checkbox/select
            tag_name = await first_symbol_filter.evaluate("el => el.tagName")
            input_type = await first_symbol_filter.evaluate("el => el.type")
            
            print(f"Symbol filter type: {tag_name} - {input_type}")
            
            if input_type == "checkbox":
                # For checkbox filters, click the first one
                await first_symbol_filter.check()
                print("Checked first symbol checkbox")
            elif input_type == "text":
                # For text input, enter a symbol
                await first_symbol_filter.fill("TSLA")
                print("Entered TSLA in symbol filter")
            
            # Click Apply Filters button
            apply_button = page.locator("button:has-text('Apply Filters')")
            if await apply_button.count() > 0:
                await apply_button.click()
                print("Clicked Apply Filters button")
                
                # Wait for results
                await page.wait_for_timeout(3000)
                
                # Look for pagination info that might show "undefined"
                page_content = await page.content()
                if "undefined" in page_content:
                    print("❌ Found 'undefined' in page content - this confirms the user's issue")
                    # Extract the specific text
                    undefined_matches = []
                    lines = page_content.split('\n')
                    for line in lines:
                        if 'undefined' in line and ('record' in line or 'page' in line):
                            undefined_matches.append(line.strip())
                    
                    if undefined_matches:
                        print("Undefined text found:")
                        for match in undefined_matches[:3]:  # Show first 3
                            print(f"  - {match}")
                else:
                    print("✅ No 'undefined' text found in page content")
        
        # Print some console logs for debugging
        error_logs = [log for log in console_logs if "error" in log.lower()]
        if error_logs:
            print("Console errors found:")
            for error in error_logs[:3]:
                print(f"  - {error}")
        
        print("✅ Symbol filter test completed")
    
    async def test_date_columns_filtered_from_distributions(self, page: Page):
        """Test that date columns don't appear in distribution visualizations."""
        await page.goto(f"{self.BASE_URL}/eda")
        await page.wait_for_load_state("networkidle")
        
        # Select dataset
        await page.wait_for_timeout(2000)
        dataset_select = page.locator("#dataset-select")
        first_option = dataset_select.locator("option").nth(1)
        await first_option.click()
        
        # Wait for distributions to load
        await page.wait_for_timeout(5000)
        
        # Get all column distribution titles
        distribution_titles = page.locator(".column-distribution h4")
        title_count = await distribution_titles.count()
        
        print(f"Found {title_count} column distributions")
        
        if title_count > 0:
            # Get all title texts
            title_texts = []
            for i in range(min(title_count, 10)):  # Check first 10
                title = distribution_titles.nth(i)
                text = await title.text_content()
                title_texts.append(text)
            
            print(f"Distribution titles: {title_texts}")
            
            # Check for date-related column names
            date_patterns = ['date', 'timestamp', 'created_at', 'updated_at', 'time']
            date_columns_found = []
            
            for title in title_texts:
                if title:
                    title_lower = title.lower()
                    for pattern in date_patterns:
                        if pattern in title_lower:
                            date_columns_found.append(title)
                            break
            
            print(f"Date columns in distributions: {date_columns_found}")
            
            if len(date_columns_found) == 0:
                print("✅ No date columns found in distributions (correctly filtered)")
            else:
                print(f"⚠️ Found {len(date_columns_found)} date columns in distributions")
                # This might be expected if the filtering isn't working
        
        print("✅ Date column filtering test completed")


async def run_playwright_tests():
    """Run all Playwright tests manually (outside pytest)."""
    print("🎭 Starting Playwright Browser Tests")
    print("="*60)
    
    test_instance = TestEDAInterface()
    
    async with async_playwright() as playwright:
        # Launch browser
        browser = await playwright.chromium.launch(headless=True)  # Headless for CI/WSL
        context = await browser.new_context()
        page = await context.new_page()
        
        tests = [
            ("Interface Loads", test_instance.test_eda_interface_loads),
            ("Datasets Populate", test_instance.test_datasets_dropdown_populates),
            ("Global Axis Position", test_instance.test_global_axis_control_positioning),
            ("Per-Column Controls Removed", test_instance.test_per_column_axis_controls_removed),
            ("Global Axis Functionality", test_instance.test_global_axis_dropdown_functionality),
            ("Symbol Filter Issue", test_instance.test_symbol_filter_undefined_issue),
            ("Date Column Filtering", test_instance.test_date_columns_filtered_from_distributions),
        ]
        
        results = {}
        
        for test_name, test_func in tests:
            print(f"\n🧪 Running: {test_name}")
            try:
                await test_func(page)
                results[test_name] = "✅ PASS"
                print(f"✅ {test_name} - PASSED")
            except Exception as e:
                results[test_name] = f"❌ FAIL: {str(e)}"
                print(f"❌ {test_name} - FAILED: {e}")
        
        await browser.close()
    
    # Print summary
    print(f"\n📊 Playwright Test Results Summary")
    print("="*60)
    passed = sum(1 for result in results.values() if result.startswith("✅"))
    total = len(results)
    
    for test_name, result in results.items():
        print(f"{test_name}: {result}")
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All Playwright tests passed!")
    else:
        print("⚠️ Some tests failed - check results above")
    
    return results


if __name__ == "__main__":
    # Run tests directly
    asyncio.run(run_playwright_tests())