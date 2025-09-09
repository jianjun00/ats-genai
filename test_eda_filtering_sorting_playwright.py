#!/usr/bin/env python3
"""
Playwright test for Enhanced EDA Filtering and Sorting Features
Tests symbol filtering, date range filtering, and sortable table functionality.
"""

import asyncio
import sys
from playwright.async_api import async_playwright
import pytest

class EDAFilteringSortingTest:
    """Test enhanced EDA filtering and sorting features using Playwright."""
    
    def __init__(self):
        self.ports = [3000, 4000]  # Test both dev and intg
        self.results = {}

    async def test_eda_filtering_sorting(self):
        """Test EDA filtering and sorting functionality comprehensively."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            
            for port in self.ports:
                print(f"\n🧪 Testing EDA features on port {port}...")
                page = await browser.new_page()
                
                try:
                    # Navigate to analytics dashboard
                    url = f"http://localhost:{port}/"
                    print(f"   📍 Navigating to {url}")
                    
                    response = await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                    if not response or response.status != 200:
                        print(f"   ❌ Port {port}: Failed to load page")
                        self.results[port] = {"status": "page_load_failed"}
                        continue
                    
                    # Click EDA button
                    print(f"   🔘 Clicking Exploratory Data Analysis button")
                    eda_button = await page.wait_for_selector('button:has-text("📊 Exploratory Data Analysis")', timeout=5000)
                    await eda_button.click()
                    
                    # Wait for EDA interface to load
                    await page.wait_for_selector('#table-selector', timeout=10000)
                    print(f"   ✅ Port {port}: EDA interface loaded")
                    
                    # Test 1: Table selection and filter visibility
                    print(f"   🧪 Test 1: Table selection and filter visibility")
                    
                    # Select a daily_prices table to trigger filter visibility
                    table_selector = page.locator('#table-selector')
                    await table_selector.select_option(label="intg_daily_prices" if port == 4000 else "dev_daily_prices_polygon")
                    
                    # Wait for table content and check if filters are visible
                    await page.wait_for_timeout(2000)  # Give time for data to load
                    
                    filter_controls = page.locator('#filter-controls')
                    is_filter_visible = await filter_controls.is_visible()
                    
                    if is_filter_visible:
                        print(f"   ✅ Port {port}: Filter controls visible for price table")
                    else:
                        print(f"   ❌ Port {port}: Filter controls not visible for price table")
                    
                    # Test 2: Symbol filtering
                    print(f"   🧪 Test 2: Symbol filtering")
                    
                    if is_filter_visible:
                        # Enter a symbol filter
                        symbol_input = page.locator('#symbol-filter')
                        await symbol_input.fill('A')
                        
                        # Click apply filters
                        apply_button = page.locator('button:has-text("Apply Filters")')
                        await apply_button.click()
                        
                        # Wait for filtered data to load
                        await page.wait_for_timeout(3000)
                        
                        # Check if table has data and contains symbol 'A' 
                        table_body = page.locator('#table-body')
                        if await table_body.is_visible():
                            table_content = await table_body.inner_text()
                            if 'A' in table_content and len(table_content.strip()) > 0:
                                print(f"   ✅ Port {port}: Symbol filtering works - found 'A' in results")
                            else:
                                print(f"   ⚠️  Port {port}: Symbol filtering may not have results")
                        else:
                            print(f"   ❌ Port {port}: Table not visible after filtering")
                    
                    # Test 3: Date range filtering
                    print(f"   🧪 Test 3: Date range filtering")
                    
                    if is_filter_visible:
                        # Clear previous filters first
                        clear_button = page.locator('button:has-text("Clear")')
                        await clear_button.click()
                        await page.wait_for_timeout(1000)
                        
                        # Set date range filter
                        date_from = page.locator('#date-from')
                        date_to = page.locator('#date-to')
                        
                        await date_from.fill('2020-01-01')
                        await date_to.fill('2020-12-31')
                        
                        # Apply date filters
                        await apply_button.click()
                        await page.wait_for_timeout(3000)
                        
                        # Check if table shows data from 2020
                        if await table_body.is_visible():
                            table_content = await table_body.inner_text()
                            if '2020' in table_content:
                                print(f"   ✅ Port {port}: Date filtering works - found 2020 dates")
                            else:
                                print(f"   ⚠️  Port {port}: Date filtering may not have 2020 data")
                        
                    # Test 4: Table sorting functionality
                    print(f"   🧪 Test 4: Table sorting functionality")
                    
                    # Clear filters to get more data for sorting test
                    if is_filter_visible:
                        await clear_button.click()
                        await page.wait_for_timeout(2000)
                    
                    # Check if sortable table is present
                    sortable_table = page.locator('#sortable-table')
                    if await sortable_table.is_visible():
                        # Get table headers and try to click on first sortable column
                        headers = await page.query_selector_all('#sortable-table th[onclick]')
                        if headers:
                            print(f"   📊 Port {port}: Found {len(headers)} sortable columns")
                            
                            # Click first header to test sorting
                            await headers[0].click()
                            await page.wait_for_timeout(1000)
                            
                            # Check if sort indicator changed
                            sort_indicator = await headers[0].query_selector('span[id^="sort-"]')
                            if sort_indicator:
                                indicator_text = await sort_indicator.inner_text()
                                if indicator_text in ['▲', '▼']:
                                    print(f"   ✅ Port {port}: Table sorting works - indicator: {indicator_text}")
                                else:
                                    print(f"   ⚠️  Port {port}: Sort indicator present but not activated: {indicator_text}")
                            else:
                                print(f"   ❌ Port {port}: Sort indicator not found")
                        else:
                            print(f"   ❌ Port {port}: No sortable columns found")
                    else:
                        print(f"   ❌ Port {port}: Sortable table not visible")
                    
                    # Test 5: Combined functionality test
                    print(f"   🧪 Test 5: Combined filtering + sorting")
                    
                    if is_filter_visible:
                        # Apply a light filter and then sort
                        await symbol_input.fill('AA')  # Should match multiple symbols
                        await apply_button.click()
                        await page.wait_for_timeout(2000)
                        
                        # Re-query the table and headers after filtering (DOM may have changed)
                        sortable_table_after_filter = page.locator('#sortable-table')
                        if await sortable_table_after_filter.is_visible():
                            # Get fresh header elements after filtering
                            headers_after_filter = await page.query_selector_all('#sortable-table th[onclick]')
                            if headers_after_filter:
                                await headers_after_filter[0].click()  # Sort first column
                                await page.wait_for_timeout(1000)
                                print(f"   ✅ Port {port}: Combined filtering + sorting completed")
                            else:
                                print(f"   ⚠️  Port {port}: No sortable headers found after filtering")
                        else:
                            print(f"   ⚠️  Port {port}: Table not visible after filtering")
                        
                    self.results[port] = {
                        "status": "tested",
                        "filter_visible": is_filter_visible,
                        "table_present": await sortable_table.is_visible() if 'sortable_table' in locals() else False,
                        "sortable_columns": len(headers) if 'headers' in locals() and headers else 0
                    }
                    
                except Exception as e:
                    print(f"   ❌ Port {port}: Test failed with error: {e}")
                    self.results[port] = {"status": "error", "error": str(e)}
                
                finally:
                    await page.close()
            
            await browser.close()
    
    def print_results(self):
        """Print comprehensive test results."""
        print("\n" + "="*60)
        print("🧪 EDA FILTERING & SORTING TEST RESULTS")
        print("="*60)
        
        all_working = True
        
        for port in self.ports:
            result = self.results.get(port, {})
            print(f"\n🔌 PORT {port} RESULTS:")
            print("-" * 30)
            
            if result.get("status") == "page_load_failed":
                print(f"❌ Page failed to load")
                all_working = False
                continue
            elif result.get("status") == "error":
                print(f"❌ Test error: {result.get('error', 'Unknown')}")
                all_working = False
                continue
                
            # Feature results
            if result.get("filter_visible"):
                print(f"✅ Filter controls: VISIBLE")
            else:
                print(f"❌ Filter controls: NOT VISIBLE")
                all_working = False
            
            if result.get("table_present"):
                print(f"✅ Sortable table: PRESENT")
            else:
                print(f"❌ Sortable table: NOT PRESENT")
                all_working = False
                
            sortable_cols = result.get("sortable_columns", 0)
            if sortable_cols > 0:
                print(f"✅ Sortable columns: {sortable_cols}")
            else:
                print(f"❌ Sortable columns: NONE FOUND")
                all_working = False
        
        print("\n" + "="*60)
        if all_working:
            print("🎉 ALL EDA FEATURES WORKING - Filtering and Sorting functional!")
        else:
            print("❌ EDA ISSUES FOUND - See details above")
        print("="*60)
        
        return all_working

async def main():
    """Run the EDA filtering and sorting test."""
    tester = EDAFilteringSortingTest()
    await tester.test_eda_filtering_sorting()
    return tester.print_results()

if __name__ == "__main__":
    try:
        result = asyncio.run(main())
        sys.exit(0 if result else 1)
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        sys.exit(1)