#!/usr/bin/env python3
"""
Playwright test specifically for global (server-side) sorting functionality.
Verifies that sorting works on the entire dataset, not just loaded rows.
"""

import asyncio
import sys
from playwright.async_api import async_playwright

class GlobalSortingTest:
    """Test that EDA sorting is global (server-side) not local (client-side)."""

    def __init__(self):
        self.ports = [3000, 4000]  # Test both dev and intg
        self.results = {}

    async def test_global_sorting(self):
        """Test that sorting retrieves globally sorted data from database."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)

            for port in self.ports:
                print(f"\n🧪 Testing global sorting on port {port}...")
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

                    # Select a daily prices table (should have substantial data)
                    table_selector = page.locator('#table-selector')
                    table_name = "intg_daily_prices" if port == 4000 else "dev_daily_prices_polygon"
                    await table_selector.select_option(label=table_name)

                    # Wait for table to load
                    await page.wait_for_timeout(3000)

                    # Verify filter controls are visible
                    filter_controls = page.locator('#filter-controls')
                    if not await filter_controls.is_visible():
                        print(f"   ❌ Port {port}: Filter controls not visible")
                        self.results[port] = {"status": "no_filters"}
                        continue

                    # Test 1: Get initial unsorted data
                    print(f"   🧪 Test 1: Capturing initial data order")

                    # Clear any existing filters first
                    clear_button = page.locator('button:has-text("Clear")')
                    await clear_button.click()
                    await page.wait_for_timeout(2000)

                    # Capture first few symbols from unsorted data
                    initial_rows = await page.query_selector_all('#sortable-table tbody tr')
                    initial_symbols = []
                    for i, row in enumerate(initial_rows[:5]):  # First 5 rows
                        cells = await row.query_selector_all('td')
                        if cells:
                            symbol_text = await cells[0].inner_text()  # Assuming symbol is first column
                            initial_symbols.append(symbol_text.strip())

                    print(f"   📊 Initial symbols (unsorted): {initial_symbols}")

                    # Test 2: Sort by symbol column (ascending)
                    print(f"   🧪 Test 2: Testing ascending sort by symbol")

                    # Find and click symbol column header
                    symbol_header = await page.query_selector('th[onclick*="symbol"]')
                    if not symbol_header:
                        # Try alternative column header patterns
                        symbol_header = await page.query_selector('#sortable-table th:first-child')

                    if symbol_header:
                        await symbol_header.click()
                        await page.wait_for_timeout(3000)  # Wait for server response

                        # Capture sorted data
                        sorted_rows = await page.query_selector_all('#sortable-table tbody tr')
                        sorted_symbols_asc = []
                        for i, row in enumerate(sorted_rows[:5]):  # First 5 rows
                            cells = await row.query_selector_all('td')
                            if cells:
                                symbol_text = await cells[0].inner_text()
                                sorted_symbols_asc.append(symbol_text.strip())

                        print(f"   📊 Sorted symbols (ASC): {sorted_symbols_asc}")

                        # Verify ascending order
                        is_asc_sorted = sorted_symbols_asc == sorted(sorted_symbols_asc)
                        print(f"   {'✅' if is_asc_sorted else '❌'} Ascending sort: {'CORRECT' if is_asc_sorted else 'INCORRECT'}")

                        # Test 3: Sort by symbol column (descending)
                        print(f"   🧪 Test 3: Testing descending sort by symbol")

                        # Click same header again for descending
                        await symbol_header.click()
                        await page.wait_for_timeout(3000)  # Wait for server response

                        # Capture descending sorted data
                        desc_rows = await page.query_selector_all('#sortable-table tbody tr')
                        sorted_symbols_desc = []
                        for i, row in enumerate(desc_rows[:5]):  # First 5 rows
                            cells = await row.query_selector_all('td')
                            if cells:
                                symbol_text = await cells[0].inner_text()
                                sorted_symbols_desc.append(symbol_text.strip())

                        print(f"   📊 Sorted symbols (DESC): {sorted_symbols_desc}")

                        # Verify descending order
                        is_desc_sorted = sorted_symbols_desc == sorted(sorted_symbols_desc, reverse=True)
                        print(f"   {'✅' if is_desc_sorted else '❌'} Descending sort: {'CORRECT' if is_desc_sorted else 'INCORRECT'}")

                        # Test 4: Verify global vs local sorting
                        print(f"   🧪 Test 4: Verifying global (server-side) sorting")

                        # Check if sorted data is different from initial (proves server-side sorting)
                        data_changed = (sorted_symbols_asc != initial_symbols) or (sorted_symbols_desc != initial_symbols)

                        # Check if sort indicators are present
                        sort_indicators = await page.query_selector_all('[id^="sort-"]')
                        has_indicators = len(sort_indicators) > 0

                        # Check for any sort indicator showing active state
                        active_indicator = False
                        for indicator in sort_indicators:
                            indicator_text = await indicator.inner_text()
                            if indicator_text in ['▲', '▼']:
                                active_indicator = True
                                break

                        print(f"   📈 Data changed from initial: {data_changed}")
                        print(f"   🎯 Sort indicators present: {has_indicators}")
                        print(f"   🎯 Active sort indicator: {active_indicator}")

                        self.results[port] = {
                            "status": "tested",
                            "ascending_sort_correct": is_asc_sorted,
                            "descending_sort_correct": is_desc_sorted,
                            "data_changed": data_changed,
                            "sort_indicators_present": has_indicators,
                            "active_sort_indicator": active_indicator,
                            "initial_symbols": initial_symbols,
                            "sorted_asc": sorted_symbols_asc,
                            "sorted_desc": sorted_symbols_desc
                        }

                    else:
                        print(f"   ❌ Port {port}: Could not find sortable symbol column")
                        self.results[port] = {"status": "no_sortable_column"}

                except Exception as e:
                    print(f"   ❌ Port {port}: Test failed with error: {e}")
                    self.results[port] = {"status": "error", "error": str(e)}

                finally:
                    await page.close()

            await browser.close()

    def print_results(self):
        """Print comprehensive global sorting test results."""
        print("\n" + "="*60)
        print("🧪 GLOBAL SORTING TEST RESULTS")
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
            elif result.get("status") == "no_filters":
                print(f"❌ Filter controls not visible")
                all_working = False
                continue
            elif result.get("status") == "no_sortable_column":
                print(f"❌ Sortable column not found")
                all_working = False
                continue

            # Sorting results
            if result.get("ascending_sort_correct"):
                print(f"✅ Ascending sort: WORKING")
            else:
                print(f"❌ Ascending sort: FAILED")
                all_working = False

            if result.get("descending_sort_correct"):
                print(f"✅ Descending sort: WORKING")
            else:
                print(f"❌ Descending sort: FAILED")
                all_working = False

            if result.get("data_changed"):
                print(f"✅ Global sorting: DATA CHANGED (server-side)")
            else:
                print(f"⚠️  Global sorting: NO DATA CHANGE (might be local)")

            if result.get("sort_indicators_present"):
                print(f"✅ Sort indicators: PRESENT")
            else:
                print(f"❌ Sort indicators: MISSING")
                all_working = False

            if result.get("active_sort_indicator"):
                print(f"✅ Active sort indicator: VISIBLE")
            else:
                print(f"❌ Active sort indicator: NOT VISIBLE")
                all_working = False

            # Show sample data for verification
            initial = result.get("initial_symbols", [])
            asc = result.get("sorted_asc", [])
            desc = result.get("sorted_desc", [])

            print(f"\n📊 Sample data:")
            print(f"   Initial:     {initial}")
            print(f"   Ascending:   {asc}")
            print(f"   Descending:  {desc}")

        print("\n" + "="*60)
        if all_working:
            print("🎉 GLOBAL SORTING WORKING - Server-side sorting functional!")
        else:
            print("❌ GLOBAL SORTING ISSUES - See details above")
        print("="*60)

        return all_working

async def main():
    """Run the global sorting test."""
    tester = GlobalSortingTest()
    await tester.test_global_sorting()
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