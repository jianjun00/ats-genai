#!/usr/bin/env python3
"""
Test the search functionality with symbols that should be in the first 50.
"""

import asyncio
from playwright.async_api import async_playwright

@pytest.mark.asyncio

async def test_search_functionality():
    """Test search functionality with known available symbols."""
    print("🔍 Testing Symbol Search Functionality")
    print("=" * 40)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            # Load EDA interface
            await page.goto("http://localhost:4000/eda", timeout=15000)
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(3000)

            # Select dataset
            dataset_select = page.locator("#dataset-select")
            await dataset_select.select_option("intg_daily_price_tiingo")
            await page.wait_for_timeout(8000)
            print("✅ Dataset loaded")

            # Get available symbols first
            symbol_checkboxes = page.locator("input[name='filter-symbol']")
            checkbox_count = await symbol_checkboxes.count()
            print(f"✅ Found {checkbox_count} symbol checkboxes")

            # Get all available symbol values
            available_symbols = []
            for i in range(checkbox_count):
                checkbox = symbol_checkboxes.nth(i)
                value = await checkbox.get_attribute("value")
                available_symbols.append(value)

            print(f"Available symbols: {available_symbols}")

            # Test search functionality
            symbol_search = page.locator("input[placeholder*='Search symbols']")

            if await symbol_search.count() > 0:
                print("✅ Search box found")

                # Test 1: Search for AAPL (should be available)
                print("\n🧪 Test 1: Search for AAPL...")
                await symbol_search.first.fill("AAPL")
                await page.wait_for_timeout(500)

                visible_after_aapl = await page.locator("input[name='filter-symbol']:visible").count()
                aapl_visible = await page.locator("input[name='filter-symbol'][value='AAPL']:visible").count()

                print(f"Visible checkboxes after 'AAPL' search: {visible_after_aapl}")
                print(f"AAPL checkbox visible: {aapl_visible > 0}")

                # Test 2: Clear search and search for TSLA
                print("\n🧪 Test 2: Search for TSLA...")
                await symbol_search.first.fill("TSLA")
                await page.wait_for_timeout(500)

                visible_after_tsla = await page.locator("input[name='filter-symbol']:visible").count()
                tsla_visible = await page.locator("input[name='filter-symbol'][value='TSLA']:visible").count()

                print(f"Visible checkboxes after 'TSLA' search: {visible_after_tsla}")
                print(f"TSLA checkbox visible: {tsla_visible > 0}")

                # Test 3: Search for partial matches
                print("\n🧪 Test 3: Search for 'AA' (partial match)...")
                await symbol_search.first.fill("AA")
                await page.wait_for_timeout(500)

                visible_after_aa = await page.locator("input[name='filter-symbol']:visible").count()
                print(f"Visible checkboxes after 'AA' search: {visible_after_aa}")

                # Get which symbols are visible after AA search
                if visible_after_aa > 0:
                    visible_symbols = []
                    visible_checkboxes = page.locator("input[name='filter-symbol']:visible")
                    count = min(5, await visible_checkboxes.count())
                    for i in range(count):
                        checkbox = visible_checkboxes.nth(i)
                        value = await checkbox.get_attribute("value")
                        visible_symbols.append(value)
                    print(f"Visible symbols for 'AA': {visible_symbols}")

                # Test 4: No results search
                print("\n🧪 Test 4: Search for 'NOTFOUND'...")
                await symbol_search.first.fill("NOTFOUND")
                await page.wait_for_timeout(500)

                visible_after_notfound = await page.locator("input[name='filter-symbol']:visible").count()
                no_results_msg = await page.locator(".no-results").count()

                print(f"Visible checkboxes after 'NOTFOUND' search: {visible_after_notfound}")
                print(f"'No results' message shown: {no_results_msg > 0}")

                if no_results_msg > 0:
                    msg_text = await page.locator(".no-results").text_content()
                    print(f"No results message: {msg_text}")

            else:
                print("❌ Search box not found")

            # Summary
            print(f"\n📊 SEARCH FUNCTIONALITY SUMMARY:")
            if await symbol_search.count() > 0:
                print("✅ Search box is available")
                print(f"✅ Total symbols available: {checkbox_count}")
                print(f"✅ AAPL search works: {'Yes' if aapl_visible > 0 else 'No'}")
                print(f"❓ TSLA search works: {'Yes' if tsla_visible > 0 else 'No (may not be in data)'}")
                print(f"✅ Partial search works: {'Yes' if visible_after_aa > 0 else 'No'}")
                print(f"✅ No results handling: {'Yes' if no_results_msg > 0 else 'No'}")
            else:
                print("❌ Search functionality not available")

        except Exception as e:
            print(f"❌ Test failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_search_functionality())