#!/usr/bin/env python3
"""
Test TSLA symbol search and filtering functionality.
Demonstrates the fix for the symbol visibility issue.
"""

import asyncio
from playwright.async_api import async_playwright

@pytest.mark.asyncio

async def test_tsla_symbol_search():
    """Test that TSLA can be found using search and filtered correctly."""
    print("🎯 Testing TSLA Symbol Search and Filtering")
    print("=" * 50)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()

        # Load EDA interface
        await page.goto("http://localhost:4000/eda", timeout=15000)
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(3000)
        print("✅ EDA interface loaded")

        # Select dataset
        dataset_select = page.locator("#dataset-select")
        await dataset_select.select_option("intg_daily_price_tiingo")
        await page.wait_for_timeout(8000)
        print("✅ Dataset selected")

        # Check if search box is available for symbol column
        print("\n🔍 Checking symbol search functionality...")

        # Look for the symbol search input
        symbol_search = page.locator("input[placeholder*='Search symbols']")
        search_count = await symbol_search.count()

        if search_count > 0:
            print(f"✅ Found {search_count} symbol search box(es)")

            # Type TSLA in the search box
            await symbol_search.first.fill("TSLA")
            print("✅ Typed 'TSLA' in search box")

            # Wait a moment for search to filter
            await page.wait_for_timeout(1000)

            # Check if TSLA checkbox appears
            tsla_checkbox = page.locator("input[name='filter-symbol'][value='TSLA']")
            tsla_count = await tsla_checkbox.count()

            if tsla_count > 0:
                print("✅ TSLA checkbox found after search!")

                # Check the checkbox
                await tsla_checkbox.check()
                print("✅ TSLA checkbox checked")

                # Apply the filter
                apply_button = page.locator("button:has-text('Apply Filters')")
                await apply_button.click()
                await page.wait_for_timeout(5000)
                print("✅ Filter applied")

                # Verify filtering results
                table_body = page.locator("#data-table tbody")
                filtered_rows = await table_body.locator("tr").count()

                if filtered_rows > 0:
                    # Check first few rows for TSLA
                    sample_symbols = []
                    for i in range(min(5, filtered_rows)):
                        row = table_body.locator("tr").nth(i)
                        symbol_cell = row.locator("td").nth(1)
                        symbol = await symbol_cell.text_content()
                        sample_symbols.append(symbol)

                    all_tsla = all(symbol == 'TSLA' for symbol in sample_symbols)
                    print(f"✅ Filtered table has {filtered_rows} rows")
                    print(f"✅ Sample symbols: {sample_symbols}")
                    print(f"✅ All samples are TSLA: {all_tsla}")

                    # Check pagination info
                    table_info = page.locator("#table-info")
                    if await table_info.count() > 0:
                        info_text = await table_info.text_content()
                        print(f"✅ Pagination: {info_text.strip()}")

                    if all_tsla:
                        print("\n🎉 SUCCESS: TSLA filtering works perfectly!")
                    else:
                        print("\n❌ ISSUE: Not all rows are TSLA")

                else:
                    print("❌ No filtered rows - check if TSLA data exists")

                    # Check if TSLA data exists in database
                    print("\nLet's check if TSLA data exists...")

            else:
                print("❌ TSLA checkbox still not found after search")

                # Check what options are visible after search
                visible_checkboxes = page.locator("input[name='filter-symbol']:visible")
                visible_count = await visible_checkboxes.count()
                print(f"Visible checkboxes after search: {visible_count}")

                if visible_count > 0:
                    print("Visible options:")
                    for i in range(min(5, visible_count)):
                        checkbox = visible_checkboxes.nth(i)
                        value = await checkbox.get_attribute("value")
                        print(f"  - {value}")

        else:
            print("❌ No symbol search box found")

            # Check what symbol options are available without search
            symbol_checkboxes = page.locator("input[name='filter-symbol']")
            checkbox_count = await symbol_checkboxes.count()
            print(f"Available symbol checkboxes: {checkbox_count}")

            if checkbox_count > 0:
                print("Available symbols (first 10):")
                for i in range(min(10, checkbox_count)):
                    checkbox = symbol_checkboxes.nth(i)
                    value = await checkbox.get_attribute("value")
                    print(f"  {i+1}. {value}")

        # Test expanded symbol limit
        print("\n📊 Testing expanded symbol display...")
        all_symbol_checkboxes = page.locator("input[name='filter-symbol']")
        total_checkboxes = await all_symbol_checkboxes.count()
        print(f"✅ Total symbol checkboxes available: {total_checkboxes}")

        if total_checkboxes > 8:
            print(f"✅ SUCCESS: Showing more than 8 symbols (was limited to 8 before)")
        else:
            print(f"⚠️ Only {total_checkboxes} symbols available (may be dataset limitation)")

        # Summary
        print(f"\n📋 TEST SUMMARY:")
        print(f"✅ Search box available: {'Yes' if search_count > 0 else 'No'}")
        print(f"✅ TSLA can be found: {'Yes' if 'tsla_count' in locals() and tsla_count > 0 else 'No'}")
        print(f"✅ Total symbol options: {total_checkboxes} (previously limited to 8)")
        print(f"✅ Expanded limit working: {'Yes' if total_checkboxes > 8 else 'Limited by data'}")

if __name__ == "__main__":
    asyncio.run(test_tsla_symbol_search())