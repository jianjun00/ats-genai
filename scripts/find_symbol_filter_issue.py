#!/usr/bin/env python3
"""
Find and fix the symbol filter issue
"""

import asyncio
from playwright.async_api import async_playwright

async def find_symbol_filter_issue():
    """Find the root cause of symbol filter not working."""
    print("🔍 Finding Symbol Filter Issue")
    print("="*40)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto("http://localhost:4000/eda", timeout=15000)
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(3000)

        # Select dataset
        print("🧪 Selecting dataset...")
        dataset_select = page.locator("#dataset-select")
        await dataset_select.select_option("intg_daily_price_tiingo")
        await page.wait_for_timeout(8000)  # Wait longer for data to load

        # Find ALL tables on the page
        print("🔍 Looking for tables...")
        tables = page.locator("table")
        table_count = await tables.count()
        print(f"Found {table_count} table(s)")

        for i in range(table_count):
            table = tables.nth(i)
            table_id = await table.get_attribute("id")
            table_class = await table.get_attribute("class")
            print(f"  Table {i+1}: id='{table_id}', class='{table_class}'")

            # Check if this table has data
            tbody = table.locator("tbody")
            if await tbody.count() > 0:
                rows = await tbody.locator("tr").count()
                print(f"    -> Has {rows} rows")

                if rows > 0:
                    # Get first row data
                    first_row = tbody.locator("tr").first
                    cells = await first_row.locator("td").count()
                    if cells > 0:
                        first_cell_text = await first_row.locator("td").first.text_content()
                        print(f"    -> First cell: {first_cell_text}")

        # Look for filter controls
        print("\n🔧 Looking for filter controls...")

        # Look for checkboxes
        checkboxes = page.locator("input[type='checkbox']")
        checkbox_count = await checkboxes.count()
        print(f"Checkboxes found: {checkbox_count}")

        symbol_checkboxes = 0
        if checkbox_count > 0:
            print("📝 Checkbox details:")
            for i in range(min(10, checkbox_count)):
                cb = checkboxes.nth(i)
                name = await cb.get_attribute("name") or ""
                id_attr = await cb.get_attribute("id") or ""
                value = await cb.get_attribute("value") or ""

                # Check if this is a symbol-related checkbox
                is_symbol = "symbol" in name.lower() or "symbol" in id_attr.lower() or "symbol" in value.lower()
                if is_symbol:
                    symbol_checkboxes += 1
                    print(f"  ✅ SYMBOL {i+1}. name='{name}', id='{id_attr}', value='{value}'")
                else:
                    print(f"  {i+1}. name='{name}', id='{id_attr}', value='{value}'")

        print(f"Symbol-related checkboxes: {symbol_checkboxes}")

        # Look for Apply Filters button
        apply_buttons = page.locator("button:has-text('Apply')")
        apply_count = await apply_buttons.count()
        print(f"Apply buttons: {apply_count}")

        # Test the actual filtering if we found symbol checkboxes
        if symbol_checkboxes > 0 and apply_count > 0:
            print("\n🧪 Testing symbol filtering...")

            # Get initial row count
            main_table = page.locator("table").first
            tbody = main_table.locator("tbody")
            initial_rows = await tbody.locator("tr").count()
            print(f"Initial rows: {initial_rows}")

            # Check some symbol checkboxes
            checked_symbols = []
            for i in range(min(3, symbol_checkboxes)):
                cb = page.locator("input[type='checkbox']").nth(i)
                name = await cb.get_attribute("name") or ""
                value = await cb.get_attribute("value") or ""

                if "symbol" in name.lower() or "symbol" in value.lower():
                    await cb.check()
                    checked_symbols.append(value)
                    print(f"✅ Checked symbol: {value}")

            # Click Apply Filters
            apply_btn = apply_buttons.first
            await apply_btn.click()
            print("🔧 Clicked Apply Filters")

            # Wait for filtering
            await page.wait_for_timeout(5000)

            # Check new row count
            new_rows = await tbody.locator("tr").count()
            print(f"Rows after filtering: {new_rows}")

            if new_rows != initial_rows:
                print(f"✅ Filtering worked! {initial_rows} → {new_rows}")

                # Check if filtered data actually contains the selected symbols
                if new_rows > 0:
                    print("📊 Checking if filtered data contains selected symbols...")
                    for i in range(min(5, new_rows)):
                        row = tbody.locator("tr").nth(i)
                        row_text = await row.text_content()

                        # Check if any of our selected symbols appear in this row
                        symbol_found = any(symbol in row_text for symbol in checked_symbols if symbol)
                        print(f"  Row {i+1}: {'✅' if symbol_found else '❌'} {row_text[:80]}...")
            else:
                print("❌ Filtering did not change row count - not working!")

                # Check pagination info for issues
                page_content = await page.content()
                if "undefined" in page_content:
                    print("❌ Found 'undefined' in page - pagination issue!")

                # Look at network requests
                print("🔍 Checking for recent API calls...")
        else:
            print("⚠️ Cannot test filtering - missing controls or buttons")

if __name__ == "__main__":
    asyncio.run(find_symbol_filter_issue())