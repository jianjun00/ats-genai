#!/usr/bin/env python3
"""
Simple inspection of symbol filter functionality
"""

import asyncio
from playwright.async_api import async_playwright

async def inspect_symbol_filter():
    """Inspect the current state of symbol filtering."""
    print("🔍 Inspecting Symbol Filter Current State")
    print("="*50)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            # Load page
            await page.goto("http://localhost:4000/eda", timeout=15000)
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(3000)

            # Select dataset
            dataset_select = page.locator("#dataset-select")
            await dataset_select.select_option("intg_daily_prices_tiingo")
            print("✅ Dataset selected")
            await page.wait_for_timeout(5000)

            # Check what's on the page
            print("\n📋 Page Analysis:")

            # Look for any table
            tables = page.locator("table")
            table_count = await tables.count()
            print(f"Tables found: {table_count}")

            # Look for filtered table specifically
            filtered_table = page.locator("#filtered-table")
            if await filtered_table.count() > 0:
                print("✅ Filtered table found")

                # Check table body
                tbody = filtered_table.locator("tbody")
                if await tbody.count() > 0:
                    rows = await tbody.locator("tr").count()
                    print(f"✅ Table has {rows} rows")

                    if rows > 0:
                        # Get sample data
                        first_row = tbody.locator("tr").first
                        first_row_text = await first_row.text_content()
                        print(f"📊 First row: {first_row_text[:100]}...")
                else:
                    print("❌ No table body found")
            else:
                print("❌ Filtered table not found")

            # Look for filter controls
            print("\n🔧 Filter Controls:")

            # Check for symbol-related inputs/selects/checkboxes
            symbol_elements = await page.query_selector_all("[*='symbol' i], input[name*='symbol' i], select[name*='symbol' i], input[id*='symbol' i], select[id*='symbol' i]")
            print(f"Symbol elements found: {len(symbol_elements)}")

            # Look for checkboxes specifically
            checkboxes = page.locator("input[type='checkbox']")
            checkbox_count = await checkboxes.count()
            print(f"Total checkboxes: {checkbox_count}")

            if checkbox_count > 0:
                print("📝 First 5 checkboxes:")
                for i in range(min(5, checkbox_count)):
                    cb = checkboxes.nth(i)
                    name = await cb.get_attribute("name") or "no-name"
                    id_attr = await cb.get_attribute("id") or "no-id"
                    value = await cb.get_attribute("value") or "no-value"
                    print(f"  {i+1}. name='{name}', id='{id_attr}', value='{value}'")

            # Look for Apply Filters button
            apply_buttons = page.locator("button:has-text('Apply')")
            apply_count = await apply_buttons.count()
            print(f"Apply buttons found: {apply_count}")

            # Check current page content
            page_content = await page.content()

            # Look for filter sections
            if "Data Filter" in page_content:
                print("✅ Data Filter section found")
            else:
                print("❌ Data Filter section missing")

            # Check for specific symbol filtering UI patterns
            if "symbol" in page_content.lower():
                print("✅ 'symbol' text found in page")

                # Count occurrences
                symbol_count = page_content.lower().count("symbol")
                print(f"   'symbol' appears {symbol_count} times")
            else:
                print("❌ No 'symbol' text found in page")

            # Look for table info/pagination
            table_info = page.locator("#filtered-table-info")
            if await table_info.count() > 0:
                info_text = await table_info.text_content()
                print(f"📋 Table info: {info_text}")
            else:
                print("❌ Table info element not found")

        except Exception as e:
            print(f"❌ Inspection failed: {e}")

        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(inspect_symbol_filter())