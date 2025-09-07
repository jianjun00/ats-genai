#!/usr/bin/env python3
"""
Debug Symbol Filter Issue using Playwright
Identifies why symbol filtering is not working properly
"""

import asyncio
from playwright.async_api import async_playwright
import json

async def debug_symbol_filter():
    """Debug the symbol filter functionality step by step."""
    print("🔍 Debugging Symbol Filter Issue with Playwright")
    print("="*70)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)  # Headless mode for WSL
        page = await browser.new_page()

        # Track network requests
        network_requests = []
        page.on("request", lambda request: network_requests.append({
            "url": request.url,
            "method": request.method,
            "post_data": request.post_data
        }))

        # Track responses
        network_responses = []
        page.on("response", lambda response: network_responses.append({
            "url": response.url,
            "status": response.status
        }))

        console_logs = []
        page.on("console", lambda msg: console_logs.append(f"{msg.type}: {msg.text}"))

        try:
            print("🧪 Step 1: Load EDA interface")
            await page.goto("http://localhost:4000/eda", timeout=15000)
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(2000)

            print("🧪 Step 2: Select dataset")
            dataset_select = page.locator("#dataset-select")
            await page.wait_for_timeout(3000)  # Wait for datasets to load

            # Select the first dataset with data
            await dataset_select.select_option("intg_daily_prices_tiingo")
            print("✅ Selected intg_daily_prices_tiingo dataset")

            # Wait for dataset to load
            await page.wait_for_timeout(5000)

            print("🧪 Step 3: Wait for table data to appear")
            # Look for the table body to have data
            table_body = page.locator("#filtered-table tbody")
            await page.wait_for_function(
                "document.querySelector('#filtered-table tbody tr')",
                timeout=10000
            )

            # Count initial rows
            initial_rows = await table_body.locator("tr").count()
            print(f"✅ Initial table rows: {initial_rows}")

            # Get sample data from first few rows
            if initial_rows > 0:
                print("\n📊 Sample data from first 3 rows:")
                for i in range(min(3, initial_rows)):
                    row = table_body.locator("tr").nth(i)
                    cells = await row.locator("td").all()
                    row_data = []
                    for cell in cells:
                        text = await cell.text_content()
                        row_data.append(text[:20] + "..." if len(text) > 20 else text)
                    print(f"  Row {i+1}: {row_data[:5]}")  # Show first 5 columns

            print("\n🧪 Step 4: Look for symbol filter controls")

            # Look for different types of symbol filter controls
            symbol_inputs = page.locator("input[name*='symbol' i], input[id*='symbol' i]")
            symbol_selects = page.locator("select[name*='symbol' i], select[id*='symbol' i]")
            symbol_checkboxes = page.locator("input[type='checkbox'][name*='symbol' i]")

            input_count = await symbol_inputs.count()
            select_count = await symbol_selects.count()
            checkbox_count = await symbol_checkboxes.count()

            print(f"Symbol filter elements found:")
            print(f"  - Input fields: {input_count}")
            print(f"  - Select dropdowns: {select_count}")
            print(f"  - Checkboxes: {checkbox_count}")

            # Look for any element with 'symbol' in the text content or attributes
            all_symbol_elements = page.locator("[*|='symbol' i], :has-text('symbol' i)")
            all_count = await all_symbol_elements.count()
            print(f"  - All symbol-related elements: {all_count}")

            if checkbox_count > 0:
                print(f"\n🧪 Step 5: Test checkbox filtering")

                # Get checkbox details
                for i in range(min(5, checkbox_count)):
                    checkbox = symbol_checkboxes.nth(i)
                    name = await checkbox.get_attribute("name")
                    id_attr = await checkbox.get_attribute("id")
                    value = await checkbox.get_attribute("value")
                    print(f"  Checkbox {i}: name='{name}', id='{id_attr}', value='{value}'")

                # Try checking a few checkboxes
                print("\n🔧 Checking first 3 symbol checkboxes...")
                for i in range(min(3, checkbox_count)):
                    checkbox = symbol_checkboxes.nth(i)
                    await checkbox.check()
                    value = await checkbox.get_attribute("value")
                    print(f"  ✅ Checked symbol: {value}")

                # Look for Apply Filters button
                apply_button = page.locator("button:has-text('Apply Filters')")
                if await apply_button.count() > 0:
                    print("\n🔧 Clicking Apply Filters...")
                    await apply_button.click()

                    # Wait for filtering to occur
                    await page.wait_for_timeout(3000)

                    # Check if table changed
                    new_row_count = await table_body.locator("tr").count()
                    print(f"✅ Table rows after filtering: {new_row_count}")

                    if new_row_count != initial_rows:
                        print(f"✅ Filter worked! Rows changed from {initial_rows} to {new_row_count}")

                        # Show sample filtered data
                        print("\n📊 Sample filtered data:")
                        for i in range(min(3, new_row_count)):
                            row = table_body.locator("tr").nth(i)
                            cells = await row.locator("td").all()
                            row_data = []
                            for cell in cells:
                                text = await cell.text_content()
                                row_data.append(text[:20] + "..." if len(text) > 20 else text)
                            print(f"  Row {i+1}: {row_data[:5]}")
                    else:
                        print(f"❌ Filter did not change table content ({initial_rows} → {new_row_count})")

                        # Check pagination info for 'undefined'
                        pagination_info = page.locator("#filtered-table-info")
                        if await pagination_info.count() > 0:
                            info_text = await pagination_info.text_content()
                            print(f"📋 Pagination info: {info_text}")

                            if "undefined" in info_text.lower():
                                print("❌ Found 'undefined' in pagination - this is the bug!")
                else:
                    print("❌ Apply Filters button not found")

            elif input_count > 0:
                print(f"\n🧪 Step 5: Test text input filtering")
                first_input = symbol_inputs.first
                await first_input.fill("TSLA")
                print("✅ Entered 'TSLA' in symbol input field")

                # Look for search/apply button
                search_buttons = page.locator("button:has-text('Search'), button:has-text('Apply'), button:has-text('Filter')")
                if await search_buttons.count() > 0:
                    await search_buttons.first.click()
                    await page.wait_for_timeout(3000)
                    print("✅ Clicked search/apply button")
                else:
                    print("⚠️ No search/apply button found")
            else:
                print("❌ No symbol filter controls found")

            print(f"\n📡 Network Activity:")
            # Show recent API calls
            recent_requests = [r for r in network_requests if "/api/" in r["url"]][-5:]
            for req in recent_requests:
                print(f"  {req['method']} {req['url']}")
                if req['post_data']:
                    try:
                        data = json.loads(req['post_data'])
                        print(f"    Data: {data}")
                    except:
                        print(f"    Data: {req['post_data'][:100]}")

            print(f"\n💬 Console Logs:")
            for log in console_logs[-10:]:  # Last 10 logs
                print(f"  {log}")

        except Exception as e:
            print(f"❌ Debug failed: {e}")
            import traceback
            traceback.print_exc()

        finally:
            print(f"\n🔚 Debug complete.")
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_symbol_filter())