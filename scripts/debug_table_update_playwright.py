#!/usr/bin/env python3
"""
Debug why table is not updating with filtered data
"""

import asyncio
from playwright.async_api import async_playwright

async def debug_table_update():
    """Debug the table update issue in detail."""
    print("🔍 Debugging Table Update Issue")
    print("="*50)
    
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            await page.goto("http://localhost:4000/eda", timeout=15000)
            await page.wait_for_load_state("networkidle") 
            await page.wait_for_timeout(3000)
            
            # Select dataset
            print("🧪 Selecting dataset...")
            dataset_select = page.locator("#dataset-select")
            await dataset_select.select_option("intg_daily_prices_tiingo")
            await page.wait_for_timeout(8000)
            
            # Check ALL tables on page
            print("\\n🔍 Checking all tables on page...")
            tables = page.locator("table")
            table_count = await tables.count()
            print(f"Total tables found: {table_count}")
            
            for i in range(table_count):
                table = tables.nth(i)
                table_id = await table.get_attribute("id")
                tbody = table.locator("tbody")
                tbody_id = await tbody.get_attribute("id") if await tbody.count() > 0 else None
                rows = await tbody.locator("tr").count() if await tbody.count() > 0 else 0
                
                print(f"  Table {i+1}: id='{table_id}', tbody_id='{tbody_id}', rows={rows}")
                
                if rows > 0:
                    # Get sample data from this table
                    first_row = tbody.locator("tr").first
                    first_cell = first_row.locator("td").first
                    first_value = await first_cell.text_content()
                    print(f"    -> First cell: {first_value}")
            
            # Find the main data table
            main_table = page.locator("#data-table")
            if await main_table.count() > 0:
                print("\\n📊 Found main #data-table")
                tbody = main_table.locator("tbody")
                initial_rows = await tbody.locator("tr").count()
                print(f"Initial rows in #data-table: {initial_rows}")
                
                # Get sample data
                if initial_rows > 0:
                    print("Sample data before filtering:")
                    for i in range(min(3, initial_rows)):
                        row = tbody.locator("tr").nth(i)
                        symbol_cell = row.locator("td").nth(1)  # Symbol should be column 2
                        symbol = await symbol_cell.text_content()
                        print(f"  Row {i+1} symbol: {symbol}")
                
                # Apply AAPL filter
                print("\\n🎯 Applying AAPL filter...")
                aapl_checkbox = page.locator("input[name='filter-symbol'][value='AAPL']")
                if await aapl_checkbox.count() > 0:
                    await aapl_checkbox.check()
                    print("✅ AAPL checkbox checked")
                    
                    # Click Apply Filters
                    apply_button = page.locator("button:has-text('Apply Filters')")
                    await apply_button.click()
                    print("🔧 Apply Filters clicked")
                    
                    # Wait for update
                    await page.wait_for_timeout(3000)
                    
                    # Check if rows changed
                    new_rows = await tbody.locator("tr").count()
                    print(f"Rows after filtering: {new_rows}")
                    
                    if new_rows != initial_rows:
                        print("✅ Table updated!")
                        
                        # Check filtered content
                        print("Sample data after filtering:")
                        for i in range(min(5, new_rows)):
                            row = tbody.locator("tr").nth(i)
                            symbol_cell = row.locator("td").nth(1)
                            symbol = await symbol_cell.text_content()
                            print(f"  Row {i+1} symbol: {symbol}")
                    else:
                        print("❌ Table not updated!")
                        
                        # Check if there's a different table being updated
                        print("\\n🔍 Checking if other tables were updated...")
                        for i in range(table_count):
                            table = tables.nth(i)
                            table_id = await table.get_attribute("id")
                            tbody = table.locator("tbody")
                            rows = await tbody.locator("tr").count() if await tbody.count() > 0 else 0
                            print(f"  Table '{table_id}': {rows} rows")
                        
                        # Check table info/pagination elements
                        print("\\n🔍 Checking pagination elements...")
                        table_info = page.locator("#filtered-table-info, #table-info")
                        if await table_info.count() > 0:
                            info_text = await table_info.text_content()
                            print(f"Table info: {info_text}")
                        
                        # Check for any elements with 'undefined'
                        page_content = await page.content()
                        lines = page_content.split('\\n')
                        undefined_lines = [line.strip() for line in lines if 'undefined' in line.lower() and ('record' in line.lower() or 'page' in line.lower() or 'showing' in line.lower())]
                        
                        if undefined_lines:
                            print("\\n❌ Found 'undefined' in these elements:")
                            for line in undefined_lines[:3]:
                                print(f"  {line[:100]}...")
                        
                        # Check JavaScript console for errors
                        console_logs = []
                        page.on("console", lambda msg: console_logs.append(f"{msg.type}: {msg.text}"))
                        
                        # Try to manually call displayDataTable function to see if that works
                        print("\\n🔧 Manually testing displayDataTable function...")
                        try:
                            # Check if there's a JavaScript error preventing updates
                            result = await page.evaluate("""
                                // Check if functions exist
                                const functions = {
                                    'displayDataTable': typeof displayDataTable,
                                    'loadFilteredData': typeof loadFilteredData,
                                    'currentTableData': typeof currentTableData,
                                    'currentFilters': typeof currentFilters
                                };
                                
                                console.log('Functions check:', functions);
                                console.log('Current filters:', currentFilters);
                                
                                return functions;
                            """)
                            print(f"JavaScript functions status: {result}")
                        except Exception as e:
                            print(f"JavaScript evaluation error: {e}")
                else:
                    print("❌ AAPL checkbox not found")
            else:
                print("❌ Main #data-table not found")
                
        except Exception as e:
            print(f"❌ Debug failed: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_table_update())