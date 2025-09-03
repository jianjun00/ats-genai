#!/usr/bin/env python3
"""
Test the complete symbol filter workflow with network monitoring
"""

import asyncio
from playwright.async_api import async_playwright
import json

@pytest.mark.asyncio

async def test_symbol_filter_complete():
    """Test the complete symbol filter workflow and monitor network requests."""
    print("🔍 Complete Symbol Filter Test")
    print("="*50)
    
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Monitor network requests
        requests = []
        responses = []
        
        page.on("request", lambda request: requests.append({
            "url": request.url,
            "method": request.method,
            "post_data": request.post_data
        }))
        
        page.on("response", lambda response: responses.append({
            "url": response.url,
            "status": response.status
        }))
        
        console_logs = []
        page.on("console", lambda msg: console_logs.append(f"{msg.type}: {msg.text}"))
        
        try:
            await page.goto("http://localhost:4000/eda", timeout=15000)
            await page.wait_for_load_state("networkidle") 
            await page.wait_for_timeout(3000)
            
            # Select dataset
            dataset_select = page.locator("#dataset-select")
            await dataset_select.select_option("intg_daily_prices_tiingo")
            print("✅ Dataset selected")
            await page.wait_for_timeout(8000)
            
            # Check initial table state
            table = page.locator("#data-table tbody")
            initial_rows = await table.locator("tr").count()
            print(f"✅ Initial rows: {initial_rows}")
            
            # Get sample data to see what symbols are in the table
            if initial_rows > 0:
                print("📊 Sample symbols in first 5 rows:")
                for i in range(min(5, initial_rows)):
                    row = table.locator("tr").nth(i)
                    symbol_cell = row.locator("td").nth(1)  # Symbol is column 2
                    symbol = await symbol_cell.text_content()
                    print(f"  Row {i+1}: {symbol}")
            
            # Find symbol checkboxes
            checkboxes = page.locator("input[type='checkbox'][name='filter-symbol']")
            checkbox_count = await checkboxes.count()
            print(f"✅ Found {checkbox_count} symbol checkboxes")
            
            if checkbox_count > 0:
                # Get checkbox values to see what symbols are available for filtering
                print("📝 Available symbol filters:")
                checkbox_values = []
                for i in range(checkbox_count):
                    cb = checkboxes.nth(i)
                    value = await cb.get_attribute("value")
                    checkbox_values.append(value)
                    print(f"  {i+1}. {value}")
                
                # Select a specific symbol that should be in the data (like AAPL)
                target_symbol = "AAPL" if "AAPL" in checkbox_values else checkbox_values[0]
                print(f"\\n🎯 Testing filter with symbol: {target_symbol}")
                
                # Check the specific checkbox
                target_checkbox = page.locator(f"input[name='filter-symbol'][value='{target_symbol}']")
                if await target_checkbox.count() > 0:
                    await target_checkbox.check()
                    print(f"✅ Checked {target_symbol} checkbox")
                    
                    # Clear previous network data
                    requests.clear()
                    responses.clear()
                    console_logs.clear()
                    
                    # Click Apply Filters
                    apply_button = page.locator("button:has-text('Apply Filters')")
                    await apply_button.click()
                    print("🔧 Clicked Apply Filters")
                    
                    # Wait for filtering to complete
                    await page.wait_for_timeout(5000)
                    
                    # Check network requests
                    print("\\n📡 Network requests after Apply Filters:")
                    api_requests = [r for r in requests if "/api/" in r["url"]]
                    for req in api_requests:
                        print(f"  {req['method']} {req['url']}")
                        if req['post_data']:
                            try:
                                data = json.loads(req['post_data'])
                                print(f"    POST data: {json.dumps(data, indent=6)}")
                            except:
                                print(f"    POST data (raw): {req['post_data']}")
                    
                    # Check responses
                    print("\\n📥 API responses:")
                    api_responses = [r for r in responses if "/api/" in r["url"]]
                    for resp in api_responses:
                        print(f"  {resp['status']} {resp['url']}")
                    
                    # Check console logs
                    print("\\n💬 Console logs:")
                    for log in console_logs:
                        print(f"  {log}")
                    
                    # Check if table content changed
                    new_rows = await table.locator("tr").count()
                    print(f"\\n📊 Table rows after filtering: {new_rows}")
                    
                    if new_rows != initial_rows:
                        print(f"✅ Filtering worked! {initial_rows} → {new_rows}")
                        
                        # Verify filtered data contains only target symbol
                        if new_rows > 0:
                            print("📋 Checking filtered results:")
                            for i in range(min(5, new_rows)):
                                row = table.locator("tr").nth(i)
                                symbol_cell = row.locator("td").nth(1)
                                symbol = await symbol_cell.text_content()
                                matches = symbol == target_symbol
                                print(f"  Row {i+1}: {symbol} {'✅' if matches else '❌'}")
                    else:
                        print(f"❌ Filtering failed - no change in row count")
                        
                        # Check pagination info for 'undefined'
                        page_content = await page.content()
                        if "undefined" in page_content.lower():
                            undefined_matches = []
                            lines = page_content.split('\\n')
                            for line in lines:
                                if 'undefined' in line.lower() and ('record' in line.lower() or 'page' in line.lower()):
                                    undefined_matches.append(line.strip())
                            
                            if undefined_matches:
                                print("❌ Found 'undefined' in pagination:")
                                for match in undefined_matches[:3]:
                                    print(f"  {match}")
                else:
                    print(f"❌ Could not find checkbox for {target_symbol}")
            else:
                print("❌ No symbol checkboxes found")
                
        except Exception as e:
            print(f"❌ Test failed: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_symbol_filter_complete())