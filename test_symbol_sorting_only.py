#!/usr/bin/env python3
"""
Test symbol sorting specifically to verify global sorting works
"""

import asyncio
from playwright.async_api import async_playwright

async def test_symbol_sorting():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            print("📍 Navigating to dev environment...")
            await page.goto("http://localhost:3000/", wait_until="domcontentloaded")
            
            print("🔘 Clicking EDA button...")
            eda_button = await page.wait_for_selector('button:has-text("📊 Exploratory Data Analysis")', timeout=5000)
            await eda_button.click()
            
            print("⏳ Waiting for EDA to load...")
            await page.wait_for_selector('#table-selector', timeout=10000)
            
            print("🗂️ Selecting daily prices table...")
            table_selector = page.locator('#table-selector')
            await table_selector.select_option(label="dev_daily_prices_polygon")
            await page.wait_for_timeout(3000)
            
            # Clear any filters first
            print("🧹 Clearing filters...")
            clear_button = page.locator('button:has-text("Clear")')
            await clear_button.click()
            await page.wait_for_timeout(2000)
            
            # Get initial symbol data (column 1 is symbol)
            print("📊 Getting initial symbol data...")
            initial_rows = await page.query_selector_all('#sortable-table tbody tr')
            initial_symbols = []
            for i, row in enumerate(initial_rows[:10]):  # Get more samples
                cells = await row.query_selector_all('td')
                if len(cells) > 1:
                    symbol_text = await cells[1].inner_text()  # Column 1 is symbol
                    initial_symbols.append(symbol_text.strip())
            
            print(f"Initial symbols: {initial_symbols}")
            
            # Click symbol column header for ascending sort
            print("🔤 Clicking symbol header for ascending sort...")
            symbol_header = await page.wait_for_selector('th[onclick*="symbol"]', timeout=5000)
            await symbol_header.click()
            
            # Wait for server response
            await page.wait_for_timeout(4000)
            
            # Get sorted symbols
            print("📊 Getting sorted symbol data...")
            sorted_rows = await page.query_selector_all('#sortable-table tbody tr')
            sorted_symbols = []
            for i, row in enumerate(sorted_rows[:10]):
                cells = await row.query_selector_all('td')
                if len(cells) > 1:
                    symbol_text = await cells[1].inner_text()
                    sorted_symbols.append(symbol_text.strip())
            
            print(f"Sorted symbols (ASC): {sorted_symbols}")
            
            # Check if sorting worked
            expected_sorted = sorted(initial_symbols)
            is_correctly_sorted = sorted_symbols == expected_sorted[:len(sorted_symbols)]
            
            print(f"Expected order: {expected_sorted[:10]}")
            print(f"Actual order:   {sorted_symbols}")
            print(f"Correctly sorted: {is_correctly_sorted}")
            
            # Check sort indicator
            sort_indicator = await page.wait_for_selector('#sort-symbol', timeout=2000)
            indicator_text = await sort_indicator.inner_text()
            print(f"Sort indicator: '{indicator_text}'")
            
            # Test descending sort - wait and requery the header to avoid DOM detachment
            print("🔤 Clicking symbol header for descending sort...")
            await page.wait_for_timeout(1000)
            
            # Re-query the header to avoid detachment
            symbol_header_desc = await page.wait_for_selector('th[onclick*="symbol"]', timeout=5000)
            await symbol_header_desc.click()
            
            # Wait for server response
            await page.wait_for_timeout(4000)
            
            # Get descending sorted symbols
            print("📊 Getting descending sorted symbol data...")
            desc_rows = await page.query_selector_all('#sortable-table tbody tr')
            desc_symbols = []
            for i, row in enumerate(desc_rows[:10]):
                cells = await row.query_selector_all('td')
                if len(cells) > 1:
                    symbol_text = await cells[1].inner_text()
                    desc_symbols.append(symbol_text.strip())
            
            print(f"Sorted symbols (DESC): {desc_symbols}")
            
            # Check descending sort
            expected_desc = sorted(initial_symbols, reverse=True)
            is_correctly_desc_sorted = desc_symbols == expected_desc[:len(desc_symbols)]
            
            print(f"Expected desc order: {expected_desc[:10]}")
            print(f"Actual desc order:   {desc_symbols}")
            print(f"Correctly desc sorted: {is_correctly_desc_sorted}")
            
            # Check final sort indicator
            sort_indicator_final = await page.wait_for_selector('#sort-symbol', timeout=2000)
            indicator_text_final = await sort_indicator_final.inner_text()
            print(f"Final sort indicator: '{indicator_text_final}'")
            
            print(f"\n🎯 SUMMARY:")
            print(f"   ✅ Ascending sort correct: {is_correctly_sorted}")
            print(f"   ✅ Descending sort correct: {is_correctly_desc_sorted}")
            print(f"   ✅ Data changed: {initial_symbols != sorted_symbols}")
            print(f"   ✅ Sort indicators working: {indicator_text != '⇅' or indicator_text_final != '⇅'}")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_symbol_sorting())