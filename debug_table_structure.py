#!/usr/bin/env python3
"""
Debug the actual table structure to understand column layout for sorting
"""

import asyncio
from playwright.async_api import async_playwright

async def debug_table():
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
            
            print("🔍 Examining table structure...")
            
            # Get all table headers
            headers = await page.query_selector_all('#sortable-table th')
            print(f"\n📊 Found {len(headers)} table headers:")
            for i, header in enumerate(headers):
                header_text = await header.inner_text()
                onclick_attr = await header.get_attribute('onclick')
                print(f"   Column {i}: '{header_text}' (onclick: {onclick_attr})")
            
            # Get first few rows to see data structure
            rows = await page.query_selector_all('#sortable-table tbody tr')
            print(f"\n📋 Sample data from first 3 rows:")
            for i, row in enumerate(rows[:3]):
                cells = await row.query_selector_all('td')
                row_data = []
                for cell in cells:
                    cell_text = await cell.inner_text()
                    row_data.append(cell_text.strip())
                print(f"   Row {i}: {row_data}")
            
            # Check for filter controls
            filter_visible = await page.locator('#filter-controls').is_visible()
            print(f"\n🔍 Filter controls visible: {filter_visible}")
            
            # Check sort indicators
            sort_indicators = await page.query_selector_all('[id^="sort-"]')
            print(f"🎯 Found {len(sort_indicators)} sort indicators:")
            for indicator in sort_indicators:
                indicator_id = await indicator.get_attribute('id')
                indicator_text = await indicator.inner_text()
                print(f"   {indicator_id}: '{indicator_text}'")
            
        except Exception as e:
            print(f"❌ Error: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_table())