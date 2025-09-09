#!/usr/bin/env python3
"""
Debug universe interface to see actual content
"""

import asyncio
from playwright.async_api import async_playwright

async def debug_interface():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            await page.goto("http://localhost:4000/", wait_until="domcontentloaded")
            universe_button = await page.wait_for_selector('button:has-text("🌐 Universe Analytics")')
            await universe_button.click()
            await page.wait_for_timeout(2000)
            
            universe_selector = await page.query_selector('#universe-selector')
            await universe_selector.select_option(value="2")
            
            date_from = await page.query_selector('#universe-date-from')
            date_to = await page.query_selector('#universe-date-to')
            await date_from.fill('2019-01-01')
            await date_to.fill('2024-12-31')
            
            load_button = await page.query_selector('button:has-text("Load Members")')
            await load_button.click()
            await page.wait_for_timeout(3000)
            
            members_content = await page.query_selector('#universe-members-content')
            content_text = await members_content.text_content()
            
            print("=== ACTUAL INTERFACE CONTENT ===")
            print(content_text[:1000])  # First 1000 chars
            print("=== END CONTENT ===")
            
        except Exception as e:
            print(f"Error: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_interface())