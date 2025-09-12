#!/usr/bin/env python3
"""
Debug EDA interface loading with Playwright
"""

import asyncio
from playwright.async_api import async_playwright

async def debug_eda():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            # Listen for console messages and errors
            page.on("console", lambda msg: print(f"🖥️  Console: {msg.text}"))
            page.on("pageerror", lambda error: print(f"❌ JS Error: {error}"))

            print("📍 Navigating to port 4000...")
            await page.goto("http://localhost:4000/", wait_until="domcontentloaded")

            print("⏳ Waiting for page to load...")
            await page.wait_for_timeout(2000)

            print("🔍 Looking for EDA button...")
            eda_buttons = await page.query_selector_all('button')
            print(f"Found {len(eda_buttons)} buttons total")

            for i, button in enumerate(eda_buttons):
                text = await button.inner_text()
                print(f"  Button {i}: {text}")

            print("🔘 Clicking EDA button...")
            eda_button = await page.wait_for_selector('button:has-text("Exploratory Data Analysis")', timeout=5000)
            await eda_button.click()

            print("⏳ Waiting after click...")
            await page.wait_for_timeout(5000)  # Give more time for async operations

            print("🔍 Checking if loadEDA function executed...")
            # Try to manually execute loadEDA to see if it works
            try:
                await page.evaluate("loadEDA()")
                print("✅ Manual loadEDA() call succeeded")
                await page.wait_for_timeout(3000)
            except Exception as e:
                print(f"❌ Manual loadEDA() failed: {e}")

            print("🔍 Looking for table selector...")
            table_selector = await page.query_selector('#table-selector')
            if table_selector:
                print("✅ Found table selector!")
            else:
                print("❌ Table selector not found")

                # Check what's in the analysis content
                analysis_content = await page.query_selector('#analysis-content')
                if analysis_content:
                    content = await analysis_content.inner_html()
                    print(f"Analysis content HTML: {content[:500]}...")
                else:
                    print("❌ Analysis content div not found")

            # Done with debugging

        except Exception as e:
            print(f"❌ Error: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_eda())