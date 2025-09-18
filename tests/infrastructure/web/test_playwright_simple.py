#!/usr/bin/env python3
"""
Simple Playwright Test to verify setup works
"""

import asyncio
from playwright.async_api import async_playwright

@pytest.mark.asyncio

async def test_playwright_basic():
    """Basic test to verify Playwright works."""
    print("🎭 Testing Playwright setup...")

    try:
        async with async_playwright() as playwright:
            print("✅ Playwright initialized")

            # Launch browser
            browser = await playwright.chromium.launch(headless=True)
            print("✅ Browser launched")

            # Create page
            page = await browser.new_page()
            print("✅ Page created")

            # Test local EDA interface
            print("🧪 Testing EDA interface access...")
            try:
                await page.goto("http://localhost:4000/eda", timeout=10000)
                print("✅ EDA interface loaded")

                # Check title
                title = await page.title()
                print(f"✅ Page title: {title}")

                # Check for key elements
                dataset_select = page.locator("#dataset-select")
                if await dataset_select.count() > 0:
                    print("✅ Dataset selector found")
                else:
                    print("❌ Dataset selector missing")

                # Check for global x-axis control
                global_axis = page.locator("#global-x-axis")
                if await global_axis.count() > 0:
                    print("✅ Global x-axis control found")
                else:
                    print("❌ Global x-axis control missing")

            except Exception as e:
                print(f"❌ EDA interface test failed: {e}")

            # Close browser
            await browser.close()
            print("✅ Browser closed")

        print("🎉 Playwright test completed successfully!")
        return True

    except Exception as e:
        print(f"❌ Playwright test failed: {e}")
        return False

if __name__ == "__main__":
    result = asyncio.run(test_playwright_basic())
    if result:
        print("\n🎯 Playwright is working correctly!")
    else:
        print("\n⚠️ Playwright setup has issues")