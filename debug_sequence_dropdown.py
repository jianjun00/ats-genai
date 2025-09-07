#!/usr/bin/env python3
"""
Debug: Check both dropdowns separately
"""

import asyncio
from playwright.async_api import async_playwright

async def debug_dropdowns():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            # Navigate and click Training Datasets
            await page.goto("http://localhost:3000")
            await page.wait_for_load_state("networkidle")

            training_button = page.get_by_role("button", name="🤖 Training Datasets")
            await training_button.click()
            await page.wait_for_timeout(3000)

            print("🔍 EXAMINING DROPDOWNS SEPARATELY")
            print("=" * 50)

            # Find dataset dropdown by ID
            print("1️⃣ Dataset dropdown (#dataset-selector):")
            dataset_dropdown = page.locator("#dataset-selector")
            if await dataset_dropdown.count() > 0:
                options = await dataset_dropdown.locator("option").all_text_contents()
                print(f"   Found {len(options)} options")
                for i, opt in enumerate(options[:3]):
                    print(f"     {i}: {opt}")

                # Select dataset 63
                await dataset_dropdown.select_option(value="63")
                print("   ✅ Selected dataset 63")
                await page.wait_for_timeout(5000)  # Give time for sequence dropdown to populate
            else:
                print("   ❌ Dataset dropdown not found")
                return False

            # Find sequence dropdown by ID
            print("\n2️⃣ Sequence dropdown (#sequence-selector):")
            sequence_dropdown = page.locator("#sequence-selector")
            if await sequence_dropdown.count() > 0:
                # Check if enabled
                is_disabled = await sequence_dropdown.get_attribute("disabled")
                print(f"   Disabled: {is_disabled}")

                options = await sequence_dropdown.locator("option").all_text_contents()
                print(f"   Found {len(options)} options")
                for i, opt in enumerate(options):
                    print(f"     {i}: {opt}")

                if not is_disabled and len(options) > 1:
                    # Select first sequence (skip "Choose a sequence...")
                    await sequence_dropdown.select_option(index=1)
                    print("   ✅ Selected first sequence")
                    await page.wait_for_timeout(3000)

                    # Click visualize button
                    visualize_button = page.locator("text=📊 Visualize")
                    if await visualize_button.count() > 0:
                        await visualize_button.click()
                        print("   ✅ Clicked visualize button")
                        await page.wait_for_timeout(5000)

            else:
                print("   ❌ Sequence dropdown not found")
                return False

            # Check for visualization
            print("\n3️⃣ Checking for visualization:")
            viz_selectors = ["#ohlc-chart-5m", "#ohlc-chart-15m", "#ohlc-chart-1h", "#dataset-info", "#sequence-table"]
            viz_found = False

            for selector in viz_selectors:
                elements = page.locator(selector)
                count = await elements.count()
                if count > 0:
                    print(f"   ✅ Found {selector}")
                    viz_found = True

            if not viz_found:
                print("   ❌ No visualization elements found")

            # Take screenshot
            await page.screenshot(path="/tmp/debug_dropdowns.png")
            print("\n📸 Screenshot saved to /tmp/debug_dropdowns.png")

            return viz_found

        except Exception as e:
            print(f"❌ Error: {e}")
            return False

        finally:
            await browser.close()

if __name__ == "__main__":
    result = asyncio.run(debug_dropdowns())
    print(f"\n{'✅ SUCCESS' if result else '❌ FAILED'}")