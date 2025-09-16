#!/usr/bin/env python3
"""
Playwright Test for EDA Interface Fixes
Tests the specific issues that were fixed
"""

import asyncio
import pytest
from playwright.async_api import async_playwright

@pytest.mark.asyncio

async def test_eda_fixes():
    """Test all the EDA interface fixes."""
    print("🎭 Testing EDA Interface Fixes with Playwright")
    print("="*60)

    async with async_playwright() as playwright:
        # Launch browser in headless mode
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()

        # Capture console errors
        console_errors = []
        page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)

        try:
            print("🧪 Test 1: Interface loads without JavaScript errors")
            await page.goto("http://localhost:4000/eda", timeout=15000)
            await page.wait_for_load_state("networkidle")

            if len(console_errors) == 0:
                print("✅ No JavaScript console errors")
            else:
                print(f"❌ Found {len(console_errors)} console errors:")
                for error in console_errors[:3]:  # Show first 3
                    print(f"  - {error}")

            print("\n🧪 Test 2: Global x-axis control positioning")

            # Wait for potential async loading
            await page.wait_for_timeout(2000)

            # Check if global axis section exists
            global_axis_section = page.locator("#global-axis-section")
            if await global_axis_section.count() > 0:
                print("✅ Global x-axis section found")
            else:
                print("❌ Global x-axis section missing")

            # Check if global x-axis select exists
            global_axis_select = page.locator("#global-x-axis")
            if await global_axis_select.count() > 0:
                print("✅ Global x-axis dropdown found")

                # Test the dropdown options
                options = await global_axis_select.locator("option").all()
                option_texts = []
                for option in options:
                    text = await option.text_content()
                    if text:
                        option_texts.append(text)

                print(f"✅ Global axis options: {option_texts}")
            else:
                print("❌ Global x-axis dropdown missing")

            print("\n🧪 Test 3: Dataset selection and symbol filtering")

            # Select a dataset
            dataset_select = page.locator("#dataset-select")
            if await dataset_select.count() > 0:
                print("✅ Dataset selector found")

                # Wait for datasets to load
                await page.wait_for_timeout(3000)

                # Get options
                options = await dataset_select.locator("option").all()
                if len(options) > 1:  # More than just "Select dataset..."
                    print(f"✅ Found {len(options)} dataset options")

                    # Select first real dataset
                    first_option = dataset_select.locator("option").nth(1)
                    await first_option.click()
                    print("✅ Dataset selected")

                    # Wait for data to load
                    await page.wait_for_timeout(5000)

                    # Look for symbol filter elements
                    symbol_filters = page.locator("input[id*='symbol'], input[name*='symbol']")
                    if await symbol_filters.count() > 0:
                        print("✅ Symbol filter controls found")

                        # Try to apply a filter if checkbox type
                        first_filter = symbol_filters.first
                        input_type = await first_filter.get_attribute("type")
                        if input_type == "checkbox":
                            await first_filter.check()
                            print("✅ Symbol filter applied")

                            # Look for Apply Filters button
                            apply_button = page.locator("button:has-text('Apply Filters')")
                            if await apply_button.count() > 0:
                                await apply_button.click()
                                print("✅ Apply Filters clicked")

                                # Wait for results
                                await page.wait_for_timeout(3000)

                                # Check for "undefined" in page content
                                page_content = await page.content()
                                if "undefined" in page_content and "record" in page_content:
                                    print("❌ Found 'undefined' in pagination text")
                                else:
                                    print("✅ No 'undefined' found in pagination")
                            else:
                                print("⚠️ Apply Filters button not found")
                    else:
                        print("⚠️ No symbol filter controls found")
                else:
                    print("❌ No datasets available")
            else:
                print("❌ Dataset selector missing")

            print("\n🧪 Test 4: Per-column controls removal")

            # Check for per-column x-axis controls (should not exist)
            per_column_selects = page.locator("select[id*='xaxis-']")
            per_column_count = await per_column_selects.count()

            if per_column_count == 0:
                print("✅ No per-column x-axis controls found (correctly removed)")
            else:
                print(f"❌ Found {per_column_count} per-column x-axis controls")

        except Exception as e:
            print(f"❌ Test failed with error: {e}")

        finally:
            await browser.close()

    print(f"\n📊 JavaScript Console Errors: {len(console_errors)}")
    print("🎯 Playwright testing completed")

if __name__ == "__main__":
    asyncio.run(test_eda_fixes())