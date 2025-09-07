#!/usr/bin/env python3
"""
Simple Playwright Test: Sequence Selection Debug
"""

import asyncio
from playwright.async_api import async_playwright

async def test_sequence_selection_debug():
    """Debug sequence selection in the EDA UI."""

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            print("🎭 DEBUGGING SEQUENCE SELECTION")
            print("=" * 50)

            # Navigate to EDA tool
            print("1️⃣ Navigating to EDA tool...")
            await page.goto("http://localhost:3000")
            await page.wait_for_load_state("networkidle")

            # Click Training Datasets button
            print("2️⃣ Clicking Training Datasets button...")
            training_button = page.get_by_role("button", name="🤖 Training Datasets")
            await training_button.click()
            await page.wait_for_timeout(3000)

            # Find all select elements
            print("3️⃣ Finding all select elements...")
            select_elements = page.locator("select")
            select_count = await select_elements.count()
            print(f"   Found {select_count} select elements")

            # Examine each select element
            for i in range(select_count):
                select_elem = select_elements.nth(i)

                # Get the select element's attributes
                select_id = await select_elem.get_attribute("id") or "no-id"
                select_name = await select_elem.get_attribute("name") or "no-name"
                select_class = await select_elem.get_attribute("class") or "no-class"

                print(f"   Select {i}:")
                print(f"     ID: {select_id}")
                print(f"     Name: {select_name}")
                print(f"     Class: {select_class}")

                # Get options
                options = await select_elem.locator("option").all_text_contents()
                print(f"     Options ({len(options)}): {options[:5]}")  # Show first 5 options

                # Check if this could be the dataset dropdown
                if len(options) > 1 and any("63" in str(opt) or "dataset" in str(opt).lower() for opt in options):
                    print(f"   🎯 This looks like the dataset dropdown!")

                    # Select dataset 63 if available
                    dataset_63_option = None
                    for opt_idx, option in enumerate(options):
                        if "63" in str(option):
                            dataset_63_option = opt_idx
                            break

                    if dataset_63_option is not None:
                        print(f"   ✅ Selecting option {dataset_63_option}: {options[dataset_63_option]}")
                        await select_elem.select_option(index=dataset_63_option)
                        await page.wait_for_timeout(3000)  # Wait for UI to update
                        break

            # Now look for sequence dropdown after dataset selection
            print("4️⃣ Looking for sequence dropdown after dataset selection...")
            await page.wait_for_timeout(2000)  # Give time for sequence dropdown to populate

            # Check all select elements again
            select_elements = page.locator("select")
            select_count = await select_elements.count()
            print(f"   Now found {select_count} select elements")

            sequence_found = False
            for i in range(select_count):
                select_elem = select_elements.nth(i)
                options = await select_elem.locator("option").all_text_contents()

                # Look for sequence-like options
                sequence_options = [opt for opt in options if "AAPL_" in str(opt) or "_20250" in str(opt)]
                if sequence_options:
                    print(f"   ✅ Found sequence dropdown (select {i})!")
                    print(f"     Sequence options: {sequence_options}")
                    sequence_found = True

                    # Select first sequence
                    if len(sequence_options) > 0:
                        # Find the index of the first sequence option in the full options list
                        seq_option = sequence_options[0]
                        seq_index = options.index(seq_option)
                        print(f"   🎯 Selecting sequence: {seq_option}")
                        await select_elem.select_option(index=seq_index)
                        await page.wait_for_timeout(5000)  # Wait for visualization to load
                        break

            if not sequence_found:
                print("   ❌ No sequence dropdown found after dataset selection")

                # Take screenshot for debugging
                await page.screenshot(path="/tmp/no_sequence_dropdown_debug.png")
                print("   📸 Screenshot saved to /tmp/no_sequence_dropdown_debug.png")

                # Save page content
                content = await page.content()
                with open("/tmp/page_content_debug.html", "w") as f:
                    f.write(content)
                print("   📄 Page content saved to /tmp/page_content_debug.html")

                return False

            # Check if visualization loaded
            print("5️⃣ Checking if visualization loaded...")

            # Look for common visualization elements
            viz_selectors = ["canvas", "svg", ".plotly-graph-div", "table", "#chart"]
            viz_found = False

            for selector in viz_selectors:
                elements = page.locator(selector)
                count = await elements.count()
                if count > 0:
                    print(f"   ✅ Found {count} {selector} elements")
                    viz_found = True

            if not viz_found:
                print("   ❌ No visualization elements found")

                # Check for error messages
                errors = page.locator("text=/error|no.*data|failed/i")
                if await errors.count() > 0:
                    error_text = await errors.first().text_content()
                    print(f"   ❌ Error message: {error_text}")

            # Final screenshot
            await page.screenshot(path="/tmp/sequence_test_final.png")
            print("   📸 Final screenshot saved to /tmp/sequence_test_final.png")

            return viz_found

        except Exception as e:
            print(f"❌ Test failed with error: {e}")
            await page.screenshot(path="/tmp/test_error.png")
            return False

        finally:
            await browser.close()

async def main():
    success = await test_sequence_selection_debug()

    if success:
        print("\n✅ SEQUENCE SELECTION TEST PASSED!")
    else:
        print("\n❌ SEQUENCE SELECTION TEST FAILED!")

    return success

if __name__ == "__main__":
    result = asyncio.run(main())
    exit(0 if result else 1)