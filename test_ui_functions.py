#!/usr/bin/env python3
"""
Test UI JavaScript functions are defined and working
"""

import asyncio
from playwright.async_api import async_playwright

async def test_ui_functions():
    print("🧪 TESTING UI JAVASCRIPT FUNCTIONS")
    print("=" * 45)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # Track console logs
        console_logs = []
        page.on("console", lambda msg: console_logs.append(f"{msg.type()}: {msg.text()}"))

        # Track JavaScript errors
        js_errors = []
        page.on("pageerror", lambda error: js_errors.append(str(error)))

        try:
            print("📊 Step 1: Load homepage")
            await page.goto("http://localhost:3000", wait_until="networkidle")

            print("📊 Step 2: Check for JavaScript errors")
            if js_errors:
                print("❌ JavaScript errors found:")
                for error in js_errors:
                    print(f"   {error}")
                return False
            else:
                print("✅ No JavaScript errors on page load")

            print("📊 Step 3: Check if functions are defined")
            functions_to_check = [
                'loadTrainingDatasets',
                'loadUniverseAnalytics',
                'loadEDA',
                'loadBarCollectionMetrics',
                'loadRayAnalytics'
            ]

            for func_name in functions_to_check:
                is_defined = await page.evaluate(f"typeof {func_name} === 'function'")
                print(f"   {func_name}: {'✅ Defined' if is_defined else '❌ Missing'}")
                if not is_defined:
                    return False

            print("📊 Step 4: Test clicking Training Datasets button")
            training_button = page.get_by_role("button", name="🤖 Training Datasets")
            await training_button.click()
            await page.wait_for_timeout(3000)

            # Check if content loaded
            content = await page.locator("#analysis-content").inner_html()
            if "Training Datasets" in content and "Loading" not in content:
                print("✅ Training Datasets interface loaded successfully")

                # Check for dropdowns
                dataset_dropdown = page.locator("#dataset-selector")
                sequence_dropdown = page.locator("#sequence-selector")

                if await dataset_dropdown.count() > 0:
                    print("✅ Dataset dropdown found")

                    # Get options
                    options = await dataset_dropdown.locator("option").all_text_contents()
                    print(f"   Dataset options: {len(options)}")

                    # Check for dataset 63
                    has_dataset_63 = any("63" in opt for opt in options)
                    if has_dataset_63:
                        print("✅ Dataset 63 found in dropdown")

                        # Select dataset 63
                        await dataset_dropdown.select_option(value="63")
                        await page.wait_for_timeout(2000)

                        # Check sequence dropdown
                        if await sequence_dropdown.count() > 0:
                            seq_options = await sequence_dropdown.locator("option").all_text_contents()
                            print(f"   Sequence options: {seq_options}")

                            if len(seq_options) > 1:
                                print("✅ Sequence dropdown populated successfully")
                                return True
                            else:
                                print("❌ Sequence dropdown not populated")
                                return False
                        else:
                            print("❌ Sequence dropdown not found")
                            return False
                    else:
                        print("❌ Dataset 63 not found in options")
                        return False
                else:
                    print("❌ Dataset dropdown not found")
                    return False
            else:
                print("❌ Training Datasets interface not loaded properly")
                print(f"Content: {content[:200]}...")
                return False

        except Exception as e:
            print(f"❌ Test failed: {e}")
            return False

        finally:
            if console_logs:
                print("\n📋 Console logs:")
                for log in console_logs[-10:]:  # Show last 10
                    print(f"   {log}")

            if js_errors:
                print("\n❌ JavaScript errors:")
                for error in js_errors:
                    print(f"   {error}")

            await browser.close()

if __name__ == "__main__":
    result = asyncio.run(test_ui_functions())
    print(f"\n{'✅ UI TEST PASSED!' if result else '❌ UI TEST FAILED!'}")
    exit(0 if result else 1)