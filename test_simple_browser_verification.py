#!/usr/bin/env python3
"""
Simple browser test to verify sequence selector functionality.
"""
import asyncio
from playwright.async_api import async_playwright
import time

async def simple_browser_test():
    """Simple test of the sequence selector."""
    print("🎭 Simple Browser Test for Sequence Selector")
    print("=" * 50)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            # Navigate to EDA page
            print("1️⃣ Loading EDA page...")
            response = await page.goto("http://localhost:3000/eda", wait_until="networkidle", timeout=60000)
            print(f"   📄 Page loaded with status: {response.status}")

            # Wait for page to fully load
            await page.wait_for_timeout(3000)

            # Click the Training Datasets button to load the interface
            print("2️⃣ Clicking Training Datasets button...")
            training_button = page.locator('button:has-text("🤖 Training Datasets")')
            if await training_button.count() > 0:
                await training_button.click()
                print("   ✅ Training Datasets button clicked")
                await page.wait_for_timeout(3000)  # Wait for interface to load
            else:
                print("   ❌ Training Datasets button not found")
                return False

            # Check if selectors exist after loading training datasets
            print("3️⃣ Checking for selector elements...")
            dataset_selector_exists = await page.locator("#dataset-selector").count() > 0
            sequence_selector_exists = await page.locator("#sequence-selector").count() > 0

            print(f"   📊 Dataset selector exists: {dataset_selector_exists}")
            print(f"   🔢 Sequence selector exists: {sequence_selector_exists}")

            if not dataset_selector_exists:
                print("❌ Dataset selector not found - page may not have loaded correctly")
                page_content = await page.content()
                if "dataset-selector" in page_content:
                    print("   ℹ️  Element exists in HTML but not rendered yet")
                return False

            # Get initial dataset options
            print("4️⃣ Checking initial dataset options...")
            dataset_options = await page.locator("#dataset-selector option").all()
            print(f"   📊 Found {len(dataset_options)} dataset options initially")

            # Wait for datasets to load (JavaScript should populate them)
            await page.wait_for_timeout(5000)

            # Check dataset options after waiting
            dataset_options = await page.locator("#dataset-selector option").all()
            valid_datasets = []

            for option in dataset_options:
                text = await option.text_content()
                value = await option.get_attribute("value")
                if value and value != "":
                    valid_datasets.append({"text": text, "value": value})
                    print(f"      • '{text}' (value: {value})")

            if not valid_datasets:
                print("❌ No valid datasets loaded")
                return False

            print(f"   ✅ Found {len(valid_datasets)} valid datasets")

            # Select first dataset
            print("5️⃣ Selecting first dataset...")
            first_dataset = valid_datasets[0]
            await page.locator("#dataset-selector").select_option(first_dataset["value"])
            print(f"   ✅ Selected: {first_dataset['text']}")

            # Wait for sequence selector to update
            print("6️⃣ Waiting for sequence selector to update...")
            await page.wait_for_timeout(5000)

            # Check sequence options
            sequence_options = await page.locator("#sequence-selector option").all()
            valid_sequences = []
            no_sequences_found = False

            for option in sequence_options:
                text = await option.text_content()
                value = await option.get_attribute("value")
                print(f"      • '{text}' (value: '{value}')")

                if "no sequences found" in text.lower():
                    no_sequences_found = True
                elif value and value != "":
                    valid_sequences.append({"text": text, "value": value})

            # Final result
            print("7️⃣ Final Results:")
            if no_sequences_found:
                print("   ❌ FAILURE: Browser still shows 'No sequences found'")
                return False
            elif len(valid_sequences) > 0:
                print(f"   ✅ SUCCESS: Browser shows {len(valid_sequences)} valid sequences!")
                for seq in valid_sequences:
                    print(f"      ✓ {seq['text']}")
                return True
            else:
                print("   ⚠️  UNCLEAR: No 'No sequences found' but also no valid sequences")
                return False

        except Exception as e:
            print(f"❌ Test failed: {e}")
            return False
        finally:
            await browser.close()

async def main():
    success = await simple_browser_test()

    if success:
        print("\n🎉 BROWSER TEST PASSED!")
        print("The sequence selector now works correctly in the browser.")
    else:
        print("\n❌ BROWSER TEST FAILED!")
        print("The sequence selector still has issues.")

    return success

if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)