#!/usr/bin/env python3
"""
Test if dataset 65 now shows sequences in the UI
"""
import asyncio
from playwright.async_api import async_playwright
import json

async def test_dataset_sequences_ui():
    """Test if dataset 65 shows sequences in UI"""

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            print("🌐 Navigating to training datasets page...")
            await page.goto("http://localhost:3000/training-datasets", timeout=30000)
            await page.wait_for_load_state("networkidle")

            print("📋 Looking for datasets...")

            # Wait for datasets to load
            await page.wait_for_selector("[data-testid='training-datasets-container']", timeout=10000)

            # Look for our dataset
            dataset_items = await page.query_selector_all("[data-testid='dataset-item']")
            print(f"Found {len(dataset_items)} datasets")

            found_our_dataset = False
            for i, item in enumerate(dataset_items):
                text = await item.text_content()
                print(f"Dataset {i+1}: {text[:100]}...")

                if "AAPL_TSLA_20250701_20250906_Run89" in text or "AAPL" in text and "TSLA" in text:
                    print(f"🎯 Found our dataset! Clicking item {i+1}...")
                    await item.click()
                    found_our_dataset = True

                    # Wait for sequence dropdown to appear
                    await asyncio.sleep(2)

                    # Look for sequence selector
                    sequence_selector = await page.query_selector("[data-testid='sequence-selector']")
                    if sequence_selector:
                        print("✅ Sequence selector found!")

                        # Click to open dropdown
                        await sequence_selector.click()
                        await asyncio.sleep(1)

                        # Look for sequence options
                        sequence_options = await page.query_selector_all("[data-testid='sequence-option']")
                        print(f"Found {len(sequence_options)} sequence options:")

                        for j, option in enumerate(sequence_options):
                            option_text = await option.text_content()
                            print(f"  {j+1}. {option_text}")

                        if len(sequence_options) > 0:
                            print("🎉 SUCCESS: Dataset has sequences!")
                            return True
                        else:
                            print("❌ No sequence options found")
                    else:
                        print("❌ No sequence selector found")
                    break

            if not found_our_dataset:
                print("❌ Our dataset was not found in the list")

                # List all available datasets for debugging
                print("\nAll available datasets:")
                for i, item in enumerate(dataset_items):
                    text = await item.text_content()
                    print(f"  {i+1}: {text}")

            return False

        except Exception as e:
            print(f"❌ Error testing UI: {e}")
            import traceback
            traceback.print_exc()
            return False

        finally:
            await browser.close()

if __name__ == "__main__":
    result = asyncio.run(test_dataset_sequences_ui())
    if result:
        print("\n✅ DATASET SEQUENCES WORKING IN UI!")
    else:
        print("\n❌ Dataset sequences not working yet")