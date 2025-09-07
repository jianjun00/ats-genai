#!/usr/bin/env python3
"""
Take a screenshot proof of the sequence selection working
"""

import asyncio
from playwright.async_api import async_playwright

async def take_screenshot_proof():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, args=['--no-sandbox'])  # Non-headless to see what's happening
        context = await browser.new_context(
            viewport={'width': 1920, 'height': 1080}
        )
        page = await context.new_page()

        try:
            print("📸 Taking screenshot proof of sequence selection...")

            # Navigate to EDA tool
            await page.goto("http://localhost:3000")
            await page.wait_for_load_state("networkidle")

            # Take screenshot of initial state
            await page.screenshot(path="/tmp/step1_initial.png", full_page=True)
            print("✅ Screenshot 1: Initial page")

            # Click Training Datasets button
            training_button = page.get_by_role("button", name="🤖 Training Datasets")
            await training_button.click()
            await page.wait_for_timeout(3000)

            # Take screenshot after clicking Training Datasets
            await page.screenshot(path="/tmp/step2_training_datasets.png", full_page=True)
            print("✅ Screenshot 2: After clicking Training Datasets")

            # Find and select dataset 63
            dataset_dropdown = page.locator("#dataset-selector")
            await dataset_dropdown.select_option(value="63")
            await page.wait_for_timeout(3000)

            # Take screenshot after dataset selection
            await page.screenshot(path="/tmp/step3_dataset_selected.png", full_page=True)
            print("✅ Screenshot 3: After selecting dataset 63")

            # Check sequence dropdown
            sequence_dropdown = page.locator("#sequence-selector")
            options = await sequence_dropdown.locator("option").all_text_contents()
            print(f"Sequence options: {options}")

            if len(options) > 1:
                # Select first sequence
                await sequence_dropdown.select_option(index=1)
                await page.wait_for_timeout(2000)

                # Take screenshot after sequence selection
                await page.screenshot(path="/tmp/step4_sequence_selected.png", full_page=True)
                print("✅ Screenshot 4: After selecting sequence")

                # Click visualize button
                visualize_button = page.locator("text=📊 Visualize")
                await visualize_button.click()
                await page.wait_for_timeout(5000)

                # Take final screenshot with visualization
                await page.screenshot(path="/tmp/step5_visualization.png", full_page=True)
                print("✅ Screenshot 5: Final visualization")

                return True
            else:
                print("❌ No sequence options found")
                return False

        except Exception as e:
            print(f"❌ Error taking screenshot: {e}")
            await page.screenshot(path="/tmp/error_screenshot.png", full_page=True)
            return False

        finally:
            await browser.close()

if __name__ == "__main__":
    result = asyncio.run(take_screenshot_proof())
    print(f"\n{'✅ SUCCESS' if result else '❌ FAILED'}")
    print("Screenshots saved in /tmp/:")
    print("  - step1_initial.png")
    print("  - step2_training_datasets.png")
    print("  - step3_dataset_selected.png")
    print("  - step4_sequence_selected.png")
    print("  - step5_visualization.png")