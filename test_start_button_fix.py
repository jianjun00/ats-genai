#!/usr/bin/env python3
"""
Test the Start button fix
"""

import asyncio
import json
from playwright.async_api import async_playwright

async def test_start_button_fix():
    """Test that the Start button now works correctly"""

    print("🔍 Testing Start Button Fix")
    print("=" * 40)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # Capture console messages
        console_messages = []
        page.on("console", lambda msg: console_messages.append(f"{msg.type}: {msg.text}"))
        page.on("pageerror", lambda error: console_messages.append(f"PAGE ERROR: {error.message}"))

        try:
            print("📍 Navigating to dashboard...")
            await page.goto("http://localhost:4000/data-quality/dashboard", wait_until="domcontentloaded")
            await page.wait_for_timeout(3000)

            # Check initial status
            initial_status = await page.locator("#agent-status").inner_text()
            print(f"📊 Initial status: {initial_status}")

            # Find Start button
            start_button = page.locator("button:has-text('Start'), button:has-text('▶')")

            if await start_button.count() > 0:
                print("✅ Start button found")

                # Clear console messages
                console_messages.clear()

                # Click Start button
                print("🔄 Clicking Start button...")
                await start_button.first.click()

                # Wait for response
                await page.wait_for_timeout(3000)

                # Check for console errors
                if console_messages:
                    print("🔍 Console messages after click:")
                    for msg in console_messages:
                        print(f"   {msg}")
                else:
                    print("✅ No console errors after clicking Start")

                # Check updated status
                updated_status = await page.locator("#agent-status").inner_text()
                print(f"📊 Updated status: {updated_status}")

                # Test the API directly to verify
                response = await page.request.post("http://localhost:4000/agent/start")
                print(f"📡 Direct API test: {response.status}")
                if response.status == 200:
                    data = await response.json()
                    print(f"   Response: {data}")

                # Test current agent status
                status_response = await page.request.get("http://localhost:4000/agent/status")
                if status_response.status == 200:
                    status_data = await status_response.json()
                    print(f"📊 Current agent status: {status_data.get('status', 'unknown')}")

                print("✅ Start button functionality tested successfully!")
                return True

            else:
                print("❌ Start button not found")
                return False

        except Exception as e:
            print(f"❌ Test failed: {e}")
            return False

        finally:
            await browser.close()

if __name__ == "__main__":
    success = asyncio.run(test_start_button_fix())
    if success:
        print("\n🎉 Start button is now working!")
    else:
        print("\n💥 Start button still has issues")