#!/usr/bin/env python3
"""
Debug the Start button failure issue
"""

import asyncio
import json
from playwright.async_api import async_playwright

async def debug_start_button():
    """Debug why the Start button is failing"""

    print("🔍 Debugging Start Button Failure")
    print("=" * 50)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # Capture console messages and errors
        console_messages = []
        page.on("console", lambda msg: console_messages.append(f"{msg.type}: {msg.text}"))
        page.on("pageerror", lambda error: console_messages.append(f"PAGE ERROR: {error.message}"))

        try:
            print("📍 Navigating to dashboard...")
            await page.goto("http://localhost:4000/data-quality/dashboard", wait_until="domcontentloaded")
            await page.wait_for_timeout(3000)

            # Check initial agent status
            agent_status = await page.locator("#agent-status").inner_text()
            print(f"📊 Initial agent status: {agent_status}")

            # Test the agent/status API directly
            print("🔍 Testing agent status API...")
            response = await page.request.get("http://localhost:4000/agent/status")
            print(f"   Status Code: {response.status}")
            if response.status == 200:
                data = await response.json()
                print(f"   Response: {json.dumps(data, indent=2)}")
            else:
                error_text = await response.text()
                print(f"   Error: {error_text}")

            # Find and test the Start button
            print("🔍 Looking for Start button...")
            start_buttons = await page.locator("button").all()

            start_button = None
            for button in start_buttons:
                button_text = await button.inner_text()
                if "Start" in button_text or "▶" in button_text:
                    print(f"   Found Start button: '{button_text}'")
                    start_button = button
                    break

            if not start_button:
                print("❌ Start button not found!")
                return False

            # Test clicking the Start button
            print("🔍 Testing Start button click...")

            # Clear console messages before click
            console_messages.clear()

            # Click the button
            await start_button.click()
            await page.wait_for_timeout(2000)  # Wait for any async operations

            # Check for console errors after click
            print("🔍 Console messages after click:")
            for msg in console_messages:
                print(f"   {msg}")

            # Test the agent/start API directly
            print("🔍 Testing agent start API directly...")
            start_response = await page.request.post("http://localhost:4000/agent/start")
            print(f"   Start API Status Code: {start_response.status}")

            if start_response.status == 200:
                start_data = await start_response.json()
                print(f"   Start Response: {json.dumps(start_data, indent=2)}")
            else:
                start_error = await start_response.text()
                print(f"   Start Error: {start_error}")

            # Check agent status after attempting start
            await page.wait_for_timeout(1000)
            final_status = await page.locator("#agent-status").inner_text()
            print(f"📊 Final agent status: {final_status}")

            # Look for error messages on page
            print("🔍 Looking for error messages...")
            error_elements = await page.locator(".error, .alert-danger, [id*='error']").all()
            for error_elem in error_elements:
                error_text = await error_elem.inner_text()
                if error_text.strip():
                    print(f"   Error on page: {error_text}")

            # Check if there are any network errors
            print("🔍 Checking for failed network requests...")

            # Test all agent-related endpoints
            endpoints = [
                "/agent/status",
                "/agent/start",
                "/agent/stop",
                "/data-quality/api/issues"
            ]

            for endpoint in endpoints:
                try:
                    test_response = await page.request.get(f"http://localhost:4000{endpoint}")
                    print(f"   {endpoint}: {test_response.status}")
                except Exception as e:
                    print(f"   {endpoint}: ERROR - {e}")

            # Take screenshot for analysis
            await page.screenshot(path="/tmp/start_button_debug.png")
            print("📸 Screenshot saved to /tmp/start_button_debug.png")

            return True

        except Exception as e:
            print(f"❌ Debug failed: {e}")
            await page.screenshot(path="/tmp/start_button_error.png")
            return False

        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_start_button())