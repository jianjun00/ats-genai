#!/usr/bin/env python3
"""
Playwright test for Data Quality Agent Start Button
Tests the complete user workflow from UI click to agent activation
"""

import asyncio
import pytest
from playwright.async_api import async_playwright, expect
import requests
import time

BASE_URL = "http://localhost:4000"

async def test_agent_start_button_complete_workflow():
    """Test complete agent start workflow with UI interaction"""

    print("🎭 Starting Playwright test for Agent Start Button")

    # First verify service is running
    try:
        health_response = requests.get(f"{BASE_URL}/health", timeout=10)
        assert health_response.status_code == 200, f"Service not healthy: {health_response.text}"
        print("✅ Analytics service is healthy")
    except Exception as e:
        pytest.fail(f"❌ Service not available: {e}")

    async with async_playwright() as p:
        # Launch browser
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = await browser.new_context()
        page = await context.new_page()

        try:
            print(f"🌐 Navigating to {BASE_URL}")
            await page.goto(BASE_URL)
            await page.wait_for_load_state("domcontentloaded")

            # Click on Data Quality Dashboard button first
            print("🎯 Clicking on Data Quality Dashboard...")
            dq_button = page.locator('button:has-text("🎯 Data Quality Dashboard")')
            await dq_button.click()
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(2000)  # Wait for content to load

            # Check initial agent status via API
            initial_status_response = requests.get(f"{BASE_URL}/agent/status")
            initial_status = initial_status_response.json()
            print(f"📊 Initial agent status: {initial_status['status']} (ID: {initial_status['agent_id']})")

            # Navigate to agent section (look for start button)
            print("🔍 Looking for agent start button...")

            # Try to find start button by different selectors
            start_button_selectors = [
                "#start-agent-btn",
                'button:has-text("Start Agent")',
                'button:has-text("Start")',
                'input[type="button"][value*="Start"]',
                '[onclick*="startAgent"]'
            ]

            start_button = None
            for selector in start_button_selectors:
                try:
                    start_button = page.locator(selector).first
                    if await start_button.count() > 0:
                        print(f"✅ Found start button with selector: {selector}")
                        break
                except Exception as e:
                    print(f"   Selector {selector} failed: {e}")
                    continue

            if not start_button or await start_button.count() == 0:
                # Let's inspect what's actually on the page
                print("🔍 Inspecting page content for debugging...")
                page_content = await page.content()

                # Look for agent-related content
                if "Agent" in page_content:
                    print("✅ Found 'Agent' text in page")
                if "start" in page_content.lower():
                    print("✅ Found 'start' text in page")

                # Look for buttons
                buttons = await page.locator("button").all()
                print(f"🔘 Found {len(buttons)} buttons on page")
                for i, button in enumerate(buttons):
                    try:
                        text = await button.inner_text()
                        print(f"   Button {i}: '{text}'")
                    except:
                        print(f"   Button {i}: [could not get text]")

                # Look for inputs
                inputs = await page.locator("input").all()
                print(f"📝 Found {len(inputs)} inputs on page")
                for i, inp in enumerate(inputs):
                    try:
                        input_type = await inp.get_attribute("type")
                        value = await inp.get_attribute("value")
                        onclick = await inp.get_attribute("onclick")
                        print(f"   Input {i}: type='{input_type}' value='{value}' onclick='{onclick}'")
                    except:
                        print(f"   Input {i}: [could not get attributes]")

                pytest.fail("❌ Could not find agent start button on page")

            # Check button initial state
            button_text = await start_button.inner_text()
            is_disabled = await start_button.is_disabled()
            print(f"🔘 Start button text: '{button_text}', disabled: {is_disabled}")

            # Click the start button
            print("🖱️  Clicking start button...")
            await start_button.click()

            # Wait for any immediate UI changes
            await page.wait_for_timeout(2000)

            # Check if button text or state changed
            new_button_text = await start_button.inner_text()
            new_is_disabled = await start_button.is_disabled()
            print(f"🔘 After click - button text: '{new_button_text}', disabled: {new_is_disabled}")

            # Wait for agent status to change (give it some time)
            print("⏳ Waiting for agent status to change...")
            max_wait = 10  # seconds
            status_changed = False

            for attempt in range(max_wait):
                await asyncio.sleep(1)

                try:
                    status_response = requests.get(f"{BASE_URL}/agent/status", timeout=5)
                    current_status = status_response.json()

                    print(f"   Attempt {attempt+1}: Status = {current_status['status']} (ID: {current_status['agent_id']})")

                    if current_status['status'] == 'active':
                        status_changed = True
                        print("✅ Agent status changed to ACTIVE!")
                        break
                    elif current_status['status'] != initial_status['status']:
                        print(f"📊 Status changed from {initial_status['status']} to {current_status['status']}")

                except Exception as e:
                    print(f"   Error checking status: {e}")

            if not status_changed:
                print("❌ Agent status never changed to ACTIVE")
                # Get final status for debugging
                final_status_response = requests.get(f"{BASE_URL}/agent/status")
                final_status = final_status_response.json()
                print(f"🔍 Final status: {final_status}")

                # Check browser console for errors
                console_logs = []
                page.on("console", lambda msg: console_logs.append(f"{msg.type}: {msg.text}"))
                await page.wait_for_timeout(1000)

                if console_logs:
                    print("🖥️  Browser console messages:")
                    for log in console_logs[-10:]:  # Last 10 messages
                        print(f"   {log}")

                pytest.fail(f"Agent status did not change to active within {max_wait} seconds")

            # Test that subsequent clicks don't break anything
            print("🔄 Testing subsequent start button clicks...")
            await start_button.click()
            await page.wait_for_timeout(1000)

            final_status_response = requests.get(f"{BASE_URL}/agent/status")
            final_status = final_status_response.json()
            print(f"✅ Final agent status: {final_status['status']} (ID: {final_status['agent_id']})")

            assert final_status['status'] == 'active', f"Expected active status, got {final_status['status']}"

            print("🎉 Agent start button test PASSED!")

        except Exception as e:
            # Take screenshot for debugging
            await page.screenshot(path="agent_start_test_failure.png")
            print(f"📸 Screenshot saved: agent_start_test_failure.png")
            raise

        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_agent_start_button_complete_workflow())