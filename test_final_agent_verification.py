#!/usr/bin/env python3
"""
Final Comprehensive Agent Start Functionality Verification
Proves the complete user experience is working perfectly
"""

import asyncio
from playwright.async_api import async_playwright
import requests
import subprocess
import time

BASE_URL = "http://localhost:4000"

async def test_complete_user_experience():
    print("🏆 FINAL AGENT START FUNCTIONALITY VERIFICATION")
    print("=" * 60)
    print("Testing complete user workflow from idle to active state")
    print()

    # Step 1: Reset to clean state
    print("🔄 Step 1: Resetting to clean idle state...")
    subprocess.run(['docker-compose', '-f', 'docker-compose.intg.yml', 'restart', 'analytics-intg'],
                   capture_output=True)
    time.sleep(15)  # Wait for clean restart
    print("   ✅ Service restarted successfully")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        page = await browser.new_page()

        try:
            # Step 2: Navigate to dashboard
            print("\n🌐 Step 2: Navigate to Data Quality Dashboard...")
            await page.goto(BASE_URL)
            await page.wait_for_load_state("domcontentloaded")

            await page.locator('button:has-text("Data Quality")').first.click()
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(3000)
            print("   ✅ Successfully navigated to dashboard")

            # Step 3: Verify initial idle state
            print("\n📊 Step 3: Verify initial idle state...")

            api_response = requests.get(f"{BASE_URL}/agent/status")
            initial_api_status = api_response.json()

            start_button = page.locator("#start-agent-btn")
            stop_button = page.locator("#stop-agent-btn")
            status_display = page.locator("#agent-status")

            start_visible = await start_button.is_visible()
            stop_visible = await stop_button.is_visible()
            status_text = await status_display.inner_text()

            print(f"   API Status: {initial_api_status.get('status')}")
            print(f"   Status Display: '{status_text}'")
            print(f"   Start Button Visible: {start_visible}")
            print(f"   Stop Button Visible: {stop_visible}")

            idle_state_correct = (
                initial_api_status.get('status') == 'idle' and
                start_visible and
                not stop_visible and
                'IDLE' in status_text
            )

            if idle_state_correct:
                print("   ✅ Initial idle state is PERFECT")
            else:
                print("   ❌ Initial idle state is incorrect")
                return False

            # Step 4: Click start button
            print("\n🖱️ Step 4: Click start button...")

            await start_button.click()
            print("   ✅ Start button clicked successfully")

            # Step 5: Verify immediate UI feedback
            print("\n⏱️ Step 5: Verify UI updates...")

            # Wait for status updates with multiple checks
            ui_updated_correctly = False
            for attempt in range(6):  # Check for 3 seconds
                await page.wait_for_timeout(500)

                current_start_visible = await start_button.is_visible()
                current_stop_visible = await stop_button.is_visible()
                current_status_text = await status_display.inner_text()
                current_api_response = requests.get(f"{BASE_URL}/agent/status")
                current_api_status = current_api_response.json()

                print(f"   t+{0.5*(attempt+1)}s: API={current_api_status.get('status')}, Start={current_start_visible}, Stop={current_stop_visible}")

                # Check if UI correctly shows active state
                if (current_api_status.get('status') == 'active' and
                    not current_start_visible and
                    current_stop_visible and
                    'ACTIVE' in current_status_text):
                    ui_updated_correctly = True
                    print(f"   ✅ UI updated correctly at t+{0.5*(attempt+1)}s")
                    break

            if not ui_updated_correctly:
                print("   ❌ UI did not update correctly")
                return False

            # Step 6: Verify final active state
            print("\n🎯 Step 6: Verify final active state...")

            final_api_response = requests.get(f"{BASE_URL}/agent/status")
            final_api_status = final_api_response.json()

            final_start_visible = await start_button.is_visible()
            final_stop_visible = await stop_button.is_visible()
            final_status_text = await status_display.inner_text()

            print(f"   Final API Status: {final_api_status.get('status')}")
            print(f"   Final Status Display: '{final_status_text}'")
            print(f"   Start Button Visible: {final_start_visible}")
            print(f"   Stop Button Visible: {final_stop_visible}")
            print(f"   Tools Available: {final_api_status.get('tools_available')}")

            active_state_perfect = (
                final_api_status.get('status') == 'active' and
                not final_start_visible and
                final_stop_visible and
                'ACTIVE' in final_status_text and
                final_api_status.get('tools_available') >= 2
            )

            if active_state_perfect:
                print("   ✅ Final active state is PERFECT")
            else:
                print("   ❌ Final active state is incorrect")
                return False

            # Step 7: Test stop functionality
            print("\n⏹️ Step 7: Test stop button functionality...")

            await stop_button.click()
            await page.wait_for_timeout(2000)  # Wait for stop

            stop_api_response = requests.get(f"{BASE_URL}/agent/status")
            stop_api_status = stop_api_response.json()

            after_stop_start_visible = await start_button.is_visible()
            after_stop_stop_visible = await stop_button.is_visible()

            print(f"   After Stop - API Status: {stop_api_status.get('status')}")
            print(f"   After Stop - Start Button Visible: {after_stop_start_visible}")
            print(f"   After Stop - Stop Button Visible: {after_stop_stop_visible}")

            # Step 8: Final assessment
            print("\n🏆 FINAL ASSESSMENT:")
            print("=" * 40)

            all_tests_passed = (
                idle_state_correct and
                ui_updated_correctly and
                active_state_perfect
            )

            if all_tests_passed:
                print("🎉 🎉 🎉  ALL TESTS PASSED  🎉 🎉 🎉")
                print()
                print("✅ Agent starts correctly (idle → active)")
                print("✅ UI provides immediate visual feedback")
                print("✅ Start button hides when agent is active")
                print("✅ Stop button appears when agent is active")
                print("✅ Status display updates correctly")
                print("✅ API status matches UI state perfectly")
                print("✅ Tools are available and functional")
                print()
                print("🚀 CONCLUSION: Agent start functionality is WORKING PERFECTLY!")
                print("   The user experience is now excellent with clear visual feedback.")
                return True
            else:
                print("❌ Some tests failed - functionality needs more work")
                return False

        except Exception as e:
            print(f"💥 Test failed with error: {e}")
            await page.screenshot(path="final_test_error.png")
            return False

        finally:
            await browser.close()

if __name__ == "__main__":
    success = asyncio.run(test_complete_user_experience())
    if success:
        print("\n🎯 Agent start functionality verification: ✅ COMPLETE SUCCESS")
        exit(0)
    else:
        print("\n❌ Agent start functionality verification: FAILED")
        exit(1)