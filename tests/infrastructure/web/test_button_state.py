#!/usr/bin/env python3
"""
Detailed test for button state changes
"""

import asyncio
from playwright.async_api import async_playwright
import requests

BASE_URL = "http://localhost:4000"

async def test_button_states():
    print("🔘 Detailed Button State Test")
    print("=" * 40)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        page = await browser.new_page()
        
        try:
            # Navigate to dashboard
            await page.goto(BASE_URL)
            await page.wait_for_load_state("domcontentloaded")
            
            # Click Data Quality Dashboard
            await page.locator('button:has-text("Data Quality")').first.click()
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(3000)
            
            print("🔍 INITIAL BUTTON STATE:")
            
            # Check both start and stop buttons
            start_button = page.locator("#start-agent-btn")
            stop_button = page.locator("#stop-agent-btn")
            
            start_count = await start_button.count()
            stop_count = await stop_button.count()
            print(f"   Start buttons found: {start_count}")
            print(f"   Stop buttons found: {stop_count}")
            
            if start_count > 0:
                start_visible = await start_button.is_visible()
                start_text = await start_button.inner_text()
                print(f"   Start button visible: {start_visible}, text: '{start_text}'")
                
            if stop_count > 0:
                stop_visible = await stop_button.is_visible()
                stop_text = await stop_button.inner_text()
                print(f"   Stop button visible: {stop_visible}, text: '{stop_text}'")
            
            # Check agent status display
            status_element = page.locator("#agent-status")
            if await status_element.count() > 0:
                status_text = await status_element.inner_text()
                print(f"   Status display: '{status_text}'")
            
            # Check API status
            api_response = requests.get(f"{BASE_URL}/agent/status")
            api_status = api_response.json()
            print(f"   API status: {api_status.get('status')}")
            
            print("\n🖱️ CLICKING START BUTTON:")
            
            if start_count > 0 and await start_button.is_visible():
                await start_button.click()
                print("   ✅ Start button clicked")
                
                # Wait for UI updates with multiple checks
                for delay in [500, 1000, 2000]:
                    await page.wait_for_timeout(delay)
                    print(f"\n   After {delay}ms:")
                    
                    # Check button visibility again
                    start_visible_after = await start_button.is_visible() if start_count > 0 else False
                    stop_visible_after = await stop_button.is_visible() if stop_count > 0 else False
                    
                    print(f"     Start button visible: {start_visible_after}")
                    print(f"     Stop button visible: {stop_visible_after}")
                    
                    # Check status display
                    if await status_element.count() > 0:
                        status_text_after = await status_element.inner_text()
                        print(f"     Status display: '{status_text_after}'")
                    
                    # Check API status
                    api_response_after = requests.get(f"{BASE_URL}/agent/status")
                    api_status_after = api_response_after.json()
                    print(f"     API status: {api_status_after.get('status')}")
                    
                print("\n🎯 FINAL ASSESSMENT:")
                
                # Final state check
                final_start_visible = await start_button.is_visible() if start_count > 0 else False
                final_stop_visible = await stop_button.is_visible() if stop_count > 0 else False
                final_api_status = requests.get(f"{BASE_URL}/agent/status").json()
                
                expected_behavior = final_api_status.get('status') == 'active'
                ui_matches_api = (not final_start_visible and final_stop_visible) if expected_behavior else (final_start_visible and not final_stop_visible)
                
                print(f"   Agent is active: {expected_behavior}")
                print(f"   UI matches API state: {ui_matches_api}")
                
                if expected_behavior and not ui_matches_api:
                    print("   ❌ BUG: Agent is active but UI still shows start button!")
                    
                    # Debug: Check for JavaScript errors
                    logs = []
                    page.on("console", lambda msg: logs.append(f"{msg.type}: {msg.text}"))
                    await page.wait_for_timeout(1000)
                    
                    if logs:
                        print("   🐛 Browser console logs:")
                        for log in logs[-5:]:
                            print(f"      {log}")
                            
                elif expected_behavior and ui_matches_api:
                    print("   ✅ SUCCESS: UI correctly shows stop button when agent is active!")
                else:
                    print(f"   ⚠️  Unexpected state - investigate further")
                    
            else:
                print("   ❌ No start button found to click")
                
        except Exception as e:
            print(f"💥 Error: {e}")
            await page.screenshot(path="button_state_error.png")
            
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_button_states())