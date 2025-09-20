#!/usr/bin/env python3
"""
Simple User Experience Test for Agent Start
"""

import asyncio
from playwright.async_api import async_playwright
import requests

BASE_URL = "http://localhost:4000"

async def test_simple_ux():
    print("🎭 Simple Agent Start UX Test")
    print("=" * 40)
    
    # First check current agent status via API
    try:
        status_response = requests.get(f"{BASE_URL}/agent/status")
        current_status = status_response.json()
        print(f"📊 Current Agent Status: {current_status.get('status')} (ID: {current_status.get('agent_id')})")
    except Exception as e:
        print(f"❌ Can't connect to service: {e}")
        return
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        page = await browser.new_page()
        
        try:
            # Navigate to main page
            print("🌐 Navigating to main page...")
            await page.goto(BASE_URL)
            await page.wait_for_load_state("domcontentloaded")
            
            # Look for Data Quality Dashboard button
            print("🔍 Looking for Data Quality Dashboard button...")
            dashboard_buttons = await page.locator('button:has-text("Data Quality")').count()
            print(f"   Found {dashboard_buttons} Data Quality buttons")
            
            if dashboard_buttons > 0:
                print("🎯 Clicking Data Quality Dashboard...")
                await page.locator('button:has-text("Data Quality")').first.click()
                await page.wait_for_load_state("domcontentloaded")
                await page.wait_for_timeout(3000)
                
                # Look for start button
                print("🔍 Looking for agent start button...")
                start_buttons = await page.locator("#start-agent-btn").count()
                print(f"   Found {start_buttons} start buttons")
                
                if start_buttons > 0:
                    start_button = page.locator("#start-agent-btn")
                    
                    # Check button state before click
                    before_text = await start_button.inner_text()
                    before_disabled = await start_button.is_disabled()
                    print(f"🔘 Before click: Text='{before_text}', Disabled={before_disabled}")
                    
                    # Check agent status before click
                    before_api = requests.get(f"{BASE_URL}/agent/status").json()
                    print(f"📊 Before click API: {before_api.get('status')}")
                    
                    # Click the button
                    print("🖱️ Clicking start button...")
                    await start_button.click()
                    
                    # Check immediate changes
                    await page.wait_for_timeout(1000)
                    after_text = await start_button.inner_text()
                    after_disabled = await start_button.is_disabled()
                    print(f"🔘 After click (1s): Text='{after_text}', Disabled={after_disabled}")
                    
                    # Check agent status after click
                    after_api = requests.get(f"{BASE_URL}/agent/status").json()
                    print(f"📊 After click API: {after_api.get('status')}")
                    
                    # Look for any success messages or notifications
                    notifications = await page.locator("text=/success|started|active|running/i").count()
                    print(f"🔔 Success indicators found: {notifications}")
                    
                    if notifications > 0:
                        notif_elements = await page.locator("text=/success|started|active|running/i").all()
                        for i, elem in enumerate(notif_elements):
                            text = await elem.inner_text()
                            print(f"   {i+1}. '{text}'")
                    
                    # Analysis
                    print("\n🎯 UX ANALYSIS:")
                    print(f"   Button text changed: {before_text != after_text}")
                    print(f"   Button disabled changed: {before_disabled != after_disabled}")  
                    print(f"   Agent status changed: {before_api.get('status')} -> {after_api.get('status')}")
                    print(f"   Visual feedback provided: {notifications > 0}")
                    
                    # UX Assessment
                    if before_text == after_text and not after_disabled and notifications == 0:
                        print("\n❌ UX ISSUE: No clear visual feedback that action was taken!")
                        print("   - Button text stays the same")
                        print("   - Button stays enabled") 
                        print("   - No success notifications")
                        print("   - User might think nothing happened")
                    else:
                        print("\n✅ UX appears adequate - some feedback provided")
                        
                else:
                    print("❌ Start button not found on dashboard page")
            else:
                print("❌ Data Quality Dashboard button not found")
                
        except Exception as e:
            print(f"💥 Error: {e}")
            await page.screenshot(path="simple_ux_error.png")
            
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_simple_ux())