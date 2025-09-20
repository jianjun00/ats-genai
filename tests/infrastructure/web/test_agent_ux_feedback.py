#!/usr/bin/env python3
"""
User Experience Test for Agent Start Button
Focuses on visual feedback and user experience issues
"""

import asyncio
from playwright.async_api import async_playwright
import requests
import time

BASE_URL = "http://localhost:4000"

async def test_agent_ux_feedback():
    """Test user experience and visual feedback for agent start"""
    
    print("🎭 Testing Agent Start Button User Experience")
    print("=" * 60)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = await browser.new_context()
        page = await context.new_page()
        
        try:
            # Navigate to dashboard
            await page.goto(BASE_URL)
            await page.wait_for_load_state("domcontentloaded")
            
            # Click Data Quality Dashboard
            dq_button = page.locator('button:has-text("🎯 Data Quality Dashboard")')
            await dq_button.click()
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(2000)
            
            # Reset agent to idle for clean test
            print("🔄 Resetting agent to idle state...")
            import subprocess
            subprocess.run(['docker-compose', '-f', 'docker-compose.intg.yml', 'restart', 'analytics-intg'], 
                          capture_output=True)
            await asyncio.sleep(15)  # Wait for restart
            
            # Refresh page after reset
            await page.reload()
            await page.wait_for_load_state("domcontentloaded")
            
            # Click Data Quality Dashboard again
            dq_button = page.locator('button:has-text("🎯 Data Quality Dashboard")')
            await dq_button.click()
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(3000)
            
            print("\n📊 INITIAL STATE ANALYSIS")
            print("-" * 40)
            
            # Analyze initial UI state
            start_button = page.locator("#start-agent-btn")
            if await start_button.count() == 0:
                print("❌ Start button not found!")
                return
                
            initial_button_text = await start_button.inner_text()
            initial_disabled = await start_button.is_disabled()
            
            # Check for status indicators
            status_elements = await page.locator("text=/status|idle|active|running/i").count()
            
            # Check for agent status display
            agent_status_text = await page.locator("text=/agent.*status|status.*agent/i").all()
            agent_status_info = []
            for element in agent_status_text:
                text = await element.inner_text()
                agent_status_info.append(text)
            
            print(f"🔘 Button Text: '{initial_button_text}'")
            print(f"🔘 Button Disabled: {initial_disabled}")
            print(f"📊 Status Elements Found: {status_elements}")
            print(f"🤖 Agent Status Info: {agent_status_info}")
            
            # Check API status
            api_response = requests.get(f"{BASE_URL}/agent/status")
            api_status = api_response.json()
            print(f"🔗 API Status: {api_status.get('status')} (ID: {api_status.get('agent_id')})")
            
            print("\n🖱️ BUTTON CLICK ANALYSIS")
            print("-" * 40)
            
            # Record time before click
            click_start_time = time.time()
            
            # Set up console monitoring
            console_messages = []
            page.on("console", lambda msg: console_messages.append(f"{msg.type}: {msg.text}"))
            
            # Click the button
            print("🖱️ Clicking start button...")
            await start_button.click()
            
            # Monitor immediate UI changes (within first few seconds)
            ui_changes = []
            for i in range(5):  # Check for 5 seconds
                await page.wait_for_timeout(1000)
                
                current_button_text = await start_button.inner_text()
                current_disabled = await start_button.is_disabled()
                current_status_elements = await page.locator("text=/status|idle|active|running/i").count()
                
                # Check for any notifications or alerts
                notifications = await page.locator(".notification, .alert, .success, .info").count()
                
                # Record changes
                ui_changes.append({
                    'time': i + 1,
                    'button_text': current_button_text,
                    'button_disabled': current_disabled,
                    'status_elements': current_status_elements,
                    'notifications': notifications
                })
                
                print(f"t+{i+1}s: Button='{current_button_text}' Disabled={current_disabled} StatusElem={current_status_elements} Notify={notifications}")
            
            print("\n📱 VISUAL FEEDBACK ANALYSIS")
            print("-" * 40)
            
            # Analyze what visual feedback user gets
            final_button_text = ui_changes[-1]['button_text']
            button_text_changed = initial_button_text != final_button_text
            button_disabled_changed = initial_disabled != ui_changes[-1]['button_disabled']
            
            notifications_appeared = any(change['notifications'] > 0 for change in ui_changes)
            status_elements_changed = any(change['status_elements'] != status_elements for change in ui_changes)
            
            print(f"✨ Button Text Changed: {button_text_changed} ('{initial_button_text}' -> '{final_button_text}')")
            print(f"🔒 Button Disabled State Changed: {button_disabled_changed}")
            print(f"🔔 Notifications Appeared: {notifications_appeared}")
            print(f"📊 Status Elements Changed: {status_elements_changed}")
            
            # Check console messages for JS errors or feedback
            if console_messages:
                print(f"🖥️ Console Messages ({len(console_messages)}):")
                for msg in console_messages[-5:]:  # Last 5 messages
                    print(f"   {msg}")
            
            print("\n🎯 USER EXPERIENCE ASSESSMENT")  
            print("-" * 40)
            
            # Check final API status
            final_api_response = requests.get(f"{BASE_URL}/agent/status")
            final_api_status = final_api_response.json()
            
            agent_actually_started = final_api_status.get('status') == 'active'
            
            print(f"✅ Agent Actually Started: {agent_actually_started}")
            print(f"🎯 Final API Status: {final_api_status.get('status')}")
            
            # UX Assessment
            good_ux_indicators = 0
            ux_issues = []
            
            if button_text_changed:
                good_ux_indicators += 1
                print("✅ UX: Button text provides feedback")
            else:
                ux_issues.append("Button text doesn't change to indicate action taken")
                
            if button_disabled_changed:
                good_ux_indicators += 1
                print("✅ UX: Button state changes to prevent double-clicks")
            else:
                ux_issues.append("Button remains enabled, could cause confusion")
                
            if notifications_appeared:
                good_ux_indicators += 1
                print("✅ UX: Notifications provide user feedback")
            else:
                ux_issues.append("No notifications to confirm action")
                
            if status_elements_changed:
                good_ux_indicators += 1
                print("✅ UX: Status display updates to reflect changes")
            else:
                ux_issues.append("No visible status change indicators")
            
            print(f"\n🏆 UX Score: {good_ux_indicators}/4 good indicators")
            
            if ux_issues:
                print("\n🔧 UX IMPROVEMENT OPPORTUNITIES:")
                for i, issue in enumerate(ux_issues, 1):
                    print(f"   {i}. {issue}")
            
            # Final assessment
            technical_working = agent_actually_started
            ux_satisfactory = good_ux_indicators >= 2
            
            print(f"\n🎭 FINAL ASSESSMENT:")
            print(f"🔧 Technical Functionality: {'✅ WORKING' if technical_working else '❌ BROKEN'}")
            print(f"🎨 User Experience: {'✅ SATISFACTORY' if ux_satisfactory else '❌ NEEDS IMPROVEMENT'}")
            
            if technical_working and not ux_satisfactory:
                print("\n💡 CONCLUSION: Agent works technically but UX needs improvement!")
                print("   The user might think it's 'not working' due to lack of visual feedback.")
            elif technical_working and ux_satisfactory:
                print("\n🎉 CONCLUSION: Both technical and UX aspects are working well!")
            else:
                print("\n🚨 CONCLUSION: Technical issues found!")
            
        except Exception as e:
            print(f"💥 Test failed with error: {e}")
            await page.screenshot(path="ux_test_failure.png")
            
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_agent_ux_feedback())