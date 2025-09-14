#!/usr/bin/env python3
"""
Playwright test for Agent Status Loading Fix
Tests that the data quality dashboard properly loads agent status instead of showing "Loading..."
"""

import asyncio
import sys
import os
from playwright.async_api import async_playwright

async def test_agent_status_loading():
    """Test that agent status loads properly on data quality dashboard"""
    
    print("🎭 Testing Agent Status Loading with Playwright")
    print("=" * 60)
    
    async with async_playwright() as p:
        # Launch browser in headless mode
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        try:
            # Navigate to data quality dashboard
            print("📍 Navigating to data quality dashboard...")
            await page.goto("http://localhost:4000/data-quality/dashboard", wait_until="domcontentloaded")
            
            # Wait for page to fully load
            await page.wait_for_timeout(3000)
            
            # Check if agent status is still showing "Loading..."
            agent_status_element = page.locator("#agent-status")
            await agent_status_element.wait_for()
            
            agent_status_text = await agent_status_element.inner_text()
            print(f"📊 Agent Status Text: {agent_status_text}")
            
            # Test 1: Should not show "Loading..."
            if "Loading..." in agent_status_text:
                print("❌ FAIL: Agent status is still stuck on 'Loading...'")
                return False
            else:
                print("✅ PASS: Agent status is not stuck on 'Loading...'")
            
            # Test 2: Should show either IDLE or ACTIVE
            if "IDLE" in agent_status_text or "ACTIVE" in agent_status_text:
                print("✅ PASS: Agent status shows proper state (IDLE/ACTIVE)")
            else:
                print(f"⚠️ WARNING: Agent status shows unexpected state: {agent_status_text}")
            
            # Test 3: Check that agent status endpoint is working
            print("🔍 Testing agent status API directly...")
            response = await page.request.get("http://localhost:4000/agent/status")
            status_data = await response.json()
            print(f"📡 API Response: {status_data}")
            
            if response.status == 200:
                print("✅ PASS: Agent status API is responding")
                print(f"   - Status: {status_data.get('status', 'unknown')}")
                print(f"   - Tools Available: {status_data.get('tools_available', 0)}")
                print(f"   - MCP Tools Ready: {status_data.get('mcp_tools_ready', False)}")
            else:
                print(f"❌ FAIL: Agent status API returned {response.status}")
                return False
            
            # Test 4: Check data quality issues loading
            print("🔍 Testing data quality issues loading...")
            issues_element = page.locator("#issues-list")
            await issues_element.wait_for()
            
            # Wait for issues to load (should not show "Loading..." indefinitely)
            await page.wait_for_timeout(5000)
            issues_text = await issues_element.inner_text()
            
            if "Loading data quality issues" in issues_text and "Refreshing data" not in issues_text:
                print("⚠️ WARNING: Issues list might be stuck loading")
            else:
                print("✅ PASS: Issues list loaded successfully")
                # Count issues if loaded
                issue_divs = await page.locator(".issue").count()
                print(f"   - Issues detected: {issue_divs}")
            
            # Test 5: Check stats are populated
            print("🔍 Testing dashboard statistics...")
            total_issues = await page.locator("#total-issues").inner_text()
            high_issues = await page.locator("#high-issues").inner_text()
            critical_issues = await page.locator("#critical-issues").inner_text()
            
            print(f"📊 Dashboard Statistics:")
            print(f"   - Total Issues: {total_issues}")
            print(f"   - High Issues: {high_issues}")
            print(f"   - Critical Issues: {critical_issues}")
            
            if total_issues != "-" and high_issues != "-":
                print("✅ PASS: Dashboard statistics loaded properly")
            else:
                print("❌ FAIL: Dashboard statistics not loaded")
                return False
            
            # Screenshot for debugging
            await page.screenshot(path="/tmp/agent_status_test.png")
            print("📸 Screenshot saved to /tmp/agent_status_test.png")
            
            return True
            
        except Exception as e:
            print(f"❌ Test failed with error: {e}")
            await page.screenshot(path="/tmp/agent_status_error.png")
            return False
            
        finally:
            await browser.close()

async def main():
    """Run the test"""
    success = await test_agent_status_loading()
    
    if success:
        print("\n🎉 All tests passed! Agent status loading is fixed.")
        sys.exit(0)
    else:
        print("\n💥 Tests failed! Agent status loading needs more work.")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())