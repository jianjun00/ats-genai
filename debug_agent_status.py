#!/usr/bin/env python3
"""
Debug script to check agent status loading issue
"""

import asyncio
import json
from playwright.async_api import async_playwright

async def debug_agent_status():
    """Debug the agent status loading issue"""
    
    print("🔍 Debugging Agent Status Loading Issue")
    print("=" * 50)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        # Enable console logging
        page.on("console", lambda msg: print(f"🖥️ CONSOLE {msg.type}: {msg.text}"))
        page.on("pageerror", lambda error: print(f"💥 PAGE ERROR: {error.message}"))
        
        try:
            print("📍 Navigating to dashboard...")
            await page.goto("http://localhost:4000/data-quality/dashboard")
            
            print("⏱️ Waiting for DOM content loaded...")
            await page.wait_for_load_state("domcontentloaded")
            
            # Wait a bit for JavaScript to execute
            await page.wait_for_timeout(5000)
            
            # Check if the function exists
            print("🔍 Checking if loadAgentStatus function exists...")
            load_agent_exists = await page.evaluate("typeof loadAgentStatus === 'function'")
            print(f"   loadAgentStatus function exists: {load_agent_exists}")
            
            # Check current agent status
            agent_status_text = await page.locator("#agent-status").inner_text()
            print(f"📊 Current agent status: {agent_status_text}")
            
            # Manually call the function if it exists
            if load_agent_exists:
                print("🔄 Manually calling loadAgentStatus()...")
                try:
                    await page.evaluate("loadAgentStatus()")
                    await page.wait_for_timeout(3000)
                    
                    new_status = await page.locator("#agent-status").inner_text()
                    print(f"📊 Status after manual call: {new_status}")
                except Exception as e:
                    print(f"❌ Error calling loadAgentStatus(): {e}")
            
            # Test the API endpoint directly
            print("🔍 Testing /agent/status endpoint directly...")
            response = await page.request.get("http://localhost:4000/agent/status")
            print(f"   Status Code: {response.status}")
            if response.status == 200:
                data = await response.json()
                print(f"   Response: {json.dumps(data, indent=2)}")
            else:
                print(f"   Error: {await response.text()}")
            
            # Check network requests
            print("🔍 Monitoring network requests...")
            requests = []
            
            def log_request(request):
                if "agent/status" in request.url:
                    requests.append(request.url)
                    print(f"📡 Network request to: {request.url}")
            
            page.on("request", log_request)
            
            # Reload the page to see requests
            await page.reload(wait_until="domcontentloaded")
            await page.wait_for_timeout(5000)
            
            print(f"📡 Total agent/status requests captured: {len(requests)}")
            
            final_status = await page.locator("#agent-status").inner_text()
            print(f"📊 Final agent status: {final_status}")
            
        except Exception as e:
            print(f"💥 Debug failed: {e}")
            
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_agent_status())