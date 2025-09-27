"""
Debug data quality dashboard start button issue with comprehensive logging
"""
import pytest
import asyncio
from playwright.async_api import async_playwright
from pathlib import Path
import sys

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent.parent))


class TestDataQualityDebug:
    
    @pytest.mark.asyncio  
    async def test_comprehensive_start_button_debug(self):
        """Comprehensive debug test for the start button functionality"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            # Track everything
            requests = []
            responses = []
            console_logs = []
            
            def handle_request(request):
                requests.append({
                    'url': request.url,
                    'method': request.method,
                    'headers': dict(request.headers)
                })
            
            def handle_response(response):
                responses.append({
                    'url': response.url,
                    'status': response.status,
                    'method': response.request.method
                })
            
            def handle_console(msg):
                console_logs.append(f"{msg.type}: {msg.text}")
            
            page.on("request", handle_request)
            page.on("response", handle_response)
            page.on("console", handle_console)
            
            print("=== STARTING COMPREHENSIVE DEBUG TEST ===")
            
            # Navigate to dashboard
            print("\n1. Navigating to dashboard...")
            await page.goto("http://localhost:4000/data-quality/dashboard", wait_until="networkidle")
            
            # Wait for page to fully load
            print("2. Waiting for page elements...")
            await page.wait_for_selector("#agent-status", timeout=10000)
            await page.wait_for_timeout(3000)  # Give JS time to execute
            
            # Check console logs for errors
            print(f"\n3. Console logs ({len(console_logs)}):")
            for log in console_logs[-10:]:  # Show last 10 logs
                print(f"  {log}")
            
            # Check if critical functions exist
            print("\n4. Testing JavaScript function availability...")
            
            # Test loadAgentStatus
            result = await page.evaluate("typeof loadAgentStatus")
            print(f"✅ loadAgentStatus type: {result}")
            
            if result == "function":
                print("5. Calling loadAgentStatus()...")
                await page.evaluate("loadAgentStatus()")
                await page.wait_for_timeout(2000)
                print("✅ loadAgentStatus() executed")
            
            result = await page.evaluate("typeof startAgent")
            print(f"✅ startAgent type: {result}")
            
            if result == "function":
                print("6. Calling startAgent() directly...")
                await page.evaluate("startAgent()")
                await page.wait_for_timeout(2000)
                print("✅ startAgent() executed")
            
            agent_requests = [r for r in requests if '/agent' in r['url']]
            print(f"\n7. Agent-related requests after function calls ({len(agent_requests)}):")
            for req in agent_requests:
                print(f"  - {req['method']} {req['url']}")
            
            # Check current agent status
            print("\n8. Checking agent status element...")
            agent_status = await page.locator("#agent-status").inner_text()
            print(f"Current agent status text: '{agent_status}'")
            
            # Check button visibility and properties
            print("\n9. Analyzing start button...")
            start_button = page.locator("#start-agent-btn")
            is_visible = await start_button.is_visible()
            is_enabled = await start_button.is_enabled()
            print(f"Start button - Visible: {is_visible}, Enabled: {is_enabled}")
            
            if is_visible:
                # Get button onclick handler
                onclick = await start_button.get_attribute("onclick")
                print(f"Button onclick attribute: {onclick}")
                
                print("\n10. Testing actual button click...")
                
                # Clear previous requests
                requests.clear()
                responses.clear()
                
                # Click the button
                await start_button.click()
                print("Button clicked!")
                
                # Wait and check for network activity
                await page.wait_for_timeout(3000)
                
                # Check for new requests
                new_requests = [r for r in requests if '/agent' in r['url']]
                print(f"Requests after button click ({len(new_requests)}):")
                for req in new_requests:
                    print(f"  - {req['method']} {req['url']}")
                
                # Check for new responses
                new_responses = [r for r in responses if '/agent' in r['url']]
                print(f"Responses after button click ({len(new_responses)}):")
                for resp in new_responses:
                    print(f"  - {resp['method']} {resp['url']} → {resp['status']}")
                
                # Check if agent status changed
                new_agent_status = await page.locator("#agent-status").inner_text()
                print(f"Agent status after click: '{new_agent_status}'")
                
                # Check if stop button appeared
                stop_button = page.locator("#stop-agent-btn")
                stop_visible = await stop_button.is_visible()
                print(f"Stop button now visible: {stop_visible}")
            
            # Final console logs check
            print(f"\n11. Final console logs ({len(console_logs)}):")
            for log in console_logs[-5:]:  # Show last 5 logs
                print(f"  {log}")
            
            # Test the agent status endpoint directly via JavaScript
            print("\n12. Testing agent endpoints via JavaScript fetch...")
            # Test agent status endpoint
            status_result = await page.evaluate("""
                fetch('/agent/status')
                    .then(response => response.json())
                    .then(data => data)
                    .catch(error => ({ error: error.message }))
            """)
            print(f"Agent status endpoint result: {status_result}")
            
            # Test agent start endpoint
            start_result = await page.evaluate("""
                fetch('/agent/start', { method: 'POST' })
                    .then(response => response.json())
                    .then(data => data)
                    .catch(error => ({ error: error.message }))
            """)
            print(f"Agent start endpoint result: {start_result}")
            
            print("\n=== DEBUG TEST COMPLETE ===")
            
if __name__ == "__main__":
    # Run the test directly
    asyncio.run(TestDataQualityDebug().test_comprehensive_start_button_debug())