"""
Debug JavaScript execution and console errors
"""
import pytest
import asyncio
from playwright.async_api import async_playwright


class TestJavaScriptDebug:
    
    @pytest.mark.asyncio  
    async def test_javascript_console_and_errors(self):
        """Test JavaScript console output and error messages"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            console_messages = []
            page_errors = []
            
            def handle_console(msg):
                console_messages.append(f"[{msg.type}] {msg.text}")
            
            def handle_error(error):
                page_errors.append(str(error))
            
            page.on("console", handle_console)
            page.on("pageerror", handle_error)
            
            print("=== JAVASCRIPT DEBUG TEST ===")
            
            # Navigate to dashboard
            print("1. Navigating to dashboard...")
            await page.goto("http://localhost:4000/data-quality/dashboard", wait_until="networkidle")
            
            # Wait for the page to fully load
            print("2. Waiting for page elements...")
            await page.wait_for_selector("#agent-status", timeout=10000)
            await page.wait_for_timeout(8000)  # Give JavaScript plenty of time to execute
            
            # Print console messages
            print(f"\n3. Console messages ({len(console_messages)}):")
            for msg in console_messages:
                print(f"   {msg}")
            
            # Print page errors
            print(f"\n4. Page errors ({len(page_errors)}):")
            for error in page_errors:
                print(f"   {error}")
            
            # Check agent status element
            print("\n5. Checking agent status...")
            agent_status = await page.locator("#agent-status").inner_text()
            print(f"   Current status: '{agent_status}'")
            
            # Try to manually call loadAgentStatus
            print("\n6. Manually calling loadAgentStatus...")
            result = await page.evaluate("""
                (async () => {
                    try {
                        console.log('Manual call: Starting loadAgentStatus');
                        await loadAgentStatus();
                        console.log('Manual call: loadAgentStatus completed');
                        return 'success';
                    } catch (error) {
                        console.error('Manual call error:', error);
                        return 'error: ' + error.message;
                    }
                })()
            """)
            print(f"   Manual call result: {result}")
            await page.wait_for_timeout(3000)
            final_status = await page.locator("#agent-status").inner_text()
            print(f"   Final status: '{final_status}'")
            
            # Check if there are any network requests to /agent/status
            print("\n7. Testing direct API call from browser...")
            api_result = await page.evaluate("""
                (async () => {
                    try {
                        console.log('Direct API call: Starting fetch');
                        const response = await fetch('/agent/status');
                        const data = await response.json();
                        console.log('Direct API call: Success', data);
                        return data;
                    } catch (error) {
                        console.error('Direct API call error:', error);
                        return { error: error.message };
                    }
                })()
            """)
            print(f"   API result: {api_result}")
            
            # Final console messages after manual tests
            await page.wait_for_timeout(2000)
            print(f"\n8. Final console messages:")
            for msg in console_messages[-10:]:  # Show last 10 messages
                print(f"   {msg}")
            
            print("\n=== DEBUG TEST COMPLETE ===")
            
if __name__ == "__main__":
    # Run the test directly
    asyncio.run(TestJavaScriptDebug().test_javascript_console_and_errors())