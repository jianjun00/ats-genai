"""
Test function existence and script execution
"""
import pytest
import asyncio
from playwright.async_api import async_playwright


class TestFunctionExistence:
    
    @pytest.mark.asyncio  
    async def test_function_definitions(self):
        """Test which functions are actually defined in the page"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            console_messages = []
            page_errors = []
            
            def handle_console(msg):
                console_messages.append(f"[{msg.type}] {msg.text}")
            
            def handle_error(error):
                page_errors.append(f"ERROR: {str(error)}")
            
            page.on("console", handle_console)
            page.on("pageerror", handle_error)
            
            try:
                print("=== FUNCTION EXISTENCE TEST ===")
                
                # Navigate to dashboard
                await page.goto("http://localhost:4000/data-quality/dashboard", wait_until="networkidle")
                await page.wait_for_selector("#agent-status", timeout=10000)
                await page.wait_for_timeout(3000)  
                
                # Check all console messages and errors
                print(f"Console messages: {len(console_messages)}")
                for msg in console_messages:
                    print(f"  {msg}")
                    
                print(f"Page errors: {len(page_errors)}")
                for error in page_errors:
                    print(f"  {error}")
                
                # Check what functions exist in the global scope
                print("\nChecking function existence in global scope...")
                global_functions = await page.evaluate("""
                    Object.getOwnPropertyNames(window).filter(name => typeof window[name] === 'function')
                """)
                print(f"Global functions: {global_functions}")
                
                # Check specifically for our functions
                print("\nChecking specific functions...")
                function_checks = await page.evaluate("""
                    ({
                        loadData: typeof loadData,
                        loadAgentStatus: typeof loadAgentStatus,
                        startAgent: typeof startAgent,
                        stopAgent: typeof stopAgent,
                        showNotification: typeof showNotification
                    })
                """)
                print(f"Function types: {function_checks}")
                
                # Try to get the script element content
                print("\nChecking script element...")
                script_info = await page.evaluate("""
                    (() => {
                        const scripts = document.getElementsByTagName('script');
                        return {
                            count: scripts.length,
                            hasContent: scripts.length > 0 && scripts[0].textContent.length > 0,
                            firstScriptLength: scripts.length > 0 ? scripts[0].textContent.length : 0,
                            containsLoadData: scripts.length > 0 ? scripts[0].textContent.includes('loadData') : false
                        };
                    })()
                """)
                print(f"Script info: {script_info}")
                
                # Test a simple function evaluation
                print("\nTesting function evaluation...")
                try:
                    test_result = await page.evaluate("2 + 2")
                    print(f"Simple evaluation works: {test_result}")
                except Exception as e:
                    print(f"Simple evaluation failed: {e}")
                
                print("\n=== TEST COMPLETE ===")
                
            finally:
                await browser.close()


if __name__ == "__main__":
    # Run the test directly
    asyncio.run(TestFunctionExistence().test_function_definitions())