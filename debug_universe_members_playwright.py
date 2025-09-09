#!/usr/bin/env python3
"""
Debug Universe Analytics member loading issue with detailed Playwright inspection
"""

import asyncio
from playwright.async_api import async_playwright

async def debug_universe_members():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)  # Run in headless mode
        page = await browser.new_page()
        
        try:
            # Enable console logging
            page.on("console", lambda msg: print(f"🖥️  Console: {msg.text}"))
            page.on("pageerror", lambda error: print(f"❌ JS Error: {error}"))
            
            print("📍 Navigating to analytics dashboard...")
            await page.goto("http://localhost:3000/", wait_until="domcontentloaded")
            
            print("🔘 Clicking Universe Analytics button...")
            universe_button = await page.wait_for_selector('button:has-text("🌐 Universe Analytics")', timeout=5000)
            await universe_button.click()
            
            print("⏳ Waiting for interface to load...")
            await page.wait_for_timeout(3000)
            
            # Check if the interface elements are present
            universe_selector = await page.query_selector('#universe-selector')
            date_from = await page.query_selector('#universe-date-from')
            date_to = await page.query_selector('#universe-date-to')
            load_button = await page.query_selector('button:has-text("Load Members")')
            members_content = await page.query_selector('#universe-members-content')
            
            print(f"🔍 Interface elements found:")
            print(f"   - Universe selector: {'✅' if universe_selector else '❌'}")
            print(f"   - Date from input: {'✅' if date_from else '❌'}")
            print(f"   - Date to input: {'✅' if date_to else '❌'}")
            print(f"   - Load button: {'✅' if load_button else '❌'}")
            print(f"   - Members content: {'✅' if members_content else '❌'}")
            
            if all([universe_selector, date_from, date_to, load_button, members_content]):
                # Check universe options
                options = await universe_selector.query_selector_all('option')
                print(f"📊 Found {len(options)} universe options")
                
                # Select first actual universe
                if len(options) > 1:
                    universe_option = options[1]
                    universe_value = await universe_option.get_attribute('value')
                    universe_text = await universe_option.inner_text()
                    
                    print(f"🎯 Selecting universe: {universe_text} (ID: {universe_value})")
                    await universe_selector.select_option(value=universe_value)
                    
                    # Check date values
                    date_from_value = await date_from.input_value()
                    date_to_value = await date_to.input_value()
                    print(f"📅 Date range: {date_from_value} to {date_to_value}")
                    
                    # Get initial members content
                    initial_content = await members_content.text_content()
                    print(f"📝 Initial members content: {initial_content[:100]}...")
                    
                    print("🔘 Clicking Load Members button...")
                    await load_button.click()
                    
                    print("⏳ Waiting for members to load...")
                    await page.wait_for_timeout(5000)  # Wait longer to see what happens
                    
                    # Get updated members content
                    final_content = await members_content.text_content()
                    print(f"📝 Final members content: {final_content[:200]}...")
                    
                    # Check for specific content that should appear
                    has_universe_info = "Universe:" in final_content
                    has_member_count = "Total Members:" in final_content
                    has_active_members = "Active Members" in final_content
                    has_loading = "Loading" in final_content
                    has_error = "Error" in final_content or "error" in final_content
                    
                    print(f"📊 Content analysis:")
                    print(f"   - Has universe info: {'✅' if has_universe_info else '❌'}")
                    print(f"   - Has member count: {'✅' if has_member_count else '❌'}")
                    print(f"   - Has active members: {'✅' if has_active_members else '❌'}")
                    print(f"   - Still loading: {'⏳' if has_loading else '✅'}")
                    print(f"   - Has error: {'❌' if has_error else '✅'}")
                    
                    # Test the API call manually from the browser
                    print("🧪 Testing API call from browser...")
                    api_result = await page.evaluate(f"""
                        (async () => {{
                            try {{
                                const response = await fetch('/api/universe-members/{universe_value}?date_from={date_from_value}&date_to={date_to_value}');
                                const data = await response.json();
                                return {{ success: true, data: data }};
                            }} catch (error) {{
                                return {{ success: false, error: error.message }};
                            }}
                        }})()
                    """)
                    
                    print(f"🌐 API test result: {api_result}")
                    
                    # Check if loadUniverseMembers function exists
                    function_exists = await page.evaluate("typeof loadUniverseMembers !== 'undefined'")
                    print(f"🔧 loadUniverseMembers function exists: {'✅' if function_exists else '❌'}")
                    
                    if not function_exists:
                        print("❌ The loadUniverseMembers function is not defined!")
                    
            else:
                print("❌ Some interface elements are missing!")
                
            print("\n⏸️  Pausing for 10 seconds for manual inspection...")
            await page.wait_for_timeout(10000)
            
        except Exception as e:
            print(f"❌ Error during debugging: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_universe_members())