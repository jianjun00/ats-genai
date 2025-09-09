#!/usr/bin/env python3
"""
Debug Universe Analytics on integration environment
"""

import asyncio
from playwright.async_api import async_playwright

async def debug_intg_universe_analytics():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            # Enable console logging
            page.on("console", lambda msg: print(f"🖥️  Console: {msg.text}"))
            page.on("pageerror", lambda error: print(f"❌ JS Error: {error}"))
            
            print("📍 Navigating to integration analytics dashboard...")
            await page.goto("http://localhost:4000/", wait_until="domcontentloaded")
            
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
            
            # Get the full content to see what's actually there
            content = await page.content()
            if "Universe Analytics" in content:
                print("✅ Universe Analytics section found in page")
                # Look for any error messages
                if "Error" in content:
                    print(f"❌ Found error in page content")
            else:
                print("❌ Universe Analytics section not found")
            
            # Test the universes API directly
            print("🧪 Testing universes API...")
            api_result = await page.evaluate("""
                (async () => {
                    try {
                        const response = await fetch('/api/universes');
                        const data = await response.json();
                        return { success: true, data: data };
                    } catch (error) {
                        return { success: false, error: error.message };
                    }
                })()
            """)
            
            print(f"🌐 Universes API result: {api_result}")
            
            # Check if loadUniverseAnalytics function exists and works
            function_exists = await page.evaluate("typeof loadUniverseAnalytics !== 'undefined'")
            print(f"🔧 loadUniverseAnalytics function exists: {'✅' if function_exists else '❌'}")
            
            if not function_exists:
                print("❌ The loadUniverseAnalytics function is not defined!")
            
        except Exception as e:
            print(f"❌ Error during debugging: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_intg_universe_analytics())