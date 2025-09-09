#!/usr/bin/env python3
"""
Playwright test to verify all analytics dashboard buttons work correctly.
Tests both port 3000 (dev) and port 4000 (intg) for complete functionality.
"""

import asyncio
import sys
from playwright.async_api import async_playwright
import pytest

class AnalyticsDashboardTest:
    """Test all analytics dashboard buttons work correctly."""
    
    def __init__(self):
        self.ports = [3000, 4000]  # Test both dev and intg
        self.expected_buttons = [
            "📊 Exploratory Data Analysis",
            "📈 Bar Collection Metrics", 
            "🌐 Universe Analytics",
            "🤖 Training Datasets",
            "📰 News Events",
            "📊 Earnings Events", 
            "⚡ Gap Events",
            "🎨 Multi-Panel Trading Charts",
            "⚡ Distributed Analytics"
        ]
        self.results = {}

    async def test_button_functionality(self):
        """Test that all buttons display content and not dummy pages."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            
            for port in self.ports:
                print(f"\n🔍 Testing port {port}...")
                page = await browser.new_page()
                
                try:
                    # Navigate to analytics dashboard
                    url = f"http://localhost:{port}/"
                    print(f"   📍 Navigating to {url}")
                    
                    response = await page.goto(url, wait_until="domcontentloaded", timeout=10000)
                    if not response or response.status != 200:
                        print(f"   ❌ Port {port}: Failed to load page (status: {response.status if response else 'No response'})")
                        self.results[port] = {"status": "page_load_failed", "buttons": {}}
                        continue
                    
                    # Wait for page to load
                    await page.wait_for_selector("h1", timeout=5000)
                    
                    # Check character encoding
                    title = await page.text_content("h1")
                    if "🚀 ATS Unified Analytics Dashboard" in title:
                        print(f"   ✅ Port {port}: Character encoding correct")
                        encoding_ok = True
                    else:
                        print(f"   ❌ Port {port}: Character encoding issue - got: {title}")
                        encoding_ok = False
                    
                    # Test each button
                    button_results = {}
                    
                    for button_text in self.expected_buttons:
                        try:
                            print(f"   🔘 Testing button: {button_text}")
                            
                            # Find and click button
                            button = await page.wait_for_selector(f"button:has-text('{button_text}')", timeout=3000)
                            if not button:
                                print(f"   ❌ Button not found: {button_text}")
                                button_results[button_text] = {"status": "not_found", "content": ""}
                                continue
                                
                            await button.click()
                            
                            # Wait for content to load 
                            await page.wait_for_timeout(1000)  # Give it time to load
                            
                            # Check content area
                            content_element = await page.wait_for_selector("#analysis-content", timeout=2000)
                            content = await content_element.text_content() if content_element else ""
                            
                            # Check if it's a dummy page
                            is_dummy = any(dummy_text in content for dummy_text in [
                                "Loading ML dataset management",
                                "Loading bar collection data", 
                                "Loading news events",
                                "Loading earnings events",
                                "Loading visualization",
                                "Loading Ray distributed computing"
                            ])
                            
                            if is_dummy:
                                print(f"   ⚠️  Button shows dummy content: {button_text}")
                                button_results[button_text] = {"status": "dummy_content", "content": content[:100]}
                            else:
                                print(f"   ✅ Button works: {button_text}")
                                button_results[button_text] = {"status": "working", "content": content[:100]}
                                
                        except Exception as e:
                            print(f"   ❌ Button error: {button_text} - {e}")
                            button_results[button_text] = {"status": "error", "content": str(e)}
                    
                    self.results[port] = {
                        "status": "tested",
                        "encoding_ok": encoding_ok,
                        "buttons": button_results
                    }
                    
                except Exception as e:
                    print(f"   ❌ Port {port}: Test failed with error: {e}")
                    self.results[port] = {"status": "error", "error": str(e), "buttons": {}}
                
                finally:
                    await page.close()
            
            await browser.close()
    
    def print_results(self):
        """Print comprehensive test results."""
        print("\n" + "="*60)
        print("🧪 ANALYTICS DASHBOARD TEST RESULTS")
        print("="*60)
        
        all_working = True
        
        for port in self.ports:
            result = self.results.get(port, {})
            print(f"\n🔌 PORT {port} RESULTS:")
            print("-" * 30)
            
            if result.get("status") == "page_load_failed":
                print(f"❌ Page failed to load")
                all_working = False
                continue
            elif result.get("status") == "error":
                print(f"❌ Test error: {result.get('error', 'Unknown')}")
                all_working = False
                continue
                
            # Character encoding
            if result.get("encoding_ok"):
                print(f"✅ Character encoding: CORRECT")
            else:
                print(f"❌ Character encoding: FAILED")
                all_working = False
            
            # Button results
            buttons = result.get("buttons", {})
            working_count = sum(1 for b in buttons.values() if b.get("status") == "working")
            dummy_count = sum(1 for b in buttons.values() if b.get("status") == "dummy_content")
            error_count = sum(1 for b in buttons.values() if b.get("status") in ["error", "not_found"])
            
            print(f"📊 Button status: {working_count} working, {dummy_count} dummy, {error_count} errors")
            
            for button_text, button_result in buttons.items():
                status = button_result.get("status", "unknown")
                if status == "working":
                    print(f"   ✅ {button_text}")
                elif status == "dummy_content": 
                    print(f"   ⚠️  {button_text} (dummy content)")
                    all_working = False
                else:
                    print(f"   ❌ {button_text} ({status})")
                    all_working = False
        
        print("\n" + "="*60)
        if all_working:
            print("🎉 ALL TESTS PASSED - Dashboard fully functional!")
        else:
            print("❌ ISSUES FOUND - See details above")
        print("="*60)
        
        return all_working

async def main():
    """Run the dashboard test."""
    tester = AnalyticsDashboardTest()
    await tester.test_button_functionality()
    return tester.print_results()

if __name__ == "__main__":
    try:
        result = asyncio.run(main())
        sys.exit(0 if result else 1)
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        sys.exit(1)