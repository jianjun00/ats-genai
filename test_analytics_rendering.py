#!/usr/bin/env python3
"""
Test ATS Analytics Services Rendering with Playwright
Validates that both ATS-DEV and ATS-INTG analytics services are actually rendering content.
"""

import asyncio
import sys
from playwright.async_api import async_playwright

async def test_analytics_service(url, service_name):
    """Test an analytics service to ensure it renders actual content."""
    print(f"\n🧪 Testing {service_name} at {url}")
    
    try:
        async with async_playwright() as playwright:
            # Launch browser in headless mode
            browser = await playwright.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            
            # Navigate to the analytics dashboard
            print(f"   📍 Navigating to {url}/eda")
            response = await page.goto(f"{url}/eda", timeout=10000)
            
            if response.status != 200:
                print(f"   ❌ HTTP Error: {response.status}")
                return False
            
            # Wait for the page to load
            await page.wait_for_load_state('domcontentloaded')
            
            # Check for key elements that should be present
            print("   🔍 Checking page elements...")
            
            # Check page title
            title = await page.title()
            if "ATS Unified Analytics" not in title:
                print(f"   ❌ Wrong page title: {title}")
                return False
            print(f"   ✅ Page title: {title}")
            
            # Check for header content
            header = await page.locator(".header").first.text_content()
            if "ATS Unified Analytics Dashboard" not in header:
                print(f"   ❌ Missing main header")
                return False
            print("   ✅ Main header found")
            
            # Check for consolidated badge
            badge = await page.locator(".unified-badge").first.text_content()
            if "CONSOLIDATED" not in badge:
                print("   ❌ Missing consolidated badge")
                return False
            print("   ✅ Consolidated badge found")
            
            # Check for feature buttons
            feature_items = await page.locator(".feature-item").all()
            if len(feature_items) < 4:
                print(f"   ❌ Expected at least 4 feature items, found {len(feature_items)}")
                return False
            print(f"   ✅ Found {len(feature_items)} feature items")
            
            # Check for analysis buttons
            buttons = await page.locator("button").all()
            button_texts = []
            for button in buttons:
                text = await button.text_content()
                button_texts.append(text.strip())
            
            expected_keywords = ["Exploratory Data Analysis", "Universe Analytics", 
                                "Training Datasets", "Distributed Analytics"]
            
            missing_buttons = []
            for expected in expected_keywords:
                if not any(expected in btn_text for btn_text in button_texts):
                    missing_buttons.append(expected)
            
            if missing_buttons:
                print(f"   ❌ Missing buttons: {missing_buttons}")
                print(f"   📝 Actual buttons found: {button_texts}")
                return False
            print("   ✅ All expected buttons found")
            
            # Check for main content area
            main_content = await page.locator("#analysis-content").text_content()
            if "Select an analysis type above to begin" not in main_content:
                print("   ❌ Main content area not found or incorrect")
                return False
            print("   ✅ Main content area found")
            
            # Test button functionality by clicking one
            print("   🖱️  Testing button interaction...")
            eda_button = page.locator("button").filter(has_text="Exploratory Data Analysis")
            await eda_button.click()
            
            # Wait for content to update
            await page.wait_for_timeout(1000)
            
            updated_content = await page.locator("#analysis-content").text_content()
            if "Type-Aware EDA" not in updated_content:
                print("   ❌ Button click did not update content")
                return False
            print("   ✅ Button interaction working")
            
            # Take a screenshot for verification
            screenshot_path = f"/tmp/{service_name.lower().replace('-', '_')}_dashboard.png"
            await page.screenshot(path=screenshot_path)
            print(f"   📸 Screenshot saved: {screenshot_path}")
            
            await browser.close()
            print(f"   ✅ {service_name} analytics service is rendering correctly!")
            return True
            
    except Exception as e:
        print(f"   ❌ Error testing {service_name}: {e}")
        return False

async def test_health_endpoints():
    """Test health endpoints for both services."""
    print("\n🩺 Testing Health Endpoints")
    
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        # Test ATS-DEV health
        print("   Testing ATS-DEV health endpoint...")
        response = await page.goto("http://localhost:3000/health")
        if response.status == 200:
            content = await page.content()
            if "healthy" in content and "ats-unified-analytics" in content:
                print("   ✅ ATS-DEV health endpoint working")
            else:
                print("   ❌ ATS-DEV health endpoint not responding correctly")
        else:
            print(f"   ❌ ATS-DEV health endpoint returned {response.status}")
        
        # Test ATS-INTG health
        print("   Testing ATS-INTG health endpoint...")
        response = await page.goto("http://localhost:4000/health")
        if response.status == 200:
            content = await page.content()
            if "healthy" in content and "ats-unified-analytics" in content:
                print("   ✅ ATS-INTG health endpoint working")
            else:
                print("   ❌ ATS-INTG health endpoint not responding correctly")
        else:
            print(f"   ❌ ATS-INTG health endpoint returned {response.status}")
        
        await browser.close()

async def main():
    """Main test function."""
    print("🚀 ATS Analytics Services Rendering Validation")
    print("=" * 60)
    
    # Test health endpoints first
    await test_health_endpoints()
    
    # Test ATS-DEV analytics service
    dev_success = await test_analytics_service("http://localhost:3000", "ATS-DEV")
    
    # Test ATS-INTG analytics service  
    intg_success = await test_analytics_service("http://localhost:4000", "ATS-INTG")
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 VALIDATION SUMMARY")
    print(f"   ATS-DEV Analytics: {'✅ PASS' if dev_success else '❌ FAIL'}")
    print(f"   ATS-INTG Analytics: {'✅ PASS' if intg_success else '❌ FAIL'}")
    
    if dev_success and intg_success:
        print("\n🎉 Both analytics services are rendering correctly!")
        return 0
    else:
        print("\n⚠️  One or more analytics services have rendering issues.")
        return 1

if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Test failed with error: {e}")
        sys.exit(1)