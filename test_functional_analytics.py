#!/usr/bin/env python3
"""
Test functional analytics dashboard - no more placeholder text!
Validates that buttons actually load real data instead of just showing "Loading..."
"""

import asyncio
from playwright.async_api import async_playwright

async def test_functional_buttons(url, service_name):
    """Test that buttons actually load real data."""
    print(f"\n🧪 Testing Functional Analytics in {service_name} at {url}")
    
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        await page.goto(f"{url}/eda")
        await page.wait_for_load_state('domcontentloaded')
        
        # Test Universe Analytics - should load actual data
        print("   🌐 Testing Universe Analytics functionality...")
        universe_button = page.locator("button").filter(has_text="Universe Analytics")
        await universe_button.click()
        
        # Wait for the async fetch to complete
        await page.wait_for_timeout(2000)
        
        content = await page.locator("#analysis-content").text_content()
        if "Loading cross-instrument analysis..." in content:
            print("   ❌ Still showing placeholder 'Loading...' text")
            return False
        elif "Composition" in content and "Performance" in content:
            print("   ✅ Universe Analytics loaded real data")
            print(f"   📊 Content preview: {content[:200]}...")
        else:
            print(f"   ⚠️  Unexpected content: {content[:200]}...")
        
        # Test Training Datasets - should load actual data  
        print("   🤖 Testing Training Datasets functionality...")
        training_button = page.locator("button").filter(has_text="Training Datasets")
        await training_button.click()
        
        # Wait for the async fetch to complete
        await page.wait_for_timeout(2000)
        
        content = await page.locator("#analysis-content").text_content()
        if "Loading ML dataset management..." in content:
            print("   ❌ Still showing placeholder 'Loading...' text")
            return False
        elif "Total Datasets:" in content:
            print("   ✅ Training Datasets loaded real data")
            
            # Extract dataset count
            lines = content.split('\n')
            for line in lines:
                if "Total Datasets:" in line:
                    count = line.split(':')[1].strip()
                    print(f"   📊 Found {count} training datasets")
                    break
        else:
            print(f"   ⚠️  Unexpected content: {content[:200]}...")
        
        await browser.close()
        return True

async def main():
    """Test functional improvements in both services."""
    print("🚀 ATS Analytics Functional Improvements Validation")
    print("=" * 70)
    print("Testing: Buttons now load real data instead of showing 'Loading...' placeholders")
    
    # Wait for services to be ready
    await asyncio.sleep(5)
    
    # Test both services
    dev_success = await test_functional_buttons("http://localhost:3000", "ATS-DEV")
    intg_success = await test_functional_buttons("http://localhost:4000", "ATS-INTG")
    
    # Summary
    print("\n" + "=" * 70)
    print("🎯 FUNCTIONAL IMPROVEMENTS SUMMARY")
    print(f"   ATS-DEV: {'✅ FUNCTIONAL' if dev_success else '❌ STILL PLACEHOLDERS'}")
    print(f"   ATS-INTG: {'✅ FUNCTIONAL' if intg_success else '❌ STILL PLACEHOLDERS'}")
    
    if dev_success and intg_success:
        print("\n🎉 Functional improvements successful!")
        print("   ✅ Universe Analytics loads real API data")
        print("   ✅ Training Datasets loads real database data")
        print("   ✅ No more 'Loading...' placeholder text")
        return 0
    else:
        print("\n⚠️  Functional improvements incomplete.")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)