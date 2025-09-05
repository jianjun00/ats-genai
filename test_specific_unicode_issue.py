#!/usr/bin/env python3
"""Test the specific Unicode issue: "ðŸŒ Universe Analytics" should now be "🌐 Universe Analytics"""

import asyncio
from playwright.async_api import async_playwright

async def test_specific_issue():
    """Test the specific Unicode issue that was reported."""
    print("🔍 Testing Specific Unicode Issue Fix")
    print("=" * 60)
    print("Issue: 'ðŸŒ Universe Analytics' should display as '🌐 Universe Analytics'")
    
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        # Test ATS-DEV
        print(f"\n📍 Testing ATS-DEV (http://localhost:3000)")
        await page.goto("http://localhost:3000/eda")
        await page.wait_for_load_state('domcontentloaded')
        
        # Click Universe Analytics button
        universe_button = page.locator("button").filter(has_text="Universe Analytics")
        await universe_button.click()
        await page.wait_for_timeout(500)
        
        # Check the content that gets loaded
        content = await page.locator("#analysis-content").text_content()
        print(f"   Content after click: {content.strip()}")
        
        if "ðŸŒ" in content:
            print("   ❌ Still showing broken Unicode: ðŸŒ")
            print("   🔧 ISSUE NOT FIXED")
        elif "🌐" in content:
            print("   ✅ Correct Unicode emoji: 🌐")
            print("   🎉 ISSUE FIXED")
        else:
            print("   ⚠️  No emoji found in content")
        
        # Test ATS-INTG
        print(f"\n📍 Testing ATS-INTG (http://localhost:4000)")
        await page.goto("http://localhost:4000/eda")
        await page.wait_for_load_state('domcontentloaded')
        
        # Click Universe Analytics button
        universe_button = page.locator("button").filter(has_text="Universe Analytics")
        await universe_button.click()
        await page.wait_for_timeout(500)
        
        # Check the content that gets loaded
        content = await page.locator("#analysis-content").text_content()
        print(f"   Content after click: {content.strip()}")
        
        if "ðŸŒ" in content:
            print("   ❌ Still showing broken Unicode: ðŸŒ")
            print("   🔧 ISSUE NOT FIXED")
        elif "🌐" in content:
            print("   ✅ Correct Unicode emoji: 🌐")
            print("   🎉 ISSUE FIXED")
        else:
            print("   ⚠️  No emoji found in content")
        
        await browser.close()

asyncio.run(test_specific_issue())