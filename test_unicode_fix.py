#!/usr/bin/env python3
"""
Test Unicode emoji fix in ATS Analytics Services
Validates that emojis display correctly and button interactions work properly.
"""

import asyncio
from playwright.async_api import async_playwright

async def test_unicode_emojis(url, service_name):
    """Test Unicode emoji rendering in analytics service."""
    print(f"\n🧪 Testing Unicode emojis in {service_name} at {url}")
    
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        # Navigate to dashboard
        await page.goto(f"{url}/eda")
        await page.wait_for_load_state('domcontentloaded')
        
        # Test static emoji elements
        print("   🔍 Testing static emoji elements...")
        
        # Check header rocket emoji
        header_text = await page.locator("h1").text_content()
        if "🚀" not in header_text:
            print("   ❌ Missing rocket emoji in header")
            return False
        print("   ✅ Rocket emoji (🚀) in header")
        
        # Check feature item emojis
        feature_items = await page.locator(".feature-item").all()
        expected_emojis = ["📊", "🌐", "⚡", "🤖", "📈"]
        found_emojis = []
        
        for item in feature_items:
            text = await item.text_content()
            for emoji in expected_emojis:
                if emoji in text:
                    found_emojis.append(emoji)
                    break
        
        if len(found_emojis) != len(expected_emojis):
            print(f"   ❌ Expected {len(expected_emojis)} feature emojis, found {len(found_emojis)}: {found_emojis}")
            return False
        print(f"   ✅ All feature emojis found: {found_emojis}")
        
        # Test button emojis
        print("   🖱️  Testing button emoji rendering...")
        buttons = await page.locator("button").all()
        button_emojis = ["📊", "🌐", "🤖", "⚡"]
        found_button_emojis = []
        
        for button in buttons:
            text = await button.text_content()
            for emoji in button_emojis:
                if emoji in text:
                    found_button_emojis.append(emoji)
                    break
        
        if len(found_button_emojis) != len(button_emojis):
            print(f"   ❌ Expected {len(button_emojis)} button emojis, found {len(found_button_emojis)}")
            return False
        print(f"   ✅ All button emojis found: {found_button_emojis}")
        
        # Test button interaction emojis
        print("   🔄 Testing button interaction emoji rendering...")
        
        # Click Universe Analytics button specifically
        universe_button = page.locator("button").filter(has_text="Universe Analytics")
        await universe_button.click()
        await page.wait_for_timeout(500)
        
        # Check that the content area now shows the correct emoji
        content_area = await page.locator("#analysis-content").text_content()
        if "🌐 Universe Analytics" not in content_area:
            print(f"   ❌ Universe Analytics emoji not found in content after click")
            print(f"   📝 Actual content: {content_area[:200]}...")
            return False
        print("   ✅ Universe Analytics emoji (🌐) displays correctly after button click")
        
        # Test another button to ensure all work
        training_button = page.locator("button").filter(has_text="Training Datasets")  
        await training_button.click()
        await page.wait_for_timeout(500)
        
        content_area = await page.locator("#analysis-content").text_content()
        if "🤖 Training Datasets" not in content_area:
            print(f"   ❌ Training Datasets emoji not found in content after click")
            return False
        print("   ✅ Training Datasets emoji (🤖) displays correctly after button click")
        
        await browser.close()
        print(f"   🎉 {service_name} Unicode emojis are working correctly!")
        return True

async def main():
    """Test Unicode fix in both services."""
    print("🔤 ATS Analytics Services Unicode Emoji Fix Validation")
    print("=" * 70)
    
    # Test both services
    dev_success = await test_unicode_emojis("http://localhost:3000", "ATS-DEV")
    intg_success = await test_unicode_emojis("http://localhost:4000", "ATS-INTG")
    
    # Summary
    print("\n" + "=" * 70)
    print("🎯 UNICODE FIX VALIDATION SUMMARY")
    print(f"   ATS-DEV Unicode: {'✅ FIXED' if dev_success else '❌ STILL BROKEN'}")  
    print(f"   ATS-INTG Unicode: {'✅ FIXED' if intg_success else '❌ STILL BROKEN'}")
    
    if dev_success and intg_success:
        print("\n🎉 Unicode emoji fix successful! All emojis display correctly.")
        print("   - Header emoji: 🚀")
        print("   - Feature emojis: 📊 🌐 ⚡ 🤖 📈")  
        print("   - Button emojis: 📊 🌐 🤖 ⚡")
        print("   - Interactive content emojis work correctly")
        return 0
    else:
        print("\n⚠️  Unicode emoji fix incomplete.")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)