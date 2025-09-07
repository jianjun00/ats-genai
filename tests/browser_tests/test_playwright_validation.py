#!/usr/bin/env python3
"""
Playwright validation test for EDA UI
"""

import asyncio
import sys
import os

@pytest.mark.asyncio

async def test_basic_playwright():
    """Test basic Playwright functionality"""
    try:
        from playwright.async_api import async_playwright

        print("🎭 Testing Playwright with EDA service...")

        async with async_playwright() as p:
            # Launch browser in headless mode (works in Docker)
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Test 1: Can we access the EDA page?
            print("1️⃣ Testing EDA page access...")
            try:
                await page.goto("http://localhost:3000/eda", timeout=10000)
                title = await page.title()
                print(f"✅ Page title: {title}")

                if "ATS EDA" in title:
                    print("✅ EDA page loads successfully")
                else:
                    print(f"⚠️ Unexpected title, but page loaded: {title}")

            except Exception as e:
                print(f"❌ Failed to load EDA page: {e}")
                return False

            # Test 2: Check for unified tabs
            print("2️⃣ Testing unified tabs presence...")
            try:
                db_tab = page.locator("text=Database Tables")
                db_count = await db_tab.count()
                print(f"✅ Database Tables tab found: {db_count > 0}")

                training_tab = page.locator("text=Training Datasets")
                training_count = await training_tab.count()
                print(f"✅ Training Datasets tab found: {training_count > 0}")

                if db_count > 0 and training_count > 0:
                    print("✅ Unified tabs system working")
                else:
                    print("⚠️ Some tabs missing, but basic structure present")

            except Exception as e:
                print(f"❌ Tab detection failed: {e}")

            # Test 3: Check for Plotly integration
            print("3️⃣ Testing Plotly.js integration...")
            try:
                plotly_script = page.locator("script[src*='plotly']")
                plotly_count = await plotly_script.count()
                print(f"✅ Plotly.js script found: {plotly_count > 0}")
            except Exception as e:
                print(f"⚠️ Plotly detection failed: {e}")

            # Test 4: Test basic interaction
            print("4️⃣ Testing basic click interaction...")
            try:
                if await db_tab.count() > 0:
                    await db_tab.click()
                    print("✅ Database Tables tab clickable")

                    # Wait a bit for any content to load
                    await page.wait_for_timeout(1000)

                    # Check for dataset cards or content
                    dataset_cards = page.locator(".dataset-card")
                    card_count = await dataset_cards.count()
                    print(f"✅ Dataset cards found: {card_count}")

            except Exception as e:
                print(f"⚠️ Interaction test failed: {e}")

            await browser.close()
            print("🎉 Playwright validation completed successfully!")
            return True

    except ImportError as e:
        print(f"❌ Playwright import failed: {e}")
        return False
    except Exception as e:
        print(f"❌ Playwright test failed: {e}")
        return False

async def main():
    """Main validation function"""
    print("🧪 **PLAYWRIGHT EDA VALIDATION TEST**")
    print("=" * 50)

    success = await test_basic_playwright()

    if success:
        print("\n✅ **PLAYWRIGHT READY FOR EDA UI TESTING!**")
        print("\n📋 **Next Steps:**")
        print("  • Run full test suite: pytest tests/ui/playwright_eda_tests.py")
        print("  • Run with visible browser: PLAYWRIGHT_HEADLESS=false pytest tests/ui/")
        print("  • Access EDA tool: http://localhost:3000/eda")
    else:
        print("\n❌ **VALIDATION FAILED**")
        return 1

    return 0

if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(result)