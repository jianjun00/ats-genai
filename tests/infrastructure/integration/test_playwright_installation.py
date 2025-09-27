#!/usr/bin/env python3
"""
Test Playwright installation in Docker environment.
"""

def test_playwright():
    """Test Playwright functionality."""
    print("🚀 Testing Playwright installation...")

    import playwright
    version = playwright.__version__
    print(f"✅ Playwright imported successfully (version: {version})")

    from playwright.sync_api import sync_playwright
    print("✅ Playwright sync API imported")

    with sync_playwright() as p:
        print("✅ Playwright context started")

        # Test Chromium
        browser = p.chromium.launch(headless=True)
        print("✅ Chromium browser launched")

        page = browser.new_page()
        print("✅ New page created")

        page.goto("https://example.com")
        title = page.title()
        print(f"✅ Page loaded successfully - Title: '{title}'")

        # Take a screenshot to verify rendering works
        screenshot = page.screenshot()
        print(f"✅ Screenshot taken ({len(screenshot)} bytes)")

        browser.close()
        print("✅ Browser closed")

        print("\n🎉 Playwright is fully functional!")
        print("   - Browser automation: ✅")
        print("   - Page navigation: ✅")
        print("   - Screenshot capture: ✅")
        return True

    print(f"❌ Playwright not available: {e}")
    return False
if __name__ == "__main__":
    success = test_playwright()
    if success:
        print("\n✅ SUCCESS: Playwright is ready for use!")
    else:
        print("\n❌ FAILURE: Playwright needs attention")
    exit(0 if success else 1)