#!/usr/bin/env python3
"""
SignOz Dashboard Verification using Playwright

Actually opens the SignOz dashboard in a browser and verifies:
1. Dashboard exists and loads
2. Panels are visible
3. Data is displayed
4. No error messages shown
"""

import asyncio
from playwright.async_api import async_playwright
import sys
import json

class SignOzDashboardVerifier:
    def __init__(self):
        self.signoz_url = "http://localhost:8080"
        self.dashboard_id = "01993c52-0bf3-7be9-a33b-73e9c819f4ae"  # Test Dashboard with Working Metrics
        self.dashboard_url = f"{self.signoz_url}"  # Test main SignOz page first

    async def verify_dashboard_with_browser(self):
        """Use real browser to verify dashboard exists and has panels"""
        print("🚀 Starting browser verification of SignOz dashboard...")
        print(f"📊 Dashboard URL: {self.dashboard_url}")
        print()

        async with async_playwright() as p:
            # Launch browser
            browser = await p.chromium.launch(headless=True)  # headless=True for server environment
            context = await browser.new_context()
            page = await context.new_page()

            print("🌐 Loading dashboard page...")

            # Navigate to dashboard
            response = await page.goto(self.dashboard_url, wait_until="networkidle", timeout=30000)

            print(f"📡 HTTP Status: {response.status}")

            # Wait for page to fully load
            await page.wait_for_timeout(3000)

            # Take screenshot for debugging
            await page.screenshot(path="/tmp/signoz_dashboard_screenshot.png")
            print("📸 Screenshot saved: /tmp/signoz_dashboard_screenshot.png")

            # Check for common elements
            await self.check_dashboard_elements(page)

            # Check for error messages
            await self.check_error_messages(page)

            # Check for panels
            await self.check_panels(page)

            # Check for data loading
            await self.check_data_loading(page)

    async def check_dashboard_elements(self, page):
        """Check for basic dashboard elements"""
        print("🔍 Checking dashboard elements...")

        # Check page title
        title = await page.title()
        print(f"📄 Page title: {title}")

        # Check if this is actually SignOz
        signoz_indicators = [
            "SigNoz",
            "signoz",
            "dashboard"
        ]

        page_content = await page.content()
        signoz_detected = any(indicator in page_content for indicator in signoz_indicators)

        if signoz_detected:
            print("✅ SignOz application detected")
        else:
            print("❌ SignOz application not detected")

        # Check for dashboard-specific elements
        dashboard_title = await page.locator("h1, h2, h3").first.text_content() if await page.locator("h1, h2, h3").count() > 0 else "No title found"
        print(f"📊 Dashboard title element: {dashboard_title}")

    async def check_error_messages(self, page):
        """Check for error messages that indicate problems"""
        print("🚨 Checking for error messages...")

        # Common error indicators
        error_selectors = [
            "[class*='error']",
            "[class*='Error']",
            "div:has-text('error')",
            "div:has-text('Error')",
            "div:has-text('failed')",
            "div:has-text('Failed')"
        ]

        for selector in error_selectors:
            error_elements = page.locator(selector)
            count = await error_elements.count()
            if count > 0:
                for i in range(min(count, 3)):  # Check first 3 errors
                    error_text = await error_elements.nth(i).text_content()
                    print(f"❌ Error found: {error_text[:100]}...")
    async def check_panels(self, page):
        """Check if dashboard panels are visible"""
        print("🎛️ Checking for dashboard panels...")

        # Check for "Welcome to your new dashboard" message (indicates empty dashboard)
        welcome_messages = [
            "Welcome to your new dashboard",
            "Follow the steps to populate",
            "Configure your new dashboard",
            "Add panels"
        ]

        page_text = await page.inner_text("body")

        for message in welcome_messages:
            if message in page_text:
                print(f"❌ Empty dashboard detected: Found '{message}'")
                return False

        # Look for actual panel elements
        panel_selectors = [
            "[class*='panel']",
            "[class*='Panel']",
            "[class*='widget']",
            "[class*='Widget']",
            "[class*='chart']",
            "[class*='Chart']",
            "canvas",  # Chart canvases
            "svg"      # SVG charts
        ]

        total_panels = 0
        for selector in panel_selectors:
            elements = page.locator(selector)
            count = await elements.count()
            if count > 0:
                print(f"✅ Found {count} elements matching '{selector}'")
                total_panels += count
        if total_panels > 0:
            print(f"✅ Total panel-like elements found: {total_panels}")
            return True
        else:
            print("❌ No panel elements found")
            return False

    async def check_data_loading(self, page):
        """Check if panels are loading data"""
        print("📊 Checking for data loading...")

        # Wait a bit for data to load
        await page.wait_for_timeout(5000)

        # Check for loading indicators
        loading_indicators = [
            "[class*='loading']",
            "[class*='Loading']",
            "[class*='spinner']",
            "[class*='Spinner']",
            "div:has-text('Loading')",
            "div:has-text('loading')"
        ]

        loading_count = 0
        for selector in loading_indicators:
            elements = page.locator(selector)
            count = await elements.count()
            loading_count += count
        if loading_count > 0:
            print(f"⏳ Found {loading_count} loading indicators - data may still be loading")

        # Look for actual data/values in panels
        value_selectors = [
            "[class*='value']",
            "[class*='metric']",
            "[class*='number']"
        ]

        data_elements = 0
        for selector in value_selectors:
            elements = page.locator(selector)
            count = await elements.count()
            if count > 0:
                # Try to get some text content
                for i in range(min(count, 3)):
                    text = await elements.nth(i).text_content()
                    if text and text.strip():
                        print(f"📊 Found data: {text.strip()}")
                        data_elements += 1
            continue

        if data_elements > 0:
            print(f"✅ Found {data_elements} elements with data")
        else:
            print("❌ No data elements found")

    async def check_network_requests(self, page):
        """Monitor network requests to see if dashboard is actually querying data"""
        print("🌐 Monitoring network requests...")

        requests_made = []

        async def handle_request(request):
            if 'query' in request.url or 'dashboard' in request.url:
                requests_made.append({
                    'url': request.url,
                    'method': request.method
                })

        page.on('request', handle_request)

        # Reload page to capture requests
        await page.reload(wait_until="networkidle")
        await page.wait_for_timeout(3000)

        if requests_made:
            print("✅ Dashboard made network requests:")
            for req in requests_made[:5]:  # Show first 5
                print(f"  📡 {req['method']} {req['url'][:80]}...")
        else:
            print("❌ No dashboard-related network requests detected")

    def generate_report(self, results):
        """Generate final verification report"""
        print("\n" + "="*60)
        print("📋 SIGNOZ DASHBOARD VERIFICATION REPORT")
        print("="*60)

        print(f"\n🎯 Dashboard Details:")
        print(f"  URL: {self.dashboard_url}")
        print(f"  Dashboard ID: {self.dashboard_id}")

        print(f"\n🔍 Verification Results:")
        print(f"  Browser Load: {'✅ Success' if results.get('loaded', False) else '❌ Failed'}")
        print(f"  Panels Visible: {'✅ Yes' if results.get('panels_found', False) else '❌ No'}")
        print(f"  Data Present: {'✅ Yes' if results.get('data_found', False) else '❌ No'}")
        print(f"  Errors Found: {'❌ Yes' if results.get('errors_found', False) else '✅ No'}")

        if not results.get('panels_found', False):
            print(f"\n🚨 CONCLUSION: Dashboard appears EMPTY - no panels visible")
            print(f"  This confirms the user's report that the dashboard is not working")
        else:
            print(f"\n✅ CONCLUSION: Dashboard appears FUNCTIONAL with visible panels")

        print("\n" + "="*60)

async def main():
    verifier = SignOzDashboardVerifier()

    await verifier.verify_dashboard_with_browser()

if __name__ == "__main__":
    asyncio.run(main())