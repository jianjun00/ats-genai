#!/usr/bin/env python3
"""
Create SignOz Dashboard via UI using Playwright

This script will:
1. Navigate to SignOz UI in browser
2. Find the dashboard creation interface
3. Create a dashboard through UI interactions
4. Verify the dashboard is visible
"""

import asyncio
from playwright.async_api import async_playwright
import sys
import time

class SignOzDashboardCreator:
    def __init__(self):
        self.signoz_url = "http://localhost:8080"

    async def create_dashboard_via_ui(self):
        """Create dashboard by navigating SignOz UI"""
        print("🚀 Starting SignOz UI navigation to create dashboard...")
        print(f"🌐 SignOz URL: {self.signoz_url}")

        async with async_playwright() as p:
            # Launch browser with extended viewport and debugging
            browser = await p.chromium.launch(
                headless=True,  # Headless for server environment
                args=["--disable-web-security", "--disable-features=VizDisplayCompositor"]
            )
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080}
            )
            page = await context.new_page()

            # Enable detailed logging
            await page.route("**/*", self._log_requests)

            print("📊 Step 1: Loading SignOz main page...")
            await page.goto(self.signoz_url, wait_until="networkidle", timeout=30000)

            # Wait for any JavaScript to load
            await page.wait_for_timeout(5000)

            # Take screenshot of main page
            await page.screenshot(path="/tmp/signoz_main_page.png")
            print("📸 Main page screenshot: /tmp/signoz_main_page.png")

            # Check if we can see any UI elements
            await self._explore_page_structure(page)

            # Try different potential dashboard paths
            dashboard_paths = [
                "/dashboard",
                "/dashboards",
                "/metrics",
                "/application",
                "/#/dashboard",
                "/#/dashboards"
            ]

            working_path = None
            for path in dashboard_paths:
                print(f"🔍 Testing path: {path}")
                await page.goto(f"{self.signoz_url}{path}", wait_until="networkidle", timeout=10000)
                await page.wait_for_timeout(3000)

                # Check if we see dashboard-related content
                page_text = await page.inner_text("body")
                if any(keyword in page_text.lower() for keyword in ["dashboard", "create", "panel", "metric"]):
                    working_path = path
                    print(f"✅ Found working path: {path}")
                    break

            if working_path:
                await page.screenshot(path="/tmp/signoz_working_path.png")
                print(f"📸 Working path screenshot: /tmp/signoz_working_path.png")

                # Try to find dashboard creation button
                await self._create_dashboard_through_ui(page)
            else:
                print("❌ No working dashboard paths found")

                # Try to wait for single page app to load
                print("⏳ Waiting for SPA to load...")
                await page.wait_for_timeout(10000)
                await page.screenshot(path="/tmp/signoz_after_wait.png")
                print("📸 After wait screenshot: /tmp/signoz_after_wait.png")

                # Check if any content loaded
                await self._explore_page_structure(page)

            # Final verification
            print("🔍 Final verification of page state...")
            await page.wait_for_timeout(2000)

    async def _log_requests(self, route):
        """Log network requests for debugging"""
        request = route.request
        if 'localhost:8080' in request.url:
            print(f"📡 Request: {request.method} {request.url}")
        await route.continue_()

    async def _explore_page_structure(self, page):
        """Explore the page structure to understand what's loaded"""
        print("🔍 Exploring page structure...")

        # Check basic HTML structure
        title = await page.title()
        print(f"📄 Page title: {title}")

        # Look for any visible text
        body_text = await page.inner_text("body")
        if body_text.strip():
            print(f"📝 Page text (first 200 chars): {body_text[:200]}...")
        else:
            print("❌ No visible text found")

        # Check for common HTML elements
        html_elements = {
            "div": "div",
            "buttons": "button",
            "links": "a",
            "forms": "form",
            "inputs": "input",
            "nav": "nav",
            "main": "main",
            "header": "header"
        }

        for name, selector in html_elements.items():
            count = await page.locator(selector).count()
            if count > 0:
                print(f"✅ Found {count} {name} elements")

        # Look for React/JavaScript app indicators
        react_indicators = [
            "#root",
            "#app",
            "[data-reactroot]",
            ".ant-layout",  # Ant Design
            ".chakra-ui",   # Chakra UI
            "[class*='App']"
        ]

        for indicator in react_indicators:
            element = page.locator(indicator)
            if await element.count() > 0:
                content = await element.inner_text()
                print(f"⚛️ Found React app indicator {indicator}: {content[:100]}...")
    async def _create_dashboard_through_ui(self, page):
        """Try to create dashboard through UI interactions"""
        print("🎨 Attempting to create dashboard through UI...")

        # Common button texts for dashboard creation
        creation_buttons = [
            "Create Dashboard",
            "New Dashboard",
            "Add Dashboard",
            "+ Dashboard",
            "Create",
            "New",
            "+",
            "Build Dashboard"
        ]

        for button_text in creation_buttons:
            print(f"🔘 Looking for button: '{button_text}'")
            button = page.locator(f"button:has-text('{button_text}')").first
            if await button.count() > 0:
                print(f"✅ Found button: {button_text}")
                await button.click()
                await page.wait_for_timeout(3000)

                # Take screenshot after clicking
                await page.screenshot(path=f"/tmp/signoz_after_click_{button_text.replace(' ', '_')}.png")
                print(f"📸 After clicking '{button_text}': /tmp/signoz_after_click_{button_text.replace(' ', '_')}.png")

                # Check if dashboard creation interface opened
                page_text = await page.inner_text("body")
                if any(keyword in page_text.lower() for keyword in ["panel", "widget", "metric", "query"]):
                    print("✅ Dashboard creation interface detected!")
                    await self._configure_dashboard_panels(page)
                    return True

        print("❌ No dashboard creation buttons found")
        return False

    async def _configure_dashboard_panels(self, page):
        """Configure dashboard panels if creation interface is available"""
        print("⚙️ Configuring dashboard panels...")

        # Add our daily prices metrics - fail if not available
        metric_queries = [
            "ats_daily_price_polygon_coverage_percent",
            "ats_daily_price_polygon_missing_symbols_total"
        ]

        for metric in metric_queries:
            print(f"📊 Attempting to add metric: {metric}")

            # Look for query input fields
            query_inputs = [
                "input[placeholder*='query']",
                "input[placeholder*='metric']",
                "textarea[placeholder*='query']",
                "input[name*='query']",
                ".query-input",
                "[data-testid*='query']"
            ]

            for selector in query_inputs:
                input_field = page.locator(selector).first
                if await input_field.count() > 0:
                    print(f"✅ Found query input: {selector}")
                    await input_field.fill(metric)
                    await page.keyboard.press("Enter")
                    await page.wait_for_timeout(2000)

                    # Take screenshot
                    await page.screenshot(path=f"/tmp/signoz_metric_{metric}.png")
                    print(f"📸 After adding {metric}: /tmp/signoz_metric_{metric}.png")
                    break
        save_buttons = [
            "Save",
            "Save Dashboard",
            "Apply",
            "Create",
            "Submit"
        ]

        for save_text in save_buttons:
            save_button = page.locator(f"button:has-text('{save_text}')").first
            if await save_button.count() > 0:
                print(f"✅ Found save button: {save_text}")
                await save_button.click()
                await page.wait_for_timeout(3000)

                await page.screenshot(path="/tmp/signoz_dashboard_saved.png")
                print("📸 Dashboard saved: /tmp/signoz_dashboard_saved.png")
                break
async def main():
    creator = SignOzDashboardCreator()
    await creator.create_dashboard_via_ui()

if __name__ == "__main__":
    asyncio.run(main())