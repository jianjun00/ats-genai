#!/usr/bin/env python3
"""
Debug SignOz Console Errors using Playwright

This script will capture JavaScript console errors to understand why SignOz UI isn't rendering.
"""

import asyncio
from playwright.async_api import async_playwright
import sys

class SignOzDebugger:
    def __init__(self):
        self.signoz_url = "http://localhost:8080"
        self.console_logs = []
        self.errors = []

    async def debug_signoz_console(self):
        """Capture console errors and logs from SignOz"""
        print("🔍 Starting SignOz console debugging...")

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            # Capture console messages
            page.on("console", self._handle_console)
            page.on("pageerror", self._handle_page_error)
            page.on("requestfailed", self._handle_request_failed)

            print(f"📊 Loading SignOz at {self.signoz_url}...")
            await page.goto(self.signoz_url, wait_until="networkidle", timeout=30000)

            # Wait for any async JavaScript to execute
            print("⏳ Waiting for JavaScript execution...")
            await page.wait_for_timeout(10000)

            # Try to evaluate some basic JavaScript
            print("🔧 Testing JavaScript execution...")
            dom_ready = await page.evaluate("document.readyState")
            print(f"📄 Document ready state: {dom_ready}")

            react_root = await page.evaluate("document.querySelector('#root')")
            if react_root:
                print("⚛️ React root element found")
                root_html = await page.evaluate("document.querySelector('#root').innerHTML")
                print(f"📝 Root element HTML length: {len(root_html)} chars")
                if root_html.strip():
                    print(f"📝 Root content preview: {root_html[:200]}...")
                else:
                    print("❌ Root element is empty")
            else:
                print("❌ React root element not found")

            # Check for React DevTools
            react_version = await page.evaluate("window.React ? window.React.version : 'Not found'")
            print(f"⚛️ React version: {react_version}")

            print("🔍 Looking for SignOz-specific elements...")
            signoz_elements = [
                ".ant-layout",
                "[class*='Layout']",
                "[class*='App']",
                "[data-testid]",
                "nav",
                "header",
                "main"
            ]

            for selector in signoz_elements:
                count = await page.locator(selector).count()
                if count > 0:
                    print(f"✅ Found {count} elements matching {selector}")
                    # Try to get text content
                    text = await page.locator(selector).first.inner_text()
                    if text.strip():
                        print(f"  📝 Text content: {text[:100]}...")
                else:
                    print(f"❌ No elements found for {selector}")
            await page.screenshot(path="/tmp/signoz_debug.png")
            print("📸 Debug screenshot: /tmp/signoz_debug.png")

        self._generate_debug_report()

    async def _handle_console(self, msg):
        """Handle console messages"""
        log_entry = {
            'type': msg.type,
            'text': msg.text,
            'location': f"{msg.location.get('url', '')}:{msg.location.get('lineNumber', '')}"
        }
        self.console_logs.append(log_entry)
        print(f"📊 Console {msg.type.upper()}: {msg.text}")
        if msg.location.get('url'):
            print(f"  📍 At: {msg.location['url']}:{msg.location.get('lineNumber', '?')}")

    async def _handle_page_error(self, error):
        """Handle page errors"""
        self.errors.append(str(error))
        print(f"🚨 Page Error: {error}")

    async def _handle_request_failed(self, request):
        """Handle failed requests"""
        print(f"🌐 Request Failed: {request.method} {request.url}")
        print(f"  💔 Failure: {request.failure}")

    def _generate_debug_report(self):
        """Generate comprehensive debug report"""
        print("\n" + "="*60)
        print("🔍 SIGNOZ DEBUG REPORT")
        print("="*60)

        print(f"\n📊 Console Logs: {len(self.console_logs)} total")
        for log in self.console_logs[-10:]:  # Show last 10
            print(f"  {log['type'].upper()}: {log['text']}")
            if log['location']:
                print(f"    📍 {log['location']}")

        print(f"\n🚨 Page Errors: {len(self.errors)} total")
        for error in self.errors:
            print(f"  ❌ {error}")

        if not self.console_logs and not self.errors:
            print("\n✅ No JavaScript errors detected")
            print("❓ Issue may be related to CSS rendering or network delays")

        print("\n" + "="*60)

async def main():
    debugger = SignOzDebugger()
    await debugger.debug_signoz_console()

if __name__ == "__main__":
    asyncio.run(main())