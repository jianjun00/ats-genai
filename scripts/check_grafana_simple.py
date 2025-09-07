#!/usr/bin/env python3
"""
Simple Grafana check using Playwright to see what's on the login page.
"""

import asyncio
from playwright.async_api import async_playwright

async def check_grafana():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            print("🔍 Connecting to Grafana...")
            await page.goto("http://localhost:4002", timeout=10000)
            await page.wait_for_load_state("networkidle")

            # Take screenshot
            await page.screenshot(path="/tmp/grafana_check.png")

            # Get page title and URL
            title = await page.title()
            url = page.url
            print(f"📍 URL: {url}")
            print(f"📄 Title: {title}")

            # Get page content
            content = await page.content()
            print(f"📄 Content length: {len(content)} characters")

            # Look for login form elements
            print("\n🔍 Looking for login elements...")
            login_selectors = [
                'input[name="user"]',
                'input[name="username"]',
                'input[name="email"]',
                'input[type="text"]',
                'input[type="email"]',
                'input[placeholder*="user"]',
                'input[placeholder*="User"]',
                '.login-form input',
                'form input[type="text"]'
            ]

            for selector in login_selectors:
                try:
                    element = await page.query_selector(selector)
                    if element:
                        placeholder = await element.get_attribute('placeholder')
                        name = await element.get_attribute('name')
                        print(f"  ✅ Found input: {selector} (name: {name}, placeholder: {placeholder})")
                except:
                    continue

            # Look for password fields
            password_selectors = [
                'input[name="password"]',
                'input[type="password"]',
                '.login-form input[type="password"]'
            ]

            for selector in password_selectors:
                try:
                    element = await page.query_selector(selector)
                    if element:
                        placeholder = await element.get_attribute('placeholder')
                        name = await element.get_attribute('name')
                        print(f"  ✅ Found password: {selector} (name: {name}, placeholder: {placeholder})")
                except:
                    continue

            # Check if already logged in
            if "dashboard" in content.lower() or "grafana" in title.lower():
                print("✅ Might already be logged in or dashboard accessible")

            # Try to access API directly
            print("\n🔍 Checking API access...")
            try:
                await page.goto("http://localhost:4002/api/health")
                await page.wait_for_load_state("networkidle")
                api_content = await page.content()
                if "ok" in api_content.lower():
                    print("✅ API health check successful")
                else:
                    print("❌ API health check failed")
            except Exception as e:
                print(f"❌ API access failed: {e}")

            # Try dashboard API
            try:
                await page.goto("http://localhost:4002/api/search?type=dash-db")
                await page.wait_for_load_state("networkidle")
                dashboard_content = await page.content()
                print(f"📊 Dashboard API response length: {len(dashboard_content)} characters")

                if "instrument" in dashboard_content.lower():
                    print("✅ Found instrument-related content in dashboard API")
                else:
                    print("❌ No instrument content found in dashboard API")

            except Exception as e:
                print(f"❌ Dashboard API access failed: {e}")

            print("\n📸 Screenshot saved to /tmp/grafana_check.png")

        except Exception as e:
            print(f"❌ Error: {e}")
            await page.screenshot(path="/tmp/grafana_error.png")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(check_grafana())