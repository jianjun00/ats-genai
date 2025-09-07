#!/usr/bin/env python3
"""
Final check for ATS-INTG Instrument Coverage Dashboard
"""

import asyncio
import json
from playwright.async_api import async_playwright

async def check_dashboard():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            print("🔐 Getting auth token from Grafana...")

            # First login to get session
            await page.goto("http://localhost:4002/login")
            await page.wait_for_load_state("networkidle")

            # Fill login form with correct selectors
            try:
                # Try different possible login selectors
                await page.fill('input[name="user"]', 'admin')
            except:
                try:
                    await page.fill('input[placeholder="email or username"]', 'admin')
                except:
                    await page.fill('input[type="text"]', 'admin')

            try:
                await page.fill('input[name="password"]', 'ats-intg-monitoring-password')
            except:
                await page.fill('input[type="password"]', 'ats-intg-monitoring-password')

            # Submit login
            try:
                await page.click('button[type="submit"]')
            except:
                await page.click('button:has-text("Log in")')

            await page.wait_for_load_state("networkidle")

            print("✅ Logged into Grafana")

            # Now check dashboard API
            await page.goto("http://localhost:4002/api/search?type=dash-db")
            await page.wait_for_load_state("networkidle")

            content = await page.content()
            print(f"📊 Dashboard API response: {content[:500]}...")

            # Parse JSON response
            try:
                # Extract JSON from HTML if needed
                if "<pre>" in content:
                    json_start = content.find('[')
                    json_end = content.rfind(']') + 1
                    json_str = content[json_start:json_end]
                else:
                    json_str = content

                dashboards = json.loads(json_str)

                print(f"\n📋 Found {len(dashboards)} dashboards:")
                for i, dashboard in enumerate(dashboards, 1):
                    title = dashboard.get('title', 'No title')
                    uid = dashboard.get('uid', 'No UID')
                    url = dashboard.get('url', 'No URL')
                    print(f"  {i}. {title} (UID: {uid}, URL: {url})")

                    if "instrument" in title.lower() or "coverage" in title.lower():
                        print(f"    ✅ Found target dashboard!")

                        # Try to access the dashboard
                        dashboard_url = f"http://localhost:4002{url}"
                        print(f"    🔗 Accessing: {dashboard_url}")

                        await page.goto(dashboard_url)
                        await page.wait_for_load_state("networkidle", timeout=10000)

                        await page.screenshot(path="/tmp/grafana_dashboard.png")
                        print(f"    📸 Screenshot saved: /tmp/grafana_dashboard.png")

                        return True

            except json.JSONDecodeError as e:
                print(f"❌ JSON decode error: {e}")
                print(f"Raw content: {content}")

        except Exception as e:
            print(f"❌ Error: {e}")
            await page.screenshot(path="/tmp/grafana_final_error.png")

        finally:
            await browser.close()

        return False

if __name__ == "__main__":
    success = asyncio.run(check_dashboard())
    if success:
        print("\n✅ Dashboard validation successful!")
    else:
        print("\n❌ Dashboard not found")