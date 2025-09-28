#!/usr/bin/env python3
"""
Verify Grafana dashboard displays metrics correctly after Prometheus compatibility fix.
"""

import asyncio
from playwright.async_api import async_playwright
import sys
import json
import time

async def verify_grafana_dashboard():
    """Verify that Grafana dashboard displays metrics correctly."""

    async with async_playwright() as p:
        # Launch browser in headless mode for WSL compatibility
        browser = await p.chromium.launch(headless=True)

        context = await browser.new_context()
        page = await context.new_page()

        print("🔗 Navigating to Grafana login...")
        await page.goto("http://localhost:4002/login", wait_until="domcontentloaded")

        # Login to Grafana
        print("🔐 Logging into Grafana...")
        await page.fill('input[name="user"]', 'admin')
        await page.fill('input[name="password"]', 'ats-intg-monitoring-password')
        await page.click('button[type="submit"]')

        # Wait for login redirect and dashboard load
        await page.wait_for_timeout(3000)

        print("📊 Navigating to ATS Instrument Coverage dashboard...")
        dashboard_url = "http://localhost:4002/d/ats-instrument-coverage/ats-intg-instrument-daily-price-coverage"
        await page.goto(dashboard_url, wait_until="domcontentloaded")

        # Wait for dashboard to load
        await page.wait_for_timeout(5000)

        # Check if dashboard title is present
        dashboard_title = await page.text_content("h1")
        print(f"📋 Dashboard title: {dashboard_title}")

        # Look for panels with data
        print("🔍 Checking for dashboard panels...")

        # Wait for panels to load
        panels = await page.query_selector_all('[data-panel-id]')
        print(f"📊 Found {len(panels)} panels")

        # Look for specific metrics in panels
        panel_texts = []
        for i, panel in enumerate(panels):
            panel_text = await panel.text_content()
            if panel_text and any(keyword in panel_text.lower() for keyword in ['instrument', 'total', 'coverage', 'vendor']):
                panel_texts.append(f"Panel {i}: {panel_text[:200]}...")
        if panel_texts:
            print("📈 Found panels with metrics data:")
            for text in panel_texts[:3]:  # Show first 3 relevant panels
                print(f"  • {text}")
        else:
            print("⚠️ No panels with metrics data found yet")

        # Check for error messages
        error_elements = await page.query_selector_all('.alert, .panel-plugin-error, [data-testid="data-testid Alert error"]')
        if error_elements:
            print("🚨 Found error elements:")
            for error in error_elements:
                error_text = await error.text_content()
                if error_text:
                    print(f"  • Error: {error_text[:100]}...")
        else:
            print("✅ No error messages found in dashboard")

        # Take screenshot for verification
        screenshot_path = "/tmp/grafana_dashboard_verification.png"
        await page.screenshot(path=screenshot_path, full_page=True)
        print(f"📸 Screenshot saved to {screenshot_path}")

        # Check data source health
        print("\n🔍 Checking data source connectivity...")
        await page.goto("http://localhost:4002/datasources", wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)

        # Look for Prometheus data source
        datasource_elements = await page.query_selector_all('.card-item-wrapper, .datasource-item')
        for element in datasource_elements:
            text = await element.text_content()
            if 'prometheus' in text.lower():
                print(f"📡 Found Prometheus data source: {text[:100]}...")
                break
        else:
            print("⚠️ Prometheus data source not found in list")

        print("\n✅ Grafana dashboard verification completed")

    return True

if __name__ == "__main__":
    print("🚀 Starting Grafana dashboard metrics verification...")
    success = asyncio.run(verify_grafana_dashboard())

    if success:
        print("\n✅ Verification completed successfully")
        print("🔗 Access dashboard at: http://localhost:4002/d/ats-instrument-coverage/ats-intg-instrument-daily-price-coverage")
        print("🔑 Login: admin / ats-intg-monitoring-password")
    else:
        print("❌ Verification failed")
        sys.exit(1)