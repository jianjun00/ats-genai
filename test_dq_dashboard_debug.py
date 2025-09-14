#!/usr/bin/env python3
"""
Debug Data Quality Dashboard Display Issue
"""

import asyncio
from playwright.async_api import async_playwright
import requests

BASE_URL = "http://localhost:4000"

async def debug_dq_dashboard():
    print("🔍 DEBUGGING DATA QUALITY DASHBOARD")
    print("=" * 50)

    # Step 1: Verify API data
    print("📊 Step 1: API Data Check...")
    api_response = requests.get(f"{BASE_URL}/data-quality/api/issues")
    api_data = api_response.json()

    total_issues = api_data.get('summary', {}).get('total_issues', 0)
    high_issues = api_data.get('summary', {}).get('high', 0)
    print(f"   API Total Issues: {total_issues}")
    print(f"   API High Issues: {high_issues}")

    # Step 2: Load the data quality dashboard page
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = await browser.new_context()
        page = await context.new_page()

        # Capture console messages and errors
        console_messages = []
        errors = []

        page.on("console", lambda msg: console_messages.append(f"{msg.type}: {msg.text}"))
        page.on("pageerror", lambda error: errors.append(str(error)))

        try:
            print("\n🎭 Step 2: Loading Data Quality Dashboard...")

            # Go directly to data quality dashboard
            await page.goto(f"{BASE_URL}/data-quality/dashboard")
            await page.wait_for_load_state("domcontentloaded")

            print("   ✅ Page loaded successfully")

            # Wait for initialization to complete
            await page.wait_for_timeout(5000)

            print("\n📱 Step 3: Checking DOM Elements...")

            # Check if the stat elements exist
            total_elem = page.locator("#total-issues")
            critical_elem = page.locator("#critical-issues")
            high_elem = page.locator("#high-issues")
            symbols_elem = page.locator("#symbols-affected")

            total_exists = await total_elem.count() > 0
            critical_exists = await critical_elem.count() > 0
            high_exists = await high_elem.count() > 0
            symbols_exists = await symbols_elem.count() > 0

            print(f"   Total Issues Element Exists: {total_exists}")
            print(f"   Critical Issues Element Exists: {critical_exists}")
            print(f"   High Issues Element Exists: {high_exists}")
            print(f"   Symbols Affected Element Exists: {symbols_exists}")

            if total_exists:
                total_text = await total_elem.inner_text()
                print(f"   Total Issues Text: '{total_text}'")

            if high_exists:
                high_text = await high_elem.inner_text()
                print(f"   High Issues Text: '{high_text}'")

            print("\n🔍 Step 4: Check JavaScript Execution...")

            # Check if JavaScript variables are set
            js_check = await page.evaluate("""
                () => {
                    return {
                        loadData_exists: typeof loadData === 'function',
                        displayData_exists: typeof displayData === 'function',
                        currentPage: typeof currentPage !== 'undefined' ? currentPage : 'undefined',
                        dom_ready: document.readyState,
                        total_element: document.getElementById('total-issues') !== null,
                        total_text: document.getElementById('total-issues') ? document.getElementById('total-issues').textContent : 'not found'
                    }
                }
            """)

            print(f"   LoadData Function Exists: {js_check['loadData_exists']}")
            print(f"   DisplayData Function Exists: {js_check['displayData_exists']}")
            print(f"   Current Page Variable: {js_check['currentPage']}")
            print(f"   DOM Ready State: {js_check['dom_ready']}")
            print(f"   Total Element in DOM: {js_check['total_element']}")
            print(f"   Total Element Text: '{js_check['total_text']}'")

            print("\n🚀 Step 5: Manually Trigger API Call...")

            # Manually call the API from JavaScript
            api_result = await page.evaluate("""
                async () => {
                    try {
                        const response = await fetch('/data-quality/api/issues?page=1&page_size=50');
                        const data = await response.json();
                        return {
                            success: true,
                            total_issues: data.summary ? data.summary.total_issues : 'no summary',
                            high_issues: data.summary ? data.summary.high : 'no summary',
                            issues_count: data.issues ? data.issues.length : 'no issues array',
                            first_issue: data.issues && data.issues.length > 0 ? data.issues[0] : 'no first issue'
                        };
                    } catch (error) {
                        return {
                            success: false,
                            error: error.message
                        };
                    }
                }
            """)

            print(f"   API Call Success: {api_result['success']}")
            if api_result['success']:
                print(f"   API Total Issues: {api_result['total_issues']}")
                print(f"   API High Issues: {api_result['high_issues']}")
                print(f"   Issues Array Length: {api_result['issues_count']}")
            else:
                print(f"   API Call Error: {api_result['error']}")

            print("\n🔧 Step 6: Manually Call DisplayData...")

            # Manually trigger displayData with API data
            display_result = await page.evaluate("""
                async () => {
                    try {
                        const response = await fetch('/data-quality/api/issues?page=1&page_size=50');
                        const data = await response.json();

                        // Call displayData manually
                        displayData(data);

                        // Check if DOM was updated
                        const totalText = document.getElementById('total-issues').textContent;
                        const highText = document.getElementById('high-issues').textContent;

                        return {
                            success: true,
                            total_after_display: totalText,
                            high_after_display: highText,
                            api_data_summary: data.summary
                        };
                    } catch (error) {
                        return {
                            success: false,
                            error: error.message
                        };
                    }
                }
            """)

            print(f"   Manual DisplayData Success: {display_result['success']}")
            if display_result['success']:
                print(f"   Total After DisplayData: '{display_result['total_after_display']}'")
                print(f"   High After DisplayData: '{display_result['high_after_display']}'")
                print(f"   API Summary: {display_result['api_data_summary']}")
            else:
                print(f"   Manual DisplayData Error: {display_result['error']}")

            # Print console messages and errors
            print(f"\n📝 Step 7: Console Messages ({len(console_messages)}):")
            for msg in console_messages[-10:]:  # Last 10 messages
                print(f"   {msg}")

            print(f"\n❌ JavaScript Errors ({len(errors)}):")
            for error in errors:
                print(f"   {error}")

            # Final screenshot
            await page.screenshot(path="dq_dashboard_debug.png")
            print(f"\n📸 Debug screenshot saved: dq_dashboard_debug.png")

        except Exception as e:
            print(f"❌ Test error: {e}")
            await page.screenshot(path="dq_dashboard_debug_error.png")

        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug_dq_dashboard())