#!/usr/bin/env python3
"""
Test Universe Membership Dynamics - Verify historical entries/exits work properly
"""

import asyncio
from playwright.async_api import async_playwright

async def test_membership_dynamics():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            print("🌐 Testing Universe Membership Dynamics...")

            # Navigate and select universe
            await page.goto("http://localhost:4000/", wait_until="domcontentloaded")
            universe_button = await page.wait_for_selector('button:has-text("🌐 Universe Analytics")', timeout=5000)
            await universe_button.click()
            await page.wait_for_timeout(2000)

            # Select high volume large cap universe
            universe_selector = await page.query_selector('#universe-selector')
            await universe_selector.select_option(value="2")

            # Set date range to capture historical changes (2019-2024)
            date_from = await page.query_selector('#universe-date-from')
            date_to = await page.query_selector('#universe-date-to')
            await date_from.fill('2019-01-01')
            await date_to.fill('2024-12-31')

            # Load members
            load_button = await page.query_selector('button:has-text("Load Members")')
            await load_button.click()
            await page.wait_for_timeout(3000)

            # Check results
            members_content = await page.query_selector('#universe-members-content')
            content_text = await members_content.text_content()

            print("\n🔍 Analyzing Universe Membership Dynamics:")
            print(f"📊 Total Members Found: {content_text.count('Total Members:')} section(s)")

            # Check for active members
            if "Active Members" in content_text:
                active_count = content_text.split("Active Members (")[1].split(")")[0] if "Active Members (" in content_text else "Unknown"
                print(f"✅ Active Members: {active_count}")

            # Check for historical members
            if "Historical Members" in content_text:
                historical_count = content_text.split("Historical Members (")[1].split(")")[0] if "Historical Members (" in content_text else "Unknown"
                print(f"📉 Historical Members: {historical_count}")

            # Check for specific stocks
            stocks_to_check = {
                'AAPL': 'Apple (should be active with 1980 IPO date)',
                'TSLA': 'Tesla (should be active with 2010 IPO date)',
                'NVDA': 'NVIDIA (should be active with 1999 IPO date)',
                'PTON': 'Peloton (should be historical, exited 2022)',
                'BYND': 'Beyond Meat (should be historical, exited 2022)',
                'SMCI': 'Super Micro (should be active, AI boom addition)'
            }

            print("\n🔍 Checking Specific Stock Examples:")
            for symbol, description in stocks_to_check.items():
                if symbol in content_text:
                    print(f"   ✅ {symbol}: Found - {description}")
                else:
                    print(f"   ❌ {symbol}: Missing - {description}")

            # Check for proper date ranges
            if "1980" in content_text:
                print("   ✅ Historical IPO dates: Found entries from 1980s")
            if "2019" in content_text:
                print("   ✅ Recent entries: Found entries from 2019+")
            if "2022" in content_text:
                print("   ✅ Exit dates: Found historical exits in 2022")

            print("\n🎯 Universe Membership Dynamics Test Results:")
            has_active_members = "Active Members (665)" in content_text
            has_historical_members = "Historical Members" in content_text
            has_total_count = "670 symbols" in content_text

            if has_active_members and has_historical_members and has_total_count:
                print("✅ PASSED: Universe shows both active and historical members")
                print("✅ PASSED: Comprehensive A-Z stock coverage (665 active + 5 historical)")
                print("✅ PASSED: Historical membership tracking functional")
                print("✅ PASSED: Total membership count correct (670 total)")
                return True
            else:
                print(f"❌ FAILED: Missing features - Active:{has_active_members}, Historical:{has_historical_members}, Total:{has_total_count}")
                return False

        except Exception as e:
            print(f"❌ Test failed with error: {e}")
            return False
        finally:
            await browser.close()

if __name__ == "__main__":
    result = asyncio.run(test_membership_dynamics())
    print(f"\n🏆 Final Result: {'PASSED' if result else 'FAILED'}")
    exit(0 if result else 1)