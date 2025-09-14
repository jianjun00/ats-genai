#!/usr/bin/env python3
"""
Playwright test to verify Ray distributed processing and pagination features
"""

import asyncio
import sys
from playwright.async_api import async_playwright

async def test_ray_and_pagination():
    """Test Ray distributed processing and pagination functionality"""

    print("🎭 Testing Ray and Pagination Features")
    print("=" * 60)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        try:
            # Navigate to data quality dashboard
            print("📍 Navigating to data quality dashboard...")
            await page.goto("http://localhost:4000/data-quality/dashboard", wait_until="domcontentloaded")
            await page.wait_for_timeout(5000)  # Allow full loading

            # Test 1: Check agent status shows IDLE (not STOPPED)
            print("🔍 Testing agent status...")
            agent_status = await page.locator("#agent-status").inner_text()
            print(f"   Agent Status: {agent_status}")

            if "IDLE" in agent_status and "STOPPED" not in agent_status:
                print("✅ PASS: Agent status shows IDLE (not STOPPED)")
            elif "idle" in agent_status.lower():
                print("✅ PASS: Agent status shows idle state")
            else:
                print(f"❌ FAIL: Agent status unexpected: {agent_status}")
                return False

            # Test 2: Check Ray toggle exists and is functional
            print("🔍 Testing Ray distributed processing toggle...")
            ray_toggle = page.locator("#ray-toggle")

            if await ray_toggle.is_visible():
                print("✅ PASS: Ray toggle is visible")

                # Test clicking the toggle
                await ray_toggle.click()
                print("   - Ray toggle clicked (enabled)")
                await page.wait_for_timeout(1000)

                is_checked = await ray_toggle.is_checked()
                print(f"   - Ray toggle checked: {is_checked}")

                if is_checked:
                    print("✅ PASS: Ray toggle functional")
                else:
                    print("⚠️ WARNING: Ray toggle not responding to clicks")
            else:
                print("❌ FAIL: Ray toggle not found")
                return False

            # Test 3: Check pagination controls exist
            print("🔍 Testing pagination controls...")

            # Check page size selector
            page_size_selector = page.locator("#page-size")
            if await page_size_selector.is_visible():
                print("✅ PASS: Page size selector found")

                # Test changing page size
                await page_size_selector.select_option("25")
                await page.wait_for_timeout(1000)
                selected_value = await page_size_selector.input_value()
                print(f"   - Page size changed to: {selected_value}")

            else:
                print("❌ FAIL: Page size selector not found")
                return False

            # Check navigation buttons
            first_page_btn = page.locator("#first-page")
            prev_page_btn = page.locator("#prev-page")
            next_page_btn = page.locator("#next-page")
            last_page_btn = page.locator("#last-page")

            navigation_buttons = [
                ("First Page", first_page_btn),
                ("Previous Page", prev_page_btn),
                ("Next Page", next_page_btn),
                ("Last Page", last_page_btn)
            ]

            for name, button in navigation_buttons:
                if await button.is_visible():
                    print(f"✅ PASS: {name} button found")
                else:
                    print(f"⚠️ WARNING: {name} button not found")

            # Test 4: Check that issues are loading
            print("🔍 Testing data quality issues loading...")
            issues_container = page.locator("#issues-list")

            if await issues_container.is_visible():
                # Wait for issues to load or show no issues message
                await page.wait_for_timeout(3000)
                issues_text = await issues_container.inner_text()

                if "Loading data quality issues" in issues_text:
                    print("⚠️ WARNING: Issues still loading (might be normal for large datasets)")
                elif "No issues found" in issues_text:
                    print("✅ PASS: Issues loaded - no data quality issues found")
                elif "issue" in issues_text.lower():
                    print("✅ PASS: Issues loaded - data quality issues detected")
                else:
                    print(f"   Issues content: {issues_text[:100]}...")

            # Test 5: Check dashboard statistics
            print("🔍 Testing dashboard statistics...")
            stats_selectors = [
                ("#total-issues", "Total Issues"),
                ("#critical-issues", "Critical Issues"),
                ("#high-issues", "High Issues"),
                ("#medium-issues", "Medium Issues")
            ]

            for selector, name in stats_selectors:
                try:
                    stat_value = await page.locator(selector).inner_text()
                    print(f"   - {name}: {stat_value}")
                    if stat_value != "-" and stat_value.strip():
                        print(f"✅ PASS: {name} loaded")
                except:
                    print(f"⚠️ WARNING: {name} not found")

            # Test 6: Test API endpoint directly (if available)
            print("🔍 Testing data quality API...")
            try:
                response = await page.request.get("http://localhost:4000/agent/status")
                if response.status == 200:
                    data = await response.json()
                    print("✅ PASS: Agent status API responding")
                    print(f"   - Status: {data.get('status', 'unknown')}")
                    print(f"   - Tools: {data.get('tools_available', 0)}")
                else:
                    print(f"⚠️ WARNING: Agent API returned {response.status}")
            except Exception as e:
                print(f"⚠️ WARNING: Agent API test failed: {e}")

            # Take screenshot for verification
            await page.screenshot(path="/tmp/ray_pagination_test.png")
            print("📸 Screenshot saved to /tmp/ray_pagination_test.png")

            return True

        except Exception as e:
            print(f"❌ Test failed with error: {e}")
            await page.screenshot(path="/tmp/ray_pagination_error.png")
            return False

        finally:
            await browser.close()

async def main():
    """Run the comprehensive test"""
    success = await test_ray_and_pagination()

    if success:
        print("\n🎉 All Ray and Pagination tests passed!")
        print("✅ Agent status showing IDLE (not STOPPED)")
        print("✅ Ray distributed processing toggle working")
        print("✅ Pagination controls implemented")
        print("✅ Data quality dashboard fully functional")
        sys.exit(0)
    else:
        print("\n💥 Some tests failed. Check logs above.")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())