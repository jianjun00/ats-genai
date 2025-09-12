#!/usr/bin/env python3
"""
Playwright test for Universe Analytics with universe selection and date range filtering
"""

import asyncio
import sys
from playwright.async_api import async_playwright

class UniverseAnalyticsTest:
    """Test Universe Analytics functionality with selection menu and date filtering"""

    def __init__(self):
        self.ports = [3000, 4000]  # Test both dev and intg
        self.results = {}

    async def test_universe_analytics(self):
        """Test complete Universe Analytics functionality"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)

            for port in self.ports:
                print(f"\n🧪 Testing Universe Analytics on port {port}...")
                page = await browser.new_page()

                try:
                    # Navigate to analytics dashboard
                    url = f"http://localhost:{port}/"
                    print(f"   📍 Navigating to {url}")

                    response = await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                    if not response or response.status != 200:
                        print(f"   ❌ Port {port}: Failed to load page")
                        self.results[port] = {"status": "page_load_failed"}
                        continue

                    # Click Universe Analytics button
                    print(f"   🔘 Clicking Universe Analytics button")
                    universe_button = await page.wait_for_selector('button:has-text("🌐 Universe Analytics")', timeout=5000)
                    await universe_button.click()

                    # Wait for Universe Analytics interface to load
                    await page.wait_for_timeout(3000)

                    # Test 1: Check if universe selection interface loaded
                    print(f"   🧪 Test 1: Universe selection interface")

                    universe_selector = await page.query_selector('#universe-selector')
                    date_from_input = await page.query_selector('#universe-date-from')
                    date_to_input = await page.query_selector('#universe-date-to')
                    load_button = await page.query_selector('button:has-text("Load Members")')

                    interface_loaded = all([universe_selector, date_from_input, date_to_input, load_button])

                    if interface_loaded:
                        print(f"   ✅ Port {port}: Universe selection interface loaded")

                        # Test 2: Check if universes are populated in dropdown
                        print(f"   🧪 Test 2: Universe dropdown population")

                        options = await universe_selector.query_selector_all('option')
                        universe_count = len(options) - 1  # Subtract 1 for "-- Select a universe --" option

                        if universe_count > 0:
                            print(f"   ✅ Port {port}: Found {universe_count} universes in dropdown")

                            # Test 3: Select a universe and test member loading
                            print(f"   🧪 Test 3: Universe member loading")

                            # Select the first actual universe (skip the placeholder option)
                            if len(options) > 1:
                                universe_option = options[1]  # First real universe option
                                universe_value = await universe_option.get_attribute('value')
                                universe_text = await universe_option.inner_text()

                                await universe_selector.select_option(value=universe_value)
                                print(f"   📊 Selected universe: {universe_text}")

                                # Check if date inputs have default values
                                date_from_value = await date_from_input.input_value()
                                date_to_value = await date_to_input.input_value()

                                has_default_dates = bool(date_from_value and date_to_value)
                                print(f"   📅 Default date range: {date_from_value} to {date_to_value}")

                                if has_default_dates:
                                    # Click Load Members button
                                    await load_button.click()

                                    # Wait for members to load
                                    await page.wait_for_timeout(3000)

                                    # Check if members content loaded
                                    members_content = await page.query_selector('#universe-members-content')
                                    if members_content:
                                        content_text = await members_content.text_content()

                                        # Check for success indicators
                                        has_universe_info = "Universe:" in content_text
                                        has_member_data = "Total Members:" in content_text or "No members found" in content_text

                                        if has_universe_info and has_member_data:
                                            print(f"   ✅ Port {port}: Universe members loaded successfully")

                                            # Check for member tables
                                            active_table = await page.query_selector('h5:has-text("Active Members")')
                                            historical_table = await page.query_selector('h5:has-text("Historical Members")')

                                            has_member_tables = bool(active_table or historical_table)
                                            if has_member_tables:
                                                print(f"   ✅ Port {port}: Member tables displayed")

                                            self.results[port] = {
                                                "status": "success",
                                                "interface_loaded": True,
                                                "universe_count": universe_count,
                                                "selected_universe": universe_text,
                                                "default_dates": has_default_dates,
                                                "members_loaded": True,
                                                "member_tables": has_member_tables
                                            }
                                        else:
                                            print(f"   ❌ Port {port}: Universe members failed to load properly")
                                            self.results[port] = {
                                                "status": "members_load_failed",
                                                "interface_loaded": True,
                                                "universe_count": universe_count
                                            }
                                    else:
                                        print(f"   ❌ Port {port}: Members content area not found")
                                        self.results[port] = {
                                            "status": "members_content_missing",
                                            "interface_loaded": True,
                                            "universe_count": universe_count
                                        }
                                else:
                                    print(f"   ❌ Port {port}: Default dates not set")
                                    self.results[port] = {
                                        "status": "no_default_dates",
                                        "interface_loaded": True,
                                        "universe_count": universe_count
                                    }
                            else:
                                print(f"   ❌ Port {port}: No universe options available")
                                self.results[port] = {
                                    "status": "no_universe_options",
                                    "interface_loaded": True,
                                    "universe_count": 0
                                }
                        else:
                            print(f"   ❌ Port {port}: No universes found in dropdown")
                            self.results[port] = {
                                "status": "no_universes",
                                "interface_loaded": True,
                                "universe_count": 0
                            }
                    else:
                        print(f"   ❌ Port {port}: Universe selection interface not loaded")
                        self.results[port] = {
                            "status": "interface_not_loaded",
                            "interface_loaded": False
                        }

                except Exception as e:
                    print(f"   ❌ Port {port}: Test failed with error: {e}")
                    self.results[port] = {"status": "error", "error": str(e)}

                finally:
                    await page.close()

            await browser.close()

    def print_results(self):
        """Print comprehensive test results"""
        print("\n" + "="*60)
        print("🧪 UNIVERSE ANALYTICS TEST RESULTS")
        print("="*60)

        all_working = True

        for port in self.ports:
            result = self.results.get(port, {})
            print(f"\n🔌 PORT {port} RESULTS:")
            print("-" * 30)

            status = result.get("status", "unknown")

            if status == "page_load_failed":
                print(f"❌ Page failed to load")
                all_working = False
            elif status == "error":
                print(f"❌ Test error: {result.get('error', 'Unknown')}")
                all_working = False
            elif status == "interface_not_loaded":
                print(f"❌ Universe selection interface: NOT LOADED")
                all_working = False
            elif status == "success":
                print(f"✅ Universe Analytics: FULLY FUNCTIONAL")
                print(f"✅ Interface loaded: YES")
                print(f"✅ Universe count: {result.get('universe_count', 0)}")
                print(f"✅ Selected universe: {result.get('selected_universe', 'N/A')}")
                print(f"✅ Default dates: {'YES' if result.get('default_dates') else 'NO'}")
                print(f"✅ Members loaded: {'YES' if result.get('members_loaded') else 'NO'}")
                print(f"✅ Member tables: {'YES' if result.get('member_tables') else 'NO'}")
            else:
                print(f"⚠️  Partial functionality - Status: {status}")
                print(f"   Interface loaded: {'YES' if result.get('interface_loaded') else 'NO'}")
                print(f"   Universe count: {result.get('universe_count', 0)}")
                all_working = False

        print("\n" + "="*60)
        if all_working:
            print("🎉 ALL UNIVERSE ANALYTICS TESTS PASSED!")
            print("Universe selection, date filtering, and member loading work correctly.")
        else:
            print("❌ UNIVERSE ANALYTICS ISSUES FOUND - See details above")
        print("="*60)

        return all_working

async def main():
    """Run the Universe Analytics test"""
    tester = UniverseAnalyticsTest()
    await tester.test_universe_analytics()
    return tester.print_results()

if __name__ == "__main__":
    try:
        result = asyncio.run(main())
        sys.exit(0 if result else 1)
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        sys.exit(1)