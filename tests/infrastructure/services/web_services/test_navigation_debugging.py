#!/usr/bin/env python3
"""
Playwright Test for Navigation Debugging
Tests navigation functionality and captures debug output
"""

import pytest
from playwright.async_api import async_playwright
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestNavigationDebugging:
    """Test navigation with debugging to identify data update issues."""

    BASE_URL = "http://localhost:3000"

    @pytest.mark.asyncio
    async def test_navigation_data_update_debugging(self):
        """Test navigation and capture all debugging output to identify data flow issues."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Capture console logs
            console_messages = []

            async def handle_console(msg):
                console_messages.append(f"[{msg.type}] {msg.text}")
                print(f"🖥️ BROWSER: [{msg.type}] {msg.text}")

            page.on('console', handle_console)

            # Capture network requests
            network_requests = []

            async def handle_request(request):
                if 'navigate' in request.url or 'multi-timeframe' in request.url:
                    network_requests.append(request.url)
                    print(f"🌐 REQUEST: {request.method} {request.url}")

            async def handle_response(response):
                if 'navigate' in response.url or 'multi-timeframe' in response.url:
                    print(f"🌐 RESPONSE: {response.status} {response.url}")
                    if response.status == 200:
                        try:
                            response_data = await response.json()
                            print(f"🌐 RESPONSE DATA keys: {list(response_data.keys())}")
                            if 'table_data' in response_data:
                                print(f"🌐 RESPONSE table_data count: {len(response_data['table_data'])}")
                            if 'multi_timeframe_data' in response_data:
                                print(f"🌐 RESPONSE multi_timeframe_data keys: {list(response_data['multi_timeframe_data'].keys())}")
                        except:
                            print("🌐 RESPONSE: Could not parse JSON")

            page.on('request', handle_request)
            page.on('response', handle_response)

            try:
                print("🎯 Step 1: Navigate to EDA dashboard")
                await page.goto(self.BASE_URL, wait_until='networkidle', timeout=30000)

                print("🎯 Step 2: Click Training Datasets")
                await page.click('text=Training Datasets')
                await page.wait_for_selector('#dataset-selector', timeout=15000)

                print("🎯 Step 3: Check for available datasets")
                dataset_options = await page.locator('#dataset-selector option').count()
                print(f"📊 Found {dataset_options} dataset options")

                if dataset_options <= 1:
                    print("⚠️ No datasets available - cannot test navigation")
                    return

                print("🎯 Step 4: Select first dataset")
                await page.select_option('#dataset-selector', index=1)
                await page.wait_for_timeout(2000)

                print("🎯 Step 5: Check for sequences")
                sequence_options = await page.locator('#sequence-selector option').count()
                print(f"📊 Found {sequence_options} sequence options")

                if sequence_options <= 1:
                    print("⚠️ No sequences available - cannot test navigation")
                    return

                print("🎯 Step 6: Select first sequence")
                await page.select_option('#sequence-selector', index=1)
                await page.wait_for_timeout(1000)

                print("🎯 Step 7: Load visualization")
                load_button = await page.locator('button:has-text("Load Dataset Visualization")').count()
                if load_button > 0:
                    await page.click('button:has-text("Load Dataset Visualization")')
                    await page.wait_for_timeout(5000)  # Wait for initial load
                    print("✅ Initial visualization loaded")
                else:
                    print("⚠️ Load button not found")
                    return

                print("🎯 Step 8: Check if navigation controls are visible")
                nav_controls_visible = await page.locator('#position-slider').is_visible()
                nav_buttons_visible = await page.locator('#nav-next').is_visible()

                if not (nav_controls_visible and nav_buttons_visible):
                    print("⚠️ Navigation controls not visible")
                    return

                print("✅ Navigation controls are visible")

                print("🎯 Step 9: Capture initial table data")
                initial_table_html = await page.locator('#sequence-table').inner_html()
                print(f"📋 Initial table HTML length: {len(initial_table_html)}")

                # Extract first few table cells to compare
                initial_cells = await page.locator('#sequence-table td').all_text_contents()
                print(f"📋 Initial table cells (first 6): {initial_cells[:6] if initial_cells else 'No cells'}")

                print("🎯 Step 10: Click Next button and capture changes")
                print("=" * 50)
                print("🔽 CLICKING NEXT BUTTON - WATCH FOR DEBUG OUTPUT")
                print("=" * 50)

                await page.click('#nav-next')
                await page.wait_for_timeout(3000)  # Wait for navigation to complete

                print("🎯 Step 11: Capture table data after navigation")
                after_table_html = await page.locator('#sequence-table').inner_html()
                print(f"📋 After table HTML length: {len(after_table_html)}")

                after_cells = await page.locator('#sequence-table td').all_text_contents()
                print(f"📋 After table cells (first 6): {after_cells[:6] if after_cells else 'No cells'}")

                print("🎯 Step 12: Compare table data")
                table_changed = initial_table_html != after_table_html
                cells_changed = initial_cells != after_cells

                print(f"📊 Table HTML changed: {table_changed}")
                print(f"📊 Table cells changed: {cells_changed}")

                if table_changed or cells_changed:
                    print("✅ SUCCESS: Table data updated after navigation!")
                else:
                    print("❌ ISSUE: Table data did not change after navigation")

                print("🎯 Step 13: Test multiple navigation clicks")
                for i in range(3):
                    print(f"\n🔄 Navigation test {i+1}/3")
                    await page.click('#nav-next')
                    await page.wait_for_timeout(1000)

                    position_text = await page.locator('#position-info').text_content()
                    print(f"📍 Position after click {i+1}: {position_text}")

                print("\n📊 SUMMARY OF CAPTURED DATA:")
                print(f"   Console messages: {len(console_messages)}")
                print(f"   Network requests: {len(network_requests)}")
                print(f"   Table data changed: {table_changed or cells_changed}")

                # Print relevant console messages
                nav_messages = [msg for msg in console_messages if 'NAVIGATION' in msg or 'CLIENT DEBUG' in msg]
                if nav_messages:
                    print("\n🔍 NAVIGATION DEBUG MESSAGES:")
                    for msg in nav_messages[-10:]:  # Last 10 messages
                        print(f"   {msg}")

                await page.wait_for_timeout(2000)  # Keep browser open briefly to see final state

            except Exception as e:
                print(f"❌ Test failed with error: {e}")

            finally:
                await browser.close()

if __name__ == '__main__':
    # Run with high verbosity
    pytest.main([__file__, '-v', '--tb=short', '-s'])