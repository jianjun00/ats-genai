#!/usr/bin/env python3
"""
Complete Navigation Flow Test
Test the entire workflow: load dataset -> select sequence -> navigate
"""

from playwright.sync_api import sync_playwright
import time

def test_complete_navigation():
    """Test complete navigation workflow."""
    print("🎯 Complete Navigation Flow Test")
    print("=" * 50)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Capture console messages
        def handle_console(msg):
            if 'DEBUG' in msg.text or 'NAVIGATION' in msg.text:
                print(f"🖥️  {msg.text}")

        page.on('console', handle_console)

        try:
            print("🎯 Step 1: Open dashboard and click Training Datasets")
            page.goto("http://localhost:3000", timeout=30000)
            page.click('button:has-text("Training Datasets")')
            time.sleep(3)

            print("🎯 Step 2: Select dataset 65 (with 2 sequences)")
            page.select_option("#dataset-selector", "65")
            time.sleep(2)

            print("🎯 Step 3: Check sequences loaded")
            sequence_count = page.locator("#sequence-selector option").count()
            print(f"📊 Sequences available: {sequence_count}")

            if sequence_count > 1:
                print("🎯 Step 4: Select first sequence")
                page.select_option("#sequence-selector", index=1)
                time.sleep(1)

                print("🎯 Step 5: Click Visualize button")
                page.click('button:has-text("Visualize")')
                time.sleep(8)  # Wait for visualization to load

                print("🎯 Step 6: Check if navigation controls are visible")
                nav_visible = page.locator("#position-slider").is_visible()
                next_button_visible = page.locator("#nav-next").is_visible()

                print(f"📊 Navigation controls visible: {nav_visible and next_button_visible}")

                if nav_visible and next_button_visible:
                    print("🎯 Step 7: Test navigation")

                    # Get initial position and table data
                    initial_position = page.locator("#position-info").text_content()
                    initial_table = page.locator("#sequence-table").inner_html()
                    print(f"📍 Initial position: {initial_position}")
                    print(f"📋 Initial table length: {len(initial_table)}")

                    # Click Next button
                    print("🎯 Step 8: Click Next button")
                    page.click("#nav-next")
                    time.sleep(3)

                    # Get new position and table data
                    new_position = page.locator("#position-info").text_content()
                    new_table = page.locator("#sequence-table").inner_html()
                    print(f"📍 New position: {new_position}")
                    print(f"📋 New table length: {len(new_table)}")

                    # Check if data changed
                    position_changed = initial_position != new_position
                    table_changed = initial_table != new_table

                    print(f"✅ Position changed: {position_changed}")
                    print(f"✅ Table data changed: {table_changed}")

                    if position_changed and table_changed:
                        print("🎉 SUCCESS: Navigation is working! Data updates correctly!")

                        # Test one more navigation
                        print("🎯 Step 9: Test Previous button")
                        page.click("#nav-prev")
                        time.sleep(3)

                        final_position = page.locator("#position-info").text_content()
                        print(f"📍 Final position: {final_position}")

                    else:
                        print("❌ ISSUE: Navigation controls respond but data doesn't update")
                else:
                    print("❌ Navigation controls not visible after loading")
            else:
                print("❌ No sequences found for dataset")

        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()

        finally:
            browser.close()

    print("\n✅ Complete navigation flow test finished")

if __name__ == "__main__":
    test_complete_navigation()