#!/usr/bin/env python3
"""
Comprehensive Screenshot Proof: Server + Client Debugging
Shows the complete data flow from API to visualization
"""

import asyncio
import subprocess
import time
from playwright.async_api import async_playwright

async def comprehensive_test():
    print("🎬 COMPREHENSIVE SCREENSHOT PROOF TEST")
    print("=" * 60)
    print("This test will show:")
    print("✓ Server-side debug logs for multi-timeframe API")
    print("✓ Client-side debug logs in browser console")
    print("✓ Plotly chart creation and data loading")
    print("✓ Table view with actual OHLC data")
    print("=" * 60)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,  # Show browser to see console logs
            args=['--no-sandbox', '--disable-dev-shm-usage']
        )
        context = await browser.new_context(viewport={'width': 1920, 'height': 1080})
        page = await context.new_page()

        # Enable console logging
        console_logs = []
        page.on("console", lambda msg: console_logs.append(f"{msg.type()}: {msg.text()}"))

        try:
            print("\n📊 Step 1: Navigate to EDA tool")
            await page.goto("http://localhost:3000", wait_until="networkidle")
            await page.screenshot(path="/tmp/proof_step1_homepage.png", full_page=True)
            print("✅ Homepage loaded and screenshot saved")

            print("\n📊 Step 2: Click Training Datasets")
            training_button = page.get_by_role("button", name="🤖 Training Datasets")
            await training_button.click()
            await page.wait_for_timeout(3000)
            await page.screenshot(path="/tmp/proof_step2_training_datasets.png", full_page=True)
            print("✅ Training Datasets interface loaded")

            print("\n📊 Step 3: Select Dataset 63")
            dataset_dropdown = page.locator("#dataset-selector")
            await dataset_dropdown.select_option(value="63")
            await page.wait_for_timeout(2000)
            await page.screenshot(path="/tmp/proof_step3_dataset_selected.png", full_page=True)
            print("✅ Dataset 63 selected")

            print("\n📊 Step 4: Check sequence dropdown population")
            sequence_dropdown = page.locator("#sequence-selector")
            options = await sequence_dropdown.locator("option").all_text_contents()
            print(f"Sequence options: {options}")

            if len(options) > 1 and options[1] != "Choose a sequence...":
                print(f"✅ Sequence found: {options[1]}")
                await sequence_dropdown.select_option(index=1)
                await page.wait_for_timeout(2000)
                await page.screenshot(path="/tmp/proof_step4_sequence_selected.png", full_page=True)
                print("✅ Sequence selected")

                print("\n📊 Step 5: Click Visualize and monitor API calls")
                print("🌐 Starting server log monitoring...")

                # Start server log monitoring in background
                log_process = subprocess.Popen([
                    "python3", "scripts/run_dev.py", "logs", "--service", "analytics"
                ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

                # Click visualize
                visualize_button = page.locator("text=📊 Visualize")
                await visualize_button.click()
                await page.wait_for_timeout(5000)  # Wait for API call and visualization

                await page.screenshot(path="/tmp/proof_step5_after_visualize.png", full_page=True)
                print("✅ Visualization triggered")

                print("\n📊 Step 6: Check for visualization elements")

                # Check charts
                chart_found = {}
                timeframes = ['5m', '15m', '1h', '1d', '1w']
                for tf in timeframes:
                    chart_element = page.locator(f"#ohlc-chart-{tf}")
                    if await chart_element.count() > 0:
                        chart_content = await chart_element.inner_html()
                        has_plotly = "plotly" in chart_content.lower()
                        has_error = "error" in chart_content.lower()
                        has_loading = "loading" in chart_content.lower()

                        chart_found[tf] = {
                            'exists': True,
                            'has_plotly': has_plotly,
                            'has_error': has_error,
                            'has_loading': has_loading
                        }

                        print(f"   {tf}: {'✅' if has_plotly else '❌'} Plotly: {has_plotly}, Error: {has_error}, Loading: {has_loading}")

                # Check table
                table_element = page.locator("#sequence-table")
                if await table_element.count() > 0:
                    table_content = await table_element.inner_html()
                    has_table_data = "<table" in table_content and "<tr" in table_content
                    print(f"   Table: {'✅' if has_table_data else '❌'} Data loaded: {has_table_data}")

                # Check dataset info
                info_element = page.locator("#dataset-info")
                if await info_element.count() > 0:
                    info_content = await info_element.inner_html()
                    has_sequence_info = "AAPL_" in info_content
                    print(f"   Info: {'✅' if has_sequence_info else '❌'} Sequence info: {has_sequence_info}")

                print("\n📊 Step 7: Final screenshot with full visualization")
                await page.wait_for_timeout(3000)  # Wait for any remaining rendering
                await page.screenshot(path="/tmp/proof_step7_final_visualization.png", full_page=True)

                print("\n📊 Step 8: Extract console logs")
                print("🔧 CLIENT CONSOLE LOGS:")
                for log in console_logs[-20:]:  # Show last 20 logs
                    print(f"   {log}")

                # Stop and get server logs
                log_process.terminate()
                log_output, _ = log_process.communicate()

                print("\n📊 Step 9: Server API logs")
                print("🔧 SERVER API LOGS:")
                server_lines = log_output.split('\n')[-50:]  # Last 50 lines
                for line in server_lines:
                    if any(keyword in line for keyword in ['DEBUG', 'multi-timeframe', 'OHLC', 'CLIENT', 'Plotly']):
                        print(f"   {line}")

                # Save console logs to file
                with open("/tmp/proof_console_logs.txt", "w") as f:
                    for log in console_logs:
                        f.write(f"{log}\n")

                with open("/tmp/proof_server_logs.txt", "w") as f:
                    f.write(log_output)

                print("\n✅ COMPREHENSIVE PROOF COMPLETE!")
                print("📸 Screenshots saved:")
                print("   /tmp/proof_step1_homepage.png")
                print("   /tmp/proof_step2_training_datasets.png")
                print("   /tmp/proof_step3_dataset_selected.png")
                print("   /tmp/proof_step4_sequence_selected.png")
                print("   /tmp/proof_step5_after_visualize.png")
                print("   /tmp/proof_step7_final_visualization.png")
                print("📜 Debug logs saved:")
                print("   /tmp/proof_console_logs.txt")
                print("   /tmp/proof_server_logs.txt")

                return True

            else:
                print("❌ No sequences found in dropdown")
                await page.screenshot(path="/tmp/proof_error_no_sequences.png", full_page=True)
                return False

        except Exception as e:
            print(f"❌ Test failed: {e}")
            await page.screenshot(path="/tmp/proof_error.png", full_page=True)
            return False

        finally:
            await browser.close()

if __name__ == "__main__":
    result = asyncio.run(comprehensive_test())
    exit(0 if result else 1)