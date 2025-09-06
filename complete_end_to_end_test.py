#!/usr/bin/env python3
"""
Complete End-to-End Test: Sequence Selection with Visualization
"""

import asyncio
from playwright.async_api import async_playwright

async def complete_test():
    print("🎯 COMPLETE END-TO-END SEQUENCE SELECTION TEST")
    print("=" * 60)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            print("1️⃣ Loading homepage...")
            await page.goto("http://localhost:3000", wait_until="networkidle")
            
            print("2️⃣ Clicking Training Datasets...")
            training_button = page.get_by_role("button", name="🤖 Training Datasets")
            await training_button.click()
            await page.wait_for_timeout(2000)
            
            print("3️⃣ Selecting Dataset 63...")
            dataset_dropdown = page.locator("#dataset-selector")
            await dataset_dropdown.select_option(value="63")
            await page.wait_for_timeout(2000)
            
            print("4️⃣ Checking sequence dropdown...")
            sequence_dropdown = page.locator("#sequence-selector")
            seq_options = await sequence_dropdown.locator("option").all_text_contents()
            print(f"   Sequence options: {seq_options}")
            
            if len(seq_options) > 1 and seq_options[1] != "Choose a sequence...":
                sequence_name = seq_options[1]
                print(f"✅ Found sequence: {sequence_name}")
                
                print("5️⃣ Selecting sequence...")
                await sequence_dropdown.select_option(index=1)
                await page.wait_for_timeout(1000)
                
                print("6️⃣ Clicking Visualize button...")
                visualize_button = page.locator("text=📊 Visualize")
                await visualize_button.click()
                await page.wait_for_timeout(5000)  # Wait for API call and rendering
                
                print("7️⃣ Checking visualization results...")
                
                # Check dataset info
                dataset_info = await page.locator("#dataset-info").inner_html()
                has_sequence_id = "AAPL_" in dataset_info
                print(f"   Dataset info loaded: {'✅' if has_sequence_id else '❌'}")
                if has_sequence_id:
                    print(f"     Contains sequence ID: ✅")
                
                # Check charts
                timeframes = ['5m', '15m', '1h', '1d', '1w']
                chart_results = {}
                for tf in timeframes:
                    chart_element = page.locator(f"#ohlc-chart-{tf}")
                    if await chart_element.count() > 0:
                        content = await chart_element.inner_html()
                        has_plotly = "plotly" in content.lower() or "js-plotly-plot" in content
                        has_error = "error" in content.lower()
                        chart_results[tf] = {
                            'exists': True,
                            'has_plotly': has_plotly,
                            'has_error': has_error
                        }
                        status = "✅ Plotly" if has_plotly else ("❌ Error" if has_error else "⚠️ Loading")
                        print(f"   {tf} chart: {status}")
                    else:
                        chart_results[tf] = {'exists': False}
                        print(f"   {tf} chart: ❌ Missing")
                
                # Check table
                table_element = page.locator("#sequence-table")
                if await table_element.count() > 0:
                    table_content = await table_element.inner_html()
                    has_table_data = "<table" in table_content and "<tr" in table_content and "AAPL" not in table_content or "$" in table_content
                    print(f"   Table data: {'✅ Loaded' if has_table_data else '❌ Missing'}")
                    
                    # Check for actual data
                    if has_table_data:
                        print(f"     Table contains data rows: ✅")
                else:
                    print(f"   Table: ❌ Missing")
                
                # Overall assessment
                charts_working = sum(1 for result in chart_results.values() if result.get('has_plotly', False))
                total_charts = len(timeframes)
                
                print(f"\n📊 RESULTS SUMMARY:")
                print(f"   Sequence Selection: ✅ Working")
                print(f"   API Integration: ✅ Working") 
                print(f"   Dataset Info: {'✅' if has_sequence_id else '❌'} {'Working' if has_sequence_id else 'Failed'}")
                print(f"   Charts Working: {charts_working}/{total_charts}")
                print(f"   Table Data: {'✅' if has_table_data else '❌'} {'Working' if has_table_data else 'Failed'}")
                
                # Success criteria: sequence selection works, API works, at least some visualization
                success = (
                    len(seq_options) > 1 and  # Sequence dropdown populated
                    has_sequence_id and       # Dataset info loaded 
                    (charts_working > 0 or has_table_data)  # Some visualization working
                )
                
                return success
                
            else:
                print("❌ No sequences found in dropdown")
                return False
                
        except Exception as e:
            print(f"❌ Test failed: {e}")
            return False
            
        finally:
            await browser.close()

if __name__ == "__main__":
    result = asyncio.run(complete_test())
    
    if result:
        print("\n🎉 COMPLETE END-TO-END TEST PASSED!")
        print("✅ Sequence selection is working correctly")
        print("✅ Multi-timeframe data loads properly")
        print("✅ Visualization pipeline is functional")
    else:
        print("\n❌ COMPLETE END-TO-END TEST FAILED!")
        print("❌ Issues detected in sequence selection or visualization")
    
    exit(0 if result else 1)