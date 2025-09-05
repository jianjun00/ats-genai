#!/usr/bin/env python3
"""
Playwright Test for Training Dataset Sequence Visualization

Tests the complete workflow:
1. Dataset selection shows proper sequence names (no "undefined")
2. Sequence selection loads visualization data
3. Plotly charts render correctly with OHLC data
4. Data rows display properly in the table
"""

import asyncio
import pytest
import sys
from pathlib import Path

# Add src to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

@pytest.mark.asyncio
async def test_sequence_visualization_complete():
    """Complete test of training dataset sequence visualization."""
    from playwright.async_api import async_playwright
    
    print("🎭 Training Dataset Sequence Visualization Test with Playwright")
    print("=" * 70)
    
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=False, slow_mo=500)  # Set headless=False to see the browser
        page = await browser.new_page()
        
        console_messages = []
        console_errors = []
        
        page.on("console", lambda msg: (
            console_errors.append(msg.text) if msg.type == "error" else 
            console_messages.append(f"{msg.type}: {msg.text}")
        ))
        
        test_results = {}
        
        try:
            print("🧪 Loading EDA interface...")
            await page.goto("http://localhost:3000/eda", timeout=20000)
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(3000)  # Extra time for full loading
            
            # Test 1: Page loads without errors
            js_error_count = len([e for e in console_errors if "error" in e.lower()])
            test_results["Page Loads Clean"] = js_error_count == 0
            print(f"✅ JavaScript errors on load: {js_error_count}")
            
            # Test 2: Training Dataset section exists
            training_section = page.locator("#training-datasets-section")
            training_section_exists = await training_section.count() > 0
            test_results["Training Section Exists"] = training_section_exists
            print(f"{'✅' if training_section_exists else '❌'} Training datasets section: {training_section_exists}")
            
            if training_section_exists:
                # Test 3: Dataset dropdown selection
                print("\\n🧪 Testing dataset selection...")
                dataset_select = page.locator("#training-dataset-select")
                if await dataset_select.count() > 0:
                    # Get available dataset options  
                    options = await dataset_select.locator("option").all()
                    print(f"Found {len(options)} dataset options")
                    
                    if len(options) > 1:  # First option is usually "Choose dataset..."
                        # Select a dataset
                        first_dataset_value = await options[1].get_attribute("value")
                        dataset_text = await options[1].inner_text()
                        
                        if first_dataset_value:
                            await dataset_select.select_option(first_dataset_value)
                            print(f"✅ Selected dataset: {dataset_text} (ID: {first_dataset_value})")
                            
                            # Wait for sequences to load
                            await page.wait_for_timeout(5000)
                            
                            # Test 4: Sequence dropdown populated without "undefined"
                            print("\\n🧪 Testing sequence selection...")
                            sequence_select = page.locator("#training-sequence-select")
                            if await sequence_select.count() > 0:
                                # Check if sequences loaded
                                sequence_options = await sequence_select.locator("option").all()
                                print(f"Found {len(sequence_options)} sequence options")
                                
                                # Test for "undefined" in sequence dropdown
                                has_undefined = False
                                valid_sequences = 0
                                
                                for option in sequence_options:
                                    option_text = await option.inner_text()
                                    if "undefined" in option_text.lower():
                                        has_undefined = True
                                        print(f"❌ Found undefined in sequence: {option_text}")
                                    elif option_text != "Choose a sequence...":
                                        valid_sequences += 1
                                        print(f"✅ Valid sequence: {option_text}")
                                
                                test_results["No Undefined in Sequences"] = not has_undefined
                                test_results["Sequences Loaded"] = valid_sequences > 0
                                
                                # Test 5: Select a sequence and load visualization
                                if valid_sequences > 0:
                                    print("\\n🧪 Testing sequence visualization...")
                                    
                                    # Select the first valid sequence
                                    for option in sequence_options:
                                        option_text = await option.inner_text()
                                        if option_text != "Choose a sequence..." and "undefined" not in option_text.lower():
                                            sequence_value = await option.get_attribute("value")
                                            await sequence_select.select_option(sequence_value)
                                            print(f"✅ Selected sequence: {option_text} (ID: {sequence_value})")
                                            break
                                    
                                    # Wait for visualization to load
                                    await page.wait_for_timeout(7000)
                                    
                                    # Test 6: Plotly chart exists and renders
                                    plotly_chart = page.locator("#plotly-chart, .plotly-graph-div")
                                    plotly_exists = await plotly_chart.count() > 0
                                    test_results["Plotly Chart Exists"] = plotly_exists
                                    print(f"{'✅' if plotly_exists else '❌'} Plotly chart element: {plotly_exists}")
                                    
                                    if plotly_exists:
                                        # Check if chart has actual data (look for SVG content)
                                        svg_content = page.locator(".plotly-graph-div svg")
                                        has_svg_data = await svg_content.count() > 0
                                        test_results["Chart Has Data"] = has_svg_data
                                        print(f"{'✅' if has_svg_data else '❌'} Chart renders data (SVG): {has_svg_data}")
                                        
                                        # Test for OHLC bars (look for path elements which represent bars)
                                        ohlc_bars = page.locator(".plotly-graph-div .bars path, .plotly-graph-div .trace path")
                                        bars_count = await ohlc_bars.count()
                                        has_ohlc_bars = bars_count > 0
                                        test_results["OHLC Bars Rendered"] = has_ohlc_bars
                                        print(f"{'✅' if has_ohlc_bars else '❌'} OHLC bars rendered: {has_ohlc_bars} (count: {bars_count})")
                                    
                                    # Test 7: Data table shows rows
                                    print("\\n🧪 Testing data table...")
                                    data_table = page.locator("#sequence-data-table, table")
                                    table_exists = await data_table.count() > 0
                                    test_results["Data Table Exists"] = table_exists
                                    print(f"{'✅' if table_exists else '❌'} Data table element: {table_exists}")
                                    
                                    if table_exists:
                                        # Look for actual data rows (not just headers)
                                        data_rows = page.locator("table tbody tr, #sequence-data-table tbody tr")
                                        row_count = await data_rows.count()
                                        has_data_rows = row_count > 0
                                        test_results["Table Has Data Rows"] = has_data_rows
                                        print(f"{'✅' if has_data_rows else '❌'} Data rows in table: {has_data_rows} (count: {row_count})")
                                        
                                        # Check if rows contain actual OHLC data (not "undefined" or empty)
                                        if has_data_rows:
                                            first_row_text = await data_rows.first.inner_text()
                                            has_valid_data = ("undefined" not in first_row_text.lower() and 
                                                            len(first_row_text.strip()) > 10)
                                            test_results["Valid Data in Rows"] = has_valid_data
                                            print(f"{'✅' if has_valid_data else '❌'} Valid OHLC data in rows: {has_valid_data}")
                                            if not has_valid_data:
                                                print(f"First row text: {first_row_text}")
                                
                                else:
                                    print("❌ No valid sequences found")
                            else:
                                print("❌ Sequence selector not found")
                        else:
                            print("❌ Could not get dataset value")
                    else:
                        print("❌ No dataset options available") 
                else:
                    print("❌ Dataset selector not found")
            
            # Test 8: Check for API endpoint calls
            print("\\n🧪 Checking API endpoint responses...")
            
            # Listen for network requests
            response_statuses = []
            
            def handle_response(response):
                if "/api/v1/training-datasets" in response.url:
                    response_statuses.append(response.status)
                    print(f"API Response: {response.url} - Status: {response.status}")
            
            page.on("response", handle_response)
            
            # Trigger API calls by refreshing and selecting again
            await page.reload()
            await page.wait_for_timeout(3000)
            
            api_success = len([s for s in response_statuses if 200 <= s < 300]) > 0
            test_results["API Endpoints Working"] = api_success
            print(f"{'✅' if api_success else '❌'} API endpoints responding: {api_success}")
            
        except Exception as e:
            print(f"❌ Test failed with exception: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            await page.wait_for_timeout(2000)  # Brief pause to see final state
            await browser.close()
        
        # Summary
        print(f"\\n📊 Test Results Summary")
        print("=" * 70)
        
        for test_name, result in test_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{test_name:<30}: {status}")
        
        passed = sum(test_results.values())
        total = len(test_results)
        
        print(f"\\n🎯 Overall: {passed}/{total} tests passed ({passed/total*100:.1f}%)")
        
        # Console summary
        error_count = len([e for e in console_errors if "error" in e.lower()])
        warning_count = len([e for e in console_messages if "warning" in e.lower()])
        print(f"📋 Console: {error_count} errors, {warning_count} warnings")
        
        if error_count > 0:
            print("\\n❌ Console Errors:")
            for error in console_errors[:5]:  # Show first 5 errors
                print(f"  {error}")
        
        if passed == total:
            print("\\n🎉 All sequence visualization tests passed!")
            return True
        else:
            print("\\n⚠️ Some tests failed - see results above")
            return False

async def test_sequence_api_endpoints():
    """Test that the sequence visualization API endpoints work correctly."""
    import aiohttp
    
    print("\\n🧪 Testing API endpoints directly...")
    
    async with aiohttp.ClientSession() as session:
        endpoints_results = {}
        
        # Test 1: Training datasets list
        try:
            async with session.get("http://localhost:3000/api/v1/training-datasets") as response:
                datasets_data = await response.json()
                endpoints_results["Datasets List"] = response.status == 200 and len(datasets_data.get("datasets", [])) > 0
                print(f"✅ Datasets API: {response.status}, found {len(datasets_data.get('datasets', []))} datasets")
                
                if datasets_data.get("datasets"):
                    first_dataset_id = datasets_data["datasets"][0]["id"]
                    
                    # Test 2: Dataset sequences
                    async with session.get(f"http://localhost:3000/api/v1/training-datasets/{first_dataset_id}/sequences") as seq_response:
                        sequences_data = await seq_response.json()
                        has_sequences = seq_response.status == 200 and len(sequences_data.get("sequences", [])) > 0
                        endpoints_results["Sequences List"] = has_sequences
                        print(f"✅ Sequences API: {seq_response.status}, found {len(sequences_data.get('sequences', []))} sequences")
                        
                        # Check that sequences have required fields
                        if sequences_data.get("sequences"):
                            seq = sequences_data["sequences"][0]
                            has_required_fields = all(field in seq for field in ["sequence_id", "timeframe", "file_size_mb"])
                            endpoints_results["Sequence Fields"] = has_required_fields
                            print(f"{'✅' if has_required_fields else '❌'} Sequence has required fields: {has_required_fields}")
                            
                            # Test 3: Visualization data
                            async with session.get(f"http://localhost:3000/api/v1/training-datasets/{first_dataset_id}/visualization-data?start_idx=0&sequence_id=0") as viz_response:
                                viz_data = await viz_response.json()
                                has_viz_data = viz_response.status == 200 and len(viz_data.get("data", [])) > 0
                                endpoints_results["Visualization Data"] = has_viz_data
                                print(f"{'✅' if has_viz_data else '❌'} Visualization API: {viz_response.status}, found {len(viz_data.get('data', []))} data points")
        
        except Exception as e:
            print(f"❌ API test failed: {e}")
        
        return endpoints_results

if __name__ == "__main__":
    async def run_all_tests():
        print("🚀 Starting Complete Sequence Visualization Test Suite")
        print("=" * 70)
        
        # Run API tests first
        api_results = await test_sequence_api_endpoints()
        
        print("\\n" + "=" * 70)
        
        # Run browser tests
        browser_results = await test_sequence_visualization_complete()
        
        # Overall summary
        print("\\n🏁 Complete Test Suite Results")
        print("=" * 70)
        
        print("API Endpoints:")
        for test_name, result in api_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"  {test_name}: {status}")
        
        api_success = all(api_results.values()) if api_results else False
        browser_success = browser_results
        
        print(f"\\n📊 Summary:")
        print(f"  API Tests: {'✅ PASS' if api_success else '❌ FAIL'}")
        print(f"  Browser Tests: {'✅ PASS' if browser_success else '❌ FAIL'}")
        
        if api_success and browser_success:
            print("\\n🎉 All sequence visualization functionality working correctly!")
        else:
            print("\\n⚠️ Some issues found - check results above")
    
    asyncio.run(run_all_tests())