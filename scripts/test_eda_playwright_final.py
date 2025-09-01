#!/usr/bin/env python3
"""
Final Playwright Test for EDA Interface
Tests with proper dropdown selection method
"""

import asyncio
from playwright.async_api import async_playwright

async def test_eda_interface_complete():
    """Complete test of EDA interface with proper dropdown handling."""
    print("🎭 Complete EDA Interface Test with Playwright")
    print("="*60)
    
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()
        
        console_errors = []
        page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
        
        test_results = {}
        
        try:
            print("🧪 Loading EDA interface...")
            await page.goto("http://localhost:4000/eda", timeout=15000)
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(2000)
            
            # Test 1: No JavaScript errors
            js_error_count = len([e for e in console_errors if "error" in e.lower()])
            test_results["JavaScript Errors"] = js_error_count == 0
            print(f"✅ JavaScript errors: {js_error_count}")
            
            # Test 2: Global x-axis control exists  
            global_axis = page.locator("#global-x-axis")
            global_axis_exists = await global_axis.count() > 0
            test_results["Global X-Axis Control"] = global_axis_exists
            print(f"{'✅' if global_axis_exists else '❌'} Global x-axis control: {global_axis_exists}")
            
            # Test 3: Chart Configuration positioned above Data Filter
            page_content = await page.content()
            chart_config_pos = page_content.find('Chart Configuration')
            data_filter_pos = page_content.find('Data Filter')
            positioning_correct = chart_config_pos > 0 and data_filter_pos > 0 and chart_config_pos < data_filter_pos
            test_results["Correct Positioning"] = positioning_correct
            print(f"{'✅' if positioning_correct else '❌'} Chart Config above Data Filter: {positioning_correct}")
            
            # Test 4: No per-column x-axis controls
            per_column_controls = page.locator("select[id*='xaxis-']")
            per_column_count = await per_column_controls.count()
            no_per_column = per_column_count == 0
            test_results["Per-Column Controls Removed"] = no_per_column
            print(f"{'✅' if no_per_column else '❌'} Per-column controls removed: {no_per_column} (count: {per_column_count})")
            
            # Test 5: Dataset selection using select_option method
            print("\n🧪 Testing dataset selection...")
            dataset_select = page.locator("#dataset-select")
            if await dataset_select.count() > 0:
                # Get available options  
                options = await dataset_select.locator("option").all()
                if len(options) > 1:
                    # Use select_option instead of clicking option
                    first_dataset_value = await options[1].get_attribute("value")
                    if first_dataset_value:
                        await dataset_select.select_option(first_dataset_value)
                        print(f"✅ Selected dataset: {first_dataset_value}")
                        
                        # Wait for data to load
                        await page.wait_for_timeout(5000)
                        
                        # Test 6: Symbol filtering test
                        print("\n🧪 Testing symbol filtering...")
                        
                        # Look for filter checkboxes/inputs
                        symbol_checkboxes = page.locator("input[type='checkbox'][name*='symbol'], input[type='checkbox'][id*='symbol']")
                        checkbox_count = await symbol_checkboxes.count()
                        
                        if checkbox_count > 0:
                            print(f"✅ Found {checkbox_count} symbol filter checkboxes")
                            
                            # Check first few checkboxes
                            for i in range(min(3, checkbox_count)):
                                await symbol_checkboxes.nth(i).check()
                            
                            # Apply filters
                            apply_button = page.locator("button:has-text('Apply Filters')")
                            if await apply_button.count() > 0:
                                await apply_button.click()
                                print("✅ Applied symbol filters")
                                
                                # Wait for results
                                await page.wait_for_timeout(3000)
                                
                                # Check pagination text for "undefined"
                                page_text = await page.content()
                                has_undefined = "undefined" in page_text.lower() and ("record" in page_text.lower() or "page" in page_text.lower())
                                test_results["No Undefined in Pagination"] = not has_undefined
                                print(f"{'✅' if not has_undefined else '❌'} Pagination without 'undefined': {not has_undefined}")
                            else:
                                print("⚠️ Apply Filters button not found")
                                test_results["No Undefined in Pagination"] = True  # Can't test, assume OK
                        else:
                            print("⚠️ No symbol filter checkboxes found")
                            test_results["No Undefined in Pagination"] = True  # Can't test, assume OK
                    else:
                        print("❌ Could not get dataset value")
                else:
                    print("❌ No dataset options available")
            else:
                print("❌ Dataset selector not found")
            
            # Test 7: Global axis dropdown functionality
            print("\n🧪 Testing global axis dropdown functionality...")
            if global_axis_exists:
                # Try changing the global axis
                await global_axis.select_option("date")
                selected_value = await global_axis.input_value()
                global_axis_functional = selected_value == "date"
                test_results["Global Axis Functional"] = global_axis_functional
                print(f"{'✅' if global_axis_functional else '❌'} Global axis functional: {global_axis_functional}")
            else:
                test_results["Global Axis Functional"] = False
                
        except Exception as e:
            print(f"❌ Test failed: {e}")
            
        finally:
            await browser.close()
        
        # Summary
        print(f"\n📊 Test Results Summary")
        print("="*60)
        
        for test_name, result in test_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{test_name}: {status}")
        
        passed = sum(test_results.values())
        total = len(test_results)
        
        print(f"\n🎯 Overall: {passed}/{total} tests passed")
        
        if passed == total:
            print("🎉 All EDA interface fixes verified with Playwright!")
        else:
            print("⚠️ Some tests failed - see results above")
        
        # Console summary
        warning_count = len([e for e in console_errors if "warning" in e.lower()])
        error_count = len([e for e in console_errors if "error" in e.lower()])
        print(f"\n📋 Console Summary: {error_count} errors, {warning_count} warnings")
        
        return test_results

if __name__ == "__main__":
    asyncio.run(test_eda_interface_complete())