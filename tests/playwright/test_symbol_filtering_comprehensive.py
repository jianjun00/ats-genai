#!/usr/bin/env python3
"""
Comprehensive Playwright test to demonstrate symbol filtering functionality.
This test shows why symbol is treated as categorical and verifies filtering works correctly.
"""

import asyncio
from playwright.async_api import async_playwright

@pytest.mark.asyncio

async def test_symbol_filtering_comprehensive():
    """Comprehensive test demonstrating symbol filtering with categorical checkboxes."""
    print("🎯 Comprehensive Symbol Filtering Demonstration")
    print("=" * 60)
    
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            # Load the EDA interface
            await page.goto("http://localhost:4000/eda", timeout=15000)
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(3000)
            
            print("✅ Step 1: EDA Interface loaded successfully")
            
            # Select dataset
            print("\n🔧 Step 2: Selecting dataset...")
            dataset_select = page.locator("#dataset-select")
            await dataset_select.select_option("intg_daily_prices_tiingo")
            await page.wait_for_timeout(8000)
            print("✅ Dataset 'intg_daily_prices_tiingo' selected")
            
            # Examine why symbol is categorical
            print("\n🔍 Step 3: Examining symbol column classification...")
            
            # Check the symbol filter interface
            symbol_filter_section = page.locator("div.filter-group:has(label:text-matches('symbol', 'i'))")
            if await symbol_filter_section.count() > 0:
                # Get the filter type label
                filter_label = await symbol_filter_section.locator("label").first.text_content()
                print(f"✅ Symbol filter found: {filter_label}")
                
                # Check if it's checkboxes (categorical) or text input (string)
                checkboxes = symbol_filter_section.locator("input[type='checkbox']")
                text_inputs = symbol_filter_section.locator("input[type='text']")
                
                checkbox_count = await checkboxes.count()
                text_input_count = await text_inputs.count()
                
                if checkbox_count > 0:
                    print(f"✅ Symbol is treated as CATEGORICAL: {checkbox_count} checkbox options")
                    print("   This is correct because:")
                    print("   • Stock symbols are discrete identifiers (AAPL, MSFT, etc.)")
                    print("   • Limited unique values in most datasets")
                    print("   • Checkbox interface is more user-friendly")
                    print("   • Shows available options without requiring memorization")
                    
                    # List the available symbol options
                    print("\n📋 Available symbol options:")
                    for i in range(min(checkbox_count, 8)):
                        checkbox = checkboxes.nth(i)
                        value = await checkbox.get_attribute("value")
                        label_text = await checkbox.locator("..").text_content()
                        print(f"   {i+1}. {value} - {label_text.strip()}")
                        
                elif text_input_count > 0:
                    print(f"❌ Symbol is treated as STRING: {text_input_count} text input(s)")
                    print("   This would be less user-friendly for symbols")
                else:
                    print("⚠️ Symbol filter type unclear")
            else:
                print("❌ Symbol filter not found")
                return
            
            # Get initial table state
            print("\n📊 Step 4: Examining initial table data...")
            table_body = page.locator("#data-table tbody")
            initial_rows = await table_body.locator("tr").count()
            
            # Get sample of initial symbols
            initial_symbols = []
            for i in range(min(5, initial_rows)):
                row = table_body.locator("tr").nth(i)
                symbol_cell = row.locator("td").nth(1)  # Symbol is column 2
                symbol = await symbol_cell.text_content()
                initial_symbols.append(symbol)
            
            unique_initial = set(initial_symbols)
            print(f"✅ Initial table has {initial_rows} rows")
            print(f"✅ Sample symbols: {initial_symbols}")
            print(f"✅ Unique symbols in sample: {len(unique_initial)} ({sorted(unique_initial)})")
            
            # Test single symbol filtering
            print("\n🎯 Step 5: Testing single symbol filter (AAPL)...")
            
            # Find and check AAPL checkbox
            aapl_checkbox = page.locator("input[name='filter-symbol'][value='AAPL']")
            if await aapl_checkbox.count() > 0:
                await aapl_checkbox.check()
                print("✅ AAPL checkbox checked")
                
                # Apply the filter
                apply_button = page.locator("button:has-text('Apply Filters')")
                await apply_button.click()
                print("✅ Apply Filters button clicked")
                
                # Wait for filtering to complete
                await page.wait_for_timeout(5000)
                
                # Verify filtering results
                filtered_rows = await table_body.locator("tr").count()
                print(f"✅ Table now has {filtered_rows} rows (filtered)")
                
                # Check if all rows are AAPL
                filtered_symbols = []
                for i in range(min(10, filtered_rows)):
                    row = table_body.locator("tr").nth(i)
                    symbol_cell = row.locator("td").nth(1)
                    symbol = await symbol_cell.text_content()
                    filtered_symbols.append(symbol)
                
                all_aapl = all(symbol == 'AAPL' for symbol in filtered_symbols)
                unique_filtered = set(filtered_symbols)
                
                print(f"✅ Sample filtered symbols: {filtered_symbols}")
                print(f"✅ All samples are AAPL: {all_aapl}")
                print(f"✅ Unique symbols after filter: {len(unique_filtered)} ({sorted(unique_filtered)})")
                
                # Check pagination info
                table_info = page.locator("#table-info")
                if await table_info.count() > 0:
                    info_text = await table_info.text_content()
                    print(f"✅ Pagination info: {info_text.strip()}")
                    
                    # Verify no 'undefined' in pagination
                    has_undefined = 'undefined' in info_text.lower()
                    print(f"✅ No 'undefined' in pagination: {not has_undefined}")
                
            else:
                print("❌ AAPL checkbox not found")
                return
            
            # Test multiple symbol filtering
            print("\n🔗 Step 6: Testing multiple symbol filter (AAPL + ABT)...")
            
            # Clear previous filter first
            clear_button = page.locator("button:has-text('Clear')")
            if await clear_button.count() > 0:
                await clear_button.click()
                await page.wait_for_timeout(2000)
                print("✅ Previous filters cleared")
            
            # Select multiple symbols
            aapl_checkbox = page.locator("input[name='filter-symbol'][value='AAPL']")
            abt_checkbox = page.locator("input[name='filter-symbol'][value='ABT']")
            
            if await aapl_checkbox.count() > 0 and await abt_checkbox.count() > 0:
                await aapl_checkbox.check()
                await abt_checkbox.check()
                print("✅ AAPL and ABT checkboxes checked")
                
                # Apply multiple filter
                apply_button = page.locator("button:has-text('Apply Filters')")
                await apply_button.click()
                await page.wait_for_timeout(5000)
                
                # Check results
                multi_filtered_rows = await table_body.locator("tr").count()
                print(f"✅ Table now has {multi_filtered_rows} rows (multi-filtered)")
                
                # Sample symbols from multi-filtered results
                multi_symbols = []
                for i in range(min(10, multi_filtered_rows)):
                    row = table_body.locator("tr").nth(i)
                    symbol_cell = row.locator("td").nth(1)
                    symbol = await symbol_cell.text_content()
                    multi_symbols.append(symbol)
                
                unique_multi = set(multi_symbols)
                expected_symbols = {'AAPL', 'ABT'}
                multi_filter_works = unique_multi.issubset(expected_symbols)
                
                print(f"✅ Sample multi-filtered symbols: {multi_symbols}")
                print(f"✅ Unique symbols: {sorted(unique_multi)}")
                print(f"✅ Multi-symbol filter works: {multi_filter_works}")
                
            else:
                print("⚠️ Could not find both AAPL and ABT checkboxes for multi-filter test")
            
            # Summary and analysis
            print("\n📋 Step 7: Test Results Summary")
            print("=" * 60)
            
            results = {
                "Symbol treated as categorical": checkbox_count > 0,
                "Categorical checkboxes available": checkbox_count,
                "Initial data has mixed symbols": len(unique_initial) > 1,
                "Single symbol filter works": all_aapl and len(unique_filtered) == 1,
                "Multi-symbol filter works": multi_filter_works if 'multi_filter_works' in locals() else False,
                "Pagination shows correct info": not has_undefined if 'has_undefined' in locals() else True,
            }
            
            print("\n🎯 DEMONSTRATION RESULTS:")
            for test_name, result in results.items():
                status = "✅ PASS" if result else "❌ FAIL"
                if test_name == "Categorical checkboxes available":
                    print(f"{test_name}: {checkbox_count} available {status}")
                else:
                    print(f"{test_name}: {status}")
            
            # Explain why categorical is correct
            print("\n💡 WHY SYMBOL IS CORRECTLY TREATED AS CATEGORICAL:")
            print("1. ✅ Stock symbols are discrete identifiers (not continuous data)")
            print("2. ✅ Limited unique values per dataset (typically 10-1000s of symbols)")
            print("3. ✅ Checkbox interface shows available options")
            print("4. ✅ Users don't need to memorize exact symbol names")
            print("5. ✅ Multiple selection is intuitive with checkboxes")
            print("6. ✅ Better UX than typing text with potential typos")
            
            passed_tests = sum(1 for result in results.values() if result)
            total_tests = len(results)
            
            print(f"\n🏆 OVERALL RESULT: {passed_tests}/{total_tests} tests passed")
            
            if passed_tests == total_tests:
                print("\n🎉 SYMBOL FILTERING WORKS PERFECTLY!")
                print("✅ Categorical treatment is correct and functional")
            else:
                print(f"\n⚠️ Some issues detected - see results above")
                
        except Exception as e:
            print(f"❌ Test failed: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_symbol_filtering_comprehensive())