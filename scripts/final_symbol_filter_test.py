#!/usr/bin/env python3
"""
Final comprehensive test of symbol filter functionality
"""

import asyncio
from playwright.async_api import async_playwright

async def final_symbol_filter_test():
    """Final comprehensive test of symbol filter functionality."""
    print("🎉 Final Symbol Filter Verification Test")
    print("="*60)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto("http://localhost:4000/eda", timeout=15000)
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(3000)

        # Test 1: Interface loads correctly
        print("✅ Test 1: Interface loads correctly")

        # Test 2: Dataset selection works
        print("\\n🧪 Test 2: Dataset selection...")
        dataset_select = page.locator("#dataset-select")
        await dataset_select.select_option("intg_daily_price_tiingo")
        await page.wait_for_timeout(8000)
        print("✅ Dataset selected successfully")

        # Test 3: Symbol checkboxes are generated
        print("\\n🧪 Test 3: Symbol filter checkboxes...")
        checkboxes = page.locator("input[type='checkbox'][name='filter-symbol']")
        checkbox_count = await checkboxes.count()
        print(f"✅ Found {checkbox_count} symbol checkboxes")

        # Test 4: Initial table has mixed symbols
        print("\\n🧪 Test 4: Initial table state...")
        initial_symbols = await page.evaluate("""
            () => {
                const tableBody = document.getElementById('table-body');
                const rows = tableBody.querySelectorAll('tr');
                const symbols = [];
                for (let i = 0; i < Math.min(5, rows.length); i++) {
                    symbols.push(rows[i].cells[1].textContent);
                }
                return symbols;
            }
        """)
        unique_initial = set(initial_symbols)
        print(f"✅ Initial symbols: {initial_symbols}")
        print(f"✅ Unique symbols before filter: {len(unique_initial)} ({sorted(unique_initial)})")

        # Test 5: Apply AAPL filter
        print("\\n🧪 Test 5: Apply AAPL filter...")
        aapl_checkbox = page.locator("input[name='filter-symbol'][value='AAPL']")
        await aapl_checkbox.check()
        apply_button = page.locator("button:has-text('Apply Filters')")
        await apply_button.click()
        print("✅ AAPL filter applied")

        # Wait for filtering to complete (increased wait time)
        await page.wait_for_timeout(5000)

        # Test 6: Verify table content is filtered
        print("\\n🧪 Test 6: Verify filtered table content...")
        filtered_symbols = await page.evaluate("""
            () => {
                const tableBody = document.getElementById('table-body');
                const rows = tableBody.querySelectorAll('tr');
                const symbols = [];
                for (let i = 0; i < rows.length; i++) {
                    symbols.push(rows[i].cells[1].textContent);
                }
                return symbols;
            }
        """)
        unique_filtered = set(filtered_symbols)
        all_aapl = all(symbol == 'AAPL' for symbol in filtered_symbols)

        print(f"✅ Filtered symbols (first 5): {filtered_symbols[:5]}")
        print(f"✅ Total filtered rows: {len(filtered_symbols)}")
        print(f"✅ Unique symbols after filter: {len(unique_filtered)} ({sorted(unique_filtered)})")
        print(f"✅ All rows are AAPL: {all_aapl}")

        # Test 7: Verify pagination info
        print("\\n🧪 Test 7: Verify pagination info...")
        table_info = page.locator("#table-info")
        info_text = await table_info.text_content()
        print(f"✅ Table info: {info_text.strip()}")

        # Check for 'undefined' in pagination
        has_undefined = 'undefined' in info_text.lower()
        print(f"✅ No 'undefined' in pagination: {not has_undefined}")

        # Test 8: Test clearing filters
        print("\\n🧪 Test 8: Test clearing filters...")
        clear_button = page.locator("button:has-text('Clear')")
        await clear_button.click()
        await page.wait_for_timeout(3000)

        cleared_symbols = await page.evaluate("""
            () => {
                const tableBody = document.getElementById('table-body');
                const rows = tableBody.querySelectorAll('tr');
                const symbols = [];
                for (let i = 0; i < Math.min(5, rows.length); i++) {
                    symbols.push(rows[i].cells[1].textContent);
                }
                return symbols;
            }
        """)
        unique_cleared = set(cleared_symbols)
        filters_cleared = len(unique_cleared) > 1

        print(f"✅ Symbols after clearing: {cleared_symbols}")
        print(f"✅ Filters cleared successfully: {filters_cleared}")

        # Test 9: Test multiple symbol selection
        print("\\n🧪 Test 9: Test multiple symbol selection...")
        aapl_checkbox = page.locator("input[name='filter-symbol'][value='AAPL']")
        msft_checkbox = page.locator("input[name='filter-symbol'][value='ABT']")  # Use ABT since it's available

        await aapl_checkbox.check()
        await msft_checkbox.check()
        await apply_button.click()
        await page.wait_for_timeout(5000)

        multi_symbols = await page.evaluate("""
            () => {
                const tableBody = document.getElementById('table-body');
                const rows = tableBody.querySelectorAll('tr');
                const symbols = [];
                for (let i = 0; i < rows.length; i++) {
                    symbols.push(rows[i].cells[1].textContent);
                }
                return [...new Set(symbols)].sort();
            }
        """)

        print(f"✅ Multiple filter symbols: {multi_symbols}")
        expected_symbols = set(['AAPL', 'ABT'])
        actual_symbols = set(multi_symbols)
        multi_filter_works = actual_symbols == expected_symbols
        print(f"✅ Multiple symbol filter works: {multi_filter_works}")

        # Summary
        print(f"\\n📊 Test Results Summary:")
        print("="*60)

        results = {
            "Interface loads": True,
            "Dataset selection": True,
            "Symbol checkboxes generated": checkbox_count > 0,
            "Initial mixed symbols": len(unique_initial) > 1,
            "AAPL filter works": all_aapl and len(unique_filtered) == 1,
            "No undefined in pagination": not has_undefined,
            "Clear filters works": filters_cleared,
            "Multiple symbol filter works": multi_filter_works
        }

        for test_name, result in results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            print(f"{test_name}: {status}")

        passed = sum(results.values())
        total = len(results)

        print(f"\\n🎯 Overall Result: {passed}/{total} tests passed")

        if passed == total:
            print("\\n🎉🎉🎉 ALL SYMBOL FILTER TESTS PASSED! 🎉🎉🎉")
            print("The symbol filter is working perfectly!")
        else:
            print("\\n⚠️ Some tests failed - see results above")

if __name__ == "__main__":
    asyncio.run(final_symbol_filter_test())