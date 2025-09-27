#!/usr/bin/env python3
"""
Debug currentTableData after filtering
"""

import asyncio
from playwright.async_api import async_playwright

async def debug_current_table_data():
    """Debug the currentTableData variable after filtering."""
    print("🔍 Debugging currentTableData Variable")
    print("="*50)

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()

        # Capture console logs
        console_logs = []
        page.on("console", lambda msg: console_logs.append(f"{msg.type}: {msg.text}"))

        await page.goto("http://localhost:4000/eda", timeout=15000)
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(3000)

        # Select dataset
        print("🧪 Selecting dataset...")
        dataset_select = page.locator("#dataset-select")
        await dataset_select.select_option("intg_daily_price_tiingo")
        await page.wait_for_timeout(8000)

        # Check initial currentTableData
        print("\\n🔍 Checking initial currentTableData...")
        initial_data = await page.evaluate("""
            () => {
                if (typeof currentTableData !== 'undefined') {
                    return {
                        length: currentTableData.length,
                        firstSymbols: currentTableData.slice(0, 3).map(row => row.symbol),
                        hasData: currentTableData.length > 0
                    };
                }
                return { error: 'currentTableData not defined' };
            }
        """)
        print(f"Initial currentTableData: {initial_data}")

        # Apply AAPL filter
        print("\\n🎯 Applying AAPL filter...")
        aapl_checkbox = page.locator("input[name='filter-symbol'][value='AAPL']")
        await aapl_checkbox.check()
        apply_button = page.locator("button:has-text('Apply Filters')")
        await apply_button.click()
        await page.wait_for_timeout(3000)

        # Check currentTableData after filtering
        print("\\n🔍 Checking currentTableData after filtering...")
        filtered_data = await page.evaluate("""
            () => {
                if (typeof currentTableData !== 'undefined') {
                    return {
                        length: currentTableData.length,
                        firstSymbols: currentTableData.slice(0, 5).map(row => row.symbol),
                        allSymbols: [...new Set(currentTableData.map(row => row.symbol))],
                        hasData: currentTableData.length > 0,
                        sampleRow: currentTableData[0] || null
                    };
                }
                return { error: 'currentTableData not defined' };
            }
        """)
        print(f"Filtered currentTableData: {filtered_data}")

        # Check if displayDataTable was called
        print("\\n🔍 Checking function call history...")
        function_status = await page.evaluate("""
            () => {
                return {
                    displayDataTableExists: typeof displayDataTable !== 'undefined',
                    loadFilteredDataExists: typeof loadFilteredData !== 'undefined',
                    renderTableBodyExists: typeof renderTableBody !== 'undefined',
                    currentFiltersValue: typeof currentFilters !== 'undefined' ? currentFilters : 'undefined'
                };
            }
        """)
        print(f"Function status: {function_status}")

        # Manually call renderTableBody to see if it works
        print("\\n🧪 Manually calling renderTableBody...")
        manual_result = await page.evaluate("""
            () => {
                if (typeof renderTableBody !== 'undefined' && typeof currentTableData !== 'undefined') {
                    try {
                        renderTableBody();

                        // Check if table body was updated
                        const tableBody = document.getElementById('table-body');
                        const rows = tableBody.querySelectorAll('tr');
                        const symbols = [];

                        for (let i = 0; i < Math.min(5, rows.length); i++) {
                            const symbolCell = rows[i].cells[1]; // Symbol is column 2 (index 1)
                            if (symbolCell) {
                                symbols.push(symbolCell.textContent);
                            }
                        }

                        return {
                            success: true,
                            rowsInDOM: rows.length,
                            symbolsInDOM: symbols,
                            tableBodyHTML: tableBody.innerHTML.substring(0, 200) + '...'
                        };
                    } catch (error) {
                        return { error: error.toString() };
                    }
                }
                return { error: 'Functions not available' };
            }
        """)
        print(f"Manual renderTableBody result: {manual_result}")

        # Check actual DOM content
        print("\\n🔍 Checking actual DOM content...")
        dom_content = await page.evaluate("""
            () => {
                const tableBody = document.getElementById('table-body');
                const rows = tableBody.querySelectorAll('tr');
                const symbols = [];

                for (let i = 0; i < Math.min(5, rows.length); i++) {
                    const symbolCell = rows[i].cells[1];
                    if (symbolCell) {
                        symbols.push(symbolCell.textContent);
                    }
                }

                return {
                    totalRows: rows.length,
                    symbolsInDOM: symbols
                };
            }
        """)
        print(f"DOM content: {dom_content}")

        # Print console logs
        if console_logs:
            print("\\n💬 Console logs:")
            for log in console_logs[-10:]:  # Last 10 logs
                print(f"  {log}")

if __name__ == "__main__":
    asyncio.run(debug_current_table_data())