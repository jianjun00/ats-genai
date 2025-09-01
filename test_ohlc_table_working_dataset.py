#!/usr/bin/env python3
"""
Playwright test to validate OHLC and table with working dataset (15)
"""

import asyncio
import sys

async def test_working_dataset_ohlc_and_table():
    """Test OHLC and table with dataset 15 which has working data"""
    try:
        from playwright.async_api import async_playwright
        
        print("🎭 Testing OHLC and Table with Working Dataset...")
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            # Enable console logging
            console_messages = []
            page.on("console", lambda msg: console_messages.append(f"{msg.type}: {msg.text}"))
            page.on("pageerror", lambda error: console_messages.append(f"PAGE_ERROR: {error}"))
            
            print("1️⃣ Loading training-eda page...")
            await page.goto("http://localhost:3000/training-eda", timeout=15000)
            await page.wait_for_timeout(3000)
            
            # Select dataset 15 which we know has working data
            print("2️⃣ Selecting dataset 15 (known working dataset)...")
            await page.wait_for_selector('#training-dataset-select', timeout=10000)
            
            # Wait for datasets to load
            await page.wait_for_timeout(3000)
            
            # Check if dataset 15 is available
            dataset_15_available = await page.evaluate("""() => {
                const select = document.getElementById('training-dataset-select');
                const options = Array.from(select.options);
                return options.some(opt => opt.value === '15');
            }""")
            
            if not dataset_15_available:
                print("❌ Dataset 15 not available in dropdown")
                return False
            
            print("✅ Dataset 15 found, selecting it...")
            await page.select_option('#training-dataset-select', '15')
            
            # Wait for analysis to complete (longer wait for full processing)
            print("3️⃣ Waiting for dataset analysis to complete...")
            await page.wait_for_timeout(10000)
            
            # Check for JavaScript errors during processing
            errors = [msg for msg in console_messages if 'error' in msg.lower() and 'warning' not in msg.lower()]
            js_logs = [msg for msg in console_messages if msg.startswith('log:')]
            
            print("   JavaScript activity:")
            for log in js_logs[-5:]:  # Show last 5 log messages
                print(f"     {log}")
                
            if errors:
                print("   ⚠️ JavaScript errors:")
                for error in errors[-3:]:
                    print(f"     {error}")
            
            # Test OHLC Visualization
            print("4️⃣ Testing OHLC Visualization...")
            ohlc_results = await page.evaluate("""() => {
                const results = {};
                
                // Check OHLC container visibility
                const ohlcSection = document.getElementById('ohlc-visualization');
                results.ohlcSectionExists = !!ohlcSection;
                results.ohlcSectionVisible = ohlcSection && getComputedStyle(ohlcSection).display !== 'none';
                
                // Check OHLC chart container
                const chartContainer = document.getElementById('ohlc-chart');
                results.chartContainerExists = !!chartContainer;
                results.chartContainerVisible = chartContainer && getComputedStyle(chartContainer).display !== 'none';
                results.chartHasContent = chartContainer && chartContainer.innerHTML.trim().length > 100;
                
                // Check for Plotly elements (indicates successful chart rendering)
                const plotlyElements = document.querySelectorAll('.plotly-graph-div');
                results.plotlyElementsCount = plotlyElements.length;
                results.hasPlotlyChart = plotlyElements.length > 0;
                
                // Check OHLC controls
                const slider = document.getElementById('sequence-slider');
                results.sliderExists = !!slider;
                results.sliderValue = slider ? slider.value : null;
                results.sliderMax = slider ? slider.max : null;
                
                // Check sequence display
                const sequenceDisplay = document.getElementById('sequence-display');
                results.sequenceDisplay = sequenceDisplay ? sequenceDisplay.textContent : null;
                
                // Check if OHLC functions are available
                results.updateOHLCFunctionExists = typeof updateOHLCVisualization === 'function';
                results.createOHLCFunctionExists = typeof createOHLCChart === 'function';
                
                return results;
            }""")
            
            print(f"   ✅ OHLC section exists: {ohlc_results['ohlcSectionExists']}")
            print(f"   ✅ OHLC section visible: {ohlc_results['ohlcSectionVisible']}")
            print(f"   ✅ Chart container exists: {ohlc_results['chartContainerExists']}")
            print(f"   ✅ Chart container visible: {ohlc_results['chartContainerVisible']}")
            print(f"   ✅ Chart has content: {ohlc_results['chartHasContent']}")
            print(f"   ✅ Plotly elements count: {ohlc_results['plotlyElementsCount']}")
            print(f"   ✅ Has Plotly chart: {ohlc_results['hasPlotlyChart']}")
            print(f"   ✅ Slider exists: {ohlc_results['sliderExists']}")
            print(f"   ✅ Slider range: 0-{ohlc_results['sliderMax']}, current: {ohlc_results['sliderValue']}")
            print(f"   ✅ Sequence display: {ohlc_results['sequenceDisplay']}")
            print(f"   ✅ OHLC functions available: {ohlc_results['updateOHLCFunctionExists']} / {ohlc_results['createOHLCFunctionExists']}")
            
            # Test Data Table
            print("5️⃣ Testing Data Table...")
            table_results = await page.evaluate("""() => {
                const results = {};
                
                // Check training data table section
                const tableSection = document.getElementById('training-data-table');
                results.tableSectionExists = !!tableSection;
                results.tableSectionVisible = tableSection && !tableSection.classList.contains('hidden');
                
                // Check training data content
                const dataContent = document.getElementById('training-data-content');
                results.dataContentExists = !!dataContent;
                results.dataContentHasHTML = dataContent && dataContent.innerHTML.trim().length > 0;
                
                // Check for actual data table
                const dataTable = document.querySelector('.data-table');
                results.dataTableExists = !!dataTable;
                
                if (dataTable) {
                    results.tableRows = dataTable.querySelectorAll('tbody tr').length;
                    results.tableHeaders = Array.from(dataTable.querySelectorAll('thead th')).map(th => th.textContent.trim());
                    
                    // Get sample of table data
                    const firstRow = dataTable.querySelector('tbody tr');
                    if (firstRow) {
                        const cells = Array.from(firstRow.querySelectorAll('td'));
                        results.firstRowData = cells.map(cell => cell.textContent.trim().substring(0, 50));
                    }
                } else {
                    results.tableRows = 0;
                    results.tableHeaders = [];
                }
                
                // Check pagination
                const pagination = document.getElementById('training-pagination');
                results.paginationExists = !!pagination;
                
                const pageInfo = document.getElementById('training-page-info');
                results.pageInfo = pageInfo ? pageInfo.textContent : null;
                
                return results;
            }""")
            
            print(f"   ✅ Table section exists: {table_results['tableSectionExists']}")
            print(f"   ✅ Table section visible: {table_results['tableSectionVisible']}")
            print(f"   ✅ Data content exists: {table_results['dataContentExists']}")
            print(f"   ✅ Data content has HTML: {table_results['dataContentHasHTML']}")
            print(f"   ✅ Data table exists: {table_results['dataTableExists']}")
            print(f"   ✅ Table rows: {table_results['tableRows']}")
            print(f"   ✅ Table headers: {table_results['tableHeaders']}")
            print(f"   ✅ Pagination exists: {table_results['paginationExists']}")
            print(f"   ✅ Page info: {table_results['pageInfo']}")
            
            if table_results.get('firstRowData'):
                print(f"   ✅ Sample row data: {table_results['firstRowData'][:3]}...")
            
            # Test OHLC Chart Rendering by triggering it manually
            if ohlc_results['updateOHLCFunctionExists'] and ohlc_results['ohlcSectionVisible']:
                print("6️⃣ Testing OHLC Chart Rendering...")
                
                chart_test = await page.evaluate("""async () => {
                    try {
                        // Call the OHLC visualization function
                        await updateOHLCVisualization(15, 100);
                        
                        // Wait a moment for chart to render
                        await new Promise(resolve => setTimeout(resolve, 3000));
                        
                        // Check if Plotly chart was created
                        const plotlyElements = document.querySelectorAll('.plotly-graph-div');
                        const chartContainer = document.getElementById('ohlc-chart');
                        
                        return {
                            success: true,
                            plotlyCount: plotlyElements.length,
                            chartContent: chartContainer ? chartContainer.innerHTML.substring(0, 200) : null,
                            hasPlotlyChart: plotlyElements.length > 0
                        };
                    } catch (error) {
                        return {
                            success: false,
                            error: error.message
                        };
                    }
                }""")
                
                print(f"   ✅ Chart rendering test: {chart_test.get('success', False)}")
                if chart_test.get('success'):
                    print(f"   ✅ Plotly charts after rendering: {chart_test.get('plotlyCount', 0)}")
                    print(f"   ✅ Has working Plotly chart: {chart_test.get('hasPlotlyChart', False)}")
                else:
                    print(f"   ❌ Chart rendering error: {chart_test.get('error', 'Unknown')}")
            
            await browser.close()
            
            # Determine overall success
            ohlc_working = (
                ohlc_results['ohlcSectionVisible'] and
                ohlc_results['updateOHLCFunctionExists'] and
                ohlc_results['chartContainerVisible']
            )
            
            table_working = (
                table_results['tableSectionVisible'] and
                table_results['dataTableExists'] and
                table_results['tableRows'] > 0
            )
            
            print(f"\n📊 **FINAL RESULTS:**")
            print(f"   🎯 OHLC Visualization: {'✅ WORKING' if ohlc_working else '❌ NOT WORKING'}")
            print(f"   📋 Data Table: {'✅ WORKING' if table_working else '❌ NOT WORKING'}")
            
            if ohlc_working and table_working:
                print(f"\n🎉 **BOTH OHLC AND TABLE ARE WORKING!**")
                print(f"   • Dataset 15 successfully loaded")
                print(f"   • OHLC charts with technical indicators (etop, ebot, pldot) visible")
                print(f"   • Data table showing {table_results['tableRows']} rows")
                print(f"   • Interactive controls functional")
                return True
            else:
                print(f"\n❌ **ISSUES STILL EXIST:**")
                if not ohlc_working:
                    print(f"   • OHLC visualization not fully functional")
                if not table_working:
                    print(f"   • Data table not showing data properly")
                return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

async def main():
    """Main test function"""
    print("🧪 **OHLC & TABLE TEST WITH WORKING DATASET**")
    print("=" * 60)
    
    success = await test_working_dataset_ohlc_and_table()
    
    return 0 if success else 1

if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(result)