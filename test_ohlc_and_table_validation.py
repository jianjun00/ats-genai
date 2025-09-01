#!/usr/bin/env python3
"""
Playwright test to validate OHLC visualization and table view issues
"""

import asyncio
import sys

async def test_ohlc_and_table_visibility():
    """Test specifically for OHLC charts and data table visibility"""
    try:
        from playwright.async_api import async_playwright
        
        print("🎭 Testing OHLC Visualization and Table Visibility...")
        
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
            
            # Get datasets and select one with technical indicators
            print("2️⃣ Finding dataset with technical indicators...")
            dataset_info = await page.evaluate("""async () => {
                // Wait for datasets to load
                await new Promise(resolve => setTimeout(resolve, 2000));
                
                const select = document.getElementById('training-dataset-select');
                if (!select) return {error: 'No select element found'};
                
                const options = Array.from(select.options).filter(opt => opt.value !== '');
                
                // Look for dataset with technical indicators
                for (let option of options) {
                    if (option.textContent.includes('aapl') || option.textContent.includes('AAPL')) {
                        return {
                            value: option.value,
                            text: option.textContent,
                            found: true
                        };
                    }
                }
                
                return {
                    value: options.length > 0 ? options[0].value : null,
                    text: options.length > 0 ? options[0].textContent : 'No datasets',
                    found: options.length > 0,
                    totalOptions: options.length
                };
            }""")
            
            if not dataset_info.get('found'):
                print(f"❌ No datasets available: {dataset_info}")
                return False
                
            print(f"✅ Found dataset: {dataset_info['text']}")
            
            # Select the dataset
            print("3️⃣ Selecting dataset and waiting for analysis...")
            await page.select_option('#training-dataset-select', dataset_info['value'])
            
            # Wait longer for analysis to complete
            await page.wait_for_timeout(8000)
            
            # Check for JavaScript errors
            errors = [msg for msg in console_messages if 'error' in msg.lower() or 'PAGE_ERROR' in msg]
            if errors:
                print("⚠️ JavaScript/Page Errors:")
                for error in errors[-5:]:  # Show last 5 errors
                    print(f"     {error}")
            
            # Test 1: Check if OHLC visualization containers exist and are visible
            print("4️⃣ Testing OHLC Visualization Visibility...")
            ohlc_test_results = await page.evaluate("""() => {
                const results = {
                    ohlcContainers: [],
                    ohlcVisible: false,
                    ohlcChartElements: 0,
                    plotlyElements: 0,
                    ohlcControls: 0,
                    ohlcFunctionsExists: false
                };
                
                // Look for OHLC chart containers
                const ohlcContainers = document.querySelectorAll('[id*="ohlc-chart"]');
                results.ohlcContainers = Array.from(ohlcContainers).map(el => ({
                    id: el.id,
                    visible: getComputedStyle(el).display !== 'none' && el.offsetHeight > 0
                }));
                results.ohlcChartElements = ohlcContainers.length;
                
                // Check for Plotly elements (created when charts render)
                results.plotlyElements = document.querySelectorAll('.plotly-graph-div').length;
                
                // Check for OHLC controls (sliders, buttons)
                const ohlcControls = document.querySelectorAll('[id*="sequence-slider"], button[onclick*="OHLC"]');
                results.ohlcControls = ohlcControls.length;
                
                // Check if OHLC functions exist in global scope
                results.ohlcFunctionsExists = typeof window.updateOHLCVisualization === 'function';
                
                // Check if any OHLC container is actually visible
                results.ohlcVisible = results.ohlcContainers.some(c => c.visible);
                
                return results;
            }""")
            
            print(f"✅ OHLC chart containers found: {ohlc_test_results['ohlcChartElements']}")
            print(f"✅ OHLC containers visible: {ohlc_test_results['ohlcVisible']}")
            print(f"✅ Plotly chart elements: {ohlc_test_results['plotlyElements']}")
            print(f"✅ OHLC controls found: {ohlc_test_results['ohlcControls']}")
            print(f"✅ OHLC functions available: {ohlc_test_results['ohlcFunctionsExists']}")
            
            if ohlc_test_results['ohlcContainers']:
                print("   OHLC Container Details:")
                for container in ohlc_test_results['ohlcContainers']:
                    print(f"     - {container['id']}: visible={container['visible']}")
            
            # Test 2: Check if data table is visible with actual content
            print("5️⃣ Testing Data Table Visibility...")
            table_test_results = await page.evaluate("""() => {
                const results = {
                    tableExists: false,
                    tableVisible: false,
                    tableRows: 0,
                    tableContent: '',
                    trainingDataContent: false,
                    analysisVisible: false,
                    hiddenElements: []
                };
                
                // Check main data table
                const dataTable = document.querySelector('.data-table');
                if (dataTable) {
                    results.tableExists = true;
                    results.tableVisible = getComputedStyle(dataTable).display !== 'none' && dataTable.offsetHeight > 0;
                    results.tableRows = dataTable.querySelectorAll('tbody tr').length;
                    results.tableContent = dataTable.textContent.substring(0, 200) + '...';
                }
                
                // Check training data content div
                const trainingContent = document.getElementById('training-data-content');
                if (trainingContent) {
                    results.trainingDataContent = trainingContent.innerHTML.trim().length > 0;
                    if (!results.trainingDataContent) {
                        results.trainingDataContentHTML = trainingContent.innerHTML;
                    }
                }
                
                // Check if analysis sections are visible
                const analysisSection = document.getElementById('dataset-analysis');
                const tableSection = document.getElementById('training-data-table');
                
                results.analysisVisible = analysisSection && !analysisSection.classList.contains('hidden');
                results.tableVisible = tableSection && !tableSection.classList.contains('hidden');
                
                // Find all hidden elements that might be important
                const hiddenElements = document.querySelectorAll('.hidden');
                results.hiddenElements = Array.from(hiddenElements).map(el => ({
                    id: el.id || el.className,
                    tagName: el.tagName
                }));
                
                return results;
            }""")
            
            print(f"✅ Data table exists: {table_test_results['tableExists']}")
            print(f"✅ Data table visible: {table_test_results['tableVisible']}")
            print(f"✅ Table rows count: {table_test_results['tableRows']}")
            print(f"✅ Training data content populated: {table_test_results['trainingDataContent']}")
            print(f"✅ Analysis section visible: {table_test_results['analysisVisible']}")
            
            if table_test_results['hiddenElements']:
                print("   Hidden elements:")
                for elem in table_test_results['hiddenElements'][:5]:
                    print(f"     - {elem['tagName']}: {elem['id']}")
            
            # Test 3: Check what happens when we try to trigger OHLC visualization
            print("6️⃣ Testing OHLC Function Calls...")
            if ohlc_test_results['ohlcFunctionsExists']:
                ohlc_function_test = await page.evaluate(f"""async () => {{
                    try {{
                        // Try to call the OHLC visualization function
                        if (typeof updateOHLCVisualization === 'function') {{
                            await updateOHLCVisualization({dataset_info['value']}, 0);
                            return {{
                                success: true,
                                message: 'OHLC function called successfully'
                            }};
                        }} else {{
                            return {{
                                success: false,
                                message: 'updateOHLCVisualization function not found'
                            }};
                        }}
                    }} catch (e) {{
                        return {{
                            success: false,
                            error: e.message
                        }};
                    }}
                }}""")
                
                print(f"   OHLC function test: {ohlc_function_test}")
            else:
                print("   ❌ OHLC functions not available")
            
            # Test 4: Check API endpoints that should provide OHLC data
            print("7️⃣ Testing OHLC API Endpoints...")
            api_test = await page.evaluate(f"""async () => {{
                try {{
                    const response = await fetch('/api/v1/training-datasets/{dataset_info['value']}/visualization-data?sequence_index=0');
                    if (response.ok) {{
                        const data = await response.json();
                        return {{
                            success: true,
                            hasData: data && data.data && data.data.length > 0,
                            sampleData: data && data.data ? data.data[0] : null,
                            hasTechnicalIndicators: data && data.data && data.data[0] && 
                                                   (data.data[0].etop !== undefined || data.data[0].ebot !== undefined)
                        }};
                    }} else {{
                        return {{
                            success: false,
                            status: response.status,
                            statusText: response.statusText
                        }};
                    }}
                }} catch (e) {{
                    return {{
                        success: false,
                        error: e.message
                    }};
                }}
            }}""")
            
            print(f"✅ OHLC API working: {api_test.get('success', False)}")
            if api_test.get('success'):
                print(f"✅ API has visualization data: {api_test.get('hasData', False)}")
                print(f"✅ API has technical indicators: {api_test.get('hasTechnicalIndicators', False)}")
                if api_test.get('sampleData'):
                    sample = api_test['sampleData']
                    print(f"   Sample data keys: {list(sample.keys())[:10]}...")
            else:
                print(f"❌ API Error: {api_test}")
            
            await browser.close()
            
            # Determine if tests passed
            ohlc_working = (
                ohlc_test_results['ohlcVisible'] and 
                ohlc_test_results['plotlyElements'] > 0
            ) or api_test.get('success', False)
            
            table_working = (
                table_test_results['tableExists'] and 
                table_test_results['tableVisible'] and 
                table_test_results['tableRows'] > 0
            )
            
            print(f"\n📊 **RESULTS SUMMARY:**")
            print(f"   OHLC Visualization Working: {'✅' if ohlc_working else '❌'}")
            print(f"   Data Table Working: {'✅' if table_working else '❌'}")
            
            return ohlc_working and table_working
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

async def main():
    """Main validation function"""
    print("🧪 **OHLC AND TABLE VALIDATION TEST**")
    print("=" * 60)
    
    success = await test_ohlc_and_table_visibility()
    
    if success:
        print("\n✅ **BOTH OHLC AND TABLE ARE WORKING!**")
    else:
        print("\n❌ **ISSUES FOUND - OHLC AND/OR TABLE NOT WORKING**")
        print("\n📋 **Issues to fix:**")
        print("  1. OHLC visualization not showing up")
        print("  2. Data table not visible or empty")
        print("  3. Missing integration between dataset selection and visualization")
        
    return 0 if success else 1

if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(result)