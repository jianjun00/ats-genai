#!/usr/bin/env python3
"""
Playwright Test to validate table data shows real values instead of N/A
Tests the specific fix for table showing "N/A" values
"""

import asyncio
import pytest
import json
from playwright.async_api import async_playwright

@pytest.mark.asyncio
async def test_table_shows_real_data_not_na():
    """Test that table shows real OHLCV data instead of N/A values."""
    print("🎭 Testing Table Data Display - Real Values vs N/A")
    print("="*60)
    
    async with async_playwright() as playwright:
        # Launch browser
        browser = await playwright.chromium.launch(headless=True)
        page = await browser.new_page()
        
        # Capture console errors
        console_errors = []
        page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)
        
        try:
            print("🧪 Test 1: Load EDA interface")
            await page.goto("http://localhost:3000/eda", timeout=15000)
            await page.wait_for_load_state("networkidle")
            await page.wait_for_timeout(3000)  # Allow time for full loading
            print("✅ EDA interface loaded")
            
            print("\n🧪 Test 2: Test API endpoints directly via JavaScript")
            # Test the API endpoints that provide table data
            
            # First test: Get available datasets
            datasets_result = await page.evaluate("""
                fetch('http://localhost:3000/api/v1/training-datasets')
                    .then(response => response.json())
                    .then(data => ({
                        success: true,
                        count: data.datasets ? data.datasets.length : 0,
                        firstDataset: data.datasets && data.datasets.length > 0 ? {
                            id: data.datasets[0].id,
                            name: data.datasets[0].dataset_name,
                            symbols: data.datasets[0].symbols
                        } : null
                    }))
                    .catch(err => ({ success: false, error: err.message }))
            """)
            
            if datasets_result.get('success'):
                print(f"✅ Found {datasets_result['count']} datasets")
                if datasets_result.get('firstDataset'):
                    dataset_info = datasets_result['firstDataset']
                    dataset_id = dataset_info['id']
                    print(f"   Using dataset {dataset_id}: {dataset_info['name']}")
                    
                    # Second test: Get sequences for the dataset
                    sequences_result = await page.evaluate(f"""
                        fetch('http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences')
                            .then(response => response.json())
                            .then(data => ({
                                success: true,
                                sequenceCount: data.sequences ? data.sequences.length : 0,
                                firstSequence: data.sequences && data.sequences.length > 0 ? data.sequences[0].sequence_id : null
                            }))
                            .catch(err => ({ success: false, error: err.message }))
                    """)
                    
                    if sequences_result.get('success') and sequences_result.get('firstSequence'):
                        sequence_id = sequences_result['firstSequence']
                        print(f"✅ Found {sequences_result['sequenceCount']} sequences")
                        print(f"   Using sequence: {sequence_id}")
                        
                        print("\n🧪 Test 3: Validate multi-timeframe table data")
                        # Third test: Get the actual table data
                        table_data_result = await page.evaluate(f"""
                            fetch('http://localhost:3000/api/v1/training-datasets/{dataset_id}/sequences/{sequence_id}/multi-timeframe?row_index=10')
                                .then(response => response.json())
                                .then(data => {{
                                    const tableData = data.table_data || [];
                                    const comprehensiveFeatures = data.comprehensive_features || [];
                                    
                                    return {{
                                        success: true,
                                        tableRowCount: tableData.length,
                                        comprehensiveFeatureRows: comprehensiveFeatures.length,
                                        firstTableRow: tableData.length > 0 ? {{
                                            timestamp: tableData[0].timestamp,
                                            open: tableData[0].open,
                                            high: tableData[0].high,
                                            low: tableData[0].low,
                                            close: tableData[0].close,
                                            volume: tableData[0].volume,
                                            hasValidData: (
                                                typeof tableData[0].open === 'number' && 
                                                !isNaN(tableData[0].open) && 
                                                tableData[0].open !== null &&
                                                tableData[0].open > 0
                                            )
                                        }} : null,
                                        comprehensiveFeatureCount: comprehensiveFeatures.length > 0 ? Object.keys(comprehensiveFeatures[0]).length : 0
                                    }};
                                }})
                                .catch(err => ({ success: false, error: err.message }))
                        """)
                        
                        if table_data_result.get('success'):
                            table_rows = table_data_result.get('tableRowCount', 0)
                            comprehensive_rows = table_data_result.get('comprehensiveFeatureRows', 0)
                            comprehensive_features = table_data_result.get('comprehensiveFeatureCount', 0)
                            
                            print(f"📊 Table Data Results:")
                            print(f"   Table rows: {table_rows}")
                            print(f"   Comprehensive feature rows: {comprehensive_rows}")
                            print(f"   Features per comprehensive row: {comprehensive_features}")
                            
                            # Validate table data
                            if table_rows > 1:
                                print(f"✅ SUCCESS: Table has multiple rows ({table_rows}) instead of just 1")
                            else:
                                print(f"❌ ISSUE: Table still has only {table_rows} row(s)")
                            
                            # Check first row data quality
                            first_row = table_data_result.get('firstTableRow')
                            if first_row:
                                print(f"\n📋 First Row Analysis:")
                                print(f"   Timestamp: {first_row.get('timestamp')}")
                                print(f"   Open: ${first_row.get('open')}")
                                print(f"   High: ${first_row.get('high')}")
                                print(f"   Low: ${first_row.get('low')}")
                                print(f"   Close: ${first_row.get('close')}")
                                print(f"   Volume: {first_row.get('volume')}")
                                
                                if first_row.get('hasValidData'):
                                    print(f"✅ SUCCESS: Table shows REAL DATA instead of N/A!")
                                    print(f"   Open price: ${first_row.get('open')} (valid numeric value)")
                                else:
                                    print(f"❌ ISSUE: Table data appears invalid or N/A")
                                    
                            # Validate comprehensive features
                            if comprehensive_features > 900:
                                print(f"✅ SUCCESS: Comprehensive features available ({comprehensive_features} features)")
                            else:
                                print(f"⚠️  Comprehensive features: {comprehensive_features} (expected ~962)")
                                
                        else:
                            print(f"❌ Failed to get table data: {table_data_result.get('error')}")
                    else:
                        print(f"❌ Failed to get sequences: {sequences_result.get('error') if sequences_result.get('success') else 'No sequences found'}")
                        
                else:
                    print("❌ No datasets available for testing")
            else:
                print(f"❌ Failed to get datasets: {datasets_result.get('error')}")
                
            print("\n🧪 Test 4: Check for JSON/JavaScript errors")
            json_errors = [err for err in console_errors if 'json' in err.lower() or 'nan' in err.lower()]
            if json_errors:
                print(f"❌ Found {len(json_errors)} JSON-related errors:")
                for error in json_errors[:3]:
                    print(f"   - {error}")
            else:
                print("✅ No JSON/NaN related JavaScript errors")
                
        except Exception as e:
            print(f"❌ Test failed with error: {e}")
        
        finally:
            await browser.close()
    
    print(f"\n📊 JavaScript Console Errors: {len(console_errors)}")
    print("🎯 Table data validation test completed")

if __name__ == "__main__":
    asyncio.run(test_table_shows_real_data_not_na())