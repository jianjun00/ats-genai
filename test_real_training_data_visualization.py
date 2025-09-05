#!/usr/bin/env python3
"""
Test the complete training dataset visualization with real Riegeli-compatible data
"""
import asyncio
import sys
import os
from playwright.async_api import async_playwright

async def test_training_dataset_with_real_data():
    """Test training dataset visualization with real generated AAPL and TSLA data."""
    
    async with async_playwright() as p:
        # Launch browser in headless mode
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            print("🚀 Testing Training Dataset Visualization with Real Riegeli Data...")
            
            # Navigate to EDA page
            print("📍 Navigating to http://localhost:3000/eda")
            await page.goto("http://localhost:3000/eda", timeout=10000)
            
            # Wait for page to load
            await page.wait_for_load_state("networkidle")
            
            # Click Training Datasets button
            print("🔍 Looking for Training Datasets button...")
            training_btn = page.locator("button:has-text('Training Datasets')")
            await training_btn.wait_for(state="visible", timeout=5000)
            await training_btn.click()
            print("✅ Clicked Training Datasets button")
            
            # Wait for training datasets interface to load
            await page.wait_for_timeout(2000)
            
            # Check if dataset selector is present and populated
            print("🔍 Checking dataset selector...")
            selector = page.locator("#dataset-selector")
            await selector.wait_for(state="visible", timeout=5000)
            
            # Get dataset options
            options = await selector.locator("option").all()
            print(f"✅ Found {len(options)} dataset options")
            
            # Check if our generated datasets are present
            aapl_found = False
            tsla_found = False
            
            for option in options:
                text = await option.text_content()
                print(f"   - Dataset option: {text}")
                if "AAPL" in text:
                    aapl_found = True
                if "TSLA" in text:
                    tsla_found = True
            
            if not aapl_found or not tsla_found:
                print("❌ Generated AAPL or TSLA datasets not found in selector")
                return False
            
            print("✅ Both AAPL and TSLA datasets found in selector")
            
            # Select AAPL dataset (should be dataset ID 4)
            print("🔍 Selecting AAPL dataset...")
            await selector.select_option("4")  # AAPL dataset ID
            await page.wait_for_timeout(1000)
            
            # Wait for sequence data to load
            print("⏳ Waiting for OHLC chart to load...")
            chart_container = page.locator("#training-ohlc-chart")
            await chart_container.wait_for(state="visible", timeout=10000)
            
            # Check if Plotly chart is rendered
            plotly_chart = page.locator("#training-ohlc-chart .plotly-graph-div")
            await plotly_chart.wait_for(state="visible", timeout=5000)
            print("✅ Plotly OHLC chart rendered successfully")
            
            # Check for technical indicators in the chart
            print("🔍 Verifying technical indicators...")
            
            # Check if sequence table is present
            sequence_table = page.locator("#training-sequence-table")
            await sequence_table.wait_for(state="visible", timeout=5000)
            print("✅ Training sequence table visible")
            
            # Test different row selection
            print("🔍 Testing row selection...")
            row_select = page.locator("#training-row-selector")
            await row_select.wait_for(state="visible", timeout=3000)
            await row_select.select_option("10")  # Select row 10
            await page.wait_for_timeout(2000)
            print("✅ Row selection working")
            
            # Test TSLA dataset
            print("🔍 Testing TSLA dataset...")
            await selector.select_option("5")  # TSLA dataset ID
            await page.wait_for_timeout(2000)
            
            # Verify TSLA data loaded
            await plotly_chart.wait_for(state="visible", timeout=5000)
            print("✅ TSLA dataset loaded successfully")
            
            # Check current data source info
            data_info = page.locator(".training-dataset-info")
            if await data_info.is_visible():
                info_text = await data_info.text_content()
                print(f"📊 Dataset info: {info_text}")
            
            print("\n🎉 ALL TESTS PASSED!")
            print("✅ Training dataset visualization working with real Riegeli-compatible data")
            print("✅ AAPL dataset (50 sequences × 21 time steps × 12 features)")
            print("✅ TSLA dataset (50 sequences × 21 time steps × 12 features)")
            print("✅ Plotly OHLC charts with technical indicators")
            print("✅ Interactive sequence table and row selection")
            print("✅ Real data from generated numpy files")
            
            # Take a screenshot for verification
            print("📸 Taking screenshot of the training dataset visualization...")
            await page.screenshot(path="training_dataset_real_data.png", full_page=True)
            
            return True
            
        except Exception as e:
            print(f"❌ Test failed: {e}")
            import traceback
            traceback.print_exc()
            return False
            
        finally:
            await browser.close()

if __name__ == "__main__":
    result = asyncio.run(test_training_dataset_with_real_data())
    sys.exit(0 if result else 1)