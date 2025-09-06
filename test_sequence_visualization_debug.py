#!/usr/bin/env python3
"""
Debug script for training dataset sequence visualization issues.

Investigates the "No sequence data available" problem where Plotly charts
and sequence tables don't show up properly.
"""

import asyncio
import pytest
from playwright.async_api import async_playwright
import json

async def test_training_dataset_sequence_visualization_debug():
    """Debug training dataset sequence visualization with detailed logging."""
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, slow_mo=1000)  # Visible browser with slow motion
        context = await browser.new_context()
        page = await context.new_page()
        
        # Enable console logging to see JavaScript errors
        page.on("console", lambda msg: print(f"[CONSOLE {msg.type.upper()}] {msg.text}"))
        page.on("pageerror", lambda error: print(f"[PAGE ERROR] {error}"))
        
        try:
            print("🔍 Step 1: Navigate to analytics service")
            await page.goto("http://localhost:3000")
            await page.wait_for_timeout(2000)
            
            print("🔍 Step 2: Click on Training Datasets")
            training_datasets_link = page.locator('a[href="/api/v1/training-datasets"]')
            await training_datasets_link.click()
            await page.wait_for_timeout(3000)
            
            print("🔍 Step 3: Look for available datasets")
            # Wait for datasets to load
            await page.wait_for_selector("table", timeout=10000)
            
            # Check if any datasets are available
            dataset_rows = page.locator("tbody tr")
            row_count = await dataset_rows.count()
            print(f"🔍 Found {row_count} dataset rows")
            
            if row_count == 0:
                print("❌ No training datasets found - creating test data...")
                # We'll need to create test data first
                await browser.close()
                return
            
            print("🔍 Step 4: Click on first dataset to view details")
            first_row = dataset_rows.first
            await first_row.click()
            await page.wait_for_timeout(3000)
            
            print("🔍 Step 5: Check for dataset detail view")
            # Look for sequence data section
            sequence_section = page.locator('text="Training Sequence Data"')
            if await sequence_section.count() > 0:
                print("✅ Found Training Sequence Data section")
            else:
                print("❌ Training Sequence Data section not found")
                
            print("🔍 Step 6: Check for 'No sequence data available' message")
            no_data_message = page.locator('text="No sequence data available"')
            if await no_data_message.count() > 0:
                print("❌ Found 'No sequence data available' message")
                
                # Let's investigate the API calls
                print("🔍 Step 7: Check network requests")
                
                # Look for any sequence data API calls
                await page.wait_for_timeout(2000)
                
                # Try to find the sequence data table or Plotly container
                plotly_div = page.locator('[id*="plotly"]')
                plotly_count = await plotly_div.count()
                print(f"🔍 Found {plotly_count} Plotly containers")
                
                sequence_table = page.locator('table[class*="sequence"], .sequence-data')
                table_count = await sequence_table.count()
                print(f"🔍 Found {table_count} sequence data tables")
                
            else:
                print("✅ No 'No sequence data available' message found")
                
            print("🔍 Step 8: Check for JavaScript errors or missing data")
            # Take a screenshot for debugging
            await page.screenshot(path="training_dataset_debug.png")
            print("📸 Screenshot saved as training_dataset_debug.png")
            
            # Wait to allow manual inspection
            print("🔍 Waiting 10 seconds for manual inspection...")
            await page.wait_for_timeout(10000)
            
        except Exception as e:
            print(f"❌ Error during testing: {e}")
            await page.screenshot(path="training_dataset_error.png")
            
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_training_dataset_sequence_visualization_debug())