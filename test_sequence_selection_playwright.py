#!/usr/bin/env python3
"""
Playwright Test: Sequence Selection in EDA Training Dataset Visualization

This test verifies the actual UI sequence selection functionality.
"""

import asyncio
from playwright.async_api import async_playwright
import sys
import os

async def test_sequence_selection():
    """Test sequence selection in the EDA UI with Playwright."""
    
    async with async_playwright() as p:
        # Launch browser
        browser = await p.chromium.launch(headless=True)  # Headless mode for server environment
        context = await browser.new_context()
        page = await context.new_page()
        
        try:
            print("🎭 PLAYWRIGHT: Testing Sequence Selection in EDA")
            print("=" * 60)
            
            # Step 1: Navigate to EDA tool
            print("1️⃣ Navigating to EDA tool...")
            await page.goto("http://localhost:3000")
            await page.wait_for_load_state("networkidle")
            
            # Step 2: Navigate to Training Datasets tab
            print("2️⃣ Clicking Training Datasets tab...")
            training_button = page.get_by_role("button", name="🤖 Training Datasets")
            if await training_button.count() > 0:
                await training_button.click()
                await page.wait_for_timeout(2000)
            else:
                print("❌ Training Datasets button not found")
                return False
            
            # Step 3: Look for dataset dropdown
            print("3️⃣ Looking for dataset selection dropdown...")
            all_selects = page.locator("select")
            if await all_selects.count() > 0:
                dataset_dropdown = all_selects.first()
                # Get available options
                options = await dataset_dropdown.locator("option").all_text_contents()
                print(f"   Available datasets: {len(options)}")
                for i, option in enumerate(options[:3]):
                    print(f"     {i}: {option}")
                
                # Select dataset 63 if available
                if any("63" in option for option in options):
                    print("   ✅ Found dataset 63")
                    await dataset_dropdown.select_option(value="63")
                    await page.wait_for_timeout(3000)
                else:
                    print("   ⚠️  Dataset 63 not found, selecting first available")
                    if len(options) > 1:  # Skip empty option
                        await dataset_dropdown.select_option(index=1)
                        await page.wait_for_timeout(3000)
            else:
                print("❌ Dataset dropdown not found")
                return False
            
            # Step 4: Look for sequence selection dropdown
            print("4️⃣ Looking for sequence selection dropdown...")
            
            # Try different possible selectors for sequence dropdown
            sequence_selectors = [
                "select[id*='sequence']",
                "select[name*='sequence']", 
                "#sequenceSelect",
                ".sequence-select",
                "select:has(option[value*='AAPL'])"
            ]
            
            sequence_dropdown = None
            for selector in sequence_selectors:
                dropdown = page.locator(selector)
                if await dropdown.count() > 0:
                    sequence_dropdown = dropdown.first()
                    print(f"   ✅ Found sequence dropdown: {selector}")
                    break
            
            if not sequence_dropdown:
                # Look for any select element that might contain sequences
                all_selects = page.locator("select")
                select_count = await all_selects.count()
                print(f"   🔍 Found {select_count} select elements total")
                
                for i in range(select_count):
                    select_elem = all_selects.nth(i)
                    options = await select_elem.locator("option").all_text_contents()
                    print(f"     Select {i}: {len(options)} options")
                    
                    # Check if this looks like a sequence dropdown
                    if any("AAPL_" in str(option) or "sequence" in str(option).lower() for option in options):
                        sequence_dropdown = select_elem
                        print(f"   ✅ Found sequence dropdown (select {i})")
                        break
                    elif len(options) > 1:
                        print(f"       Sample options: {options[:3]}")
            
            if sequence_dropdown:
                # Get sequence options
                options = await sequence_dropdown.locator("option").all_text_contents()
                print(f"   📋 Sequence options: {len(options)}")
                for i, option in enumerate(options[:5]):
                    print(f"     {i}: {option}")
                
                # Try to select a sequence
                if len(options) > 1:
                    # Look for AAPL sequence
                    aapl_option = None
                    for i, option in enumerate(options):
                        if "AAPL_" in str(option):
                            aapl_option = i
                            break
                    
                    if aapl_option:
                        print(f"   🎯 Selecting AAPL sequence: {options[aapl_option]}")
                        await sequence_dropdown.select_option(index=aapl_option)
                        await page.wait_for_timeout(3000)
                    else:
                        print("   🎯 Selecting first sequence option")
                        await sequence_dropdown.select_option(index=1)
                        await page.wait_for_timeout(3000)
                
                # Step 5: Check if visualization loaded
                print("5️⃣ Checking if visualization loaded...")
                
                # Look for charts or data tables
                chart_selectors = [
                    "#chart",
                    ".chart",
                    "canvas",
                    "svg",
                    ".plotly-graph-div",
                    "table",
                    ".data-table"
                ]
                
                visualization_found = False
                for selector in chart_selectors:
                    elements = page.locator(selector)
                    count = await elements.count()
                    if count > 0:
                        print(f"   ✅ Found visualization: {count} {selector} elements")
                        visualization_found = True
                
                if not visualization_found:
                    # Check for any error messages
                    error_messages = page.locator("text=/error|Error|no.*data|No.*data|failed|Failed/i")
                    if await error_messages.count() > 0:
                        error_text = await error_messages.first().text_content()
                        print(f"   ❌ Error message found: {error_text}")
                    else:
                        print("   ⚠️  No visualization or error messages found")
                
                # Step 6: Take a screenshot for debugging
                print("6️⃣ Taking screenshot for debugging...")
                await page.screenshot(path="/tmp/sequence_selection_test.png")
                print("   📸 Screenshot saved to /tmp/sequence_selection_test.png")
                
                return visualization_found
                
            else:
                print("❌ No sequence dropdown found")
                
                # Take screenshot of current state
                await page.screenshot(path="/tmp/no_sequence_dropdown.png")
                print("   📸 Screenshot saved to /tmp/no_sequence_dropdown.png")
                
                # Get page content for debugging
                content = await page.content()
                with open("/tmp/page_content.html", "w") as f:
                    f.write(content)
                print("   📄 Page content saved to /tmp/page_content.html")
                
                return False
                
        except Exception as e:
            print(f"❌ Playwright test failed: {e}")
            await page.screenshot(path="/tmp/playwright_error.png")
            return False
            
        finally:
            await browser.close()

async def main():
    """Run the Playwright test."""
    print("🎭 Starting Playwright test for sequence selection...")
    
    success = await test_sequence_selection()
    
    if success:
        print("\n✅ PLAYWRIGHT TEST PASSED!")
        print("   Sequence selection is working in the UI")
    else:
        print("\n❌ PLAYWRIGHT TEST FAILED!")
        print("   Sequence selection is not working properly")
        print("   Check screenshots in /tmp/ for debugging")
    
    return success

if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(0 if result else 1)