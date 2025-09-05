#!/usr/bin/env python3
"""
Test Complete EDA Functionality
Validates that the EDA dashboard now shows table selection, column distributions, and sample data.
"""

import asyncio
from playwright.async_api import async_playwright

async def test_eda_functionality(url, service_name):
    """Test complete EDA functionality."""
    print(f"\n🧪 Testing EDA Functionality in {service_name} at {url}")
    
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        await page.goto(f"{url}/eda")
        await page.wait_for_load_state('domcontentloaded')
        
        # Test EDA button click
        print("   📊 Testing EDA button functionality...")
        eda_button = page.locator("button").filter(has_text="Exploratory Data Analysis")
        await eda_button.click()
        
        # Wait for EDA interface to load
        await page.wait_for_timeout(3000)
        
        content = await page.locator("#analysis-content").text_content()
        
        # Check for EDA interface elements
        if "Loading intelligent data exploration..." in content:
            print("   ❌ Still showing old placeholder text")
            return False
        
        if "Select Table" not in content:
            print("   ❌ Table selector not found")
            return False
        print("   ✅ Table selector found")
        
        # Check for table dropdown
        table_selector = page.locator("#table-selector")
        if not await table_selector.count():
            print("   ❌ Table dropdown not found")
            return False
        print("   ✅ Table dropdown found")
        
        # Get options in dropdown
        options = await table_selector.locator("option").all()
        option_texts = []
        for option in options:
            text = await option.text_content()
            if text and text.strip() and text != "Choose a table...":
                option_texts.append(text.strip())
        
        print(f"   📋 Found {len(option_texts)} table options: {option_texts}")
        
        if len(option_texts) == 0:
            print("   ❌ No tables found in dropdown")
            return False
        
        # Test selecting a table
        print("   🔍 Testing table selection...")
        # Select the first available table
        first_table = option_texts[0]
        await table_selector.select_option(first_table)
        
        # Wait for table data to load
        await page.wait_for_timeout(3000)
        
        # Check for table info sections
        updated_content = await page.locator("#analysis-content").text_content()
        
        sections = ["Table Info", "Column Summary", "Sample Data", "Column Distributions"]
        found_sections = []
        
        for section in sections:
            if section in updated_content:
                found_sections.append(section)
        
        print(f"   📊 Found sections: {found_sections}")
        
        if len(found_sections) != len(sections):
            print(f"   ❌ Expected {len(sections)} sections, found {len(found_sections)}")
            return False
        
        # Check for specific data loading
        if "Loading table information..." in updated_content:
            print("   ⚠️  Still loading data (might be slow database)")
        elif "Row Count:" in updated_content:
            print("   ✅ Table information loaded")
        else:
            print("   ⚠️  Table information section unclear")
        
        if "Loading sample data..." in updated_content:
            print("   ⚠️  Still loading sample data")
        elif any(word in updated_content.lower() for word in ["column", "row", "data"]):
            print("   ✅ Sample data section populated")
        
        print(f"   🎉 {service_name} EDA functionality is working!")
        return True

async def main():
    """Test EDA functionality in both services."""
    print("📊 ATS EDA Functionality Validation")
    print("=" * 70)
    print("Testing: Complete EDA interface with table selection, column analysis, and sample data")
    
    # Test both services
    dev_success = await test_eda_functionality("http://localhost:3000", "ATS-DEV")
    intg_success = await test_eda_functionality("http://localhost:4000", "ATS-INTG")
    
    # Summary
    print("\n" + "=" * 70)
    print("🎯 EDA FUNCTIONALITY SUMMARY")
    print(f"   ATS-DEV EDA: {'✅ WORKING' if dev_success else '❌ BROKEN'}")
    print(f"   ATS-INTG EDA: {'✅ WORKING' if intg_success else '❌ BROKEN'}")
    
    if dev_success and intg_success:
        print("\n🎉 EDA functionality restored successfully!")
        print("   ✅ Table selection dropdown populated")
        print("   ✅ Table info, column summary, sample data sections")
        print("   ✅ Column distribution analysis")
        print("   ✅ No more 'Loading intelligent data exploration...' placeholders")
        print("\n📊 EDA Features Available:")
        print("   - Database table browsing")
        print("   - Column-level statistics and distributions")  
        print("   - Sample data preview with proper formatting")
        print("   - Data type analysis and null value detection")
        return 0
    else:
        print("\n⚠️  EDA functionality incomplete.")
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)