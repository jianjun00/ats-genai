#!/usr/bin/env python3
"""
Use Playwright to test the actual sequence selector behavior in the browser.
This will show us what the user actually sees.
"""
import asyncio
import sys
import os
from playwright.async_api import async_playwright

async def test_sequence_selector_browser():
    """Test sequence selector using actual browser automation."""
    print("🎭 Testing Sequence Selector with Playwright (Real Browser Test)")
    print("=" * 70)
    
    async with async_playwright() as p:
        # Launch browser
        browser = await p.chromium.launch(headless=True)  # Run in headless mode
        context = await browser.new_context()
        page = await context.new_page()
        
        try:
            # Step 1: Navigate to EDA page
            print("\n1️⃣ Loading EDA page...")
            await page.goto("http://localhost:3000/eda", timeout=30000)
            await page.wait_for_load_state("networkidle")
            print("   ✅ EDA page loaded")
            
            # Step 2: Wait for dataset selector to populate
            print("\n2️⃣ Waiting for dataset selector to populate...")
            dataset_selector = page.locator("#dataset-selector")
            await dataset_selector.wait_for(timeout=10000)
            
            # Check dataset options
            dataset_options = await dataset_selector.locator("option").all()
            dataset_count = len(dataset_options)
            print(f"   📊 Found {dataset_count} dataset options:")
            
            for i, option in enumerate(dataset_options):
                text = await option.text_content()
                value = await option.get_attribute("value")
                print(f"      {i}: '{text}' (value: {value})")
            
            # Step 3: Select a dataset (skip default empty option)
            valid_datasets = [opt for opt in dataset_options if await opt.get_attribute("value")]
            
            if not valid_datasets:
                print("   ❌ No valid datasets found!")
                return False
            
            first_dataset = valid_datasets[0]
            dataset_text = await first_dataset.text_content()
            dataset_value = await first_dataset.get_attribute("value")
            
            print(f"\n3️⃣ Selecting dataset: '{dataset_text}' (ID: {dataset_value})")
            await dataset_selector.select_option(dataset_value)
            
            # Wait for the change to process
            await page.wait_for_timeout(2000)
            
            # Step 4: Check sequence selector
            print(f"\n4️⃣ Checking sequence selector after dataset selection...")
            sequence_selector = page.locator("#sequence-selector")
            
            # Wait for sequence selector to potentially update
            await page.wait_for_timeout(3000)
            
            # Check sequence options
            sequence_options = await sequence_selector.locator("option").all()
            sequence_count = len(sequence_options)
            print(f"   🔢 Found {sequence_count} sequence options:")
            
            for i, option in enumerate(sequence_options):
                text = await option.text_content()
                value = await option.get_attribute("value")
                print(f"      {i}: '{text}' (value: '{value}')")
                
                # Check for the "no sequences found" message
                if "no sequences found" in text.lower():
                    print(f"   ❌ PROBLEM CONFIRMED: Browser shows 'No sequences found'")
                    
            # Step 5: Check network requests
            print(f"\n5️⃣ Checking what network requests were made...")
            
            # Listen for API calls
            api_calls = []
            
            def handle_request(request):
                if "/api/" in request.url:
                    api_calls.append({
                        "url": request.url,
                        "method": request.method
                    })
            
            page.on("request", handle_request)
            
            # Trigger dataset selection again to capture API calls
            print("   🔄 Re-selecting dataset to capture API calls...")
            await dataset_selector.select_option("")  # Clear selection
            await page.wait_for_timeout(1000)
            await dataset_selector.select_option(dataset_value)  # Re-select
            await page.wait_for_timeout(3000)
            
            print(f"   📡 API calls made:")
            for call in api_calls[-5:]:  # Show last 5 calls
                print(f"      {call['method']} {call['url']}")
            
            # Step 6: Check JavaScript console for errors
            print(f"\n6️⃣ Checking browser console for errors...")
            
            # Get console messages
            console_messages = []
            
            def handle_console(msg):
                console_messages.append(f"{msg.type}: {msg.text}")
            
            page.on("console", handle_console)
            
            # Trigger some activity to generate console messages
            await page.reload()
            await page.wait_for_load_state("networkidle")
            await dataset_selector.select_option(dataset_value)
            await page.wait_for_timeout(2000)
            
            if console_messages:
                print("   📝 Console messages (last 5):")
                for msg in console_messages[-5:]:
                    print(f"      {msg}")
            else:
                print("   ✅ No console errors")
            
            # Step 7: Check final sequence selector state
            print(f"\n7️⃣ Final sequence selector state:")
            final_sequence_options = await sequence_selector.locator("option").all()
            
            has_valid_sequences = False
            for option in final_sequence_options:
                text = await option.text_content()
                value = await option.get_attribute("value")
                print(f"      '{text}' (value: '{value}')")
                
                if value and value != "" and "no sequences" not in text.lower():
                    has_valid_sequences = True
            
            if has_valid_sequences:
                print("   ✅ SUCCESS: Valid sequences found in browser!")
                return True
            else:
                print("   ❌ FAILURE: No valid sequences in browser")
                print("   🔧 This means the frontend JavaScript is not properly")
                print("      processing the API response with total_sequences=1")
                return False
        
        except Exception as e:
            print(f"   ❌ Browser test failed: {e}")
            return False
        
        finally:
            await browser.close()

async def test_api_vs_browser():
    """Compare API response vs browser behavior."""
    print("\n🔍 API vs Browser Comparison")
    print("=" * 40)
    
    # Test API directly
    import requests
    
    print("📡 Testing API directly...")
    response = requests.get("http://localhost:3000/api/v1/training-datasets/40/visualization-data")
    if response.status_code == 200:
        data = response.json()
        total_sequences = data.get("total_sequences", 0)
        data_count = len(data.get("data", []))
        print(f"   API reports: total_sequences={total_sequences}, data_count={data_count}")
    else:
        print(f"   API failed: {response.status_code}")
    
    # Test browser behavior
    print("🎭 Testing browser behavior...")
    browser_success = await test_sequence_selector_browser()
    
    print(f"\n📊 Comparison Results:")
    print(f"   API Response: total_sequences={total_sequences}")  
    print(f"   Browser Shows: {'Valid sequences' if browser_success else 'No sequences found'}")
    
    if total_sequences > 0 and not browser_success:
        print(f"\n🐛 BUG CONFIRMED:")
        print(f"   • API correctly returns total_sequences={total_sequences}")
        print(f"   • But browser still shows 'No sequences found'")
        print(f"   • Issue is in frontend JavaScript processing")
        
        return False
    
    return browser_success

async def main():
    """Run Playwright sequence selector test."""
    try:
        success = await test_api_vs_browser()
        
        if success:
            print(f"\n✅ SEQUENCE SELECTOR WORKS IN BROWSER")
        else:
            print(f"\n❌ SEQUENCE SELECTOR BROKEN IN BROWSER")
            print(f"🔧 Need to fix frontend JavaScript logic")
        
        return success
    except Exception as e:
        print(f"\n❌ Playwright test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    # Install playwright if needed
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("Installing playwright...")
        os.system("pip install playwright")
        os.system("playwright install chromium")
        from playwright.async_api import async_playwright
    
    success = asyncio.run(main())
    exit(0 if success else 1)