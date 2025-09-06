#!/usr/bin/env python3
"""
Simple UI test to verify functions are working
"""

import asyncio
from playwright.async_api import async_playwright

async def test_ui_simple():
    print("🧪 SIMPLE UI TEST")
    print("=" * 25)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            print("📊 Loading homepage...")
            await page.goto("http://localhost:3000", wait_until="networkidle")
            
            print("📊 Checking JavaScript functions...")
            functions_exist = await page.evaluate("""
                () => {
                    const functions = [
                        'loadTrainingDatasets', 
                        'loadUniverseAnalytics',
                        'loadEDA'
                    ];
                    
                    const results = {};
                    for (const func of functions) {
                        results[func] = typeof window[func] === 'function';
                    }
                    return results;
                }
            """)
            
            print("Function check results:")
            all_good = True
            for func, exists in functions_exist.items():
                print(f"   {func}: {'✅ OK' if exists else '❌ Missing'}")
                if not exists:
                    all_good = False
            
            if not all_good:
                return False
                
            print("📊 Testing Training Datasets click...")
            training_button = page.get_by_role("button", name="🤖 Training Datasets")
            await training_button.click()
            await page.wait_for_timeout(2000)
            
            # Check if content loaded
            analysis_content = await page.locator("#analysis-content").inner_html()
            
            if "Training Datasets" in analysis_content:
                print("✅ Training Datasets loaded")
                
                # Check for dropdown
                dropdown_exists = await page.locator("#dataset-selector").count() > 0
                if dropdown_exists:
                    print("✅ Dataset dropdown found")
                    
                    # Get options
                    options = await page.locator("#dataset-selector option").all_text_contents()
                    print(f"   Found {len(options)} dataset options")
                    
                    # Look for dataset 63
                    has_63 = any("63" in opt for opt in options)
                    if has_63:
                        print("✅ Dataset 63 available")
                        return True
                    else:
                        print("❌ Dataset 63 not found")
                        print(f"   Available options: {options}")
                        return False
                else:
                    print("❌ Dataset dropdown not found")
                    print(f"   Content: {analysis_content[:300]}")
                    return False
            else:
                print("❌ Training Datasets not loaded")
                print(f"   Content: {analysis_content[:300]}")
                return False
                
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
            
        finally:
            await browser.close()

if __name__ == "__main__":
    result = asyncio.run(test_ui_simple())
    print(f"\n{'✅ SUCCESS!' if result else '❌ FAILED!'}")
    exit(0 if result else 1)