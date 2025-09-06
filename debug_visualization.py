#!/usr/bin/env python3
"""
Debug why visualization is not loading
"""

import asyncio
from playwright.async_api import async_playwright

async def debug_visualization():
    print("🔍 DEBUGGING VISUALIZATION LOADING")
    print("=" * 40)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        # Monitor network requests
        network_requests = []
        page.on("request", lambda request: network_requests.append({
            'url': request.url,
            'method': request.method
        }))
        
        # Monitor responses
        network_responses = []
        page.on("response", lambda response: network_responses.append({
            'url': response.url,
            'status': response.status
        }))
        
        try:
            print("1️⃣ Setup complete, navigating...")
            await page.goto("http://localhost:3000", wait_until="networkidle")
            
            print("2️⃣ Clicking Training Datasets...")
            training_button = page.get_by_role("button", name="🤖 Training Datasets")
            await training_button.click()
            await page.wait_for_timeout(2000)
            
            print("3️⃣ Selecting dataset and sequence...")
            await page.locator("#dataset-selector").select_option(value="63")
            await page.wait_for_timeout(2000)
            
            await page.locator("#sequence-selector").select_option(index=1)
            await page.wait_for_timeout(1000)
            
            print("4️⃣ Clicking Visualize button...")
            visualize_button = page.locator("text=📊 Visualize")
            
            # Clear previous network logs
            network_requests.clear()
            network_responses.clear()
            
            await visualize_button.click()
            await page.wait_for_timeout(5000)  # Wait for API calls
            
            print("5️⃣ Checking network activity...")
            print("   API Requests made:")
            api_requests = [req for req in network_requests if 'training-datasets' in req['url']]
            for req in api_requests:
                print(f"     {req['method']} {req['url']}")
            
            print("   API Responses:")
            api_responses = [res for res in network_responses if 'training-datasets' in res['url']]
            for res in api_responses:
                print(f"     {res['status']} {res['url']}")
            
            print("6️⃣ Checking page state...")
            
            # Get values from form
            dataset_id = await page.locator("#dataset-selector").input_value()
            sequence_id = await page.locator("#sequence-selector").input_value()
            print(f"   Dataset ID: {dataset_id}")
            print(f"   Sequence ID: {sequence_id}")
            
            # Check what's in the visualization divs
            dataset_info_content = await page.locator("#dataset-info").inner_html()
            print(f"   Dataset info content: {dataset_info_content[:100]}...")
            
            # Check chart divs
            for tf in ['5m', '15m', '1h']:
                chart_content = await page.locator(f"#ohlc-chart-{tf}").inner_html()
                print(f"   {tf} chart content: {chart_content[:100]}...")
            
            # Check if there are JavaScript errors by injecting a test
            js_test_result = await page.evaluate("""
                () => {
                    const errors = [];
                    
                    // Check if required functions exist
                    if (typeof loadDatasetVisualization !== 'function') {
                        errors.push('loadDatasetVisualization function missing');
                    }
                    
                    // Check if Plotly is loaded
                    if (typeof Plotly === 'undefined') {
                        errors.push('Plotly library not loaded');
                    }
                    
                    // Check if visualization div is visible
                    const vizDiv = document.getElementById('dataset-visualization');
                    if (!vizDiv) {
                        errors.push('dataset-visualization div missing');
                    } else if (vizDiv.style.display === 'none') {
                        errors.push('dataset-visualization div is hidden');
                    }
                    
                    return {
                        errors: errors,
                        plotlyAvailable: typeof Plotly !== 'undefined',
                        visualizationVisible: vizDiv && vizDiv.style.display !== 'none'
                    };
                }
            """)
            
            print("7️⃣ JavaScript environment check:")
            print(f"   Plotly available: {js_test_result['plotlyAvailable']}")
            print(f"   Visualization visible: {js_test_result['visualizationVisible']}")
            if js_test_result['errors']:
                print("   Errors found:")
                for error in js_test_result['errors']:
                    print(f"     ❌ {error}")
            else:
                print("   ✅ No JavaScript errors detected")
            
            return len(js_test_result['errors']) == 0 and len(api_responses) > 0
            
        except Exception as e:
            print(f"❌ Debug failed: {e}")
            return False
            
        finally:
            await browser.close()

if __name__ == "__main__":
    result = asyncio.run(debug_visualization())
    print(f"\n{'✅ Debug complete' if result else '❌ Issues found'}")
    exit(0 if result else 1)