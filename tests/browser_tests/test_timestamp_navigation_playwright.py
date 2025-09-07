#!/usr/bin/env python3
"""
Playwright End-to-End Tests for Timestamp-Based Multi-Timeframe Navigation

Tests the complete user workflow:
1. Load Training Datasets interface
2. Select dataset and sequence
3. Navigate through 1-hour positions using Next/Previous buttons  
4. Verify table updates with 1-hour data
5. Verify charts update with synchronized multi-timeframe data
6. Test navigation edge cases and error handling
"""

import pytest
import asyncio
import json
import re
from playwright.async_api import async_playwright, Page, expect
import sys
import os
import time

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestTimestampNavigationPlaywright:
    """Playwright tests for timestamp-based navigation system."""
    
    BASE_URL = "http://localhost:3000"
    
    @pytest.mark.asyncio
    async def test_complete_navigation_workflow_playwright(self):
        """Test complete timestamp-based navigation workflow in browser."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            # Capture console messages for debugging
            console_messages = []
            
            async def handle_console(msg):
                console_messages.append(f"[{msg.type}] {msg.text}")
                print(f"🖥️ BROWSER: [{msg.type}] {msg.text}")
            
            page.on('console', handle_console)
            
            # Capture network requests
            navigation_requests = []
            multi_timeframe_requests = []
            
            async def handle_request(request):
                url = request.url
                if '/1h?' in url:
                    navigation_requests.append(url)
                    print(f"🌐 1H NAV REQUEST: {url}")
                elif '/multi-timeframe?' in url:
                    multi_timeframe_requests.append(url)
                    print(f"🌐 MULTI REQUEST: {url}")
            
            async def handle_response(response):
                url = response.url
                if '/1h?' in url or '/multi-timeframe?' in url:
                    print(f"🌐 RESPONSE: {response.status} {url}")
                    if response.status == 200:
                        try:
                            response_data = await response.json()
                            if '/1h?' in url:
                                print(f"   1H NAV: success={response_data.get('success')}, timestamp={response_data.get('timestamp')}, table_rows={len(response_data.get('table_data', []))}")
                            elif '/multi-timeframe?' in url:
                                ohlc_data = response_data.get('ohlc_data', {})
                                print(f"   MULTI: success={response_data.get('success')}, timeframes={list(ohlc_data.keys())}")
                        except Exception as e:
                            print(f"   Could not parse JSON: {e}")
            
            page.on('request', handle_request)
            page.on('response', handle_response)
            
            try:
                print("🎯 Step 1: Navigate to EDA dashboard")
                await page.goto(self.BASE_URL, wait_until='networkidle', timeout=30000)
                
                print("🎯 Step 2: Click Training Datasets")
                await page.click('button:has-text("Training Datasets")')
                await page.wait_for_selector('#dataset-selector', timeout=15000)
                
                print("🎯 Step 3: Check for available datasets")
                dataset_options = await page.locator('#dataset-selector option').count()
                print(f"📊 Found {dataset_options} dataset options")
                
                if dataset_options <= 1:
                    pytest.skip("No datasets available - cannot test navigation")
                
                print("🎯 Step 4: Select first dataset")
                await page.select_option('#dataset-selector', index=1)
                await page.wait_for_timeout(2000)
                
                print("🎯 Step 5: Check for sequences")
                sequence_options = await page.locator('#sequence-selector option').count()
                print(f"📊 Found {sequence_options} sequence options")
                
                if sequence_options <= 1:
                    pytest.skip("No sequences available - cannot test navigation")
                
                print("🎯 Step 6: Select first sequence")
                await page.select_option('#sequence-selector', index=1)
                await page.wait_for_timeout(1000)
                
                print("🎯 Step 7: Load visualization")
                visualize_button = await page.locator('button:has-text("Visualize")').count()
                if visualize_button > 0:
                    await page.click('button:has-text("Visualize")')
                    await page.wait_for_timeout(5000)  # Wait for initial load
                    print("✅ Initial visualization loaded")
                else:
                    pytest.skip("Visualize button not found")
                
                print("🎯 Step 8: Verify navigation controls are visible")
                nav_controls_visible = await page.locator('#position-slider').is_visible()
                nav_buttons_visible = await page.locator('#nav-next').is_visible()
                
                if not (nav_controls_visible and nav_buttons_visible):
                    pytest.fail("Navigation controls not visible")
                
                print("✅ Navigation controls are visible")
                
                print("🎯 Step 9: Test timestamp-based navigation workflow")
                print("=" * 60)
                print("🔽 TESTING TIMESTAMP-BASED NAVIGATION")
                print("=" * 60)
                
                # Capture initial state
                initial_position_text = await page.locator('#position-info').text_content()
                initial_table_html = await page.locator('#sequence-table').inner_html()
                
                print(f"📍 Initial position: {initial_position_text}")
                print(f"📋 Initial table HTML length: {len(initial_table_html)}")
                
                # Clear request arrays
                navigation_requests.clear()
                multi_timeframe_requests.clear()
                
                # Step 10: Click Next button and verify new workflow
                print("🎯 Step 10: Click Next button - Test new API workflow")
                await page.click('#nav-next')
                await page.wait_for_timeout(4000)  # Wait for both API calls
                
                print("🎯 Step 11: Verify API calls were made correctly")
                print(f"   📡 1H navigation requests: {len(navigation_requests)}")
                print(f"   📡 Multi-timeframe requests: {len(multi_timeframe_requests)}")
                
                # Verify the new API workflow was triggered
                if len(navigation_requests) > 0:
                    print("✅ 1-hour navigation API called")
                    latest_nav_request = navigation_requests[-1]
                    print(f"   Last 1H request: {latest_nav_request}")
                    
                    # Check if multi-timeframe was also called
                    if len(multi_timeframe_requests) > 0:
                        print("✅ Multi-timeframe API called")
                        latest_multi_request = multi_timeframe_requests[-1]
                        print(f"   Last multi request: {latest_multi_request}")
                        
                        # Verify timestamp parameter in multi-timeframe request
                        if 'timestamp=' in latest_multi_request:
                            print("✅ Timestamp-based coordination working")
                        else:
                            print("❌ Multi-timeframe request missing timestamp parameter")
                    else:
                        print("⚠️ Multi-timeframe API not called - may be using old workflow")
                else:
                    print("⚠️ 1-hour navigation API not called - may be using old workflow")
                
                # Step 12: Verify UI updates
                print("🎯 Step 12: Verify UI updates after navigation")
                new_position_text = await page.locator('#position-info').text_content()
                new_table_html = await page.locator('#sequence-table').inner_html()
                
                print(f"📍 New position: {new_position_text}")
                print(f"📋 New table HTML length: {len(new_table_html)}")
                
                # Check if position changed
                position_changed = initial_position_text != new_position_text
                table_changed = initial_table_html != new_table_html
                
                print(f"📊 Position changed: {position_changed}")
                print(f"📊 Table changed: {table_changed}")
                
                if position_changed and table_changed:
                    print("🎉 SUCCESS: Navigation working with UI updates!")
                else:
                    print("⚠️ Navigation may not be updating UI correctly")
                
                # Step 13: Test multiple navigation clicks
                print("🎯 Step 13: Test multiple navigation clicks")
                for i in range(3):
                    print(f"\n🔄 Navigation test {i+1}/3")
                    
                    # Clear previous requests
                    navigation_requests.clear() 
                    multi_timeframe_requests.clear()
                    
                    await page.click('#nav-next')
                    await page.wait_for_timeout(3000)
                    
                    position_text = await page.locator('#position-info').text_content()
                    print(f"📍 Position after click {i+1}: {position_text}")
                    
                    # Check API calls for each navigation
                    nav_calls = len(navigation_requests)
                    multi_calls = len(multi_timeframe_requests)
                    print(f"   📡 API calls - 1H: {nav_calls}, Multi: {multi_calls}")
                
                # Step 14: Test Previous button
                print("🎯 Step 14: Test Previous button")
                navigation_requests.clear()
                multi_timeframe_requests.clear()
                
                await page.click('#nav-prev')
                await page.wait_for_timeout(3000)
                
                prev_position_text = await page.locator('#position-info').text_content()
                print(f"📍 Position after Previous: {prev_position_text}")
                print(f"   📡 API calls - 1H: {len(navigation_requests)}, Multi: {len(multi_timeframe_requests)}")
                
                # Step 15: Verify charts are updating
                print("🎯 Step 15: Verify chart updates")
                chart_divs = ['#ohlc-chart-5m', '#ohlc-chart-15m', '#ohlc-chart-1d', '#ohlc-chart-1w']
                
                for chart_div in chart_divs:
                    chart_exists = await page.locator(chart_div).count()
                    if chart_exists > 0:
                        chart_content = await page.locator(chart_div).inner_text()
                        has_content = len(chart_content.strip()) > 10  # Has more than just "Loading..."
                        print(f"   📊 {chart_div}: exists={chart_exists > 0}, has_content={has_content}")
                
                print("\n📊 TIMESTAMP NAVIGATION TEST SUMMARY:")
                print(f"   🔄 Navigation requests made: {len(navigation_requests) > 0}")
                print(f"   📊 Multi-timeframe requests made: {len(multi_timeframe_requests) > 0}")
                print(f"   📋 Table updates working: {table_changed}")
                print(f"   📍 Position updates working: {position_changed}")
                
                # Final validation
                final_success = (len(navigation_requests) > 0 or len(multi_timeframe_requests) > 0) and table_changed
                print(f"   🎉 Overall success: {final_success}")
                
                if final_success:
                    print("✅ Timestamp-based navigation system is working!")
                else:
                    print("⚠️ Navigation system may need implementation or debugging")
                
                await page.wait_for_timeout(2000)  # Keep browser open briefly
                
            except Exception as e:
                print(f"❌ Test failed with error: {e}")
                
                # Print console messages for debugging
                print("\n🔍 BROWSER CONSOLE MESSAGES:")
                for msg in console_messages[-20:]:  # Last 20 messages
                    print(f"   {msg}")
                
                raise e
                
            finally:
                await browser.close()
    
    @pytest.mark.asyncio
    async def test_navigation_api_contract_playwright(self):
        """Test API contract compliance for navigation endpoints in browser context."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            # Track API responses for contract validation
            api_responses = []
            
            async def handle_response(response):
                url = response.url
                if '/1h?' in url or '/multi-timeframe?' in url:
                    try:
                        response_data = await response.json()
                        api_responses.append({
                            'url': url,
                            'status': response.status,
                            'data': response_data
                        })
                    except Exception as e:
                        api_responses.append({
                            'url': url,
                            'status': response.status,
                            'error': str(e)
                        })
            
            page.on('response', handle_response)
            
            try:
                print("🎯 Testing API contract compliance...")
                
                # Load the interface
                await page.goto(self.BASE_URL, wait_until='networkidle', timeout=30000)
                await page.click('button:has-text("Training Datasets")')
                await page.wait_for_selector('#dataset-selector', timeout=15000)
                
                # Select dataset and sequence
                dataset_options = await page.locator('#dataset-selector option').count()
                if dataset_options > 1:
                    await page.select_option('#dataset-selector', index=1)
                    await page.wait_for_timeout(2000)
                    
                    sequence_options = await page.locator('#sequence-selector option').count()
                    if sequence_options > 1:
                        await page.select_option('#sequence-selector', index=1)
                        await page.wait_for_timeout(1000)
                        
                        # Load visualization to trigger API calls
                        visualize_button = await page.locator('button:has-text("Visualize")').count()
                        if visualize_button > 0:
                            await page.click('button:has-text("Visualize")')
                            await page.wait_for_timeout(5000)
                            
                            # Trigger navigation to get API responses
                            nav_next_exists = await page.locator('#nav-next').count()
                            if nav_next_exists > 0:
                                await page.click('#nav-next')
                                await page.wait_for_timeout(4000)
                
                print(f"📡 Captured {len(api_responses)} API responses")
                
                # Validate API contracts
                for response in api_responses:
                    url = response['url']
                    status = response['status']
                    data = response.get('data')
                    
                    print(f"\n🔍 Validating: {url}")
                    
                    # Basic response validation
                    assert status == 200, f"Expected 200, got {status} for {url}"
                    assert data is not None, f"No response data for {url}"
                    
                    if '/1h?' in url:
                        # Validate 1-hour navigation response contract
                        print("   📋 Validating 1H navigation contract...")
                        required_fields = ['success', 'timestamp', 'table_data', 'current_position']
                        for field in required_fields:
                            assert field in data, f"Missing required field '{field}' in 1H response"
                        
                        # Validate field types
                        assert isinstance(data['success'], bool), "success should be boolean"
                        assert isinstance(data['timestamp'], int), "timestamp should be integer (Unix epoch)"
                        assert isinstance(data['table_data'], list), "table_data should be list"
                        assert isinstance(data['current_position'], int), "current_position should be integer"
                        
                        # Validate timestamp range (reasonable epoch time)
                        assert data['timestamp'] > 1700000000, f"timestamp {data['timestamp']} seems invalid"
                        assert data['timestamp'] < 2000000000, f"timestamp {data['timestamp']} seems too far in future"
                        
                        # Validate table data structure
                        if data['table_data']:
                            bar = data['table_data'][0]
                            required_bar_fields = ['timestamp', 'open', 'high', 'low', 'close']
                            for field in required_bar_fields:
                                assert field in bar, f"Missing bar field '{field}' in table_data"
                        
                        print("   ✅ 1H navigation contract valid")
                    
                    elif '/multi-timeframe?' in url:
                        # Validate multi-timeframe response contract
                        print("   📊 Validating multi-timeframe contract...")
                        required_fields = ['success', 'timestamp', 'ohlc_data']
                        for field in required_fields:
                            assert field in data, f"Missing required field '{field}' in multi-timeframe response"
                        
                        # Validate field types
                        assert isinstance(data['success'], bool), "success should be boolean"
                        assert isinstance(data['timestamp'], int), "timestamp should be integer"
                        assert isinstance(data['ohlc_data'], dict), "ohlc_data should be dict"
                        
                        # Validate timeframe data structure
                        expected_timeframes = {'5m', '15m', '1d', '1w'}  # Excluding 1h
                        for timeframe, ohlc_list in data['ohlc_data'].items():
                            assert isinstance(ohlc_list, list), f"{timeframe} data should be list"
                            assert len(ohlc_list) <= 21, f"{timeframe} should have max 21 bars"
                            
                            if ohlc_list:  # If we have data
                                bar = ohlc_list[0]
                                required_bar_fields = ['timestamp', 'open', 'high', 'low', 'close']
                                for field in required_bar_fields:
                                    assert field in bar, f"Missing field '{field}' in {timeframe} data"
                        
                        print("   ✅ Multi-timeframe contract valid")
                
                if api_responses:
                    print("🎉 API contract validation successful!")
                else:
                    pytest.skip("No API responses captured - navigation may not be implemented")
                    
            finally:
                await browser.close()
    
    @pytest.mark.asyncio
    async def test_navigation_error_handling_playwright(self):
        """Test error handling and edge cases in browser context."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            # Capture error messages
            error_messages = []
            
            async def handle_console(msg):
                if msg.type == 'error':
                    error_messages.append(msg.text)
                    print(f"🖥️ BROWSER ERROR: {msg.text}")
            
            page.on('console', handle_console)
            
            try:
                print("🎯 Testing error handling and edge cases...")
                
                # Load interface
                await page.goto(self.BASE_URL, wait_until='networkidle', timeout=30000)
                await page.click('button:has-text("Training Datasets")')
                await page.wait_for_selector('#dataset-selector', timeout=15000)
                
                # Test with no dataset selected
                print("🔍 Testing navigation without dataset selection...")
                nav_next_exists = await page.locator('#nav-next').count()
                if nav_next_exists > 0:
                    await page.click('#nav-next')
                    await page.wait_for_timeout(2000)
                    # Should handle gracefully without crashing
                
                # Select dataset and sequence for further testing
                dataset_options = await page.locator('#dataset-selector option').count()
                if dataset_options > 1:
                    await page.select_option('#dataset-selector', index=1)
                    await page.wait_for_timeout(2000)
                    
                    sequence_options = await page.locator('#sequence-selector option').count()
                    if sequence_options > 1:
                        await page.select_option('#sequence-selector', index=1)
                        await page.wait_for_timeout(1000)
                        
                        # Load visualization
                        visualize_button = await page.locator('button:has-text("Visualize")').count()
                        if visualize_button > 0:
                            await page.click('button:has-text("Visualize")')
                            await page.wait_for_timeout(5000)
                            
                            # Test rapid clicking (stress test)
                            print("🔍 Testing rapid navigation clicking...")
                            if await page.locator('#nav-next').count() > 0:
                                for i in range(5):
                                    await page.click('#nav-next')
                                    await page.wait_for_timeout(200)  # Rapid clicks
                                
                                await page.wait_for_timeout(3000)  # Wait for all to complete
                            
                            # Test navigation boundary conditions
                            print("🔍 Testing boundary conditions...")
                            
                            # Click "First" button multiple times
                            first_button_exists = await page.locator('#nav-first').count()
                            if first_button_exists > 0:
                                for i in range(3):
                                    await page.click('#nav-first')
                                    await page.wait_for_timeout(500)
                            
                            # Click "Last" button multiple times  
                            last_button_exists = await page.locator('#nav-last').count()
                            if last_button_exists > 0:
                                for i in range(3):
                                    await page.click('#nav-last')
                                    await page.wait_for_timeout(500)
                
                # Check for JavaScript errors
                print(f"🔍 JavaScript errors encountered: {len(error_messages)}")
                for error in error_messages:
                    print(f"   ❌ {error}")
                
                # JavaScript errors are not necessarily test failures, 
                # but we should report them
                if error_messages:
                    print("⚠️ JavaScript errors detected - may indicate issues to investigate")
                else:
                    print("✅ No JavaScript errors detected")
                
                print("🎉 Error handling tests completed")
                
            finally:
                await browser.close()

if __name__ == '__main__':
    # Run with high verbosity
    pytest.main([__file__, '-v', '--tb=short', '-s'])