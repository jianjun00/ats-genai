#!/usr/bin/env python3
"""
Dashboard vs API Consistency Test
Verifies that the dashboard displays match the actual API data
"""

import asyncio
from playwright.async_api import async_playwright
import requests
import json

BASE_URL = "http://localhost:4000"

async def test_dashboard_api_consistency():
    print("🔍 DASHBOARD vs API CONSISTENCY TEST")
    print("=" * 50)
    
    # Step 1: Get actual API data
    print("\n📊 Step 1: Fetch actual API data...")
    
    try:
        api_response = requests.get(f"{BASE_URL}/data-quality/api/issues")
        api_data = api_response.json()
        
        api_total = api_data.get('summary', {}).get('total_issues', 0)
        api_critical = api_data.get('summary', {}).get('critical', 0)
        api_high = api_data.get('summary', {}).get('high', 0)
        api_medium = api_data.get('summary', {}).get('medium', 0)
        api_low = api_data.get('summary', {}).get('low', 0)
        
        print(f"   API Total Issues: {api_total}")
        print(f"   API Critical: {api_critical}")
        print(f"   API High: {api_high}")
        print(f"   API Medium: {api_medium}")
        print(f"   API Low: {api_low}")
        
        if api_total == 0:
            print("   ❌ API shows no issues - agent may not be working")
            return False
        else:
            print(f"   ✅ API shows {api_total} issues - agent is working!")
            
    except Exception as e:
        print(f"   ❌ Failed to get API data: {e}")
        return False
    
    # Step 2: Check dashboard display
    print("\n🎭 Step 2: Check dashboard display...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        page = await browser.new_page()
        
        try:
            # Navigate to data quality dashboard
            await page.goto(BASE_URL)
            await page.wait_for_load_state("domcontentloaded")
            
            # Click Data Quality Dashboard
            await page.locator('button:has-text("Data Quality")').first.click()
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(5000)  # Wait for data to load
            
            print("   📄 Extracting dashboard content...")
            
            # Get all text content from the page
            page_content = await page.content()
            page_text = await page.locator("body").inner_text()
            
            # Look for issue count displays
            print("\n   🔍 Searching for issue count displays...")
            
            # Search for various patterns that might show issue counts
            issue_patterns = [
                r'(\d+)\s*Total Issues',
                r'(\d+)\s*Critical', 
                r'(\d+)\s*High Priority',
                r'(\d+)\s*High',
                r'(\d+)\s*Medium',
                r'(\d+)\s*Symbols Affected',
                r'(\d+)\s*Issues Found',
                r'(\d+)\s*Problems Detected'
            ]
            
            import re
            found_counts = {}
            
            for pattern in issue_patterns:
                matches = re.findall(pattern, page_text, re.IGNORECASE)
                if matches:
                    found_counts[pattern] = matches
                    print(f"   Found pattern '{pattern}': {matches}")
            
            # Look for specific text that indicates zero issues
            zero_indicators = [
                "0 Total Issues",
                "0 Critical", 
                "0 High Priority",
                "0 Symbols Affected",
                "No issues found",
                "No problems detected"
            ]
            
            zero_found = []
            for indicator in zero_indicators:
                if indicator.lower() in page_text.lower():
                    zero_found.append(indicator)
                    print(f"   ❌ Found zero indicator: '{indicator}'")
            
            # Look for actual issue data
            issue_symbols = ['AAL', 'AAPL', 'MVSTW', 'ALXO', 'AMD', 'AMGN']  # From API
            symbols_in_ui = []
            for symbol in issue_symbols:
                if symbol in page_text:
                    symbols_in_ui.append(symbol)
                    print(f"   ✅ Found issue symbol in UI: {symbol}")
            
            # Look for issue types
            issue_types = ['stale_data', 'extreme_volume', 'stale data', 'high volume']
            types_in_ui = []
            for issue_type in issue_types:
                if issue_type.lower() in page_text.lower():
                    types_in_ui.append(issue_type)
                    print(f"   ✅ Found issue type in UI: {issue_type}")
            
            # Step 3: Compare API vs Dashboard
            print(f"\n🔍 Step 3: Consistency Analysis...")
            
            dashboard_shows_issues = len(symbols_in_ui) > 0 or len(types_in_ui) > 0
            dashboard_shows_zeros = len(zero_found) > 0
            
            print(f"   API Reports: {api_total} total issues")
            print(f"   Dashboard Shows Issues: {dashboard_shows_issues}")
            print(f"   Dashboard Shows Zeros: {dashboard_shows_zeros}")
            print(f"   Issue Symbols in UI: {len(symbols_in_ui)}")
            print(f"   Issue Types in UI: {len(types_in_ui)}")
            
            # Determine the discrepancy
            if api_total > 0 and dashboard_shows_zeros and not dashboard_shows_issues:
                print(f"\n❌ DASHBOARD BUG CONFIRMED!")
                print(f"   • API correctly shows {api_total} issues")
                print(f"   • Dashboard incorrectly shows zero indicators: {zero_found}")
                print(f"   • No actual issue data visible in dashboard")
                print(f"\n🔧 ISSUE: Dashboard is not displaying API data correctly")
                
                # Take screenshot for debugging
                await page.screenshot(path="dashboard_bug_screenshot.png")
                print(f"   📸 Screenshot saved: dashboard_bug_screenshot.png")
                
                return False
                
            elif api_total > 0 and dashboard_shows_issues and not dashboard_shows_zeros:
                print(f"\n✅ DASHBOARD WORKING CORRECTLY!")
                print(f"   • API shows {api_total} issues")
                print(f"   • Dashboard correctly displays issue data")
                print(f"   • No zero indicators found")
                return True
                
            else:
                print(f"\n⚠️ MIXED RESULTS - NEEDS INVESTIGATION")
                print(f"   • API Total: {api_total}")
                print(f"   • Dashboard Issues: {dashboard_shows_issues}")
                print(f"   • Dashboard Zeros: {dashboard_shows_zeros}")
                
                await page.screenshot(path="dashboard_mixed_results.png")
                print(f"   📸 Screenshot saved: dashboard_mixed_results.png")
                
                return False
                
        except Exception as e:
            print(f"   ❌ Error checking dashboard: {e}")
            await page.screenshot(path="dashboard_error.png")
            return False
            
        finally:
            await browser.close()
    
if __name__ == "__main__":
    success = asyncio.run(test_dashboard_api_consistency())
    
    if success:
        print(f"\n🎉 CONCLUSION: Dashboard and API are consistent - everything working!")
    else:
        print(f"\n🚨 CONCLUSION: Dashboard display bug found - agent works but UI doesn't show data!")
        print(f"   SOLUTION: Fix dashboard rendering to display API data correctly")
    
    exit(0 if success else 1)