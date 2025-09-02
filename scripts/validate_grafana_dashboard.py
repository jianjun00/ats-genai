#!/usr/bin/env python3
"""
Validate Grafana Dashboard Availability using Playwright

This script logs into ATS-INTG Grafana and checks for available dashboards.
"""

import asyncio
import sys
from playwright.async_api import async_playwright

async def validate_grafana_dashboard():
    """Validate Grafana dashboard availability and take screenshots."""
    
    async with async_playwright() as p:
        # Launch browser
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            print("🔍 Connecting to ATS-INTG Grafana...")
            
            # Navigate to Grafana login
            await page.goto("http://localhost:4002")
            await page.wait_for_load_state("networkidle")
            
            print("📸 Taking screenshot of login page...")
            await page.screenshot(path="/tmp/grafana_login.png")
            
            # Login to Grafana
            print("🔑 Logging into Grafana...")
            await page.fill('input[name="user"]', 'admin')
            await page.fill('input[name="password"]', 'ats-intg-monitoring-password')
            await page.click('button[type="submit"]')
            
            # Wait for dashboard to load
            await page.wait_for_load_state("networkidle", timeout=10000)
            print("📸 Taking screenshot after login...")
            await page.screenshot(path="/tmp/grafana_home.png")
            
            # Check if we're on the home page or redirected
            current_url = page.url
            print(f"📍 Current URL: {current_url}")
            
            # Navigate to dashboards
            print("🔍 Looking for dashboards...")
            
            # Try to click on dashboards menu
            try:
                # Look for dashboards in sidebar or menu
                await page.click('a[href="/dashboards"]', timeout=5000)
                await page.wait_for_load_state("networkidle")
            except:
                # Try alternative dashboard navigation
                try:
                    await page.click('text=Dashboards')
                    await page.wait_for_load_state("networkidle")
                except:
                    print("⚠️  Could not find dashboards menu, trying direct navigation...")
                    await page.goto("http://localhost:4002/dashboards")
                    await page.wait_for_load_state("networkidle")
            
            print("📸 Taking screenshot of dashboards page...")
            await page.screenshot(path="/tmp/grafana_dashboards.png")
            
            # List all visible dashboards
            print("\n📋 Available dashboards:")
            
            # Look for dashboard links/titles
            dashboard_selectors = [
                'a[href*="/d/"]',  # Dashboard links
                '.dashboard-link',  # Dashboard link class
                '.card-item-name',  # Card item names
                'div[data-testid*="dashboard"]',  # Test ID selectors
                'h3, h4, .dashboard-title'  # Title elements
            ]
            
            found_dashboards = []
            
            for selector in dashboard_selectors:
                try:
                    elements = await page.query_selector_all(selector)
                    for element in elements:
                        text = await element.text_content()
                        href = await element.get_attribute('href')
                        if text and text.strip():
                            found_dashboards.append({
                                'text': text.strip(),
                                'href': href,
                                'selector': selector
                            })
                except Exception as e:
                    continue
            
            if found_dashboards:
                for i, dashboard in enumerate(found_dashboards, 1):
                    print(f"  {i}. '{dashboard['text']}' (href: {dashboard['href']})")
            else:
                print("  ❌ No dashboards found")
            
            # Search specifically for our dashboard
            target_dashboard = "ATS-INTG Instrument Daily Price Coverage"
            print(f"\n🎯 Searching for '{target_dashboard}'...")
            
            found_target = False
            for dashboard in found_dashboards:
                if target_dashboard.lower() in dashboard['text'].lower():
                    print(f"  ✅ Found target dashboard: '{dashboard['text']}'")
                    found_target = True
                    break
            
            if not found_target:
                print(f"  ❌ Target dashboard '{target_dashboard}' not found")
            
            # Check for provisioning directory
            print("\n🔍 Checking dashboard files...")
            
            # Try to access API to list dashboards
            try:
                await page.goto("http://localhost:4002/api/search?type=dash-db")
                await page.wait_for_load_state("networkidle")
                content = await page.content()
                print("📸 Taking screenshot of API response...")
                await page.screenshot(path="/tmp/grafana_api_dashboards.png")
                
                if "ATS-INTG" in content:
                    print("  ✅ Found ATS-INTG dashboard in API response")
                else:
                    print("  ❌ ATS-INTG dashboard not found in API response")
                    
            except Exception as e:
                print(f"  ⚠️  Could not access dashboard API: {e}")
            
            print("\n📸 Screenshots saved:")
            print("  - /tmp/grafana_login.png")  
            print("  - /tmp/grafana_home.png")
            print("  - /tmp/grafana_dashboards.png")
            print("  - /tmp/grafana_api_dashboards.png")
            
            return found_target
            
        except Exception as e:
            print(f"❌ Error validating Grafana: {e}")
            await page.screenshot(path="/tmp/grafana_error.png")
            return False
        finally:
            await browser.close()

async def main():
    """Main function."""
    print("🎯 Validating ATS-INTG Grafana Dashboard...")
    print("=" * 60)
    
    success = await validate_grafana_dashboard()
    
    print("\n" + "=" * 60)
    if success:
        print("✅ Dashboard validation completed successfully!")
    else:
        print("❌ Dashboard validation failed - check screenshots for details")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))