#!/usr/bin/env python3
"""
Test the new high_volume_large_cap universe in integration environment
"""

import asyncio
from playwright.async_api import async_playwright

async def test_new_universe():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            print("📍 Navigating to integration analytics dashboard...")
            await page.goto("http://localhost:4000/", wait_until="domcontentloaded")
            
            print("🔘 Clicking Universe Analytics button...")
            universe_button = await page.wait_for_selector('button:has-text("🌐 Universe Analytics")', timeout=5000)
            await universe_button.click()
            
            print("⏳ Waiting for interface to load...")
            await page.wait_for_timeout(3000)
            
            # Find and select our new universe
            universe_selector = await page.query_selector('#universe-selector')
            options = await universe_selector.query_selector_all('option')
            
            print(f"📊 Found {len(options)} universe options:")
            for option in options:
                text = await option.inner_text()
                value = await option.get_attribute('value')
                print(f"   - {text} (ID: {value})")
            
            # Select our high volume large cap universe (should be ID 2)
            high_volume_option = None
            for option in options:
                text = await option.inner_text()
                if "high_volume_large_cap" in text:
                    high_volume_option = option
                    break
            
            if high_volume_option:
                universe_value = await high_volume_option.get_attribute('value')
                universe_text = await high_volume_option.inner_text()
                
                print(f"🎯 Selecting universe: {universe_text}")
                await universe_selector.select_option(value=universe_value)
                
                # Click Load Members
                load_button = await page.query_selector('button:has-text("Load Members")')
                print("🔘 Clicking Load Members...")
                await load_button.click()
                
                # Wait for results
                await page.wait_for_timeout(3000)
                
                # Check results
                members_content = await page.query_selector('#universe-members-content')
                content_text = await members_content.text_content()
                
                print("📊 Universe Members Results:")
                if "Total Members:" in content_text:
                    lines = content_text.split('\n')
                    for line in lines:
                        if "Total Members:" in line or "Universe:" in line or "Description:" in line:
                            print(f"   {line.strip()}")
                
                # Check for member tables
                if "Active Members" in content_text:
                    print("✅ Active members table found")
                    # Extract some symbols
                    if "AAPL" in content_text:
                        print("✅ AAPL found in members")
                    if "AMZN" in content_text:
                        print("✅ AMZN found in members")
                else:
                    print("❌ Active members table not found")
                    print(f"Content: {content_text[:200]}...")
                
            else:
                print("❌ High volume large cap universe not found in dropdown")
                
        except Exception as e:
            print(f"❌ Error during test: {e}")
            import traceback
            traceback.print_exc()
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_new_universe())