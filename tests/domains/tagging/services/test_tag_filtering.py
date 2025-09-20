#!/usr/bin/env python3
"""
Playwright test for comprehensive tag filtering functionality
"""
import asyncio
import pytest
from playwright.async_api import async_playwright, Page
import time

async def test_tag_filtering_comprehensive():
    """Test the complete tag filtering workflow"""
    
    async with async_playwright() as p:
        # Launch browser
        browser = await p.chromium.launch(headless=True)  # Headless mode for server environment
        context = await browser.new_context()
        page = await context.new_page()
        
        try:
            print("🧪 Starting Comprehensive Tag Filtering Test")
            print("=" * 60)
            
            # Step 1: Navigate to dashboard  
            print("\n📋 Step 1: Loading Data Quality Dashboard")
            await page.goto("http://localhost:4006/data-quality/dashboard")
            await page.wait_for_load_state('networkidle')
            
            # Verify page loaded
            title = await page.title()
            print(f"   ✅ Page loaded: {title}")
            
            # Step 2: Open tag filters
            print("\n🏷️ Step 2: Opening Tag Filters Panel")
            await page.click('button:has-text("🏷️ Tag Filters")')
            await page.wait_for_timeout(2000)  # Wait for panel to open and tags to load
            
            # Verify tag filters panel is visible
            tag_panel = await page.locator('#tag-filters-panel').is_visible()
            print(f"   ✅ Tag filters panel visible: {tag_panel}")
            
            # Step 3: Check available tags loaded
            print("\n📊 Step 3: Verifying Available Tags Display")
            await page.wait_for_selector('#available-tags-container', timeout=10000)
            
            # Count available tags
            tag_elements = await page.locator('.available-tag').count()
            print(f"   ✅ Available tags loaded: {tag_elements} tags")
            
            # Step 4: Select some tags
            print("\n🎯 Step 4: Selecting Tags for Filtering")
            selected_tags = []
            
            # Try to select "Critical" tag if available
            critical_tag = page.locator('.available-tag:has-text("Critical")')
            if await critical_tag.count() > 0:
                await critical_tag.click()
                selected_tags.append("Critical")
                print("   ✅ Selected 'Critical' tag")
                await page.wait_for_timeout(500)
            
            # Try to select "Polygon" tag if available
            polygon_tag = page.locator('.available-tag:has-text("Polygon")')
            if await polygon_tag.count() > 0:
                await polygon_tag.click()
                selected_tags.append("Polygon")
                print("   ✅ Selected 'Polygon' tag")
                await page.wait_for_timeout(500)
            
            # Try to select "Data Gap" tag if available
            data_gap_tag = page.locator('.available-tag:has-text("Data Gap")')
            if await data_gap_tag.count() > 0:
                await data_gap_tag.click()
                selected_tags.append("Data Gap")
                print("   ✅ Selected 'Data Gap' tag")
                await page.wait_for_timeout(500)
            
            print(f"   📋 Total selected tags: {len(selected_tags)} - {selected_tags}")
            
            # Step 5: Verify selected tags display
            print("\n🔍 Step 5: Verifying Selected Tags Display")
            await page.wait_for_timeout(1000)  # Wait for UI update
            
            selected_container = page.locator('#selected-tags')
            selected_content = await selected_container.inner_html()
            print(f"   📋 Selected tags container content: {selected_content[:200]}...")
            
            # Step 6: Count issues before filtering
            print("\n📊 Step 6: Counting Issues Before Filtering")
            try:
                # Wait for issues to load
                await page.wait_for_selector('.issue', timeout=5000)
                issues_before = await page.locator('.issue').count()
                print(f"   ✅ Issues before filtering: {issues_before}")
            except:
                issues_before = 0
                print("   ⚠️ No issues found on page")
            
            # Step 7: Apply filters
            print("\n⚡ Step 7: Applying Tag Filters")
            
            # Look for Apply Filters button
            apply_button = page.locator('button:has-text("Apply Filters")')
            if await apply_button.count() > 0:
                await apply_button.click()
                print("   ✅ Clicked 'Apply Filters' button")
                await page.wait_for_timeout(3000)  # Wait for filtering
            else:
                print("   ❌ 'Apply Filters' button not found")
                
                # Try alternative filter trigger
                try:
                    await page.evaluate("applyFilters()")
                    print("   ✅ Called applyFilters() function directly")
                    await page.wait_for_timeout(3000)
                except:
                    print("   ❌ applyFilters() function not available")
            
            # Step 8: Count issues after filtering
            print("\n🔍 Step 8: Counting Issues After Filtering")
            await page.wait_for_timeout(2000)  # Wait for filter results
            
            try:
                issues_after = await page.locator('.issue').count()
                print(f"   ✅ Issues after filtering: {issues_after}")
                
                if issues_after < issues_before:
                    print(f"   🎉 SUCCESS: Filtering reduced issues from {issues_before} to {issues_after}")
                elif issues_after == issues_before:
                    print(f"   ⚠️ ISSUE: Filtering didn't change issue count ({issues_before} → {issues_after})")
                else:
                    print(f"   ❌ ERROR: Issue count increased ({issues_before} → {issues_after})")
                    
            except:
                print("   ❌ Could not count issues after filtering")
            
            # Step 9: Check network requests
            print("\n🌐 Step 9: Analyzing Network Activity")
            
            # Set up network monitoring
            responses = []
            page.on("response", lambda response: responses.append(response))
            
            try:
                # Trigger filter again to capture network request
                if await apply_button.count() > 0:
                    await apply_button.click()
                    await page.wait_for_timeout(2000)  # Wait for network activity
                
                # Analyze captured responses
                api_responses = [r for r in responses if 'api' in r.url or 'issues' in r.url]
                
                if api_responses:
                    print(f"   ✅ Captured {len(api_responses)} API responses:")
                    for response in api_responses[-3:]:  # Show last 3
                        print(f"      URL: {response.url}")
                        print(f"      Status: {response.status}")
                        
                        # Check query parameters
                        if '?' in response.url:
                            url_params = response.url.split('?')[1]
                            print(f"      Query params: {url_params}")
                else:
                    print("   ❌ No API requests detected for filtering")
                    
            except Exception as e:
                print(f"   ❌ Network monitoring error: {e}")
            
            # Step 10: Inspect JavaScript console
            print("\n🖥️ Step 10: Checking JavaScript Console")
            
            # Get console messages
            console_messages = []
            page.on("console", lambda msg: console_messages.append(f"{msg.type}: {msg.text}"))
            
            # Try filtering again to capture console output
            try:
                await page.evaluate("console.log('Testing tag filtering:', selectedTags)")
                await page.wait_for_timeout(1000)
            except:
                pass
            
            if console_messages:
                print("   📝 Console messages:")
                for msg in console_messages[-5:]:  # Show last 5 messages
                    print(f"      {msg}")
            else:
                print("   📝 No recent console messages")
            
            # Step 11: Check JavaScript state
            print("\n🔍 Step 11: Inspecting JavaScript State")
            
            try:
                available_tags_count = await page.evaluate("availableTags ? availableTags.length : 0")
                selected_tags_count = await page.evaluate("selectedTags ? selectedTags.length : 0")
                selected_tags_values = await page.evaluate("selectedTags || []")
                
                print(f"   📊 Available tags in JS: {available_tags_count}")
                print(f"   🎯 Selected tags in JS: {selected_tags_count}")
                print(f"   📋 Selected tag values: {selected_tags_values}")
                
            except Exception as e:
                print(f"   ❌ Error checking JS state: {e}")
            
            print("\n🎉 Test Completed!")
            print("=" * 60)
            
            # Keep browser open for manual inspection
            print("\n⏳ Browser will stay open for 30 seconds for manual inspection...")
            await page.wait_for_timeout(30000)
            
        except Exception as e:
            print(f"\n❌ Test failed with error: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(test_tag_filtering_comprehensive())