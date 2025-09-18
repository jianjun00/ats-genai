"""
Test data quality dashboard functionality with Playwright
"""
import pytest
import asyncio
from playwright.async_api import async_playwright, Page
from pathlib import Path
import sys

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent.parent))


class TestDataQualityDashboard:
    
    @pytest.mark.asyncio
    async def test_dashboard_loads(self):
        """Test that the data quality dashboard loads correctly"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)  # Set to True for headless execution
            page = await browser.new_page()
            
            try:
                # Navigate to dashboard
                await page.goto("http://localhost:4000/data-quality/dashboard", wait_until="networkidle")
                
                # Check page title
                title = await page.title()
                assert "ATS Data Quality Dashboard" in title
                
                # Check main elements are present
                assert await page.locator("h1").inner_text() == "🎯 ATS Data Quality Dashboard"
                
                # Check stats cards are present
                stats_cards = page.locator(".stat-card")
                assert await stats_cards.count() >= 4  # Should have total, critical, high, symbols affected
                
                print("✅ Dashboard loaded successfully")
                
            finally:
                await browser.close()
    
    @pytest.mark.asyncio
    async def test_agent_start_button_functionality(self):
        """Test the agent start button and verify it makes API calls"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            # Track network requests
            requests = []
            
            def handle_request(request):
                requests.append({
                    'url': request.url,
                    'method': request.method,
                    'headers': dict(request.headers)
                })
            
            page.on("request", handle_request)
            
            try:
                # Navigate to dashboard
                await page.goto("http://localhost:4000/data-quality/dashboard", wait_until="networkidle")
                
                # Wait for page to fully load
                await page.wait_for_selector("#agent-status")
                
                # Check initial agent status
                agent_status = await page.locator("#agent-status").inner_text()
                print(f"Initial agent status: {agent_status}")
                
                # Find the start button
                start_button = page.locator("#start-agent-btn")
                
                # Check if start button is visible
                is_visible = await start_button.is_visible()
                print(f"Start button visible: {is_visible}")
                
                if is_visible:
                    # Click the start button
                    print("Clicking start button...")
                    await start_button.click()
                    
                    # Wait a moment for the request to be made
                    await page.wait_for_timeout(2000)
                    
                    # Check if any POST requests were made to /agent/start
                    start_requests = [r for r in requests if '/agent/start' in r['url'] and r['method'] == 'POST']
                    print(f"Found {len(start_requests)} start requests")
                    
                    for req in start_requests:
                        print(f"  - {req['method']} {req['url']}")
                    
                    # Check if agent status changed
                    new_agent_status = await page.locator("#agent-status").inner_text()
                    print(f"New agent status: {new_agent_status}")
                    
                    # Check if stop button appeared
                    stop_button = page.locator("#stop-agent-btn")
                    stop_visible = await stop_button.is_visible()
                    print(f"Stop button visible after start: {stop_visible}")
                    
                else:
                    print("Start button not visible - agent may already be running")
                
                # Print all network requests for debugging
                print("\nAll network requests:")
                for req in requests:
                    if '/agent' in req['url'] or '/data-quality' in req['url']:
                        print(f"  - {req['method']} {req['url']}")
                
            finally:
                await browser.close()
    
    @pytest.mark.asyncio  
    async def test_api_endpoints_directly(self):
        """Test the API endpoints directly to debug the issue"""
        import aiohttp
        
        print("Testing API endpoints directly...")
        
        async with aiohttp.ClientSession() as session:
            try:
                # Test agent status endpoint
                async with session.get('http://localhost:4000/agent/status') as response:
                    status_data = await response.json()
                    print(f"Agent status: {status_data}")
                    
            except Exception as e:
                print(f"Error getting agent status: {e}")
            
            try:
                # Test agent start endpoint
                async with session.post('http://localhost:4000/agent/start') as response:
                    start_result = await response.json()
                    print(f"Agent start result: {start_result}")
                    
            except Exception as e:
                print(f"Error starting agent: {e}")
            
            try:
                # Test data quality issues endpoint
                async with session.get('http://localhost:4000/data-quality/api/issues') as response:
                    issues_data = await response.json()
                    print(f"Issues data: {issues_data}")
                    
            except Exception as e:
                print(f"Error getting issues: {e}")


if __name__ == "__main__":
    # Run the tests
    asyncio.run(TestDataQualityDashboard().test_dashboard_loads())
    asyncio.run(TestDataQualityDashboard().test_agent_start_button_functionality())
    asyncio.run(TestDataQualityDashboard().test_api_endpoints_directly())