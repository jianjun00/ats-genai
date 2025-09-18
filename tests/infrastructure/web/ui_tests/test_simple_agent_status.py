"""
Simple test to verify agent status functionality
"""
import pytest
import asyncio
from playwright.async_api import async_playwright


class TestSimpleAgentStatus:
    
    @pytest.mark.asyncio  
    async def test_agent_status_loads_correctly(self):
        """Test that agent status loads and displays correctly"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            try:
                # Navigate to dashboard
                await page.goto("http://localhost:4000/data-quality/dashboard", wait_until="networkidle")
                
                # Wait for the page to fully load
                await page.wait_for_selector("#agent-status", timeout=10000)
                
                # Wait a bit for JavaScript to execute
                await page.wait_for_timeout(5000)
                
                # Check current agent status text
                agent_status = await page.locator("#agent-status").inner_text()
                print(f"Agent status: {agent_status}")
                
                # The status should no longer be "Loading..." after our fix
                assert "Loading..." not in agent_status, f"Agent status still loading: {agent_status}"
                assert "IDLE" in agent_status or "ACTIVE" in agent_status, f"Expected IDLE or ACTIVE, got: {agent_status}"
                
                # Check that start button is visible for IDLE agent
                if "IDLE" in agent_status:
                    start_button = page.locator("#start-agent-btn")
                    is_visible = await start_button.is_visible()
                    assert is_visible, "Start button should be visible when agent is IDLE"
                    print("✅ Start button is visible for IDLE agent")
                
                # Check that tools and MCP status are shown
                assert "Tools:" in agent_status, f"Tools info missing from status: {agent_status}"
                assert "MCP Ready:" in agent_status, f"MCP Ready info missing from status: {agent_status}"
                
                print("✅ Agent status is displaying correctly!")
                
            finally:
                await browser.close()


if __name__ == "__main__":
    # Run the test directly
    asyncio.run(TestSimpleAgentStatus().test_agent_status_loads_correctly())