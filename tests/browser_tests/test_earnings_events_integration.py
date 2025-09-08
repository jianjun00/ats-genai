#!/usr/bin/env python3
"""
Playwright integration tests for earnings events functionality in the analytics dashboard.

This test verifies that:
1. The earnings events button appears in the dashboard
2. Clicking the button loads earnings events data
3. The earnings events display properly with summary cards and table
4. The API endpoint returns proper data structure
"""

import pytest
import asyncio
import time
from playwright.async_api import async_playwright, expect
from playwright.sync_api import sync_playwright


class TestEarningsEventsIntegration:
    """Test earnings events integration in the analytics dashboard."""

    @pytest.fixture(scope="class")
    def analytics_url(self):
        """Analytics service URL for testing."""
        return "http://localhost:3000"

    def test_earnings_events_button_exists(self, analytics_url):
        """Test that the earnings events button exists in the dashboard."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            # Navigate to analytics dashboard
            page.goto(analytics_url)
            page.wait_for_load_state("networkidle")
            
            # Check that the earnings events button exists
            earnings_button = page.locator('button:has-text("📊 Earnings Events")')
            assert earnings_button.is_visible()
            
            browser.close()

    def test_earnings_events_api_endpoint(self, analytics_url):
        """Test that the earnings events API endpoint returns valid data."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            # Test API endpoint directly
            response = page.request.get(f"{analytics_url}/api/earnings-events?limit=5")
            assert response.status == 200
            
            # Parse JSON response
            data = response.json()
            
            # Verify response structure
            assert "success" in data
            assert "events" in data
            assert "total_events" in data
            assert "unique_symbols" in data
            assert "summary" in data
            
            # Verify summary structure
            summary = data["summary"]
            assert "eps_beats" in summary
            assert "eps_misses" in summary
            assert "revenue_beats" in summary
            assert "revenue_misses" in summary
            assert "guidance_raised" in summary
            assert "guidance_lowered" in summary
            
            browser.close()

    def test_earnings_events_dashboard_interaction(self, analytics_url):
        """Test full earnings events dashboard interaction."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            # Navigate to analytics dashboard
            page.goto(analytics_url)
            page.wait_for_load_state("networkidle")
            
            # Click the earnings events button
            earnings_button = page.locator('button:has-text("📊 Earnings Events")')
            earnings_button.click()
            
            # Wait for content to load
            page.wait_for_timeout(3000)
            
            # Check for loading message initially
            content_div = page.locator('#analysis-content')
            
            # Wait for actual content to load (not loading message)
            page.wait_for_function(
                """() => {
                    const content = document.getElementById('analysis-content');
                    return content && content.innerHTML.includes('Earnings Events Analysis');
                }""",
                timeout=10000
            )
            
            # Verify earnings events content is displayed
            assert "📊 Earnings Events Analysis" in content_div.inner_text()
            
            # Check for summary cards
            assert page.locator('text=Total Events').is_visible()
            assert page.locator('text=EPS Beats').is_visible()
            assert page.locator('text=Revenue Beats').is_visible()
            assert page.locator('text=Guidance Changes').is_visible()
            assert page.locator('text=Unique Symbols').is_visible()
            
            # Check for table headers
            assert page.locator('th:has-text("Symbol")').is_visible()
            assert page.locator('th:has-text("Period")').is_visible()
            assert page.locator('th:has-text("EPS")').is_visible()
            assert page.locator('th:has-text("Revenue")').is_visible()
            assert page.locator('th:has-text("Beats")').is_visible()
            assert page.locator('th:has-text("Guidance")').is_visible()
            
            browser.close()

    def test_earnings_events_error_handling(self, analytics_url):
        """Test earnings events error handling."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            # Test API endpoint with invalid parameters
            response = page.request.get(f"{analytics_url}/api/earnings-events?limit=invalid")
            
            # Should still return 200 but handle the error gracefully
            assert response.status == 200
            
            data = response.json()
            # Should either succeed or return error structure
            assert "success" in data or "error" in data
            
            browser.close()

    def test_earnings_events_symbol_filtering(self, analytics_url):
        """Test earnings events symbol filtering."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            # Test API endpoint with symbol filter
            response = page.request.get(f"{analytics_url}/api/earnings-events?symbol=HP&limit=10")
            assert response.status == 200
            
            data = response.json()
            assert "success" in data
            assert "symbol_filter" in data
            assert data["symbol_filter"] == "HP"
            
            # If events exist, they should be for the specified symbol
            if data.get("events"):
                for event in data["events"]:
                    assert event.get("symbol") == "HP"
            
            browser.close()

    def test_earnings_events_performance_summary(self, analytics_url):
        """Test that earnings events include performance summary calculations.""" 
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            # Navigate to analytics dashboard
            page.goto(analytics_url)
            page.wait_for_load_state("networkidle")
            
            # Click the earnings events button
            earnings_button = page.locator('button:has-text("📊 Earnings Events")')
            earnings_button.click()
            
            # Wait for content to load
            page.wait_for_function(
                """() => {
                    const content = document.getElementById('analysis-content');
                    return content && content.innerHTML.includes('Performance Summary');
                }""",
                timeout=10000
            )
            
            # Check for performance summary section
            assert page.locator('text=📈 Performance Summary').is_visible()
            assert page.locator('text=EPS Performance').is_visible()
            assert page.locator('text=Revenue Performance').is_visible()
            assert page.locator('text=EPS Success Rate').is_visible()
            assert page.locator('text=Revenue Success Rate').is_visible()
            
            browser.close()


if __name__ == "__main__":
    # Run the tests directly for debugging
    import subprocess
    subprocess.run([
        "python", "-m", "pytest", __file__, "-v", "--tb=short"
    ])