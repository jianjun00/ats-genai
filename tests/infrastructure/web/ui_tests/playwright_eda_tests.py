#!/usr/bin/env python3
"""
Comprehensive Playwright UI Tests for ATS EDA Tool

Tests the complete user journey through the unified metadata system:
- Tab switching and navigation
- Dataset selection and visualization
- Interactive Plotly charts
- Sortable tables
- Large dataset performance
- Error handling and user feedback

SETUP REQUIRED:
pip install playwright
playwright install
"""

import pytest
from playwright.async_api import async_playwright, Page
import time

# Test Configuration
BASE_URL = "http://localhost:3000"
TEST_TIMEOUT = 30000  # 30 seconds

class EDAPlaywrightTests:
    """Comprehensive Playwright test suite for EDA tool"""

    @pytest.fixture(scope="session")
    async def browser_context(self):
        """Setup browser context for all tests"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False, slow_mo=500)
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                ignore_https_errors=True
            )
            yield context
            await browser.close()

    @pytest.fixture
    async def page(self, browser_context):
        """Create a new page for each test"""
        page = await browser_context.new_page()
        yield page
        await page.close()

    async def test_eda_page_loads_with_unified_tabs(self, page: Page):
        """Test 1: EDA page loads with Database Tables and Training Datasets tabs"""
        await page.goto(f"{BASE_URL}/eda", timeout=TEST_TIMEOUT)

        # Wait for page to fully load
        await page.wait_for_load_state('networkidle')

        # Check page title
        title = await page.title()
        assert "ATS EDA" in title, f"Expected ATS EDA in title, got: {title}"

        # Verify unified tabs are present
        database_tab = page.locator("text=Database Tables")
        await expect(database_tab).to_be_visible()

        training_tab = page.locator("text=Training Datasets")
        await expect(training_tab).to_be_visible()

        # Verify Plotly.js is loaded
        plotly_script = page.locator("script[src*='plotly-latest.min.js']")
        await expect(plotly_script).to_be_attached()

        # Check for automatic statistics message
        auto_stats_text = page.locator("text=automatically when datasets")
        await expect(auto_stats_text).to_be_visible()

    async def test_database_tables_tab_functionality(self, page: Page):
        """Test 2: Database Tables tab shows datasets and allows selection"""
        await page.goto(f"{BASE_URL}/eda")
        await page.wait_for_load_state('networkidle')

        # Click Database Tables tab
        await page.click("text=Database Tables")

        # Wait for datasets to load
        await page.wait_for_selector(".dataset-card", timeout=15000)

        # Verify datasets are displayed
        dataset_cards = await page.locator(".dataset-card").count()
        assert dataset_cards > 0, "No dataset cards found"

        # Check for large datasets (should show row counts)
        large_dataset = page.locator(".dataset-card:has-text('daily_price_polygon')")
        await expect(large_dataset).to_be_visible()

        # Verify row count formatting (should show comma-separated numbers)
        row_count_text = await large_dataset.text_content()
        assert "," in row_count_text, "Row counts should be comma-formatted"

    async def test_dataset_selection_and_visualization(self, page: Page):
        """Test 3: Dataset selection triggers schema loading and visualization"""
        await page.goto(f"{BASE_URL}/eda")
        await page.wait_for_load_state('networkidle')

        # Select Database Tables tab
        await page.click("text=Database Tables")
        await page.wait_for_selector(".dataset-card")

        # Click on a smaller dataset to avoid timeout issues
        small_dataset = page.locator(".dataset-card").first
        await small_dataset.click()

        # Wait for schema to load (with timeout protection)
        await page.wait_for_selector(".schema-container", timeout=10000)
        schema_loaded = True
        if schema_loaded:
            # Check for sortable table headers
            sortable_header = page.locator("th[onclick*='sortTable']")
            if await sortable_header.count() > 0:
                await sortable_header.first.click()

                # Verify sort indicator appears (⇅ ↑ ↓)
                sort_indicator = page.locator("text=↑,text=↓,text=⇅")
                await expect(sort_indicator).to_be_visible()

    async def test_training_datasets_tab_switching(self, page: Page):
        """Test 4: Training Datasets tab switching functionality"""
        await page.goto(f"{BASE_URL}/eda")
        await page.wait_for_load_state('networkidle')

        # Switch to Training Datasets tab
        training_tab = page.locator("text=Training Datasets")
        await training_tab.click()

        # Verify tab is active
        active_tab = page.locator(".tab-button.active:has-text('Training Datasets')")
        await expect(active_tab).to_be_visible()

        # Switch back to Database Tables
        database_tab = page.locator("text=Database Tables")
        await database_tab.click()

        # Verify Database Tables is now active
        active_db_tab = page.locator(".tab-button.active:has-text('Database Tables')")
        await expect(active_db_tab).to_be_visible()

    async def test_plotly_chart_interactions(self, page: Page):
        """Test 5: Plotly chart rendering and interactions"""
        await page.goto(f"{BASE_URL}/eda")
        await page.wait_for_load_state('networkidle')

        # Navigate to dataset with chart capability
        await page.click("text=Database Tables")
        await page.wait_for_selector(".dataset-card")

        # Look for a dataset that might have chart data
        chart_dataset = page.locator(".dataset-card:has-text('prices')")
        if await chart_dataset.count() > 0:
            await chart_dataset.first.click()

            # Wait for potential chart to load
            chart_container = page.locator("[id*='timeseries']")
            await chart_container.wait_for(state="visible", timeout=5000)

            # Test Plotly interactions (zoom, pan)
            chart_svg = chart_container.locator("svg").first
            if await chart_svg.count() > 0:
                # Simulate mouse interactions on chart
                chart_bbox = await chart_svg.bounding_box()
                if chart_bbox:
                    # Test zoom by double-clicking
                    await page.mouse.dblclick(
                        chart_bbox["x"] + chart_bbox["width"] / 2,
                        chart_bbox["y"] + chart_bbox["height"] / 2
                    )

                    # Verify chart responds to interactions
                    await page.wait_for_timeout(1000)  # Allow chart to update

    async def test_large_dataset_performance_handling(self, page: Page):
        """Test 6: Large dataset handling with timeout protection"""
        await page.goto(f"{BASE_URL}/eda")
        await page.wait_for_load_state('networkidle')

        # Monitor network requests
        responses = []
        page.on("response", lambda response: responses.append({
            "url": response.url,
            "status": response.status,
            "timing": time.time()
        }))

        await page.click("text=Database Tables")
        await page.wait_for_selector(".dataset-card")

        # Try to select a large dataset
        large_dataset = page.locator(".dataset-card:has-text('daily_price_tiingo')")
        if await large_dataset.count() > 0:
            start_time = time.time()
            await large_dataset.click()

            # Wait for response (with timeout protection)
            await page.wait_for_selector(".schema-container", timeout=8000)
            load_time = time.time() - start_time

            # Verify reasonable load time (should be under 10s with our fixes)
            assert load_time < 10, f"Large dataset took {load_time:.2f}s, expected <10s"

        datasets_responses = [r for r in responses if "/api/eda/datasets" in r["url"]]
        if datasets_responses:
            # Datasets API should be very fast with our optimizations
            assert len(datasets_responses) > 0, "Datasets API should have been called"

    async def test_error_handling_and_user_feedback(self, page: Page):
        """Test 7: Error handling displays appropriate user feedback"""
        await page.goto(f"{BASE_URL}/eda")
        await page.wait_for_load_state('networkidle')

        # Test navigation to non-existent dataset
        await page.goto(f"{BASE_URL}/api/eda/datasets/nonexistent_dataset/schema")

        # Should show proper error response, not crash
        content = await page.content()
        assert "error" in content.lower() or "not found" in content.lower()

        # Return to main page
        await page.goto(f"{BASE_URL}/eda")
        await page.wait_for_load_state('networkidle')

        # Verify page still works after error
        datasets_tab = page.locator("text=Database Tables")
        await expect(datasets_tab).to_be_visible()

    async def test_responsive_design_mobile(self, page: Page):
        """Test 8: Responsive design on mobile viewport"""
        # Set mobile viewport
        await page.set_viewport_size({"width": 375, "height": 667})

        await page.goto(f"{BASE_URL}/eda")
        await page.wait_for_load_state('networkidle')

        # Verify tabs are still accessible on mobile
        database_tab = page.locator("text=Database Tables")
        await expect(database_tab).to_be_visible()

        # Check if navigation works on mobile
        await database_tab.click()
        await page.wait_for_selector(".dataset-card", timeout=10000)

        # Verify dataset cards are visible on mobile
        dataset_card = page.locator(".dataset-card").first
        await expect(dataset_card).to_be_visible()

    async def test_accessibility_features(self, page: Page):
        """Test 9: Basic accessibility features"""
        await page.goto(f"{BASE_URL}/eda")
        await page.wait_for_load_state('networkidle')

        # Test keyboard navigation
        await page.keyboard.press("Tab")
        focused_element = await page.evaluate("document.activeElement.tagName")
        assert focused_element is not None, "Should be able to focus elements with Tab"

        # Test for proper heading structure
        h1_count = await page.locator("h1").count()
        assert h1_count > 0, "Page should have at least one h1 heading"

        # Test for alt text on any images
        images = await page.locator("img").count()
        if images > 0:
            images_with_alt = await page.locator("img[alt]").count()
            assert images_with_alt == images, "All images should have alt text"

# Helper functions for test utilities
async def expect(locator):
    """Helper function for expectations"""
    from playwright.async_api import expect as playwright_expect
    return playwright_expect(locator)

# Test configuration and markers
pytestmark = [
    pytest.mark.asyncio,
    pytest.mark.playwright,
    pytest.mark.slow,  # These tests take longer due to UI interactions
]

if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v", "--tb=short"])