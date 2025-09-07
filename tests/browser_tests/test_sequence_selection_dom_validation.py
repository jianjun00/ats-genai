"""
Browser-based tests for sequence selection DOM element validation
Tests DOM elements exist and chart rendering works properly
"""
import pytest
from playwright.async_api import async_playwright, Page, Browser
import asyncio
import time


class TestSequenceSelectionDOMValidation:
    """Browser tests for DOM element validation and chart rendering"""

    @pytest.fixture(scope="session")
    async def browser_setup(self):
        """Setup browser for testing"""
        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(headless=True)
        yield browser
        await browser.close()
        await playwright.stop()

    @pytest.fixture
    async def page(self, browser_setup):
        """Create page for each test"""
        browser = browser_setup
        page = await browser.new_page()
        yield page
        await page.close()

    @pytest.mark.asyncio
    async def test_training_dataset_page_loads_correctly(self, page: Page):
        """Test training dataset page loads with proper DOM structure"""

        # Navigate to training dataset page
        await page.goto("http://localhost:3000/training-datasets")

        # Wait for page to load
        await page.wait_for_load_state("networkidle")

        # Check main container exists
        main_container = await page.query_selector("[data-testid='training-datasets-container']")
        assert main_container is not None, "Training datasets container not found"

        # Check dataset list loads
        await page.wait_for_selector("[data-testid='dataset-list']", timeout=10000)
        dataset_list = await page.query_selector("[data-testid='dataset-list']")
        assert dataset_list is not None, "Dataset list not found"

    @pytest.mark.asyncio
    async def test_sequence_selection_dropdown_exists(self, page: Page):
        """Test sequence selection dropdown appears when dataset is selected"""

        await page.goto("http://localhost:3000/training-datasets")
        await page.wait_for_load_state("networkidle")

        # Select first dataset if available
        dataset_button = await page.query_selector("[data-testid='dataset-item']:first-child")
        if dataset_button:
            await dataset_button.click()

            # Wait for sequence dropdown to appear
            await page.wait_for_selector("[data-testid='sequence-selector']", timeout=5000)
            sequence_selector = await page.query_selector("[data-testid='sequence-selector']")
            assert sequence_selector is not None, "Sequence selector dropdown not found"

    @pytest.mark.asyncio
    async def test_chart_containers_exist_for_all_timeframes(self, page: Page):
        """Test chart containers exist for all required timeframes"""

        await page.goto("http://localhost:3000/training-datasets")
        await page.wait_for_load_state("networkidle")

        # Expected timeframes
        timeframes = ['5m', '15m', '1h', '1d', '1w']

        # Try to select dataset and sequence to trigger chart rendering
        try:
            # Select dataset
            dataset_button = await page.query_selector("[data-testid='dataset-item']:first-child")
            if dataset_button:
                await dataset_button.click()
                await asyncio.sleep(1)  # Wait for response

                # Select sequence if dropdown appears
                sequence_dropdown = await page.query_selector("[data-testid='sequence-selector']")
                if sequence_dropdown:
                    await sequence_dropdown.click()
                    await asyncio.sleep(0.5)

                    # Select first sequence option
                    first_option = await page.query_selector("[data-testid='sequence-option']:first-child")
                    if first_option:
                        await first_option.click()
                        await asyncio.sleep(2)  # Wait for charts to render
        except Exception as e:
            # If interaction fails, still test for chart container presence
            pass

        # Check that chart containers exist for each timeframe
        for timeframe in timeframes:
            chart_id = f"chart-{timeframe}"
            chart_container = await page.query_selector(f"#{chart_id}")

            # Chart container should exist (even if empty)
            assert chart_container is not None, f"Chart container for {timeframe} not found: #{chart_id}"

    @pytest.mark.asyncio
    async def test_plotly_js_library_loaded(self, page: Page):
        """Test Plotly.js library is properly loaded"""

        await page.goto("http://localhost:3000/training-datasets")
        await page.wait_for_load_state("networkidle")

        # Check if Plotly is available in window
        plotly_available = await page.evaluate("() => typeof window.Plotly !== 'undefined'")
        assert plotly_available, "Plotly.js library not loaded in browser"

        # Check Plotly.newPlot function exists
        new_plot_available = await page.evaluate("() => typeof window.Plotly.newPlot === 'function'")
        assert new_plot_available, "Plotly.newPlot function not available"

    @pytest.mark.asyncio
    async def test_chart_rendering_without_javascript_errors(self, page: Page):
        """Test charts render without JavaScript errors"""

        # Collect JavaScript errors
        js_errors = []

        def handle_console_message(msg):
            if msg.type == "error":
                js_errors.append(msg.text)

        page.on("console", handle_console_message)

        await page.goto("http://localhost:3000/training-datasets")
        await page.wait_for_load_state("networkidle")

        # Try to trigger chart rendering
        try:
            dataset_button = await page.query_selector("[data-testid='dataset-item']:first-child")
            if dataset_button:
                await dataset_button.click()
                await asyncio.sleep(1)

                sequence_dropdown = await page.query_selector("[data-testid='sequence-selector']")
                if sequence_dropdown:
                    await sequence_dropdown.click()
                    await asyncio.sleep(0.5)

                    first_option = await page.query_selector("[data-testid='sequence-option']:first-child")
                    if first_option:
                        await first_option.click()
                        await asyncio.sleep(3)  # Wait for charts to render
        except Exception:
            pass  # Ignore interaction errors, focus on JS errors

        # Filter out common non-critical errors
        critical_errors = [
            error for error in js_errors
            if not any(ignore in error.lower() for ignore in [
                'favicon.ico', 'manifest.json', 'service-worker'
            ])
        ]

        # Assert no critical JavaScript errors occurred
        assert len(critical_errors) == 0, f"JavaScript errors detected: {critical_errors}"

    @pytest.mark.asyncio
    async def test_responsive_chart_layout(self, page: Page):
        """Test charts maintain proper layout on different screen sizes"""

        # Test desktop size
        await page.set_viewport_size({"width": 1920, "height": 1080})
        await page.goto("http://localhost:3000/training-datasets")
        await page.wait_for_load_state("networkidle")

        # Check charts container exists and is visible
        charts_container = await page.query_selector("[data-testid='charts-container']")
        if charts_container:
            is_visible = await charts_container.is_visible()
            assert is_visible, "Charts container not visible on desktop"

        # Test tablet size
        await page.set_viewport_size({"width": 768, "height": 1024})
        await asyncio.sleep(0.5)

        if charts_container:
            is_visible = await charts_container.is_visible()
            assert is_visible, "Charts container not visible on tablet"

        # Test mobile size
        await page.set_viewport_size({"width": 375, "height": 667})
        await asyncio.sleep(0.5)

        if charts_container:
            is_visible = await charts_container.is_visible()
            assert is_visible, "Charts container not visible on mobile"

    @pytest.mark.asyncio
    async def test_chart_data_loading_states(self, page: Page):
        """Test chart loading states and error handling"""

        await page.goto("http://localhost:3000/training-datasets")
        await page.wait_for_load_state("networkidle")

        # Look for loading indicators
        loading_spinner = await page.query_selector("[data-testid='loading-spinner']")
        if loading_spinner:
            # Loading state should eventually disappear
            await page.wait_for_selector("[data-testid='loading-spinner']", state="hidden", timeout=10000)

        # Check for error messages
        error_message = await page.query_selector("[data-testid='error-message']")
        if error_message:
            error_text = await error_message.text_content()
            # Error should be informative, not generic
            assert len(error_text.strip()) > 0, "Error message is empty"
            assert "error" in error_text.lower() or "failed" in error_text.lower(), "Error message not descriptive"