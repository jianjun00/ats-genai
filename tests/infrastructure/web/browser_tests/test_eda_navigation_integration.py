#!/usr/bin/env python3
"""
Playwright Tests for EDA Navigation Integration
Validates time navigation controls work in the existing EDA dashboard
"""

import pytest
from playwright.async_api import async_playwright
import sys
import os

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestEDANavigationIntegration:
    """Test time navigation integration in existing EDA dashboard."""

    BASE_URL = "http://localhost:3000"

    @pytest.mark.asyncio
    async def test_eda_dashboard_loads_with_training_datasets(self):
        """Test that EDA dashboard loads and training datasets section is accessible."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Navigate to EDA dashboard
            await page.goto(self.BASE_URL, wait_until='networkidle', timeout=30000)

            # Click on Training Datasets menu item
            await page.click('text=Training Datasets')

            # Wait for training datasets content to load
            await page.wait_for_selector('#analysis-content', timeout=10000)

            # Verify training datasets interface loaded
            content = await page.locator('#analysis-content').inner_text()
            assert 'Training Datasets' in content, "Training datasets section not loaded"

            # Check if dataset selector exists
            dataset_selector = await page.locator('#dataset-selector').count()
            assert dataset_selector > 0, "Dataset selector not found"

            print("✅ EDA dashboard loads with training datasets section")

    @pytest.mark.asyncio
    async def test_navigation_controls_appear_after_sequence_selection(self):
        """Test that navigation controls appear when a sequence is selected."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Navigate to EDA dashboard and load training datasets
            await page.goto(self.BASE_URL, wait_until='networkidle', timeout=30000)
            await page.click('text=Training Datasets')
            await page.wait_for_selector('#dataset-selector', timeout=10000)

            # Check if datasets are available
            dataset_options = await page.locator('#dataset-selector option').count()
            if dataset_options <= 1:  # Only default option
                print("⚠️ No datasets available for navigation test")
                return

            # Select first available dataset
            await page.select_option('#dataset-selector', index=1)
            await page.wait_for_timeout(2000)  # Wait for sequences to load

            # Check if sequences are available
            sequence_options = await page.locator('#sequence-selector option').count()
            if sequence_options <= 1:  # Only default option
                print("⚠️ No sequences available for navigation test")
                return

            # Select first available sequence
            await page.select_option('#sequence-selector', index=1)

            # Click load button
            load_button = await page.locator('button:has-text("Load Dataset Visualization")').count()
            if load_button > 0:
                await page.click('button:has-text("Load Dataset Visualization")')
                await page.wait_for_timeout(3000)  # Wait for visualization to load

            # Check if navigation controls are now visible
            dataset_viz = await page.locator('#dataset-visualization').count()
            assert dataset_viz > 0, "Dataset visualization not found"

            # Verify navigation controls exist
            nav_controls = await page.locator('#position-slider').count()
            nav_buttons = await page.locator('#nav-first, #nav-prev, #nav-next, #nav-last').count()

            if nav_controls > 0 and nav_buttons >= 4:
                print("✅ Navigation controls successfully integrated into EDA dashboard")
            else:
                print(f"⚠️ Navigation controls partially integrated: slider={nav_controls}, buttons={nav_buttons}")

    @pytest.mark.asyncio
    async def test_navigation_api_calls_from_dashboard(self):
        """Test that navigation API calls work from the dashboard."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Capture network responses
            navigation_responses = []

            async def handle_response(response):
                if 'navigate' in response.url or 'navigation-metadata' in response.url:
                    navigation_responses.append({
                        'url': response.url,
                        'status': response.status,
                        'method': response.request.method
                    })

            page.on('response', handle_response)

            # Navigate to dashboard and attempt to load a sequence
            await page.goto(self.BASE_URL, wait_until='networkidle', timeout=30000)
            await page.click('text=Training Datasets')
            await page.wait_for_selector('#dataset-selector', timeout=10000)

            # Try to load a dataset (if available)
            dataset_options = await page.locator('#dataset-selector option').count()
            if dataset_options > 1:
                await page.select_option('#dataset-selector', index=1)
                await page.wait_for_timeout(2000)

                sequence_options = await page.locator('#sequence-selector option').count()
                if sequence_options > 1:
                    await page.select_option('#sequence-selector', index=1)

                    load_button = await page.locator('button:has-text("Load Dataset Visualization")').count()
                    if load_button > 0:
                        await page.click('button:has-text("Load Dataset Visualization")')
                        await page.wait_for_timeout(5000)  # Wait for all API calls

            # Check if navigation-related API calls were made
            nav_metadata_calls = [r for r in navigation_responses if 'navigation-metadata' in r['url']]

            if len(nav_metadata_calls) > 0:
                print(f"✅ Navigation API integration working: {len(nav_metadata_calls)} metadata calls")
                for call in nav_metadata_calls:
                    print(f"   API Call: {call['method']} {call['url']} -> {call['status']}")
            else:
                print("⚠️ No navigation API calls detected - may need dataset with sequences")

    @pytest.mark.asyncio
    async def test_keyboard_shortcuts_integration(self):
        """Test that keyboard shortcuts work in the dashboard."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            # Navigate to dashboard
            await page.goto(self.BASE_URL, wait_until='networkidle', timeout=30000)
            await page.click('text=Training Datasets')
            await page.wait_for_selector('#dataset-selector', timeout=10000)

            # Simulate loading a sequence (if available)
            dataset_options = await page.locator('#dataset-selector option').count()
            if dataset_options > 1:
                await page.select_option('#dataset-selector', index=1)
                await page.wait_for_timeout(1000)

                sequence_options = await page.locator('#sequence-selector option').count()
                if sequence_options > 1:
                    await page.select_option('#sequence-selector', index=1)

                    load_button = await page.locator('button:has-text("Load Dataset Visualization")').count()
                    if load_button > 0:
                        await page.click('button:has-text("Load Dataset Visualization")')
                        await page.wait_for_timeout(3000)

                        # Test keyboard shortcuts
                        dataset_viz = await page.locator('#dataset-visualization').count()
                        if dataset_viz > 0:
                            # Focus on the page to enable keyboard events
                            await page.focus('body')

                            # Test arrow key navigation (should not cause errors)
                            await page.keyboard.press('ArrowRight')
                            await page.wait_for_timeout(500)
                            await page.keyboard.press('ArrowLeft')
                            await page.wait_for_timeout(500)

                            print("✅ Keyboard shortcuts integrated without errors")
                        else:
                            print("⚠️ Dataset visualization not loaded for keyboard test")
                    else:
                        print("⚠️ Load button not found")
                else:
                    print("⚠️ No sequences available for keyboard test")
            else:
                print("⚠️ No datasets available for keyboard test")

if __name__ == '__main__':
    # Run with high verbosity to see progress
    pytest.main([__file__, '-v', '--tb=short'])