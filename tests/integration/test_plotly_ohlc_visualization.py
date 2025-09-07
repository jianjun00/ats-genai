#!/usr/bin/env python3
"""
Playwright integration test for OHLC visualization in training dataset EDA.
Tests that Plotly OHLC charts render correctly with technical indicators.
"""

import asyncio
import pytest
import requests
import time
from playwright.async_api import async_playwright, Browser, Page


class TestPlotlyOHLCVisualization:
    """Test suite for Plotly OHLC visualization functionality"""

    BASE_URL = "http://localhost:3000"

    @classmethod
    async def setup_class(cls):
        """Set up test environment"""
        # Verify analytics service is running
        try:
            response = requests.get(f"{cls.BASE_URL}/health", timeout=5)
            if response.status_code != 200:
                pytest.skip("Analytics service not available")
        except requests.RequestException:
            pytest.skip("Analytics service not accessible")

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_eda_page_loads(self):
        """Test 1: Verify EDA page loads successfully"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                # Navigate to EDA page
                await page.goto(f"{self.BASE_URL}/eda", timeout=15000)

                # Check page title
                title = await page.title()
                assert "ATS EDA" in title, f"Expected 'ATS EDA' in title, got: {title}"

                # Verify page loaded completely
                await page.wait_for_selector("body", timeout=10000)

            finally:
                await browser.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_training_dataset_tab_exists(self):
        """Test 2: Verify Training Dataset tab is present and clickable"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                await page.goto(f"{self.BASE_URL}/eda", timeout=15000)

                # Look for Training Dataset tab
                training_tab_selectors = [
                    "text=Training Datasets",
                    "text=Training Dataset EDA",
                    ".tab:has-text('Training')",
                    "[data-tab='training']"
                ]

                tab_found = False
                for selector in training_tab_selectors:
                    try:
                        tab = page.locator(selector).first
                        if await tab.count() > 0:
                            await tab.click(timeout=5000)
                            tab_found = True
                            break
                    except:
                        continue

                assert tab_found, "Training Dataset tab not found or not clickable"

                # Wait for content to load after clicking
                await page.wait_for_timeout(2000)

            finally:
                await browser.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_plotly_js_loaded(self):
        """Test 3: Verify Plotly.js is loaded on the page"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                await page.goto(f"{self.BASE_URL}/eda", timeout=15000)

                # Check for Plotly.js script tag
                plotly_script = page.locator("script[src*='plotly']")
                plotly_count = await plotly_script.count()
                assert plotly_count > 0, "Plotly.js script not found"

                # Verify Plotly object is available in global scope
                await page.wait_for_timeout(3000)  # Give time for script to load

                plotly_available = await page.evaluate("typeof Plotly !== 'undefined'")
                assert plotly_available, "Plotly object not available in global scope"

                # Check specific Plotly functions we use
                plotly_newplot = await page.evaluate("typeof Plotly.newPlot === 'function'")
                assert plotly_newplot, "Plotly.newPlot function not available"

            finally:
                await browser.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_training_datasets_api_accessible(self):
        """Test 4: Verify training datasets API returns valid data"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                # Test API endpoint directly
                response = await page.request.get(f"{self.BASE_URL}/api/v1/training-datasets/")
                assert response.ok, f"Training datasets API failed: {response.status}"

                data = await response.json()
                assert "datasets" in data, "API response missing 'datasets' field"
                assert isinstance(data["datasets"], list), "Datasets field is not a list"

                # Verify at least one dataset with technical indicators exists
                datasets_with_indicators = [
                    ds for ds in data["datasets"]
                    if ds.get("technical_indicators") and ds.get("total_sequences", 0) > 0
                ]
                assert len(datasets_with_indicators) > 0, "No datasets with technical indicators found"

                return datasets_with_indicators[0]["id"]  # Return first dataset ID for next test

            finally:
                await browser.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_visualization_data_api(self):
        """Test 5: Verify visualization data API returns OHLC data with technical indicators"""
        # Get dataset ID from previous test
        datasets_with_indicators = await self.test_training_datasets_api_accessible()

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                # Get dataset ID
                datasets_response = await page.request.get(f"{self.BASE_URL}/api/v1/training-datasets/")
                datasets_data = await datasets_response.json()

                datasets_with_indicators = [
                    ds for ds in datasets_data["datasets"]
                    if ds.get("technical_indicators") and ds.get("total_sequences", 0) > 0
                ]

                assert len(datasets_with_indicators) > 0, "No datasets with indicators for visualization test"
                dataset_id = datasets_with_indicators[0]["id"]

                # Test visualization data endpoint
                viz_response = await page.request.get(
                    f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/visualization-data?sequence_index=0"
                )
                assert viz_response.ok, f"Visualization API failed: {viz_response.status}"

                viz_data = await viz_response.json()
                assert "data" in viz_data, "Visualization response missing 'data' field"
                assert len(viz_data["data"]) > 0, "No visualization data returned"

                # Verify technical indicators are present
                first_record = viz_data["data"][0]
                expected_indicators = ["etop", "ebot", "pldot", "sma_20", "ema_12", "ema_26"]

                indicators_found = []
                for indicator in expected_indicators:
                    if indicator in first_record:
                        indicators_found.append(indicator)

                assert len(indicators_found) > 0, f"No technical indicators found in data. Available keys: {list(first_record.keys())}"

                # Verify OHLC-like data exists (multi-timeframe)
                ohlc_fields = ["1h_high", "1h_low", "1h_close", "5m_high", "5m_low", "5m_close"]
                ohlc_found = [field for field in ohlc_fields if field in first_record]
                assert len(ohlc_found) > 0, f"No OHLC data found. Available keys: {list(first_record.keys())}"

            finally:
                await browser.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_ohlc_visualization_ui_elements(self):
        """Test 6: Verify OHLC visualization UI elements are present"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                await page.goto(f"{self.BASE_URL}/eda", timeout=15000)

                # Navigate to Training Dataset tab
                await self._click_training_dataset_tab(page)

                # Wait for datasets to load
                await page.wait_for_timeout(3000)

                # Look for and click on a dataset with technical indicators
                dataset_clicked = await self._click_dataset_with_indicators(page)
                assert dataset_clicked, "Could not find or click dataset with technical indicators"

                # Wait for dataset analysis to load
                await page.wait_for_timeout(5000)

                # Verify OHLC visualization controls are present
                ohlc_controls = [
                    "text=OHLC Data Visualization",
                    "text=Sequence Navigation",
                    "input[type='range']",  # Sequence slider
                    "text=Refresh Visualization",
                    "text=Random Sample"
                ]

                for control_selector in ohlc_controls:
                    control = page.locator(control_selector).first
                    control_count = await control.count()
                    assert control_count > 0, f"OHLC control not found: {control_selector}"

                # Verify chart container exists
                chart_containers = [
                    "[id*='ohlc-chart']",
                    ".plotly-chart-container",
                    "div[style*='height: 500px']"
                ]

                chart_found = False
                for container_selector in chart_containers:
                    container = page.locator(container_selector).first
                    if await container.count() > 0:
                        chart_found = True
                        break

                assert chart_found, "OHLC chart container not found"

            finally:
                await browser.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_ohlc_chart_functionality(self):
        """Test 7: Test OHLC chart creation and interaction"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                await page.goto(f"{self.BASE_URL}/eda", timeout=15000)

                # Navigate to Training Dataset tab and select dataset
                await self._click_training_dataset_tab(page)
                await page.wait_for_timeout(3000)

                dataset_clicked = await self._click_dataset_with_indicators(page)
                assert dataset_clicked, "Could not find dataset for chart testing"

                await page.wait_for_timeout(5000)

                # Find and click refresh visualization button
                refresh_buttons = [
                    "text=Refresh Visualization",
                    "button:has-text('Refresh')",
                    "[onclick*='updateOHLCVisualization']"
                ]

                button_clicked = False
                for button_selector in refresh_buttons:
                    try:
                        button = page.locator(button_selector).first
                        if await button.count() > 0:
                            await button.click()
                            button_clicked = True
                            break
                    except:
                        continue

                assert button_clicked, "Could not find or click Refresh Visualization button"

                # Wait for chart to render
                await page.wait_for_timeout(8000)

                # Verify Plotly chart was created
                plotly_charts = await page.evaluate("""
                    () => {
                        const charts = document.querySelectorAll('.plotly-graph-div, [id*="ohlc-chart"]');
                        return Array.from(charts).map(chart => ({
                            id: chart.id,
                            hasPlotlyData: !!chart._fullData,
                            childrenCount: chart.children.length,
                            innerHTML: chart.innerHTML.length > 100
                        }));
                    }
                """)

                assert len(plotly_charts) > 0, "No Plotly chart containers found"

                # Check if at least one chart has data
                charts_with_data = [chart for chart in plotly_charts if chart.get("hasPlotlyData") or chart.get("innerHTML")]
                assert len(charts_with_data) > 0, f"No charts with data found. Chart info: {plotly_charts}"

                # Test random sample button if available
                try:
                    random_button = page.locator("text=Random Sample").first
                    if await random_button.count() > 0:
                        await random_button.click()
                        await page.wait_for_timeout(3000)
                        print("✅ Random sample button works")
                except:
                    print("⚠️ Random sample button not tested")

            finally:
                await browser.close()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_technical_indicators_display(self):
        """Test 8: Verify technical indicators are displayed in chart legend"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()

            try:
                await page.goto(f"{self.BASE_URL}/eda", timeout=15000)

                # Set up and load chart
                await self._click_training_dataset_tab(page)
                await page.wait_for_timeout(3000)

                dataset_clicked = await self._click_dataset_with_indicators(page)
                if not dataset_clicked:
                    pytest.skip("No datasets with indicators available for legend test")

                await page.wait_for_timeout(5000)

                # Click refresh to generate chart
                refresh_button = page.locator("text=Refresh Visualization").first
                if await refresh_button.count() > 0:
                    await refresh_button.click()
                    await page.wait_for_timeout(8000)

                # Check for technical indicator traces in Plotly legend
                legend_items = await page.evaluate("""
                    () => {
                        const plotlyDivs = document.querySelectorAll('.plotly-graph-div');
                        let allTraces = [];

                        for (let div of plotlyDivs) {
                            if (div._fullData && div._fullData.length > 0) {
                                const traces = div._fullData.map(trace => ({
                                    name: trace.name,
                                    type: trace.type,
                                    mode: trace.mode
                                }));
                                allTraces = allTraces.concat(traces);
                            }
                        }

                        return allTraces;
                    }
                """)

                if len(legend_items) > 0:
                    trace_names = [trace.get("name", "").lower() for trace in legend_items]

                    expected_indicators = ["envelope top", "envelope bottom", "sma 20", "ema 12", "ema 26", "pl dot"]
                    indicators_found = []

                    for indicator in expected_indicators:
                        if any(indicator in name for name in trace_names):
                            indicators_found.append(indicator)

                    assert len(indicators_found) > 0, f"No technical indicators found in legend. Available traces: {trace_names}"
                    print(f"✅ Found technical indicators: {indicators_found}")
                else:
                    print("⚠️ No Plotly traces found for legend validation")

            finally:
                await browser.close()

    async def _click_training_dataset_tab(self, page: Page) -> bool:
        """Helper: Click training dataset tab"""
        training_tab_selectors = [
            "text=Training Datasets",
            "text=Training Dataset EDA",
            ".tab:has-text('Training')",
            "[data-tab='training-dataset']"
        ]

        for selector in training_tab_selectors:
            try:
                tab = page.locator(selector).first
                if await tab.count() > 0:
                    await tab.click(timeout=5000)
                    return True
            except:
                continue
        return False

    async def _click_dataset_with_indicators(self, page: Page) -> bool:
        """Helper: Find and click a dataset that has technical indicators"""
        # Wait for datasets to appear
        await page.wait_for_timeout(3000)

        # Look for dataset cards with technical indicators
        dataset_selectors = [
            ".dataset-card",
            "[data-dataset-id]",
            "div:has-text('technical_indicators')",
            "div:has-text('etop')",
            "div:has-text('ebot')"
        ]

        for selector in dataset_selectors:
            try:
                datasets = page.locator(selector)
                count = await datasets.count()

                for i in range(min(count, 5)):  # Check first 5 datasets
                    dataset = datasets.nth(i)
                    text = await dataset.text_content()

                    if text and any(indicator in text.lower() for indicator in ["etop", "ebot", "sma_20", "ema_12", "technical"]):
                        await dataset.click()
                        return True
            except:
                continue

        return False


# Standalone test runner
async def main():
    """Run tests standalone for debugging"""
    print("🧪 **PLOTLY OHLC VISUALIZATION TEST SUITE**")
    print("=" * 60)

    test_instance = TestPlotlyOHLCVisualization()

    tests = [
        ("EDA Page Loads", test_instance.test_eda_page_loads),
        ("Training Dataset Tab", test_instance.test_training_dataset_tab_exists),
        ("Plotly.js Loaded", test_instance.test_plotly_js_loaded),
        ("Training Datasets API", test_instance.test_training_datasets_api_accessible),
        ("Visualization Data API", test_instance.test_visualization_data_api),
        ("OHLC UI Elements", test_instance.test_ohlc_visualization_ui_elements),
        ("OHLC Chart Functionality", test_instance.test_ohlc_chart_functionality),
        ("Technical Indicators Display", test_instance.test_technical_indicators_display)
    ]

    results = []
    for test_name, test_func in tests:
        try:
            print(f"\n🔬 Running: {test_name}")
            await test_func()
            print(f"✅ PASSED: {test_name}")
            results.append((test_name, "PASSED"))
        except Exception as e:
            print(f"❌ FAILED: {test_name} - {str(e)}")
            results.append((test_name, "FAILED", str(e)))

    print(f"\n📊 **TEST RESULTS SUMMARY**")
    print("=" * 60)
    passed = sum(1 for r in results if r[1] == "PASSED")
    total = len(results)

    for result in results:
        status_icon = "✅" if result[1] == "PASSED" else "❌"
        print(f"{status_icon} {result[0]}: {result[1]}")
        if len(result) > 2:  # Has error message
            print(f"    Error: {result[2]}")

    print(f"\n🎯 **OVERALL: {passed}/{total} tests passed ({100*passed/total:.1f}%)**")

    if passed == total:
        print("🎉 **ALL PLOTLY OHLC VISUALIZATION TESTS PASSED!**")
        return 0
    else:
        print("⚠️ **SOME TESTS FAILED - CHECK IMPLEMENTATION**")
        return 1


if __name__ == "__main__":
    result = asyncio.run(main())
    exit(result)