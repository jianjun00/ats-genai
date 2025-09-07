"""
Playwright test cases for the Training EDA Dashboard
Tests the training data visualization and analysis functionality
"""

import pytest
from playwright.sync_api import Page, expect
import time
import json

class TestTrainingEDADashboard:
    """Test suite for Training EDA Dashboard functionality"""

    @pytest.fixture(autouse=True)
    def setup(self, page: Page):
        """Setup before each test"""
        self.page = page
        self.base_url = "http://localhost:3000"

    def test_training_eda_page_loads(self):
        """Test that the training EDA page loads successfully"""
        # Navigate to training EDA dashboard
        self.page.goto(f"{self.base_url}/training-eda")

        # Check page title
        expect(self.page).to_have_title("Training Dataset EDA Dashboard")

        # Check main heading
        heading = self.page.locator("h1")
        expect(heading).to_contain_text("Training Dataset EDA Dashboard")

        # Check navigation links are present
        nav_links = self.page.locator(".nav-links a")
        expect(nav_links).to_have_count(2)

        # Verify specific nav link text
        expect(nav_links.nth(0)).to_contain_text("Table EDA Dashboard")
        expect(nav_links.nth(1)).to_contain_text("System Health")

    def test_dataset_selection_dropdown_loads(self):
        """Test that the dataset selection dropdown loads with training datasets"""
        self.page.goto(f"{self.base_url}/training-eda")

        # Wait for dropdown to be populated
        select = self.page.locator("#training-dataset-select")

        # Wait for the API call to complete and dropdown to be populated
        self.page.wait_for_function(
            "document.getElementById('training-dataset-select').options.length > 1",
            timeout=10000
        )

        # Check that dropdown has options (more than just the default "Select..." option)
        options = select.locator("option")
        expect(options).to_have_count_greater_than(1)

        # Check that we have our AAPL and TSLA datasets
        option_texts = []
        for i in range(options.count()):
            option_texts.append(options.nth(i).text_content())

        # Look for our generated datasets
        aapl_dataset_found = any("AAPL" in text and "historical" in text for text in option_texts)
        tsla_dataset_found = any("TSLA" in text and "historical" in text for text in option_texts)

        assert aapl_dataset_found, f"AAPL historical dataset not found in options: {option_texts}"
        assert tsla_dataset_found, f"TSLA historical dataset not found in options: {option_texts}"

    def test_dataset_selection_shows_stats(self):
        """Test that selecting a dataset shows the statistics cards"""
        self.page.goto(f"{self.base_url}/training-eda")

        # Wait for dropdown to be populated
        self.page.wait_for_function(
            "document.getElementById('training-dataset-select').options.length > 1",
            timeout=10000
        )

        select = self.page.locator("#training-dataset-select")

        # Find and select the AAPL historical dataset
        options = select.locator("option")
        aapl_option = None
        for i in range(options.count()):
            text = options.nth(i).text_content()
            if "AAPL" in text and "historical" in text:
                aapl_option = options.nth(i)
                break

        assert aapl_option is not None, "Could not find AAPL historical dataset option"

        # Select the AAPL dataset
        select.select_option(aapl_option.get_attribute("value"))

        # Wait for dataset info to be displayed
        dataset_info = self.page.locator("#dataset-info")
        expect(dataset_info).to_be_visible(timeout=5000)

        # Check that stat cards are populated with actual data
        total_sequences = self.page.locator("#total-sequences")
        feature_count = self.page.locator("#feature-count")
        quality_score = self.page.locator("#quality-score")

        # Verify AAPL dataset statistics
        expect(total_sequences).not_to_have_text("0")  # Should have sequences
        expect(feature_count).to_have_text("14")       # Should have 14 features
        expect(quality_score).to_have_text("1.00")     # Should have perfect quality score

        # Check date range shows historical data
        date_range = self.page.locator("#date-range")
        expect(date_range).to_contain_text("1995")  # Should start from 1995
        expect(date_range).to_contain_text("2025")  # Should go to 2025

    def test_tsla_dataset_selection(self):
        """Test selecting TSLA dataset and verify its specific characteristics"""
        self.page.goto(f"{self.base_url}/training-eda")

        # Wait for dropdown to be populated
        self.page.wait_for_function(
            "document.getElementById('training-dataset-select').options.length > 1",
            timeout=10000
        )

        select = self.page.locator("#training-dataset-select")

        # Find and select the TSLA historical dataset
        options = select.locator("option")
        tsla_option = None
        for i in range(options.count()):
            text = options.nth(i).text_content()
            if "TSLA" in text and "historical" in text:
                tsla_option = options.nth(i)
                break

        assert tsla_option is not None, "Could not find TSLA historical dataset option"

        # Select the TSLA dataset
        select.select_option(tsla_option.get_attribute("value"))

        # Wait for dataset info to be displayed
        dataset_info = self.page.locator("#dataset-info")
        expect(dataset_info).to_be_visible(timeout=5000)

        # Check TSLA-specific statistics
        feature_count = self.page.locator("#feature-count")
        expect(feature_count).to_have_text("14")  # Should have 14 features

        # Check date range shows TSLA IPO date (2010)
        date_range = self.page.locator("#date-range")
        expect(date_range).to_contain_text("2010")  # Should start from 2010 (TSLA IPO)
        expect(date_range).to_contain_text("2025")  # Should go to 2025

        # Verify sequences count is reasonable for TSLA (should be less than AAPL due to shorter history)
        total_sequences = self.page.locator("#total-sequences")
        sequences_text = total_sequences.text_content()
        sequences_count = int(sequences_text.replace(",", ""))
        assert sequences_count > 3000, f"TSLA should have > 3000 sequences, got {sequences_count}"
        assert sequences_count < 8000, f"TSLA should have < 8000 sequences (less than AAPL), got {sequences_count}"

    def test_api_endpoints_accessibility(self):
        """Test that the required API endpoints are accessible"""
        # Test training datasets list endpoint
        response = self.page.request.get(f"{self.base_url}/api/v1/training-datasets")
        assert response.status == 200, f"Training datasets API failed: {response.status}"

        data = response.json()
        assert "datasets" in data, "API response should contain datasets"
        assert len(data["datasets"]) > 0, "Should have at least one dataset"

        # Find our historical datasets
        aapl_dataset = None
        tsla_dataset = None
        for dataset in data["datasets"]:
            if "AAPL" in dataset["dataset_name"] and "historical" in dataset["dataset_name"]:
                aapl_dataset = dataset
            elif "TSLA" in dataset["dataset_name"] and "historical" in dataset["dataset_name"]:
                tsla_dataset = dataset

        assert aapl_dataset is not None, "AAPL historical dataset not found in API"
        assert tsla_dataset is not None, "TSLA historical dataset not found in API"

        # Test individual dataset endpoints
        aapl_id = aapl_dataset["id"]
        tsla_id = tsla_dataset["id"]

        # Test distributions endpoint
        dist_response = self.page.request.get(f"{self.base_url}/api/v1/training-datasets/{aapl_id}/distributions")
        assert dist_response.status == 200, f"Distributions API failed: {dist_response.status}"

        # Test data endpoint
        data_response = self.page.request.get(f"{self.base_url}/api/v1/training-datasets/{aapl_id}/data?page=1&limit=5")
        assert data_response.status == 200, f"Data API failed: {data_response.status}"

    def test_analysis_content_loads_on_selection(self):
        """Test that analysis content loads when a dataset is selected"""
        self.page.goto(f"{self.base_url}/training-eda")

        # Wait for dropdown to be populated
        self.page.wait_for_function(
            "document.getElementById('training-dataset-select').options.length > 1",
            timeout=10000
        )

        select = self.page.locator("#training-dataset-select")

        # Select first available dataset (not the default option)
        options = select.locator("option")
        first_dataset_option = None
        for i in range(1, options.count()):  # Skip the first "Select..." option
            if options.nth(i).get_attribute("value"):
                first_dataset_option = options.nth(i)
                break

        assert first_dataset_option is not None, "No valid dataset option found"

        # Select the dataset
        select.select_option(first_dataset_option.get_attribute("value"))

        # Wait for analysis content to load
        analysis_content = self.page.locator("#analysis-content")

        # Should show loading state first, then actual content
        self.page.wait_for_function(
            "!document.getElementById('analysis-content').textContent.includes('Select a training dataset')",
            timeout=10000
        )

        # Verify analysis content is no longer showing the default message
        expect(analysis_content).not_to_contain_text("Select a training dataset above")

    def test_training_data_table_section(self):
        """Test that the training data table section appears when a dataset is selected"""
        self.page.goto(f"{self.base_url}/training-eda")

        # Wait for dropdown and select a dataset
        self.page.wait_for_function(
            "document.getElementById('training-dataset-select').options.length > 1",
            timeout=10000
        )

        select = self.page.locator("#training-dataset-select")
        options = select.locator("option")

        # Find AAPL dataset
        for i in range(options.count()):
            text = options.nth(i).text_content()
            if "AAPL" in text and "historical" in text:
                select.select_option(options.nth(i).get_attribute("value"))
                break

        # Wait for table section to become visible
        table_section = self.page.locator("#training-data-table-section")
        expect(table_section).to_be_visible(timeout=10000)

        # Check table pagination controls
        prev_button = self.page.locator("#training-prev-page")
        next_button = self.page.locator("#training-next-page")
        page_info = self.page.locator("#training-page-info")

        expect(prev_button).to_be_visible()
        expect(next_button).to_be_visible()
        expect(page_info).to_contain_text("Page 1")

        # Check Ray processing checkbox
        ray_checkbox = self.page.locator("#use-ray-processing")
        expect(ray_checkbox).to_be_visible()

    def test_error_handling_for_invalid_dataset(self):
        """Test error handling when an invalid dataset ID is accessed via API"""
        # Test with invalid dataset ID
        response = self.page.request.get(f"{self.base_url}/api/v1/training-datasets/99999/distributions")
        # Should handle gracefully (either 404 or empty data, not crash)
        assert response.status in [404, 200], f"Invalid dataset should return 404 or handle gracefully, got {response.status}"

    def test_responsive_design_elements(self):
        """Test that key responsive design elements are present"""
        self.page.goto(f"{self.base_url}/training-eda")

        # Check that main responsive containers exist
        container = self.page.locator(".container")
        expect(container).to_be_visible()

        main_card = self.page.locator(".main-card")
        expect(main_card).to_be_visible()

        # Check stats grid exists (will be visible when dataset selected)
        stats_grid = self.page.locator(".stats-grid")
        expect(stats_grid).to_be_attached()  # Present in DOM even if not visible

        # Check that controls are properly styled
        controls = self.page.locator(".controls")
        expect(controls).to_be_visible()

    def test_navigation_links_work(self):
        """Test that navigation links work correctly"""
        self.page.goto(f"{self.base_url}/training-eda")

        # Test Table EDA Dashboard link
        table_eda_link = self.page.locator('a[href="/eda"]')
        expect(table_eda_link).to_be_visible()

        # Test System Health link
        health_link = self.page.locator('a[href="/health"]')
        expect(health_link).to_be_visible()

        # Click health link and verify it navigates
        health_link.click()
        expect(self.page).to_have_url(f"{self.base_url}/health")

        # Go back to training EDA
        self.page.goto(f"{self.base_url}/training-eda")

        # Click table EDA link and verify it navigates
        table_eda_link = self.page.locator('a[href="/eda"]')
        table_eda_link.click()
        expect(self.page).to_have_url(f"{self.base_url}/eda")

if __name__ == "__main__":
    # Run with: python -m pytest tests/playwright/test_training_eda_dashboard.py -v
    import subprocess
    subprocess.run([
        "python", "-m", "pytest",
        "tests/playwright/test_training_eda_dashboard.py",
        "-v", "--headed"  # Add --headed to see browser during testing
    ])