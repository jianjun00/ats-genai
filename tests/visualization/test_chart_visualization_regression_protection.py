#!/usr/bin/env python3
"""
Chart Visualization Regression Protection Test Suite

This test suite protects against regressions in the chart visualization functionality.
It ensures that the Chart.js integration, modal system, and interactive visualizations
continue to work correctly after any changes to the codebase.

Critical Areas Protected:
1. Chart.js library inclusion
2. Modal system functionality
3. Visualization button functionality
4. JavaScript chart rendering functions
5. CSS styling for charts
6. API endpoint integration
7. Real data visualization workflow

Run this before any deployment to prevent visualization regressions!
"""

import pytest
import requests
import re
import json
from typing import Dict, List, Any
import time


class TestChartVisualizationRegression:
    """
    Comprehensive regression protection for chart visualization functionality.

    This test class covers all critical components that could break visualization:
    - Chart.js library loading
    - Modal HTML structure
    - JavaScript function definitions
    - CSS styling integrity
    - Button onclick handlers
    - API data integration
    - End-to-end workflow
    """

    BASE_URL = "http://localhost:3000"

    def setup_method(self):
        """Setup for each test - verify service is running"""
        try:
            response = requests.get(f"{self.BASE_URL}/health", timeout=5)
            assert response.status_code == 200, "Service not running - start port-forward"
        except requests.exceptions.RequestException:
            pytest.fail("Service not accessible - ensure kubectl port-forward is running")

    def test_chartjs_library_inclusion(self):
        """
        CRITICAL: Verify Chart.js library is included

        Regression Risk: HIGH
        - Someone might remove the Chart.js script tag
        - CDN URL might be changed or broken
        - Script tag might be moved to wrong location
        """
        response = requests.get(f"{self.BASE_URL}/")
        html_content = response.text

        # Test 1: Chart.js script tag exists
        chartjs_pattern = r'<script\s+src="[^"]*chart\.js[^"]*"'
        assert re.search(chartjs_pattern, html_content, re.IGNORECASE), \
            "Chart.js script tag missing - visualization will be completely broken!"

        # Test 2: CDN URL is correct
        assert "cdn.jsdelivr.net/npm/chart.js" in html_content, \
            "Chart.js CDN URL incorrect - charts will not load!"

        # Test 3: Script tag is in head section
        head_section = re.search(r'<head.*?</head>', html_content, re.DOTALL)
        assert head_section and "chart.js" in head_section.group(), \
            "Chart.js not in head section - may cause loading issues!"

    def test_modal_html_structure(self):
        """
        CRITICAL: Verify modal HTML structure is intact

        Regression Risk: HIGH
        - Modal divs might be accidentally deleted
        - Modal IDs might be changed
        - Modal structure might be corrupted
        """
        response = requests.get(f"{self.BASE_URL}/")
        html_content = response.text

        # Test 1: Distributions modal exists
        assert 'id="distributions-modal"' in html_content, \
            "Distributions modal missing - feature charts will not display!"

        # Test 2: OHLC modal exists
        assert 'id="ohlc-modal"' in html_content, \
            "OHLC modal missing - price charts will not display!"

        # Test 3: Modal content containers exist
        assert 'id="distributions-content"' in html_content, \
            "Distributions content container missing!"
        assert 'id="ohlc-content"' in html_content, \
            "OHLC content container missing!"

        # Test 4: Close button functionality
        assert 'onclick="closeModal(' in html_content, \
            "Modal close functionality missing - users will be stuck in modals!"

        # Test 5: Modal class structure
        assert 'class="modal"' in html_content, \
            "Modal CSS class missing - styling will be broken!"
        assert 'class="modal-content"' in html_content, \
            "Modal content CSS class missing!"

    def test_visualization_button_functionality(self):
        """
        CRITICAL: Verify visualization buttons call JavaScript functions

        Regression Risk: VERY HIGH
        - Buttons might revert to raw JSON links
        - onclick handlers might be removed
        - Function names might be changed
        """
        response = requests.get(f"{self.BASE_URL}/")
        html_content = response.text

        # Test 1: Distribution buttons use JavaScript functions
        distributions_pattern = r'onclick="showDistributions\([^)]+\)"'
        assert re.search(distributions_pattern, html_content), \
            "Distribution buttons not using JavaScript functions - will show raw JSON!"

        # Test 2: OHLC buttons use JavaScript functions
        ohlc_pattern = r'onclick="showOHLC\([^)]+\)"'
        assert re.search(ohlc_pattern, html_content), \
            "OHLC buttons not using JavaScript functions - will show raw JSON!"

        # Test 3: NO raw API links remain
        assert '"/api/v1/datasets/' not in html_content.replace('fetch(`/api/v1/datasets/', ''), \
            "Raw API links still present - visualization buttons broken!"

        # Test 4: Button CSS classes present
        assert 'class="btn-chart"' in html_content, \
            "Chart button CSS classes missing - styling will be broken!"

        # Test 5: Buttons pass dataset ID and name
        assert '${d.dataset_id}' in html_content, \
            "Dataset ID not passed to visualization functions!"
        assert '${d.dataset_name}' in html_content, \
            "Dataset name not passed to visualization functions!"

    def test_javascript_function_definitions(self):
        """
        CRITICAL: Verify all required JavaScript functions are defined

        Regression Risk: HIGH
        - Functions might be accidentally deleted
        - Function signatures might be changed
        - Function logic might be corrupted
        """
        response = requests.get(f"{self.BASE_URL}/")
        html_content = response.text

        # Test 1: showDistributions function exists
        assert 'async function showDistributions(' in html_content, \
            "showDistributions function missing - distribution charts will not work!"

        # Test 2: showOHLC function exists
        assert 'async function showOHLC(' in html_content, \
            "showOHLC function missing - OHLC charts will not work!"

        # Test 3: closeModal function exists
        assert 'function closeModal(' in html_content, \
            "closeModal function missing - users cannot close charts!"

        # Test 4: Chart.js instantiation code present
        assert 'new Chart(' in html_content, \
            "Chart.js instantiation code missing - charts will not render!"

        # Test 5: Fetch API calls for data
        assert 'fetch(`/api/v1/datasets/${datasetId}/distributions`)' in html_content, \
            "Distribution data fetch missing!"
        assert 'fetch(`/api/v1/datasets/${datasetId}/ohlc`)' in html_content, \
            "OHLC data fetch missing!"

        # Test 6: Error handling present
        assert 'catch (error)' in html_content, \
            "Error handling missing - failures will be silent!"

    def test_chart_css_styling(self):
        """
        CRITICAL: Verify chart-specific CSS styling is intact

        Regression Risk: MEDIUM
        - CSS classes might be deleted
        - Modal styling might be broken
        - Chart containers might lose sizing
        """
        response = requests.get(f"{self.BASE_URL}/")
        html_content = response.text

        critical_css_classes = [
            '.modal {',
            '.modal-content {',
            '.chart-container {',
            '.chart-grid {',
            '.chart-item {',
            '.btn-chart {',
            '.loading-spinner {'
        ]

        for css_class in critical_css_classes:
            assert css_class in html_content, \
                f"Critical CSS class {css_class} missing - styling will be broken!"

        # Test specific modal styling
        assert 'position: fixed' in html_content, \
            "Modal positioning CSS missing!"
        assert 'z-index: 1000' in html_content, \
            "Modal z-index missing - modals won't appear on top!"
        assert 'display: none' in html_content, \
            "Modal display CSS missing!"

    def test_api_endpoint_functionality(self):
        """
        CRITICAL: Verify backend APIs are working correctly

        Regression Risk: HIGH
        - API endpoints might be broken
        - Data format might have changed
        - Database connections might be failing
        """
        # Test 1: Datasets API
        response = requests.get(f"{self.BASE_URL}/api/v1/datasets")
        assert response.status_code == 200, "Datasets API broken!"

        datasets = response.json()
        assert 'datasets' in datasets, "Datasets API response format changed!"
        assert len(datasets['datasets']) > 0, "No datasets available for visualization!"

        # Get first dataset for testing
        dataset_id = datasets['datasets'][0]['dataset_id']
        dataset_name = datasets['datasets'][0]['dataset_name']

        # Test 2: Distributions API
        dist_response = requests.get(f"{self.BASE_URL}/api/v1/datasets/{dataset_id}/distributions")
        assert dist_response.status_code == 200, \
            f"Distributions API broken for dataset {dataset_name}!"

        dist_data = dist_response.json()
        assert 'distributions' in dist_data, "Distributions API response format changed!"
        assert len(dist_data['distributions']) > 0, "No distribution data available!"

        # Test 3: OHLC API
        ohlc_response = requests.get(f"{self.BASE_URL}/api/v1/datasets/{dataset_id}/ohlc")
        assert ohlc_response.status_code == 200, \
            f"OHLC API broken for dataset {dataset_name}!"

        ohlc_data = ohlc_response.json()
        assert 'ohlc_data' in ohlc_data, "OHLC API response format changed!"
        assert len(ohlc_data['ohlc_data']) > 0, "No OHLC data available!"

        # Test 4: Data structure integrity
        first_dist = list(dist_data['distributions'].values())[0]
        required_fields = ['histogram_bins', 'histogram_counts', 'mean_value', 'std_value']
        for field in required_fields:
            assert field in first_dist, f"Distribution data missing {field} field!"

        first_ohlc = ohlc_data['ohlc_data'][0]
        ohlc_fields = ['date', 'open', 'high', 'low', 'close', 'volume']
        for field in ohlc_fields:
            assert field in first_ohlc, f"OHLC data missing {field} field!"

    def test_chart_data_processing_logic(self):
        """
        CRITICAL: Verify chart data processing logic is correct

        Regression Risk: HIGH
        - Data transformation logic might be broken
        - Chart configuration might be corrupted
        - Histogram bin calculations might be wrong
        """
        response = requests.get(f"{self.BASE_URL}/")
        html_content = response.text

        # Test 1: Histogram bin center calculation
        bin_calc_pattern = r'const center = \(bin \+ feature\.histogram_bins\[i \+ 1\]\) / 2'
        assert re.search(bin_calc_pattern, html_content), \
            "Histogram bin center calculation missing or wrong!"

        # Test 2: Chart data structure
        assert 'datasets: [{' in html_content, \
            "Chart.js dataset structure missing!"

        # Test 3: Chart options configuration
        assert 'responsive: true' in html_content, \
            "Chart responsive configuration missing!"
        assert 'maintainAspectRatio: false' in html_content, \
            "Chart aspect ratio configuration missing!"

        # Test 4: Chart type configurations
        assert "type: 'bar'" in html_content, \
            "Bar chart configuration missing for histograms!"
        assert "type: 'line'" in html_content, \
            "Line chart configuration missing for OHLC!"

        # Test 5: Axis configurations
        assert 'scales:' in html_content, \
            "Chart axis configuration missing!"
        assert 'title:' in html_content, \
            "Chart title configuration missing!"

    def test_user_experience_features(self):
        """
        IMPORTANT: Verify user experience features are working

        Regression Risk: MEDIUM
        - Loading indicators might be missing
        - Error messages might be broken
        - Modal interactions might not work
        """
        response = requests.get(f"{self.BASE_URL}/")
        html_content = response.text

        # Test 1: Loading indicators
        assert 'Loading distributions...' in html_content, \
            "Distribution loading indicator missing!"
        assert 'Loading OHLC data...' in html_content, \
            "OHLC loading indicator missing!"

        # Test 2: Error handling messages
        assert 'Error loading distributions:' in html_content, \
            "Distribution error handling missing!"
        assert 'Error loading OHLC data:' in html_content, \
            "OHLC error handling missing!"

        # Test 3: Modal title updates
        assert '.modal-title' in html_content, \
            "Modal title update functionality missing!"

        # Test 4: Click-outside-to-close functionality
        assert 'window.onclick = function(event)' in html_content, \
            "Click-outside-to-close functionality missing!"

        # Test 5: Statistical summaries
        assert 'Mean:' in html_content and 'Std:' in html_content, \
            "Statistical summary display missing!"

    def test_no_raw_json_links_remain(self):
        """
        CRITICAL: Ensure no raw JSON links remain in buttons

        Regression Risk: VERY HIGH
        - This was the original bug - must not regress!
        - Raw links would break user experience completely
        """
        response = requests.get(f"{self.BASE_URL}/")
        html_content = response.text

        # Extract the action buttons section
        button_sections = re.findall(r'<td>\s*<button.*?</td>', html_content, re.DOTALL)

        for section in button_sections:
            # Test 1: No raw API href links in buttons
            assert 'href="/api/v1/datasets/' not in section, \
                "Raw API links found in buttons - REGRESSION DETECTED!"

            # Test 2: No target="_blank" attributes
            assert 'target="_blank"' not in section, \
                "target='_blank' found in buttons - indicates raw links!"

            # Test 3: Must use onclick handlers
            if 'Distributions' in section:
                assert 'onclick="showDistributions(' in section, \
                    "Distribution button missing onclick handler!"
            if 'OHLC' in section:
                assert 'onclick="showOHLC(' in section, \
                    "OHLC button missing onclick handler!"


class TestChartVisualizationIntegration:
    """
    Integration tests that verify the complete visualization workflow
    """

    BASE_URL = "http://localhost:3000"

    def test_end_to_end_visualization_workflow(self):
        """
        Test complete workflow: Data → API → JavaScript → Charts
        """
        # Step 1: Get datasets
        datasets_response = requests.get(f"{self.BASE_URL}/api/v1/datasets")
        assert datasets_response.status_code == 200

        datasets = datasets_response.json()
        assert len(datasets['datasets']) > 0, "No datasets for testing!"

        dataset_id = datasets['datasets'][0]['dataset_id']

        # Step 2: Verify distributions data quality
        dist_response = requests.get(f"{self.BASE_URL}/api/v1/datasets/{dataset_id}/distributions")
        assert dist_response.status_code == 200

        dist_data = dist_response.json()
        distributions = dist_data['distributions']

        # Verify each feature has required data for charting
        for feature_name, feature_data in distributions.items():
            assert 'histogram_bins' in feature_data, f"No histogram bins for {feature_name}"
            assert 'histogram_counts' in feature_data, f"No histogram counts for {feature_name}"
            assert len(feature_data['histogram_bins']) > 1, f"Insufficient bins for {feature_name}"
            assert len(feature_data['histogram_counts']) > 0, f"No count data for {feature_name}"

        # Step 3: Verify OHLC data quality
        ohlc_response = requests.get(f"{self.BASE_URL}/api/v1/datasets/{dataset_id}/ohlc")
        assert ohlc_response.status_code == 200

        ohlc_data = ohlc_response.json()
        assert len(ohlc_data['ohlc_data']) > 0, "No OHLC data for charting!"

        # Verify OHLC data structure
        for point in ohlc_data['ohlc_data'][:5]:  # Check first 5 points
            assert all(field in point for field in ['date', 'open', 'high', 'low', 'close', 'volume']), \
                "OHLC data missing required fields!"

    def test_visualization_buttons_in_dataset_table(self):
        """
        Verify visualization buttons appear correctly in dataset table
        """
        response = requests.get(f"{self.BASE_URL}/")
        html_content = response.text

        # Verify button structure in table generation code
        table_generation = re.search(r'tableBody\.innerHTML = datasets\.datasets\.map.*?join\(\'\'\);',
                                   html_content, re.DOTALL)
        assert table_generation, "Dataset table generation code missing!"

        table_code = table_generation.group()
        assert 'showDistributions(' in table_code, "Distribution button missing from table!"
        assert 'showOHLC(' in table_code, "OHLC button missing from table!"


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v", "--tb=short"])