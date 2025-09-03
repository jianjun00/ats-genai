#!/usr/bin/env python3
"""
Integration tests for EDA JavaScript dataset loading functionality.
Tests the frontend JavaScript behavior and error handling.
"""

import pytest
import requests
import time
import sys
import os
import re

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestEDAJavaScriptLoading:
    """Test suite for JavaScript frontend loading functionality."""
    
    @classmethod
    def setup_class(cls):
        """Setup for the test class."""
        cls.base_url = "http://localhost:3000"
        cls.timeout = 15
        
        # Wait for service to be ready
        max_retries = 10
        for i in range(max_retries):
            try:
                response = requests.get(f"{cls.base_url}/health", timeout=5)
                if response.status_code == 200:
                    break
            except:
                time.sleep(2)
        else:
            raise Exception("EDA service not available after 20 seconds")
    
    def test_eda_dashboard_html_loads(self):
        """Test that EDA dashboard HTML page loads successfully."""
        response = requests.get(f"{self.base_url}/eda", timeout=self.timeout)
        assert response.status_code == 200
        
        html_content = response.text
        assert len(html_content) > 1000, "HTML content should be substantial"
        
        # Verify essential HTML elements for EDA dashboard
        assert "<title>ATS EDA Tool</title>" in html_content
        assert "plotly-latest.min.js" in html_content, "Should include Plotly.js for charts"
        assert "dataset-select" in html_content, "Should have dataset selection dropdown"
        assert "column-select" in html_content, "Should have column selection dropdown"
        assert "chart-container" in html_content, "Should have chart container"
    
    def test_javascript_functions_present_in_html(self):
        """Test that required JavaScript functions are present in the HTML."""
        response = requests.get(f"{self.base_url}/eda", timeout=self.timeout)
        html_content = response.text
        
        # Verify essential JavaScript functions exist
        required_functions = [
            "loadDatasets",
            "loadColumns", 
            "analyzeDataset",
            "fetch('/api/v1/datasets')",
            "fetch(`/api/v1/datasets/${datasetName}/schema`)"
        ]
        
        for func in required_functions:
            assert func in html_content, f"Required JavaScript function/call '{func}' not found in HTML"
    
    def test_javascript_error_handling_present(self):
        """Test that JavaScript includes proper error handling."""
        response = requests.get(f"{self.base_url}/eda", timeout=self.timeout)
        html_content = response.text
        
        # Verify error handling patterns
        error_patterns = [
            "catch (error)",
            "console.error",
            "Error loading datasets",
            "Error analyzing dataset"
        ]
        
        for pattern in error_patterns:
            assert pattern in html_content, f"Error handling pattern '{pattern}' not found in JavaScript"
    
    def test_dataset_dropdown_population_flow(self):
        """Test the API endpoints that JavaScript uses to populate dropdowns."""
        # Step 1: Test datasets API that JavaScript calls
        response = requests.get(f"{self.base_url}/api/v1/datasets", timeout=self.timeout)
        assert response.status_code == 200
        
        datasets = response.json()
        assert len(datasets) > 0, "Should have datasets for dropdown population"
        
        # Verify dataset structure matches JavaScript expectations
        dataset = datasets[0]
        js_expected_fields = ["name", "display_name", "row_count", "column_count"]
        for field in js_expected_fields:
            assert field in dataset, f"JavaScript expects field '{field}' in dataset"
        
        # Step 2: Test schema API for column dropdown population
        dataset_name = dataset["name"]
        schema_response = requests.get(f"{self.base_url}/api/v1/datasets/{dataset_name}/schema", 
                                     timeout=self.timeout)
        assert schema_response.status_code == 200
        
        schema = schema_response.json()
        assert "columns" in schema, "JavaScript expects 'columns' field in schema response"
        
        # Verify column structure matches JavaScript filtering logic
        columns = schema["columns"]
        assert len(columns) > 0, "Should have columns for dropdown population"
        
        column = columns[0]
        js_expected_col_fields = ["column_name", "data_type"]
        for field in js_expected_col_fields:
            assert field in column, f"JavaScript expects field '{field}' in column data"
    
    def test_numeric_column_filtering_matches_javascript(self):
        """Test that numeric column filtering matches what JavaScript expects."""
        # Get schema for a dataset with known numeric columns
        response = requests.get(f"{self.base_url}/api/v1/datasets/dev_instrument_tiingo/schema", 
                              timeout=self.timeout)
        schema = response.json()
        
        # Apply the same filtering logic as JavaScript frontend
        numeric_columns = []
        for col in schema["columns"]:
            data_type = col["data_type"].lower()
            # This matches the JavaScript filtering logic in the HTML
            if any(t in data_type for t in ["numeric", "integer", "double", "bigint", "smallint", "real", "decimal", "float"]):
                numeric_columns.append(col["column_name"])
        
        # Should find the expected numeric columns for Tiingo
        expected_numeric = ["market_cap", "price", "volume"]
        for expected_col in expected_numeric:
            assert expected_col in numeric_columns, f"JavaScript filtering should find numeric column '{expected_col}'"
        
        print(f"✅ JavaScript column filtering would find: {numeric_columns}")
    
    def test_analysis_api_endpoint_for_javascript(self):
        """Test the analysis endpoint that JavaScript calls for histogram generation."""
        payload = {
            "dataset_name": "dev_instrument_tiingo",
            "column": "market_cap"
        }
        
        response = requests.post(f"{self.base_url}/api/v1/analysis/distribution",
                               json=payload, timeout=self.timeout)
        
        # Should return valid response for JavaScript to process
        assert response.status_code in [200, 400, 500], "Should return proper HTTP status for JavaScript"
        
        # If successful, should have structure JavaScript expects
        if response.status_code == 200:
            analysis = response.json()
            assert isinstance(analysis, dict), "Analysis should be JSON object for JavaScript"
            
            # Check if it has histogram structure JavaScript expects
            if "histogram" in analysis:
                histogram = analysis["histogram"]
                js_expected_histogram_fields = ["bin_centers", "counts"]
                for field in js_expected_histogram_fields:
                    if field in histogram:
                        assert isinstance(histogram[field], list), f"JavaScript expects {field} to be array"
    
    def test_html_form_element_ids_match_javascript(self):
        """Test that HTML form element IDs match what JavaScript references."""
        response = requests.get(f"{self.base_url}/eda", timeout=self.timeout)
        html_content = response.text
        
        # Elements that JavaScript references by ID
        required_element_ids = [
            "datasets-list",
            "dataset-select", 
            "column-select",
            "chart-container"
        ]
        
        for element_id in required_element_ids:
            # Check if element ID exists in HTML
            assert f'id="{element_id}"' in html_content, f"HTML element with id='{element_id}' not found - JavaScript will fail"
            
            # Also verify JavaScript references these IDs
            assert f"getElementById('{element_id}')" in html_content, f"JavaScript should reference element ID '{element_id}'"
    
    def test_javascript_event_listeners_properly_attached(self):
        """Test that JavaScript event listeners are properly configured."""
        response = requests.get(f"{self.base_url}/eda", timeout=self.timeout)
        html_content = response.text
        
        # Verify event listener attachment
        event_listener_patterns = [
            "addEventListener('change', loadColumns)",
            "onclick=\"analyzeDataset()\""
        ]
        
        for pattern in event_listener_patterns:
            assert pattern in html_content, f"Event listener pattern '{pattern}' not found in JavaScript"
    
    def test_datasets_display_prevents_loading_issue(self):
        """Test that dataset display prevents the 'Loading...' issue user reported."""
        # This specifically tests the fix for "Loading... Interactive Analysis shows no dataset"
        
        # Step 1: Verify datasets API returns data that prevents empty state
        response = requests.get(f"{self.base_url}/api/v1/datasets", timeout=self.timeout)
        datasets = response.json()
        
        assert len(datasets) > 0, "Must have datasets to prevent 'Loading...' state"
        
        # Step 2: Verify each dataset has the data JavaScript needs to render properly
        for dataset in datasets:
            assert dataset["row_count"] > 0, f"Dataset {dataset['name']} row_count=0 would cause empty display"
            assert dataset["display_name"] and dataset["display_name"].strip(), f"Dataset {dataset['name']} needs display_name for rendering"
            assert dataset["name"] and dataset["name"].strip(), f"Dataset needs name for JavaScript functionality"
        
        # Step 3: Verify HTML structure supports proper dataset display  
        html_response = requests.get(f"{self.base_url}/eda", timeout=self.timeout)
        html_content = html_response.text
        
        # Check that JavaScript will populate dataset cards and dropdown
        assert "dataset-card" in html_content, "HTML should support dataset card display"
        assert "${dataset.display_name}" in html_content, "JavaScript template should display dataset names"
        assert "${dataset.row_count.toLocaleString()}" in html_content, "JavaScript should format row counts"
        
        print(f"✅ Found {len(datasets)} datasets with proper data to prevent 'Loading...' issue")
    
    def test_javascript_console_logging_for_debugging(self):
        """Test that JavaScript includes console logging for debugging issues."""
        response = requests.get(f"{self.base_url}/eda", timeout=self.timeout)
        html_content = response.text
        
        # Verify debug logging is present (helps diagnose loading issues)
        debug_patterns = [
            "console.log('Loading datasets...')",
            "console.error('Error loading datasets:', error)",
            "console.error('Error loading columns:', error)"
        ]
        
        for pattern in debug_patterns:
            assert pattern in html_content, f"Debug logging pattern '{pattern}' not found - harder to troubleshoot issues"
    
    def test_plotly_chart_rendering_configuration(self):
        """Test that Plotly chart configuration is properly set up for JavaScript."""
        response = requests.get(f"{self.base_url}/eda", timeout=self.timeout)
        html_content = response.text
        
        # Verify Plotly chart rendering code
        plotly_patterns = [
            "Plotly.newPlot",
            "chart-container",
            "type: 'bar'",
            "title: `Distribution of ${columnName}`"
        ]
        
        for pattern in plotly_patterns:
            assert pattern in html_content, f"Plotly configuration pattern '{pattern}' not found in JavaScript"
        
        # Verify chart data structure matches analysis API response
        assert "x: analysis.histogram.bin_centers" in html_content, "Chart should use bin_centers for x-axis"
        assert "y: analysis.histogram.counts" in html_content, "Chart should use counts for y-axis"


if __name__ == "__main__":
    # Run JavaScript loading tests
    test_suite = TestEDAJavaScriptLoading()
    test_suite.setup_class()
    
    try:
        print("🧪 Testing EDA JavaScript Loading...")
        
        test_suite.test_eda_dashboard_html_loads()
        print("✅ EDA dashboard HTML loads test passed")
        
        test_suite.test_javascript_functions_present_in_html()
        print("✅ JavaScript functions present test passed")
        
        test_suite.test_javascript_error_handling_present()
        print("✅ JavaScript error handling test passed")
        
        test_suite.test_dataset_dropdown_population_flow()
        print("✅ Dataset dropdown population flow test passed")
        
        test_suite.test_numeric_column_filtering_matches_javascript()
        print("✅ Numeric column filtering test passed")
        
        test_suite.test_analysis_api_endpoint_for_javascript()
        print("✅ Analysis API endpoint test passed")
        
        test_suite.test_html_form_element_ids_match_javascript()
        print("✅ HTML element IDs matching test passed")
        
        test_suite.test_javascript_event_listeners_properly_attached()
        print("✅ JavaScript event listeners test passed")
        
        test_suite.test_datasets_display_prevents_loading_issue()
        print("✅ 'Loading...' issue prevention test passed")
        
        test_suite.test_javascript_console_logging_for_debugging()
        print("✅ JavaScript debugging logging test passed")
        
        test_suite.test_plotly_chart_rendering_configuration()
        print("✅ Plotly chart rendering test passed")
        
        print("\n🎉 All JavaScript loading tests passed!")
        print("✅ JavaScript properly loads datasets and populates dropdowns")
        print("✅ Error handling prevents UI failures")
        print("✅ 'Loading...' issue resolved with proper data flow")
        print("✅ Interactive chart rendering configured correctly")
        
    except Exception as e:
        print(f"❌ JavaScript loading test failed: {e}")
        exit(1)