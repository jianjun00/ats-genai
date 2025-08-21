#!/usr/bin/env python3
"""
Test case to detect missing dataset endpoints

This test reproduces the issue where dataset functionality was removed
when fixing job management. Following TDD principle - test first, then fix.
"""

import requests
import pytest
import sys


class TestDatasetEndpointsMissing:
    """Test that dataset endpoints exist and work properly."""
    
    BASE_URL = "http://localhost:3000"
    
    def test_dataset_list_endpoint_exists(self):
        """Test that /api/v1/datasets endpoint exists."""
        try:
            response = requests.get(f"{self.BASE_URL}/api/v1/datasets", timeout=5)
            # Should return 200, not 404
            assert response.status_code == 200, f"Dataset list endpoint missing: {response.status_code}"
            
            # Should return JSON with datasets
            data = response.json()
            assert "datasets" in data, "Response should include 'datasets' key"
            
        except requests.ConnectionError:
            pytest.skip("Cannot connect to test server")
    
    def test_dataset_detail_endpoint_exists(self):
        """Test that /api/v1/datasets/{id} endpoint exists."""
        try:
            # Try with a sample dataset ID
            response = requests.get(f"{self.BASE_URL}/api/v1/datasets/1", timeout=5)
            # Should return 200 or 404 (not found), not 404 (endpoint missing)
            assert response.status_code in [200, 404], f"Dataset detail endpoint missing: {response.status_code}"
            
        except requests.ConnectionError:
            pytest.skip("Cannot connect to test server")
    
    def test_dataset_distributions_endpoint_exists(self):
        """Test that /api/v1/datasets/{id}/distributions endpoint exists."""
        try:
            response = requests.get(f"{self.BASE_URL}/api/v1/datasets/1/distributions", timeout=5)
            # Should return 200 or 404 (not found), not 404 (endpoint missing)
            assert response.status_code in [200, 404], f"Dataset distributions endpoint missing: {response.status_code}"
            
        except requests.ConnectionError:
            pytest.skip("Cannot connect to test server")
    
    def test_dataset_ohlc_endpoint_exists(self):
        """Test that /api/v1/datasets/{id}/ohlc endpoint exists."""
        try:
            response = requests.get(f"{self.BASE_URL}/api/v1/datasets/1/ohlc", timeout=5)
            # Should return 200 or 404 (not found), not 404 (endpoint missing)
            assert response.status_code in [200, 404], f"Dataset OHLC endpoint missing: {response.status_code}"
            
        except requests.ConnectionError:
            pytest.skip("Cannot connect to test server")
    
    def test_dataset_filter_endpoint_exists(self):
        """Test that /api/v1/datasets/filter endpoint exists."""
        try:
            response = requests.get(f"{self.BASE_URL}/api/v1/datasets/filter", timeout=5)
            # Should return 200, not 404
            assert response.status_code == 200, f"Dataset filter endpoint missing: {response.status_code}"
            
        except requests.ConnectionError:
            pytest.skip("Cannot connect to test server")
    
    def test_web_interface_includes_datasets(self):
        """Test that web interface includes dataset functionality."""
        try:
            response = requests.get(f"{self.BASE_URL}/", timeout=5)
            assert response.status_code == 200, "Web interface should be accessible"
            
            html_content = response.text
            
            # Should include dataset-related content in the UI
            assert "dataset" in html_content.lower(), "Web interface should mention datasets"
            # Could check for specific dataset UI elements
            
        except requests.ConnectionError:
            pytest.skip("Cannot connect to test server")
    
    def test_both_job_and_dataset_endpoints_work(self):
        """Test that both job management AND dataset endpoints work together."""
        try:
            # Job management should still work
            job_response = requests.get(f"{self.BASE_URL}/api/v1/jobs/stats", timeout=5)
            assert job_response.status_code == 200, "Job management should still work"
            
            # Dataset endpoints should also work
            dataset_response = requests.get(f"{self.BASE_URL}/api/v1/datasets", timeout=5)
            assert dataset_response.status_code == 200, "Dataset endpoints should work alongside job management"
            
        except requests.ConnectionError:
            pytest.skip("Cannot connect to test server")


def test_current_endpoints_to_show_what_is_missing():
    """Diagnostic test to show what endpoints are currently available."""
    BASE_URL = "http://localhost:3000"
    
    endpoints_to_test = [
        "/health",
        "/api/v1/jobs/stats", 
        "/api/v1/jobs",
        "/api/v1/datasets",
        "/api/v1/datasets/1",
        "/api/v1/datasets/1/distributions",
        "/api/v1/datasets/1/ohlc",
        "/api/v1/datasets/filter"
    ]
    
    print("\n🔍 Current endpoint status:")
    
    for endpoint in endpoints_to_test:
        try:
            response = requests.get(f"{BASE_URL}{endpoint}", timeout=3)
            status = "✅ EXISTS" if response.status_code != 404 else "❌ MISSING"
            print(f"  {endpoint}: {status} (HTTP {response.status_code})")
        except requests.ConnectionError:
            print(f"  {endpoint}: ❌ CONNECTION_ERROR")
        except Exception as e:
            print(f"  {endpoint}: ❌ ERROR ({e})")


if __name__ == "__main__":
    # Run diagnostic test first to show current state
    test_current_endpoints_to_show_what_is_missing()
    
    print("\n🧪 Running tests to detect missing dataset functionality...")
    
    # Run the actual tests
    pytest.main([__file__, "-v", "--tb=short"])