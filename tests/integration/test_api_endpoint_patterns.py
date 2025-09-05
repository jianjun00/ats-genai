#!/usr/bin/env python3
"""
API Endpoint Pattern Tests

Tests the critical fixes made for API endpoint URL format inconsistencies
in the EDA training dataset sequences functionality.

Based on fixes documented in PRD: ArrayRecord Training Data System (September 4, 2025)
Issue: EDA frontend expected /api/v1/training-datasets/{id}/sequences but service used query parameters
Solution: Aligned endpoint patterns with path-based dataset ID extraction
"""

import pytest
import requests
import sys
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))


@pytest.mark.integration
def test_sequences_endpoint_url_format():
    """Test that sequences endpoint accepts correct URL format."""
    base_url = "http://localhost:3000"
    
    # Test that analytics service is running
    try:
        health_response = requests.get(f"{base_url}/health", timeout=5)
        if health_response.status_code != 200:
            pytest.skip("Analytics service not running or not healthy")
    except requests.ConnectionError:
        pytest.skip("Analytics service not accessible")
    
    # Get available datasets first
    try:
        datasets_response = requests.get(f"{base_url}/api/v1/training-datasets", timeout=10)
        if datasets_response.status_code != 200:
            pytest.skip("Training datasets API not available")
            
        datasets_data = datasets_response.json()
        if not datasets_data.get("datasets"):
            pytest.skip("No training datasets available for testing")
            
        # Use the first available dataset for testing
        dataset_id = datasets_data["datasets"][0]["id"]
        
    except (requests.RequestException, KeyError) as e:
        pytest.skip(f"Cannot access training datasets: {e}")
    
    # Test correct path-based format
    sequences_url = f"{base_url}/api/v1/training-datasets/{dataset_id}/sequences"
    response = requests.get(sequences_url, timeout=10)
    
    assert response.status_code != 404, f"Path-based URL format not recognized: {sequences_url}"
    assert response.status_code == 200, f"Unexpected status code: {response.status_code}"
    
    # Verify response structure
    data = response.json()
    assert "sequences" in data, "Response missing 'sequences' field"
    assert "datasets" in data, "Response missing 'datasets' field" 
    assert "total_count" in data, "Response missing 'total_count' field"
    
    # Verify dataset information is included
    assert isinstance(data["datasets"], list), "Datasets should be a list"
    assert len(data["datasets"]) > 0, "Datasets list should not be empty"
    assert data["datasets"][0].get("dataset_name"), "Dataset should have name"


@pytest.mark.integration
def test_sequences_endpoint_returns_arrayrecord_files():
    """Test that sequences endpoint discovers ArrayRecord files."""
    base_url = "http://localhost:3000"
    
    try:
        # Get datasets
        datasets_response = requests.get(f"{base_url}/api/v1/training-datasets", timeout=10)
        if datasets_response.status_code != 200:
            pytest.skip("Training datasets API not available")
            
        datasets_data = datasets_response.json()
        if not datasets_data.get("datasets"):
            pytest.skip("No training datasets available")
        
        # Look for a dataset that likely has ArrayRecord files
        # Prefer more recent datasets which are more likely to use ArrayRecord format
        datasets = sorted(datasets_data["datasets"], key=lambda x: x.get("created_at", ""), reverse=True)
        
        found_arrayrecord = False
        for dataset in datasets[:3]:  # Check up to 3 most recent datasets
            dataset_id = dataset["id"]
            
            sequences_response = requests.get(f"{base_url}/api/v1/training-datasets/{dataset_id}/sequences", timeout=10)
            if sequences_response.status_code != 200:
                continue
                
            data = sequences_response.json()
            
            if data.get("sequences"):
                # Check that ArrayRecord files are returned
                for sequence in data["sequences"]:
                    assert "filename" in sequence, "Sequence missing filename"
                    
                    if sequence["filename"].endswith(".arrayrecord"):
                        found_arrayrecord = True
                        assert "path" in sequence, "ArrayRecord sequence missing path"
                        assert "/data/training_data/" in sequence["path"], f"Incorrect path format: {sequence['path']}"
                        assert "symbol" in sequence, "ArrayRecord sequence missing symbol"
                        break
                        
            if found_arrayrecord:
                break
        
        if not found_arrayrecord:
            pytest.skip("No ArrayRecord files found in available datasets")
            
    except requests.RequestException as e:
        pytest.skip(f"Cannot test ArrayRecord files: {e}")


@pytest.mark.integration
def test_sequences_endpoint_error_handling():
    """Test sequences endpoint error handling for invalid dataset IDs."""
    base_url = "http://localhost:3000"
    
    try:
        # Test with non-existent dataset ID
        response = requests.get(f"{base_url}/api/v1/training-datasets/99999/sequences", timeout=5)
        
        # Should return structured error, not 500
        assert response.status_code in [200, 404], f"Unexpected status code for non-existent dataset: {response.status_code}"
        
        if response.status_code == 200:
            data = response.json()
            # Should return empty sequences or error info
            assert "sequences" in data
            assert "total_count" in data
            # Either empty sequences or error in datasets
            assert data["total_count"] == 0 or any("error" in str(ds) for ds in data.get("datasets", []))
            
    except requests.RequestException as e:
        pytest.skip(f"Cannot test error handling: {e}")


@pytest.mark.integration
def test_sequences_endpoint_path_parsing():
    """Test that sequences endpoint correctly parses dataset ID from path."""
    base_url = "http://localhost:3000"
    
    # Test various dataset ID formats
    test_cases = [
        ("123", "numeric ID"),
        ("1", "single digit ID"),
    ]
    
    for dataset_id, description in test_cases:
        try:
            response = requests.get(f"{base_url}/api/v1/training-datasets/{dataset_id}/sequences", timeout=5)
            
            # Should not be 404 (URL pattern recognized) or 500 (parsing error)
            assert response.status_code != 404, f"URL pattern not recognized for {description}"
            assert response.status_code != 500, f"Server error parsing {description}"
            
            # Status should be 200 (found) or structured response for not found
            assert response.status_code == 200, f"Unexpected status for {description}: {response.status_code}"
            
            # Should return valid JSON structure
            data = response.json()
            assert isinstance(data, dict), f"Invalid response format for {description}"
            assert "sequences" in data, f"Missing sequences field for {description}"
            
        except requests.RequestException as e:
            pytest.skip(f"Cannot test path parsing for {description}: {e}")


@pytest.mark.integration 
def test_training_datasets_list_endpoint():
    """Test that training datasets list endpoint works correctly."""
    base_url = "http://localhost:3000"
    
    try:
        response = requests.get(f"{base_url}/api/v1/training-datasets", timeout=10)
        
        assert response.status_code == 200, f"Training datasets list failed: {response.status_code}"
        
        data = response.json()
        assert isinstance(data, dict), "Response should be a dictionary"
        assert "datasets" in data, "Response should contain datasets field"
        assert "total_count" in data, "Response should contain total_count field"
        
        datasets = data["datasets"]
        assert isinstance(datasets, list), "Datasets should be a list"
        
        if datasets:
            # Check structure of dataset objects
            dataset = datasets[0]
            required_fields = ["id", "dataset_name", "created_at"]
            
            for field in required_fields:
                assert field in dataset, f"Dataset missing required field: {field}"
            
            # Verify ID is usable for sequences endpoint
            dataset_id = dataset["id"]
            assert isinstance(dataset_id, int), "Dataset ID should be an integer"
            assert dataset_id > 0, "Dataset ID should be positive"
            
    except requests.RequestException as e:
        pytest.skip(f"Cannot test training datasets list: {e}")


@pytest.mark.integration
def test_api_endpoint_consistency():
    """Test consistency between different API endpoints."""
    base_url = "http://localhost:3000"
    
    try:
        # Get datasets list
        datasets_response = requests.get(f"{base_url}/api/v1/training-datasets", timeout=10)
        if datasets_response.status_code != 200:
            pytest.skip("Cannot access datasets list")
            
        datasets_data = datasets_response.json()
        if not datasets_data.get("datasets"):
            pytest.skip("No datasets available for consistency testing")
        
        dataset = datasets_data["datasets"][0]
        dataset_id = dataset["id"]
        dataset_name = dataset["dataset_name"]
        
        # Get sequences for this dataset
        sequences_response = requests.get(f"{base_url}/api/v1/training-datasets/{dataset_id}/sequences", timeout=10)
        assert sequences_response.status_code == 200, "Sequences endpoint should work for valid dataset"
        
        sequences_data = sequences_response.json()
        
        # Verify consistency between endpoints
        assert sequences_data["datasets"], "Sequences response should include dataset info"
        returned_dataset = sequences_data["datasets"][0]
        
        # Dataset name should match
        assert returned_dataset.get("dataset_name") == dataset_name, "Dataset name inconsistency between endpoints"
        
    except requests.RequestException as e:
        pytest.skip(f"Cannot test API consistency: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])