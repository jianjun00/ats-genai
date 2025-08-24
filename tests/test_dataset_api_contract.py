#!/usr/bin/env python3
"""
Comprehensive API contract tests for datasets functionality to prevent regressions

This test suite validates:
1. Dataset API parameter handling (sorting, pagination)
2. Dataset response structure and field types
3. Dataset functionality consistency over time
4. Regression detection for critical changes

Purpose: Detect when dataset functionality is accidentally simplified or broken
"""

import pytest
import requests
import json
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

# Test configuration  
TEST_BASE_URL = "http://localhost:9998"  # Port forward URL

@dataclass
class DatasetAPIExpectations:
    """Expected dataset API behavior - update this when making intentional changes"""
    
    # Required query parameters
    REQUIRED_SORT_PARAMS = ["sort_by", "sort_dir"]
    VALID_SORT_FIELDS = ["creation_timestamp", "dataset_name", "symbol", "file_size_mb", "record_count"]
    VALID_SORT_DIRECTIONS = ["asc", "desc"]
    
    # Required response fields
    REQUIRED_DATASET_FIELDS = [
        "dataset_id", "dataset_name", "symbols", "total_sequences",
        "feature_count", "sequence_length", "label_count", "start_date", 
        "end_date", "created_at", "file_size_mb", "status"
    ]
    
    # Required response structure  
    REQUIRED_RESPONSE_FIELDS = ["datasets", "total"]
    
    # Expected data types
    FIELD_TYPES = {
        "dataset_id": int,
        "dataset_name": str, 
        "symbols": list,
        "total_sequences": int,
        "feature_count": int,
        "sequence_length": int,
        "label_count": int,
        "file_size_mb": (int, float),
        "status": str,
        "total": int
    }


class TestDatasetAPIContract:
    """Contract tests to prevent dataset functionality regressions"""
    
    def test_dataset_api_supports_sorting_parameters(self):
        """CRITICAL: Ensure sort_by and sort_dir parameters work"""
        print("\\n🧪 Testing dataset sorting parameter support")
        
        # Test each valid sort field
        for sort_field in DatasetAPIExpectations.VALID_SORT_FIELDS:
            for sort_dir in DatasetAPIExpectations.VALID_SORT_DIRECTIONS:
                url = f"{TEST_BASE_URL}/api/v1/datasets"
                params = {
                    "sort_by": sort_field,
                    "sort_dir": sort_dir,
                    "limit": 10
                }
                
                print(f"   Testing sort_by={sort_field}, sort_dir={sort_dir}")
                
                response = requests.get(url, params=params, timeout=10)
                
                # Should return 200 OK
                assert response.status_code == 200, f"Sorting failed for {sort_field} {sort_dir}: {response.text}"
                
                # Should return valid JSON
                data = response.json()
                
                # Should have expected structure
                assert "datasets" in data, f"Missing 'datasets' field when sorting by {sort_field}"
                assert "total" in data, f"Missing 'total' field when sorting by {sort_field}"
                
                # Should have datasets
                datasets = data["datasets"]
                assert isinstance(datasets, list), "Datasets should be a list"
                
                if len(datasets) >= 2:
                    # Verify sorting actually works
                    self._verify_sorting_order(datasets, sort_field, sort_dir)
                    
        print("   ✅ All sorting parameters work correctly")

    def _verify_sorting_order(self, datasets: List[Dict], sort_field: str, sort_dir: str):
        """Verify datasets are actually sorted correctly"""
        values = []
        for dataset in datasets:
            if sort_field in dataset and dataset[sort_field] is not None:
                values.append(dataset[sort_field])
                
        if len(values) < 2:
            return  # Can't verify sorting with < 2 values
            
        # Check if sorted correctly
        if sort_dir == "asc":
            assert values == sorted(values), f"Datasets not sorted ASC by {sort_field}: {values}"
        else:
            assert values == sorted(values, reverse=True), f"Datasets not sorted DESC by {sort_field}: {values}"

    def test_dataset_response_structure_contract(self):
        """CRITICAL: Ensure dataset response has all expected fields"""
        print("\\n🧪 Testing dataset response structure contract")
        
        url = f"{TEST_BASE_URL}/api/v1/datasets"
        response = requests.get(url, timeout=10)
        
        assert response.status_code == 200, f"Dataset API failed: {response.text}"
        data = response.json()
        
        # Test top-level structure
        for field in DatasetAPIExpectations.REQUIRED_RESPONSE_FIELDS:
            assert field in data, f"Missing required response field: {field}"
            
        datasets = data["datasets"]
        assert isinstance(datasets, list), "Datasets field must be a list"
        
        # Test individual dataset structure
        if datasets:
            dataset = datasets[0]
            print(f"   Validating dataset structure: {list(dataset.keys())}")
            
            for field in DatasetAPIExpectations.REQUIRED_DATASET_FIELDS:
                assert field in dataset, f"Missing required dataset field: {field}"
                
            # Test field types
            for field, expected_type in DatasetAPIExpectations.FIELD_TYPES.items():
                if field in dataset and dataset[field] is not None:
                    actual_value = dataset[field]
                    if isinstance(expected_type, tuple):
                        assert isinstance(actual_value, expected_type), f"Field {field} has wrong type: {type(actual_value)} (expected {expected_type})"
                    else:
                        assert isinstance(actual_value, expected_type), f"Field {field} has wrong type: {type(actual_value)} (expected {expected_type})"
                        
        print("   ✅ Dataset response structure is correct")

    def test_dataset_pagination_functionality(self):
        """CRITICAL: Ensure pagination works correctly"""  
        print("\\n🧪 Testing dataset pagination functionality")
        
        # Get first page
        response1 = requests.get(f"{TEST_BASE_URL}/api/v1/datasets?limit=2&offset=0", timeout=10)
        assert response1.status_code == 200
        data1 = response1.json()
        
        # Get second page  
        response2 = requests.get(f"{TEST_BASE_URL}/api/v1/datasets?limit=2&offset=2", timeout=10)
        assert response2.status_code == 200
        data2 = response2.json()
        
        # Pages should be different (if enough data)
        if data1["total"] > 2:
            dataset_ids_1 = [d["dataset_id"] for d in data1["datasets"]]
            dataset_ids_2 = [d["dataset_id"] for d in data2["datasets"]]
            
            # Should not overlap
            overlap = set(dataset_ids_1) & set(dataset_ids_2)
            assert len(overlap) == 0, f"Pagination overlap detected: {overlap}"
            
        print("   ✅ Pagination works correctly")

    def test_dataset_sorting_actually_changes_order(self):
        """CRITICAL: Ensure sorting actually reorders results"""
        print("\\n🧪 Testing that sorting actually changes dataset order")
        
        # Get datasets in ascending order
        response_asc = requests.get(f"{TEST_BASE_URL}/api/v1/datasets?sort_by=dataset_name&sort_dir=asc", timeout=10)
        assert response_asc.status_code == 200
        data_asc = response_asc.json()
        
        # Get datasets in descending order
        response_desc = requests.get(f"{TEST_BASE_URL}/api/v1/datasets?sort_by=dataset_name&sort_dir=desc", timeout=10)  
        assert response_desc.status_code == 200
        data_desc = response_desc.json()
        
        # Should have same total
        assert data_asc["total"] == data_desc["total"], "Total count differs between sort orders"
        
        # If we have enough data, order should be different
        if data_asc["total"] > 1:
            names_asc = [d["dataset_name"] for d in data_asc["datasets"]]
            names_desc = [d["dataset_name"] for d in data_desc["datasets"]]
            
            # Orders should be different (reverse of each other)
            assert names_asc != names_desc, f"Sorting doesn't change order: ASC={names_asc}, DESC={names_desc}"
            assert names_asc == list(reversed(names_desc)), f"DESC is not reverse of ASC: ASC={names_asc}, DESC_REVERSED={list(reversed(names_desc))}"
            
        print("   ✅ Sorting changes dataset order correctly")

    def test_dataset_api_regression_detection(self):
        """META-TEST: Detect if API was simplified/dumbed down"""
        print("\\n🧪 Testing for dataset API regressions (complexity reduction)")
        
        url = f"{TEST_BASE_URL}/api/v1/datasets"
        
        # Test with complex parameters that would break a simplified API
        complex_params = {
            "limit": 3,
            "offset": 1, 
            "sort_by": "file_size_mb",
            "sort_dir": "desc"
        }
        
        response = requests.get(url, params=complex_params, timeout=10)
        assert response.status_code == 200, f"Complex parameters failed: {response.text}"
        
        data = response.json()
        datasets = data["datasets"]
        
        # Verify complex functionality still works
        assert len(datasets) <= 3, "Limit parameter not working"
        
        # Verify sorting by file_size_mb in descending order
        if len(datasets) >= 2:
            sizes = [d["file_size_mb"] for d in datasets if d["file_size_mb"] is not None]
            if len(sizes) >= 2:
                # Should be sorted descending by size
                for i in range(len(sizes) - 1):
                    assert sizes[i] >= sizes[i + 1], f"Not sorted by file_size_mb DESC: {sizes}"
                    
        print("   ✅ Complex dataset functionality preserved")

    def test_dataset_field_completeness_over_time(self):
        """Ensure dataset fields aren't accidentally removed"""
        print("\\n🧪 Testing dataset field completeness over time")
        
        response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets", timeout=10)
        assert response.status_code == 200
        data = response.json()
        
        if data["datasets"]:
            dataset = data["datasets"][0]
            actual_fields = set(dataset.keys())
            expected_fields = set(DatasetAPIExpectations.REQUIRED_DATASET_FIELDS)
            
            missing_fields = expected_fields - actual_fields
            assert len(missing_fields) == 0, f"Dataset API missing expected fields: {missing_fields}"
            
            extra_fields = actual_fields - expected_fields
            if extra_fields:
                print(f"   ℹ️ Dataset has additional fields: {extra_fields}")
                
        print("   ✅ Dataset field completeness verified")

    def test_dataset_detail_endpoints_functionality(self):
        """CRITICAL: Test that dataset detail page endpoints work"""
        print("\\n🧪 Testing dataset detail endpoints functionality")
        
        # First get a dataset ID that exists
        response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets?limit=1", timeout=10)
        assert response.status_code == 200
        data = response.json()
        
        if not data["datasets"]:
            print("   ⚠️ No datasets available for detail testing")
            return
            
        dataset_id = data["datasets"][0]["dataset_id"]
        print(f"   Testing with dataset ID: {dataset_id}")
        
        # Test dataset detail endpoint
        detail_response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets/{dataset_id}", timeout=10)
        assert detail_response.status_code == 200, f"Dataset detail failed: {detail_response.text}"
        
        detail_data = detail_response.json()
        
        # Should have key dataset detail fields
        required_detail_fields = ["dataset_id", "dataset_name", "symbol", "total_sequences", "feature_count"]
        for field in required_detail_fields:
            assert field in detail_data, f"Dataset detail missing field: {field}"
            
        # Test sequence detail endpoint  
        sequence_id = 1001  # Standard test sequence ID
        seq_response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets/{dataset_id}/sequences/{sequence_id}", timeout=10)
        assert seq_response.status_code == 200, f"Sequence detail failed: {seq_response.text}"
        
        seq_data = seq_response.json()
        required_seq_fields = ["sequence_id", "dataset_id", "sequence_name", "symbol"]
        for field in required_seq_fields:
            assert field in seq_data, f"Sequence detail missing field: {field}"
            
        # Test sequence OHLC endpoint
        ohlc_response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets/{dataset_id}/sequences/{sequence_id}/ohlc", timeout=10)
        assert ohlc_response.status_code == 200, f"OHLC data failed: {ohlc_response.text}"
        
        ohlc_data = ohlc_response.json()
        required_ohlc_fields = ["sequence_id", "dataset_id", "symbol", "ohlc_data"]
        for field in required_ohlc_fields:
            assert field in ohlc_data, f"OHLC data missing field: {field}"
            
        # Verify OHLC data structure
        if ohlc_data["ohlc_data"]:
            ohlc_point = ohlc_data["ohlc_data"][0]
            required_ohlc_point_fields = ["timestamp", "open", "high", "low", "close", "volume"]
            for field in required_ohlc_point_fields:
                assert field in ohlc_point, f"OHLC point missing field: {field}"
                
        print("   ✅ All dataset detail endpoints working correctly")


def run_dataset_contract_tests():
    """Run all dataset contract tests"""
    print("🚀 RUNNING DATASET API CONTRACT TESTS")
    print("Purpose: Prevent regressions in dataset functionality")
    print("=" * 80)
    
    test_instance = TestDatasetAPIContract()
    
    tests = [
        ("Sorting Parameters Support", test_instance.test_dataset_api_supports_sorting_parameters),
        ("Response Structure Contract", test_instance.test_dataset_response_structure_contract),  
        ("Pagination Functionality", test_instance.test_dataset_pagination_functionality),
        ("Sorting Order Changes", test_instance.test_dataset_sorting_actually_changes_order),
        ("Regression Detection", test_instance.test_dataset_api_regression_detection),
        ("Field Completeness", test_instance.test_dataset_field_completeness_over_time),
        ("Dataset Detail Endpoints", test_instance.test_dataset_detail_endpoints_functionality)
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        print(f"\\n🧪 Running: {test_name}")
        print("-" * 60)
        try:
            test_func()
            print(f"✅ PASSED: {test_name}")
            passed += 1
        except Exception as e:
            print(f"❌ FAILED: {test_name}")
            print(f"   Error: {e}")
            failed += 1
    
    print("\\n" + "=" * 80)
    print("📊 DATASET API CONTRACT TEST SUMMARY")
    print("=" * 80)
    print(f"✅ PASSED: {passed} tests")  
    print(f"❌ FAILED: {failed} tests")
    
    if failed > 0:
        print(f"\\n🚨 CRITICAL REGRESSIONS DETECTED:")
        print(f"   Dataset API functionality has been reduced or broken!")
        print(f"   This indicates the dataset section was accidentally simplified.")
        print(f"   Original functionality needs to be restored.")
    else:
        print(f"\\n🎉 NO DATASET API REGRESSIONS DETECTED!")
        print(f"   All critical dataset functionality is preserved.")
    
    return passed, failed


if __name__ == "__main__":
    run_dataset_contract_tests()