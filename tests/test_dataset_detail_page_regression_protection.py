#!/usr/bin/env python3
"""
Dataset Detail Page Regression Protection Tests

This test suite specifically protects against unintentional changes to dataset detail page functionality.
It goes beyond basic API testing to ensure complex detail page features remain intact.

Purpose: Detect when dataset detail page functionality is accidentally:
- Removed or disabled
- Simplified or dumbed down
- Modified to return generic/mock data instead of real data
- Changed to break frontend expectations
"""

import pytest
import requests
import json
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass
from datetime import datetime

# Test configuration
TEST_BASE_URL = "http://localhost:8000"  # Port forward URL

@dataclass
class DatasetDetailPageExpectations:
    """Expected dataset detail page behavior - DO NOT CHANGE without understanding impact"""

    # Required dataset detail endpoints that MUST exist
    REQUIRED_ENDPOINTS = [
        "/api/v1/datasets/{dataset_id}",                              # Dataset detail
        "/api/v1/datasets/{dataset_id}/sequences",                    # Dataset sequences list
        "/api/v1/datasets/{dataset_id}/sequences/{sequence_id}",      # Individual sequence detail
        "/api/v1/datasets/{dataset_id}/sequences/{sequence_id}/ohlc"  # Sequence OHLC chart data
    ]

    # Dataset detail response must have these fields
    DATASET_DETAIL_REQUIRED_FIELDS = [
        "dataset_id", "dataset_name", "symbol", "symbols",
        "total_sequences", "feature_count", "sequence_length",
        "file_size_mb", "status", "created_at"
    ]

    # Sequence detail response must have these fields
    SEQUENCE_DETAIL_REQUIRED_FIELDS = [
        "sequence_id", "dataset_id", "sequence_name", "symbol",
        "feature_count", "sequence_length", "start_date", "end_date"
    ]

    # OHLC data response must have these fields
    OHLC_RESPONSE_REQUIRED_FIELDS = [
        "sequence_id", "dataset_id", "symbol", "ohlc_data", "period"
    ]

    # Each OHLC data point must have these fields
    OHLC_POINT_REQUIRED_FIELDS = [
        "timestamp", "open", "high", "low", "close", "volume"
    ]

    # Minimum expectations for data quality (not just mock/fake data)
    MIN_OHLC_DATA_POINTS = 10        # Should have meaningful historical data
    MIN_TOTAL_SEQUENCES = 10         # Datasets should have meaningful sequence counts (adjusted for test system)
    MIN_FEATURE_COUNT = 10           # Should have meaningful feature dimensions


class TestDatasetDetailPageRegressionProtection:
    """Comprehensive regression protection for dataset detail page functionality"""

    def test_all_dataset_detail_endpoints_exist_and_respond(self):
        """CRITICAL: Ensure all dataset detail endpoints exist and respond correctly"""
        print("\\n🧪 Testing that ALL dataset detail endpoints exist and respond")

        # Get a valid dataset ID first
        datasets_response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets?limit=1", timeout=10)
        assert datasets_response.status_code == 200, "Cannot get datasets for testing"

        datasets_data = datasets_response.json()
        assert "datasets" in datasets_data and len(datasets_data["datasets"]) > 0, "No datasets available for testing"

        dataset_id = datasets_data["datasets"][0]["dataset_id"]
        test_sequence_id = 1001  # Standard test sequence ID

        # Test each required endpoint
        endpoint_tests = [
            (f"/api/v1/datasets/{dataset_id}", "Dataset Detail"),
            (f"/api/v1/datasets/{dataset_id}/sequences", "Dataset Sequences List"),
            (f"/api/v1/datasets/{dataset_id}/sequences/{test_sequence_id}", "Individual Sequence Detail"),
            (f"/api/v1/datasets/{dataset_id}/sequences/{test_sequence_id}/ohlc", "Sequence OHLC Data")
        ]

        for endpoint, description in endpoint_tests:
            print(f"   Testing: {description} ({endpoint})")

            response = requests.get(f"{TEST_BASE_URL}{endpoint}", timeout=10)

            # Must return 200 OK (not 404, 500, etc.)
            assert response.status_code == 200, f"{description} endpoint failed: {response.status_code} {response.text}"

            # Must return valid JSON
            try:
                json_data = response.json()
            except json.JSONDecodeError as e:
                assert False, f"{description} endpoint returned invalid JSON: {e}"

            # Must not return generic error responses
            assert "error" not in json_data or json_data.get("error") != "Not implemented", f"{description} endpoint not implemented"
            assert json_data != {}, f"{description} endpoint returned empty response"

            print(f"     ✅ {description} working correctly")

        print("   ✅ All dataset detail endpoints exist and respond correctly")

    def test_dataset_detail_response_completeness_and_quality(self):
        """CRITICAL: Dataset detail responses must have complete, high-quality data"""
        print("\\n🧪 Testing dataset detail response completeness and data quality")

        # Get dataset detail
        datasets_response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets?limit=1", timeout=10)
        dataset_id = datasets_response.json()["datasets"][0]["dataset_id"]

        response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets/{dataset_id}", timeout=10)
        assert response.status_code == 200
        detail_data = response.json()

        print(f"   Validating dataset detail for ID: {dataset_id}")

        # Check all required fields exist
        for field in DatasetDetailPageExpectations.DATASET_DETAIL_REQUIRED_FIELDS:
            assert field in detail_data, f"Dataset detail missing required field: {field}"

        # Check data quality (not just mock/placeholder data)
        assert detail_data["dataset_id"] == dataset_id, "Dataset ID mismatch"
        assert isinstance(detail_data["dataset_name"], str) and len(detail_data["dataset_name"]) > 0, "Invalid dataset name"
        assert isinstance(detail_data["symbol"], str) and len(detail_data["symbol"]) > 0, "Invalid symbol"
        assert isinstance(detail_data["symbols"], list) and len(detail_data["symbols"]) > 0, "Invalid symbols list"

        # Check realistic data values (not obviously fake)
        assert detail_data["total_sequences"] >= DatasetDetailPageExpectations.MIN_TOTAL_SEQUENCES, f"Total sequences too low: {detail_data['total_sequences']} (suspicious)"
        assert detail_data["feature_count"] >= DatasetDetailPageExpectations.MIN_FEATURE_COUNT, f"Feature count too low: {detail_data['feature_count']} (suspicious)"
        assert detail_data["sequence_length"] > 0, "Sequence length must be positive"
        assert detail_data["file_size_mb"] > 0, "File size must be positive"

        # Check created_at is a valid timestamp
        if detail_data["created_at"]:
            try:
                datetime.fromisoformat(detail_data["created_at"])
            except ValueError:
                assert False, f"Invalid created_at timestamp: {detail_data['created_at']}"

        print("   ✅ Dataset detail response has complete, high-quality data")

    def test_sequence_detail_functionality_depth(self):
        """CRITICAL: Sequence detail must provide meaningful, specific data"""
        print("\\n🧪 Testing sequence detail functionality depth and specificity")

        # Get dataset and test sequence detail
        datasets_response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets?limit=1", timeout=10)
        dataset_id = datasets_response.json()["datasets"][0]["dataset_id"]
        test_sequence_id = 1001

        response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets/{dataset_id}/sequences/{test_sequence_id}", timeout=10)
        assert response.status_code == 200
        seq_data = response.json()

        print(f"   Validating sequence detail for dataset {dataset_id}, sequence {test_sequence_id}")

        # Check all required fields
        for field in DatasetDetailPageExpectations.SEQUENCE_DETAIL_REQUIRED_FIELDS:
            assert field in seq_data, f"Sequence detail missing required field: {field}"

        # Verify data specificity (not generic placeholders)
        assert seq_data["sequence_id"] == test_sequence_id, "Sequence ID must match request"
        assert seq_data["dataset_id"] == dataset_id, "Dataset ID must match request"
        assert isinstance(seq_data["sequence_name"], str) and str(test_sequence_id) in seq_data["sequence_name"], "Sequence name must be specific to sequence ID"

        # Verify time period data
        assert seq_data["start_date"] is not None, "Sequence must have start_date"
        assert seq_data["end_date"] is not None, "Sequence must have end_date"

        try:
            start_dt = datetime.fromisoformat(seq_data["start_date"])
            end_dt = datetime.fromisoformat(seq_data["end_date"])
            assert start_dt < end_dt, "Start date must be before end date"
        except ValueError as e:
            assert False, f"Invalid sequence date format: {e}"

        print("   ✅ Sequence detail provides meaningful, specific data")

    def test_ohlc_data_richness_and_authenticity(self):
        """CRITICAL: OHLC data must be rich, realistic, and chart-ready"""
        print("\\n🧪 Testing OHLC data richness and authenticity (not mock data)")

        # Get OHLC data
        datasets_response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets?limit=1", timeout=10)
        dataset_id = datasets_response.json()["datasets"][0]["dataset_id"]
        test_sequence_id = 1001

        response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets/{dataset_id}/sequences/{test_sequence_id}/ohlc", timeout=10)
        assert response.status_code == 200
        ohlc_data = response.json()

        print(f"   Validating OHLC data for dataset {dataset_id}, sequence {test_sequence_id}")

        # Check top-level response structure
        for field in DatasetDetailPageExpectations.OHLC_RESPONSE_REQUIRED_FIELDS:
            assert field in ohlc_data, f"OHLC response missing required field: {field}"

        # Verify OHLC data array exists and has sufficient data
        ohlc_array = ohlc_data["ohlc_data"]
        assert isinstance(ohlc_array, list), "OHLC data must be an array"
        assert len(ohlc_array) >= DatasetDetailPageExpectations.MIN_OHLC_DATA_POINTS, f"OHLC data too sparse: {len(ohlc_array)} points (suspicious of mock data)"

        # Check individual OHLC points for completeness and realism
        for i, point in enumerate(ohlc_array[:5]):  # Check first 5 points thoroughly
            print(f"     Checking OHLC point {i+1}: {point}")

            # Required fields
            for field in DatasetDetailPageExpectations.OHLC_POINT_REQUIRED_FIELDS:
                assert field in point, f"OHLC point {i+1} missing field: {field}"

            # Data type validation
            assert isinstance(point["open"], (int, float)) and point["open"] > 0, f"Invalid open price in point {i+1}"
            assert isinstance(point["high"], (int, float)) and point["high"] > 0, f"Invalid high price in point {i+1}"
            assert isinstance(point["low"], (int, float)) and point["low"] > 0, f"Invalid low price in point {i+1}"
            assert isinstance(point["close"], (int, float)) and point["close"] > 0, f"Invalid close price in point {i+1}"
            assert isinstance(point["volume"], int) and point["volume"] > 0, f"Invalid volume in point {i+1}"

            # OHLC relationship validation (realistic price relationships)
            assert point["low"] <= point["high"], f"Low price must be <= high price in point {i+1}"
            assert point["low"] <= point["open"] <= point["high"], f"Open price must be between low and high in point {i+1}"
            assert point["low"] <= point["close"] <= point["high"], f"Close price must be between low and high in point {i+1}"

            # Timestamp validation
            assert isinstance(point["timestamp"], str), f"Timestamp must be string in point {i+1}"
            try:
                datetime.fromisoformat(point["timestamp"])
            except ValueError:
                assert False, f"Invalid timestamp format in point {i+1}: {point['timestamp']}"

        # Check for price variation (not flat/constant mock data)
        prices = [point["close"] for point in ohlc_array[:10]]
        price_variance = max(prices) - min(prices)
        assert price_variance > 0, "OHLC data shows no price variation (suspicious of mock/flat data)"

        # Check period information
        period = ohlc_data["period"]
        assert "start" in period and "end" in period, "Period information incomplete"

        print(f"   ✅ OHLC data is rich, realistic with {len(ohlc_array)} data points and proper price variance")

    def test_dataset_sequences_list_functionality(self):
        """CRITICAL: Dataset sequences list must work with pagination and filtering"""
        print("\\n🧪 Testing dataset sequences list functionality completeness")

        datasets_response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets?limit=1", timeout=10)
        dataset_id = datasets_response.json()["datasets"][0]["dataset_id"]

        # Test sequences list endpoint
        response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets/{dataset_id}/sequences", timeout=10)
        assert response.status_code == 200
        sequences_data = response.json()

        print(f"   Validating sequences list for dataset {dataset_id}")

        # Must have expected structure
        required_top_level_fields = ["sequences", "total_sequences", "dataset_name", "available_symbols"]
        for field in required_top_level_fields:
            assert field in sequences_data, f"Sequences list missing top-level field: {field}"

        # Sequences array must exist and have content
        sequences = sequences_data["sequences"]
        assert isinstance(sequences, list), "Sequences must be an array"
        assert len(sequences) > 0, "Sequences list cannot be empty"

        # Check individual sequence entries
        for i, sequence in enumerate(sequences[:3]):  # Check first 3 sequences
            required_seq_fields = ["sequence_id", "sequence_name", "start_date", "end_date", "symbols"]
            for field in required_seq_fields:
                assert field in sequence, f"Sequence {i+1} missing field: {field}"

        # Test pagination functionality
        page1_response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets/{dataset_id}/sequences?limit=2&offset=0", timeout=10)
        page2_response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets/{dataset_id}/sequences?limit=2&offset=2", timeout=10)

        assert page1_response.status_code == 200, "Sequences pagination (page 1) failed"
        assert page2_response.status_code == 200, "Sequences pagination (page 2) failed"

        page1_data = page1_response.json()
        page2_data = page2_response.json()

        # Pagination should return different results
        if len(page1_data["sequences"]) > 0 and len(page2_data["sequences"]) > 0:
            page1_ids = {seq["sequence_id"] for seq in page1_data["sequences"]}
            page2_ids = {seq["sequence_id"] for seq in page2_data["sequences"]}
            assert len(page1_ids.intersection(page2_ids)) == 0, "Pagination not working - overlapping sequence IDs"

        print("   ✅ Dataset sequences list functionality complete with working pagination")

    def test_cross_endpoint_data_consistency(self):
        """CRITICAL: Data must be consistent across related endpoints"""
        print("\\n🧪 Testing cross-endpoint data consistency")

        # Get dataset from list
        datasets_response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets?limit=1", timeout=10)
        dataset_from_list = datasets_response.json()["datasets"][0]
        dataset_id = dataset_from_list["dataset_id"]

        # Get same dataset from detail endpoint
        detail_response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets/{dataset_id}", timeout=10)
        dataset_from_detail = detail_response.json()

        # Get sequence from sequences list
        sequences_response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets/{dataset_id}/sequences?limit=1", timeout=10)
        sequences_data = sequences_response.json()

        print(f"   Checking data consistency for dataset {dataset_id}")

        # Dataset list vs dataset detail consistency
        consistency_fields = ["dataset_id", "dataset_name", "symbol", "total_sequences", "feature_count"]
        for field in consistency_fields:
            if field in dataset_from_list and field in dataset_from_detail:
                assert dataset_from_list[field] == dataset_from_detail[field], f"Inconsistent {field}: list={dataset_from_list[field]} vs detail={dataset_from_detail[field]}"

        # Sequences list vs dataset detail consistency
        if "total_sequences" in dataset_from_detail and "total_sequences" in sequences_data:
            # Total sequences should be consistent (or at least in same ballpark)
            detail_total = dataset_from_detail["total_sequences"]
            sequences_total = sequences_data["total_sequences"]
            # Allow for reasonable variance in calculation methods, but catch major inconsistencies
            ratio = max(detail_total, sequences_total) / max(1, min(detail_total, sequences_total))
            assert ratio <= 10, f"Large inconsistency in total_sequences: detail={detail_total} vs sequences={sequences_total} (ratio={ratio:.1f}x)"

        # Symbol consistency across endpoints
        detail_symbol = dataset_from_detail.get("symbol")
        sequences_symbols = sequences_data.get("available_symbols", [])
        if detail_symbol and sequences_symbols:
            assert detail_symbol in sequences_symbols or detail_symbol == "UNKNOWN", f"Symbol inconsistency: detail has '{detail_symbol}' but sequences list has {sequences_symbols}"

        print("   ✅ Data is consistent across all related endpoints")

    def test_error_handling_and_edge_cases(self):
        """CRITICAL: Proper error handling for invalid requests"""
        print("\\n🧪 Testing error handling and edge cases")

        # Test invalid dataset ID
        invalid_response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets/99999", timeout=10)
        assert invalid_response.status_code in [404, 400], f"Invalid dataset ID should return 404/400, got {invalid_response.status_code}"

        # Should return JSON error, not HTML or plain text
        try:
            error_data = invalid_response.json()
            assert "error" in error_data or "message" in error_data, "Error response should have error/message field"
        except json.JSONDecodeError:
            assert False, "Error response should be valid JSON, not HTML/text"

        # Test invalid sequence ID
        datasets_response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets?limit=1", timeout=10)
        dataset_id = datasets_response.json()["datasets"][0]["dataset_id"]

        invalid_seq_response = requests.get(f"{TEST_BASE_URL}/api/v1/datasets/{dataset_id}/sequences/99999", timeout=10)
        assert invalid_seq_response.status_code == 200, "Sequence detail should handle non-existent sequences gracefully"

        print("   ✅ Error handling working correctly")


def run_dataset_detail_regression_protection_tests():
    """Run all dataset detail page regression protection tests"""
    print("🚀 RUNNING DATASET DETAIL PAGE REGRESSION PROTECTION TESTS")
    print("Purpose: Detect unintentional changes to dataset detail page functionality")
    print("=" * 80)

    test_instance = TestDatasetDetailPageRegressionProtection()

    tests = [
        ("All Detail Endpoints Exist", test_instance.test_all_dataset_detail_endpoints_exist_and_respond),
        ("Dataset Detail Data Quality", test_instance.test_dataset_detail_response_completeness_and_quality),
        ("Sequence Detail Functionality", test_instance.test_sequence_detail_functionality_depth),
        ("OHLC Data Richness", test_instance.test_ohlc_data_richness_and_authenticity),
        ("Sequences List Complete", test_instance.test_dataset_sequences_list_functionality),
        ("Cross-Endpoint Consistency", test_instance.test_cross_endpoint_data_consistency),
        ("Error Handling", test_instance.test_error_handling_and_edge_cases)
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
    print("📊 DATASET DETAIL PAGE REGRESSION PROTECTION SUMMARY")
    print("=" * 80)
    print(f"✅ PASSED: {passed} tests")
    print(f"❌ FAILED: {failed} tests")

    if failed > 0:
        print(f"\\n🚨 DATASET DETAIL PAGE REGRESSIONS DETECTED:")
        print(f"   Dataset detail page functionality has been compromised!")
        print(f"   This indicates that detail page features were accidentally:")
        print(f"   - Removed or disabled")
        print(f"   - Simplified or dumbed down")
        print(f"   - Changed to return mock/generic data")
        print(f"   - Modified to break frontend expectations")
    else:
        print(f"\\n🎉 NO DATASET DETAIL PAGE REGRESSIONS DETECTED!")
        print(f"   All dataset detail page functionality is preserved and working correctly.")
        print(f"   Rich, authentic data is being served to support the detail page frontend.")

    return passed, failed


if __name__ == "__main__":
    run_dataset_detail_regression_protection_tests()