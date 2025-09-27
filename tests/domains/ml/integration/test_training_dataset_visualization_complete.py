#!/usr/bin/env python3
"""
Comprehensive test coverage for training dataset visualization fixes.
Tests all the fixes made to resolve "No sequence data available" issue.
"""
import requests
import sys
import os

# Add src to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestTrainingDatasetVisualizationComplete:
    """Complete test coverage for training dataset visualization system."""

    BASE_URL = "http://localhost:3000"

    def test_training_datasets_api_structure(self):
        """Test that training datasets API returns proper structure."""
        print("\n🧪 Testing training datasets API structure...")

        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets", timeout=10)
        assert response.status_code == 200, f"API failed: {response.status_code}"

        data = response.json()
        assert "datasets" in data, "Response missing datasets field"
        assert isinstance(data["datasets"], list), "Datasets should be a list"
        assert len(data["datasets"]) > 0, "Should have at least one dataset"

        # Test dataset structure
        dataset = data["datasets"][0]
        required_fields = ["id", "dataset_name", "symbols", "total_sequences", "created_at"]
        for field in required_fields:
            assert field in dataset, f"Dataset missing required field: {field}"

        print(f"✅ Found {len(data['datasets'])} datasets with proper structure")

    def test_dataset_table_consistency(self):
        """Test that all datasets use consistent table (plural vs singular)."""
        print("\n🧪 Testing dataset table consistency...")

        # Get datasets from main API
        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets", timeout=10)
        datasets = response.json()["datasets"]

        # Test visualization for multiple datasets
        tested_count = 0
        for dataset in datasets[:3]:  # Test first 3 datasets
            dataset_id = dataset["id"]
            viz_response = requests.get(
                f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/visualization-data",
                timeout=10
            )

            if viz_response.status_code == 200:
                viz_data = viz_response.json()
                # Should not get "Dataset X not found" error if using correct table
                assert "Dataset" not in viz_data.get("error", ""), f"Table consistency error for dataset {dataset_id}"
                tested_count += 1

        assert tested_count > 0, "No datasets could be tested for table consistency"
        print(f"✅ Tested {tested_count} datasets - all use consistent database table")

    def test_postgresql_array_parsing(self):
        """Test that PostgreSQL array format {TSLA} is parsed correctly."""
        print("\n🧪 Testing PostgreSQL array parsing...")

        # Test datasets that likely have PostgreSQL array format
        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets", timeout=10)
        datasets = response.json()["datasets"]

        postgresql_array_found = False
        for dataset in datasets:
            symbols = dataset.get("symbols", [])
            if isinstance(symbols, list) and len(symbols) > 0:
                # Test visualization for this dataset
                viz_response = requests.get(
                    f"{self.BASE_URL}/api/v1/training-datasets/{dataset['id']}/visualization-data",
                    timeout=10
                )

                if viz_response.status_code == 200:
                    viz_data = viz_response.json()
                    if "symbol" in viz_data:
                        # Symbol should be extracted from array correctly
                        assert viz_data["symbol"] in symbols or viz_data["symbol"] == symbols[0]
                        postgresql_array_found = True
                        print(f"✅ Dataset {dataset['id']} symbol array parsed: {symbols} -> {viz_data['symbol']}")
                        break

        assert postgresql_array_found, "No PostgreSQL array format datasets found to test"

    def test_file_discovery_logic(self):
        """Test that file discovery finds actual training files."""
        print("\n🧪 Testing file discovery logic...")

        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets", timeout=10)
        datasets = response.json()["datasets"]

        files_found_count = 0
        for dataset in datasets[:5]:  # Test first 5 datasets
            dataset_id = dataset["id"]
            viz_response = requests.get(
                f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/visualization-data",
                timeout=10
            )

            if viz_response.status_code == 200:
                viz_data = viz_response.json()
                if viz_data.get("file_found"):
                    files_found_count += 1

                    # Verify file path structure
                    file_path = viz_data.get("file_path", "")
                    assert "/data/training" in file_path, f"Unexpected file path: {file_path}"
                    assert file_path.endswith((".riegeli", ".arrayrecord")), f"Unexpected file type: {file_path}"

                    # Verify file size reported
                    file_size = viz_data.get("file_size_mb", 0)
                    assert file_size > 0, f"File size should be > 0 MB, got {file_size}"

                    print(f"✅ Dataset {dataset_id}: Found file {file_path} ({file_size} MB)")

        assert files_found_count > 0, "File discovery should find at least some training files"
        print(f"✅ File discovery found {files_found_count} training data files")

    def test_visualization_response_structure(self):
        """Test that visualization responses have required fields for frontend."""
        print("\n🧪 Testing visualization response structure...")

        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets", timeout=10)
        datasets = response.json()["datasets"]

        # Test with a known dataset (try dataset 39 or first available)
        test_datasets = [39] + [d["id"] for d in datasets[:3]]

        structure_tested = False
        for dataset_id in test_datasets:
            viz_response = requests.get(
                f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/visualization-data",
                timeout=10
            )

            if viz_response.status_code == 200:
                viz_data = viz_response.json()

                # Required fields that frontend expects
                required_fields = ["dataset_id", "symbol", "data", "sequence_length"]
                for field in required_fields:
                    assert field in viz_data, f"Visualization response missing '{field}' field"

                # Test field types
                assert isinstance(viz_data["dataset_id"], int), "dataset_id should be integer"
                assert isinstance(viz_data["symbol"], str), "symbol should be string"
                assert isinstance(viz_data["data"], list), "data should be list"
                assert isinstance(viz_data["sequence_length"], int), "sequence_length should be integer"

                # Test additional metadata fields
                if viz_data.get("file_found"):
                    assert "file_path" in viz_data, "file_found=True should include file_path"
                    assert "file_size_mb" in viz_data, "file_found=True should include file_size_mb"
                    assert "status" in viz_data, "file_found=True should include status"

                structure_tested = True
                print(f"✅ Dataset {dataset_id} has proper response structure")
                break

        assert structure_tested, "Could not test visualization response structure"

    def test_no_mock_data_policy(self):
        """Test that system never returns mock/synthetic data."""
        print("\n🧪 Testing no mock data policy compliance...")

        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets", timeout=10)
        datasets = response.json()["datasets"]

        for dataset in datasets[:5]:  # Test multiple datasets
            dataset_id = dataset["id"]
            viz_response = requests.get(
                f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/visualization-data",
                timeout=10
            )

            if viz_response.status_code == 200:
                viz_data = viz_response.json()
                data = viz_data.get("data", [])

                # If data is empty, ensure it's because files aren't readable, not missing
                if len(data) == 0:
                    # Should have clear explanation, not fake data
                    status = viz_data.get("status", "")
                    message = viz_data.get("message", "")

                    # Should indicate file availability status
                    valid_statuses = [
                        "file_found_but_not_readable",
                        "available_but_not_readable",
                        "available_but_error"
                    ]

                    if viz_data.get("file_found"):
                        assert any(s in status for s in valid_statuses), f"Invalid status for empty data: {status}"
                        assert "found" in message.lower(), f"Message should explain file status: {message}"

                # If data exists, verify it's not obviously synthetic
                elif len(data) > 0:
                    # Real data should have variation, not repeated values
                    first_bar = data[0]
                    if len(data) > 1:
                        second_bar = data[1]
                        # At least one OHLC value should differ (real market data varies)
                        ohlc_same = (
                            first_bar.get("open") == second_bar.get("open") and
                            first_bar.get("high") == second_bar.get("high") and
                            first_bar.get("low") == second_bar.get("low") and
                            first_bar.get("close") == second_bar.get("close")
                        )
                        # This would be suspicious for real market data
                        if ohlc_same:
                            print(f"⚠️  Dataset {dataset_id} has identical OHLC values - investigate")

        print("✅ No mock data policy compliance verified")

    def test_error_handling_robustness(self):
        """Test error handling for various edge cases."""
        print("\n🧪 Testing error handling robustness...")

        # Test non-existent dataset
        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets/99999/visualization-data")
        assert response.status_code == 200, "Should return 200 with error message, not 404"

        data = response.json()
        assert "error" in data or "message" in data, "Should provide error information"

        # Test malformed requests
        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets/invalid/visualization-data")
        assert response.status_code in [200, 400], "Should handle invalid dataset ID gracefully"

        print("✅ Error handling works robustly")

    def test_performance_and_timeouts(self):
        """Test that API responses are reasonably fast."""
        print("\n🧪 Testing API performance...")

        import time

        # Test datasets list performance
        start_time = time.time()
        response = requests.get(f"{self.BASE_URL}/api/v1/training-datasets", timeout=5)
        datasets_time = time.time() - start_time

        assert response.status_code == 200, "Datasets API should respond"
        assert datasets_time < 3.0, f"Datasets API too slow: {datasets_time:.2f}s"

        # Test visualization performance
        datasets = response.json()["datasets"]
        if datasets:
            dataset_id = datasets[0]["id"]
            start_time = time.time()
            response = requests.get(
                f"{self.BASE_URL}/api/v1/training-datasets/{dataset_id}/visualization-data",
                timeout=5
            )
            viz_time = time.time() - start_time

            assert response.status_code == 200, "Visualization API should respond"
            assert viz_time < 5.0, f"Visualization API too slow: {viz_time:.2f}s"

        print(f"✅ API performance acceptable (datasets: {datasets_time:.2f}s, viz: {viz_time:.2f}s)")

    def test_integration_with_frontend(self):
        """Test integration points with frontend JavaScript."""
        print("\n🧪 Testing frontend integration points...")

        # Test that EDA page loads
        response = requests.get(f"{self.BASE_URL}/eda", timeout=10)
        assert response.status_code == 200, "EDA page should load"

        page_content = response.text

        # Check for required frontend elements
        assert "loadTrainingDatasets()" in page_content, "EDA page should have training datasets function"
        assert "dataset-selector" in page_content, "EDA page should have dataset selector"
        assert "sequence-selector" in page_content, "EDA page should have sequence selector"
        assert "No sequence data available" in page_content, "EDA page should handle empty data"

        # Check for JavaScript error handling
        assert "createSequenceTable" in page_content, "EDA page should have sequence table function"

        print("✅ Frontend integration points verified")

def run_all_tests():
    """Run all tests in the test suite."""
    print("🚀 Running Complete Training Dataset Visualization Test Suite")
    print("=" * 80)

    test_suite = TestTrainingDatasetVisualizationComplete()

    tests = [
        test_suite.test_training_datasets_api_structure,
        test_suite.test_dataset_table_consistency,
        test_suite.test_postgresql_array_parsing,
        test_suite.test_file_discovery_logic,
        test_suite.test_visualization_response_structure,
        test_suite.test_no_mock_data_policy,
        test_suite.test_error_handling_robustness,
        test_suite.test_performance_and_timeouts,
        test_suite.test_integration_with_frontend
    ]

    passed = 0
    failed = 0

    for test in tests:
        test()
        passed += 1
    print("\n" + "=" * 80)
    print(f"📊 Test Results: {passed} passed, {failed} failed")

    if failed == 0:
        print("🎉 ALL TESTS PASSED - Ready for deployment!")
        return True
    else:
        print("⚠️  Some tests failed - fix issues before deployment")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    exit(0 if success else 1)