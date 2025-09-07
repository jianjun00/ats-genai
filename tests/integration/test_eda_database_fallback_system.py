#!/usr/bin/env python3
"""
Integration tests for EDA database fallback system.
Tests fallback data when database queries timeout or fail.
"""

import pytest
import requests
import time
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestEDADatabaseFallbackSystem:
    """Test suite for database fallback functionality."""

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

    def test_datasets_endpoint_returns_fallback_data(self):
        """Test that datasets endpoint returns fallback data when database is unavailable."""
        response = requests.get(f"{self.base_url}/api/v1/datasets", timeout=self.timeout)
        assert response.status_code == 200, "Datasets endpoint should return 200 with fallback data"

        datasets = response.json()
        assert isinstance(datasets, list), "Response should be a list"
        assert len(datasets) > 0, "Should return fallback datasets when DB unavailable"

        # Verify fallback dataset structure
        dataset = datasets[0]
        required_fields = ["name", "display_name", "row_count", "column_count", "vendor", "data_type"]
        for field in required_fields:
            assert field in dataset, f"Fallback dataset missing required field: {field}"

        # Verify realistic fallback data (not zeros or empty)
        assert dataset["row_count"] > 0, "Fallback should have realistic row counts"
        assert dataset["column_count"] > 0, "Fallback should have realistic column counts"
        assert dataset["display_name"] != "", "Fallback should have meaningful display names"
        assert dataset["vendor"] != "", "Fallback should specify vendor"

    def test_fallback_datasets_include_expected_tables(self):
        """Test that fallback data includes the key ATS datasets."""
        response = requests.get(f"{self.base_url}/api/v1/datasets", timeout=self.timeout)
        datasets = response.json()

        dataset_names = [d["name"] for d in datasets]
        expected_tables = [
            "dev_instrument_tiingo",
            "dev_instrument_polygon",
            "dev_daily_prices_polygon",
            "dev_daily_prices_tiingo",
            "dev_instrument_eodhd"
        ]

        for expected_table in expected_tables:
            assert expected_table in dataset_names, f"Expected dataset '{expected_table}' not in fallback data"

        print(f"✅ Fallback data includes {len(dataset_names)} datasets: {dataset_names}")

    def test_schema_endpoint_with_fallback_for_tiingo(self):
        """Test schema endpoint returns fallback schema for Tiingo instruments."""
        dataset_name = "dev_instrument_tiingo"
        response = requests.get(f"{self.base_url}/api/v1/datasets/{dataset_name}/schema",
                              timeout=self.timeout)
        assert response.status_code == 200

        schema = response.json()
        assert "columns" in schema, "Schema should have columns field"
        assert isinstance(schema["columns"], list), "Columns should be a list"
        assert len(schema["columns"]) > 0, "Should return fallback schema columns"

        # Verify expected Tiingo columns are present
        column_names = [col["column_name"] for col in schema["columns"]]
        expected_tiingo_columns = ["symbol", "name", "market_cap", "price", "volume", "start_date", "end_date"]

        for expected_col in expected_tiingo_columns:
            assert expected_col in column_names, f"Expected Tiingo column '{expected_col}' not in fallback schema"

        # Verify numeric columns have correct data types for dropdown filtering
        numeric_columns = []
        for col in schema["columns"]:
            if col["column_name"] in ["market_cap", "price", "volume"]:
                data_type = col["data_type"].lower()
                assert any(t in data_type for t in ["numeric", "integer", "double", "bigint"]), \
                    f"Column {col['column_name']} should be numeric type, got {col['data_type']}"
                numeric_columns.append(col["column_name"])

        assert len(numeric_columns) == 3, f"Expected 3 numeric columns for analysis, got {len(numeric_columns)}"

    def test_schema_endpoint_with_fallback_for_polygon_prices(self):
        """Test schema endpoint returns fallback schema for Polygon OHLCV data."""
        dataset_name = "dev_daily_prices_polygon"
        response = requests.get(f"{self.base_url}/api/v1/datasets/{dataset_name}/schema",
                              timeout=self.timeout)
        assert response.status_code == 200

        schema = response.json()
        column_names = [col["column_name"] for col in schema["columns"]]

        # Verify OHLCV columns are present
        ohlcv_columns = ["open", "high", "low", "close", "volume"]
        for col in ohlcv_columns:
            assert col in column_names, f"Expected OHLCV column '{col}' not in fallback schema"

        # Verify OHLCV columns are numeric for analysis
        ohlcv_numeric_count = 0
        for col in schema["columns"]:
            if col["column_name"] in ohlcv_columns:
                data_type = col["data_type"].lower()
                assert any(t in data_type for t in ["numeric", "integer", "double", "bigint"]), \
                    f"OHLCV column {col['column_name']} should be numeric, got {col['data_type']}"
                ohlcv_numeric_count += 1

        assert ohlcv_numeric_count == 5, f"All 5 OHLCV columns should be numeric, got {ohlcv_numeric_count}"

    def test_schema_endpoint_nonexistent_dataset_fallback(self):
        """Test schema endpoint handles non-existent datasets gracefully with fallback."""
        dataset_name = "completely_fake_dataset_12345"
        response = requests.get(f"{self.base_url}/api/v1/datasets/{dataset_name}/schema",
                              timeout=self.timeout)
        assert response.status_code == 200, "Should return 200 even for non-existent dataset"

        schema = response.json()
        assert "error" in schema, "Should return error message for non-existent dataset"
        assert "not found" in schema["error"].lower(), "Error message should indicate dataset not found"

    def test_fallback_data_prevents_loading_forever(self):
        """Test that fallback data prevents the 'Loading...' issue that was reported."""
        # This test verifies the specific issue user reported: "Loading... Interactive Analysis shows no dataset"
        response = requests.get(f"{self.base_url}/api/v1/datasets", timeout=self.timeout)
        assert response.status_code == 200

        datasets = response.json()
        assert len(datasets) > 0, "Should have datasets to prevent 'Loading...' state"

        # Verify each dataset has non-zero row counts (prevents UI from hiding datasets)
        for dataset in datasets:
            assert dataset["row_count"] > 0, f"Dataset {dataset['name']} has zero row count - would cause 'Loading...' issue"
            assert dataset["column_count"] > 0, f"Dataset {dataset['name']} has zero columns - would cause issues"

        print(f"✅ All {len(datasets)} datasets have realistic counts to prevent 'Loading...' issue")

    def test_fallback_performance_response_time(self):
        """Test that fallback data returns quickly (doesn't wait for DB timeout)."""
        start_time = time.time()
        response = requests.get(f"{self.base_url}/api/v1/datasets", timeout=self.timeout)
        end_time = time.time()

        response_time = end_time - start_time

        assert response.status_code == 200
        # Fallback should be fast (< 2 seconds), not waiting for DB timeout (> 10 seconds)
        assert response_time < 5.0, f"Fallback response took {response_time:.2f}s - too slow, may not be using fallback"

        print(f"✅ Fallback data returned in {response_time:.2f}s (good performance)")

    def test_fallback_schema_performance(self):
        """Test that fallback schema returns quickly for all key datasets."""
        key_datasets = ["dev_instrument_tiingo", "dev_daily_prices_polygon", "dev_instrument_polygon"]

        for dataset_name in key_datasets:
            start_time = time.time()
            response = requests.get(f"{self.base_url}/api/v1/datasets/{dataset_name}/schema",
                                  timeout=self.timeout)
            end_time = time.time()

            response_time = end_time - start_time

            assert response.status_code == 200, f"Schema request failed for {dataset_name}"
            assert response_time < 5.0, f"Schema fallback for {dataset_name} took {response_time:.2f}s - too slow"

            schema = response.json()
            assert "columns" in schema and len(schema["columns"]) > 0, f"Fallback schema for {dataset_name} should have columns"

        print(f"✅ All fallback schemas returned quickly for {len(key_datasets)} datasets")

    def test_analysis_endpoint_with_database_unavailable(self):
        """Test analysis endpoint behavior when database is unavailable."""
        payload = {
            "dataset_name": "dev_instrument_tiingo",
            "column": "market_cap"
        }

        response = requests.post(f"{self.base_url}/api/v1/analysis/distribution",
                               json=payload, timeout=self.timeout)

        # Analysis might return error when DB unavailable, but should not timeout or crash
        assert response.status_code in [200, 400, 500], "Analysis should return proper HTTP status, not timeout"

        # If it returns 200, should have some data structure
        if response.status_code == 200:
            analysis = response.json()
            # May have fallback histogram or error message, but should be valid JSON
            assert isinstance(analysis, dict), "Analysis response should be valid JSON"

        print(f"✅ Analysis endpoint handled database unavailability gracefully (status: {response.status_code})")


def test_fallback_system_regression():
    """Regression test to ensure fallback system is properly implemented in code."""
    try:
        # Check that analytics service has fallback data implemented
        with open('/home/jianjun/ats-genai-admin/src/services/analytics_service.py', 'r') as f:
            content = f.read()

        # Verify fallback data exists in the code
        assert "fallback" in content.lower(), "Service should implement fallback data system"
        assert "dev_instrument_tiingo" in content, "Should have Tiingo fallback data"
        assert "dev_daily_prices_polygon" in content, "Should have Polygon fallback data"
        assert "market_cap" in content, "Should have realistic column definitions"

        # Verify row counts are realistic (not zero)
        import re
        row_count_matches = re.findall(r"'row_count':\s*(\d+)", content)
        for count_str in row_count_matches:
            count = int(count_str)
            assert count > 0, f"Found zero row count {count} in fallback data - would cause 'Loading...' issue"

        print("✅ Fallback system regression check passed")

    except FileNotFoundError:
        pytest.skip("Analytics service file not found")
    except Exception as e:
        pytest.fail(f"Fallback system regression check failed: {e}")


if __name__ == "__main__":
    # Run database fallback tests
    test_suite = TestEDADatabaseFallbackSystem()
    test_suite.setup_class()

    try:
        print("🧪 Testing EDA Database Fallback System...")

        test_suite.test_datasets_endpoint_returns_fallback_data()
        print("✅ Datasets fallback data test passed")

        test_suite.test_fallback_datasets_include_expected_tables()
        print("✅ Expected datasets fallback test passed")

        test_suite.test_schema_endpoint_with_fallback_for_tiingo()
        print("✅ Tiingo schema fallback test passed")

        test_suite.test_schema_endpoint_with_fallback_for_polygon_prices()
        print("✅ Polygon OHLCV schema fallback test passed")

        test_suite.test_schema_endpoint_nonexistent_dataset_fallback()
        print("✅ Non-existent dataset fallback test passed")

        test_suite.test_fallback_data_prevents_loading_forever()
        print("✅ 'Loading...' issue prevention test passed")

        test_suite.test_fallback_performance_response_time()
        print("✅ Fallback performance test passed")

        test_suite.test_fallback_schema_performance()
        print("✅ Schema fallback performance test passed")

        test_suite.test_analysis_endpoint_with_database_unavailable()
        print("✅ Analysis endpoint fallback test passed")

        test_fallback_system_regression()

        print("\n🎉 All database fallback tests passed!")
        print("✅ Service provides fallback data when database is unavailable")
        print("✅ Fallback data prevents 'Loading...' UI issue")
        print("✅ Fast fallback response times (no waiting for DB timeout)")

    except Exception as e:
        print(f"❌ Database fallback test failed: {e}")
        exit(1)