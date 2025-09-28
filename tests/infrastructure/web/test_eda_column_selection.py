#!/usr/bin/env python3

import requests
import time

class TestEDAColumnSelection:

    @classmethod
    def setup_class(cls):
        """Setup for the test class."""
        cls.base_url = "http://localhost:3000"
        # Wait for service to be ready
        time.sleep(2)

    def test_health_endpoint(self):
        """Test that EDA service is running."""
        response = requests.get(f"{self.base_url}/health", timeout=5)
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"

    def test_datasets_endpoint_returns_data(self):
        """Test that datasets endpoint returns expected structure."""
        response = requests.get(f"{self.base_url}/api/v1/datasets", timeout=10)
        assert response.status_code == 200

        datasets = response.json()
        assert isinstance(datasets, list)
        assert len(datasets) > 0

        # Check first dataset structure
        dataset = datasets[0]
        required_fields = ["name", "display_name", "row_count", "column_count", "vendor", "data_type"]
        for field in required_fields:
            assert field in dataset, f"Missing field: {field}"

    def test_schema_endpoint_tiingo_instruments(self):
        """Test schema endpoint for Tiingo instruments with numeric columns."""
        dataset_name = "dev_instrument_tiingo"
        response = requests.get(f"{self.base_url}/api/v1/datasets/{dataset_name}/schema", timeout=10)
        assert response.status_code == 200

        schema = response.json()
        assert "columns" in schema
        assert isinstance(schema["columns"], list)
        assert len(schema["columns"]) > 0

        # Check for numeric columns that should be available for analysis
        numeric_columns = []
        for col in schema["columns"]:
            assert "column_name" in col
            assert "data_type" in col
            assert "is_nullable" in col

            # Check if column is numeric (for dropdown filtering)
            if any(t in col["data_type"] for t in ["numeric", "integer", "double", "bigint"]):
                numeric_columns.append(col["column_name"])

        # Should have numeric columns for analysis
        assert len(numeric_columns) > 0, f"No numeric columns found for {dataset_name}"
        expected_numeric = ["market_cap", "price", "volume"]
        for col in expected_numeric:
            assert col in numeric_columns, f"Expected numeric column '{col}' not found"

    def test_schema_endpoint_polygon_prices(self):
        """Test schema endpoint for Polygon prices with OHLCV columns."""
        dataset_name = "dev_daily_price_polygon"
        response = requests.get(f"{self.base_url}/api/v1/datasets/{dataset_name}/schema", timeout=10)
        assert response.status_code == 200

        schema = response.json()
        assert "columns" in schema

        # Check for OHLCV columns
        column_names = [col["column_name"] for col in schema["columns"]]
        ohlcv_columns = ["open", "high", "low", "close", "volume"]
        for col in ohlcv_columns:
            assert col in column_names, f"Missing OHLCV column: {col}"

        # All OHLCV columns should be numeric for analysis
        numeric_columns = []
        for col in schema["columns"]:
            if col["column_name"] in ohlcv_columns:
                assert any(t in col["data_type"] for t in ["numeric", "integer", "bigint"]), \
                    f"Column {col['column_name']} should be numeric, got {col['data_type']}"
                numeric_columns.append(col["column_name"])

        assert len(numeric_columns) == 5, f"Expected 5 numeric OHLCV columns, got {len(numeric_columns)}"

    def test_schema_endpoint_nonexistent_dataset(self):
        """Test schema endpoint with non-existent dataset."""
        dataset_name = "nonexistent_dataset"
        response = requests.get(f"{self.base_url}/api/v1/datasets/{dataset_name}/schema", timeout=10)
        assert response.status_code == 200

        schema = response.json()
        assert "error" in schema
        assert "not found" in schema["error"].lower()

    def test_column_dropdown_filtering_logic(self):
        """Test that only numeric columns would be shown in dropdown."""
        dataset_name = "dev_instrument_tiingo"
        response = requests.get(f"{self.base_url}/api/v1/datasets/{dataset_name}/schema", timeout=10)
        schema = response.json()

        # Simulate frontend dropdown filtering logic
        dropdown_columns = []
        for col in schema["columns"]:
            data_type = col["data_type"].lower()
            if any(t in data_type for t in ["numeric", "integer", "double", "bigint"]):
                dropdown_columns.append(col["column_name"])

        # Should have exactly 3 numeric columns for Tiingo instruments
        assert len(dropdown_columns) == 3, f"Expected 3 dropdown columns, got {len(dropdown_columns)}: {dropdown_columns}"
        expected = ["market_cap", "price", "volume"]
        for col in expected:
            assert col in dropdown_columns, f"Missing expected column: {col}"

    def test_column_selection_integration_flow(self):
        """Test the full flow: get datasets -> select dataset -> get schema -> select numeric column."""
        # Step 1: Get datasets
        datasets_response = requests.get(f"{self.base_url}/api/v1/datasets", timeout=10)
        assert datasets_response.status_code == 200
        datasets = datasets_response.json()

        # Step 2: Select first dataset
        dataset = datasets[0]
        dataset_name = dataset["name"]

        # Step 3: Get schema for selected dataset
        schema_response = requests.get(f"{self.base_url}/api/v1/datasets/{dataset_name}/schema", timeout=10)
        assert schema_response.status_code == 200
        schema = schema_response.json()

        # Step 4: Find numeric columns (simulate dropdown population)
        numeric_columns = []
        for col in schema["columns"]:
            if any(t in col["data_type"].lower() for t in ["numeric", "integer", "double", "bigint"]):
                numeric_columns.append(col["column_name"])

        # Step 5: Verify we have columns to select from
        assert len(numeric_columns) > 0, f"No numeric columns available for analysis in {dataset_name}"

        # This simulates successful column dropdown population
        print(f"✅ Dataset '{dataset['display_name']}' has {len(numeric_columns)} numeric columns: {numeric_columns}")

def test_manual_column_selection_verification():
    """Manual test that can be run to verify column dropdown works in browser."""
    print("\n🧪 Manual Verification Steps:")
    print("1. Open http://localhost:3003 in your browser")
    print("2. Select 'Tiingo Instruments' from the dataset dropdown")
    print("3. Verify the column dropdown populates with: market_cap, price, volume")
    print("4. Select 'Polygon Daily Prices 30 Year' from the dataset dropdown")
    print("5. Verify the column dropdown populates with: open, high, low, close, volume")
    print("6. Try selecting a column and clicking 'Analyze Distribution'")

if __name__ == "__main__":
    # Run tests
    test_suite = TestEDAColumnSelection()
    test_suite.setup_class()

    print("🧪 Testing EDA Column Selection...")
    test_suite.test_health_endpoint()
    print("✅ Health endpoint test passed")

    test_suite.test_datasets_endpoint_returns_data()
    print("✅ Datasets endpoint test passed")

    test_suite.test_schema_endpoint_tiingo_instruments()
    print("✅ Tiingo schema test passed")

    test_suite.test_schema_endpoint_polygon_prices()
    print("✅ Polygon schema test passed")

    test_suite.test_schema_endpoint_nonexistent_dataset()
    print("✅ Non-existent dataset test passed")

    test_suite.test_column_dropdown_filtering_logic()
    print("✅ Column dropdown filtering test passed")

    test_suite.test_column_selection_integration_flow()
    print("✅ Integration flow test passed")

    print("\n🎉 All column selection tests passed!")

    test_manual_column_selection_verification()

