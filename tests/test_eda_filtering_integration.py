#!/usr/bin/env python3
"""
Integration tests for EDA filtering functionality.
Tests the actual API endpoints and full filtering workflow.
"""

import requests
import json
import pytest
import time

class TestEDAFilteringIntegration:
    
    @classmethod
    def setup_class(cls):
        """Setup for the test class."""
        cls.base_url = "http://localhost:3000"
        # Wait for service to be ready
        time.sleep(2)
    
    def test_health_endpoint(self):
        """Test that analytics service is running."""
        response = requests.get(f"{self.base_url}/health", timeout=5)
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
    
    def test_column_values_endpoint_categorical(self):
        """Test column values endpoint for categorical columns."""
        dataset_name = "dev_instruments"
        column_name = "symbol"
        
        response = requests.get(
            f"{self.base_url}/api/eda/datasets/{dataset_name}/columns/{column_name}/values?limit=5",
            timeout=10
        )
        assert response.status_code == 200
        
        data = response.json()
        
        # Validate response structure
        assert "column" in data
        assert "data_type" in data
        assert "values" in data
        assert "total_unique" in data
        
        # Validate column info
        assert data["column"] == column_name
        assert data["data_type"] == "categorical"
        
        # Validate values structure
        assert isinstance(data["values"], list)
        assert len(data["values"]) <= 5  # Respects limit parameter
        
        for value_item in data["values"]:
            assert "value" in value_item
            assert "count" in value_item
            assert isinstance(value_item["count"], int)
    
    def test_column_values_endpoint_numeric(self):
        """Test column values endpoint for numeric columns."""
        dataset_name = "dev_daily_prices"
        column_name = "price"
        
        response = requests.get(
            f"{self.base_url}/api/eda/datasets/{dataset_name}/columns/{column_name}/values?limit=10",
            timeout=10
        )
        assert response.status_code == 200
        
        data = response.json()
        
        # Validate response structure for numeric column
        assert data["column"] == column_name
        assert data["data_type"] == "numeric"
        
        # Should have min/max for numeric columns
        assert "min_value" in data or "values" in data
        assert "max_value" in data or "values" in data
        
        if "min_value" in data and "max_value" in data:
            assert isinstance(data["min_value"], (int, float))
            assert isinstance(data["max_value"], (int, float))
            assert data["min_value"] <= data["max_value"]
    
    def test_column_values_endpoint_with_different_limits(self):
        """Test column values endpoint with different limit parameters."""
        dataset_name = "dev_instruments"
        column_name = "exchange"
        
        # Test with small limit
        response1 = requests.get(
            f"{self.base_url}/api/eda/datasets/{dataset_name}/columns/{column_name}/values?limit=3",
            timeout=10
        )
        assert response1.status_code == 200
        data1 = response1.json()
        
        # Test with larger limit
        response2 = requests.get(
            f"{self.base_url}/api/eda/datasets/{dataset_name}/columns/{column_name}/values?limit=10",
            timeout=10
        )
        assert response2.status_code == 200
        data2 = response2.json()
        
        # Should respect limit (if there are enough unique values)
        assert len(data1["values"]) <= 3
        assert len(data2["values"]) <= 10
    
    def test_filtered_data_endpoint_no_filters(self):
        """Test filtered data endpoint without any filters."""
        dataset_name = "dev_instruments"
        
        payload = {
            "filters": {},
            "page": 1,
            "page_size": 10
        }
        
        response = requests.post(
            f"{self.base_url}/api/eda/datasets/{dataset_name}/data",
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=10
        )
        assert response.status_code == 200
        
        data = response.json()
        
        # Validate response structure
        required_keys = ["data", "pagination", "filters_applied", "table_name"]
        for key in required_keys:
            assert key in data
        
        # Validate pagination
        pagination = data["pagination"]
        assert pagination["current_page"] == 1
        assert pagination["page_size"] == 10
        assert isinstance(pagination["total_count"], int)
        assert isinstance(pagination["total_pages"], int)
        assert isinstance(pagination["has_next"], bool)
        assert isinstance(pagination["has_prev"], bool)
        
        # Validate data
        assert isinstance(data["data"], list)
        assert len(data["data"]) <= 10  # Respects page_size
        
        # Validate table name
        assert data["table_name"] == dataset_name
        
        # No filters applied
        assert data["filters_applied"] == {}
    
    def test_filtered_data_endpoint_categorical_filter(self):
        """Test filtered data endpoint with categorical filter."""
        dataset_name = "dev_instruments"
        
        payload = {
            "filters": {
                "symbol": {
                    "type": "values",
                    "values": ["AAPL", "GOOGL", "MSFT"]
                }
            },
            "page": 1,
            "page_size": 20
        }
        
        response = requests.post(
            f"{self.base_url}/api/eda/datasets/{dataset_name}/data",
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=10
        )
        assert response.status_code == 200
        
        data = response.json()
        
        # Validate filters were applied
        assert "symbol" in data["filters_applied"]
        assert data["filters_applied"]["symbol"]["type"] == "values"
        assert data["filters_applied"]["symbol"]["values"] == ["AAPL", "GOOGL", "MSFT"]
        
        # Validate pagination with filters
        assert data["pagination"]["page_size"] == 20
        assert data["pagination"]["current_page"] == 1
    
    def test_filtered_data_endpoint_numeric_filter(self):
        """Test filtered data endpoint with numeric range filter."""
        dataset_name = "dev_daily_prices"
        
        payload = {
            "filters": {
                "price": {
                    "type": "range",
                    "min": 100.0,
                    "max": 200.0
                }
            },
            "page": 1,
            "page_size": 15
        }
        
        response = requests.post(
            f"{self.base_url}/api/eda/datasets/{dataset_name}/data",
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=10
        )
        assert response.status_code == 200
        
        data = response.json()
        
        # Validate filters were applied
        assert "price" in data["filters_applied"]
        assert data["filters_applied"]["price"]["type"] == "range"
        assert data["filters_applied"]["price"]["min"] == 100.0
        assert data["filters_applied"]["price"]["max"] == 200.0
    
    def test_filtered_data_endpoint_mixed_filters(self):
        """Test filtered data endpoint with both categorical and numeric filters."""
        dataset_name = "dev_daily_prices"
        
        payload = {
            "filters": {
                "symbol": {
                    "type": "values",
                    "values": ["AAPL", "MSFT"]
                },
                "volume": {
                    "type": "range",
                    "min": 1000000
                }
            },
            "page": 1,
            "page_size": 25
        }
        
        response = requests.post(
            f"{self.base_url}/api/eda/datasets/{dataset_name}/data",
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=10
        )
        assert response.status_code == 200
        
        data = response.json()
        
        # Validate both filters were applied
        assert "symbol" in data["filters_applied"]
        assert "volume" in data["filters_applied"]
        assert len(data["filters_applied"]) == 2
    
    def test_filtered_data_endpoint_pagination(self):
        """Test filtered data endpoint pagination functionality."""
        dataset_name = "dev_instruments"
        
        # Test first page
        payload1 = {
            "filters": {},
            "page": 1,
            "page_size": 5
        }
        
        response1 = requests.post(
            f"{self.base_url}/api/eda/datasets/{dataset_name}/data",
            headers={"Content-Type": "application/json"},
            json=payload1,
            timeout=10
        )
        assert response1.status_code == 200
        data1 = response1.json()
        
        # Test second page
        payload2 = {
            "filters": {},
            "page": 2,
            "page_size": 5
        }
        
        response2 = requests.post(
            f"{self.base_url}/api/eda/datasets/{dataset_name}/data",
            headers={"Content-Type": "application/json"},
            json=payload2,
            timeout=10
        )
        assert response2.status_code == 200
        data2 = response2.json()
        
        # Validate pagination logic
        assert data1["pagination"]["current_page"] == 1
        assert data2["pagination"]["current_page"] == 2
        
        # First page should not have previous
        assert data1["pagination"]["has_prev"] is False
        
        # If there are multiple pages, first page should have next
        if data1["pagination"]["total_pages"] > 1:
            assert data1["pagination"]["has_next"] is True
            assert data2["pagination"]["has_prev"] is True
    
    def test_filtered_data_endpoint_invalid_requests(self):
        """Test filtered data endpoint error handling."""
        # Test with invalid dataset name
        payload = {
            "filters": {},
            "page": 1,
            "page_size": 10
        }
        
        response = requests.post(
            f"{self.base_url}/api/eda/datasets/nonexistent_dataset/data",
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=10
        )
        # Should handle gracefully (either 200 with error field or appropriate error code)
        assert response.status_code in [200, 404]
        
        if response.status_code == 200:
            data = response.json()
            # Should have error information or empty data
            assert "error" in data or len(data.get("data", [])) == 0
    
    def test_column_values_endpoint_invalid_requests(self):
        """Test column values endpoint error handling."""
        # Test with invalid dataset/column combination
        response = requests.get(
            f"{self.base_url}/api/eda/datasets/nonexistent/columns/invalid/values",
            timeout=10
        )
        # Should handle gracefully
        assert response.status_code in [200, 404]
        
        if response.status_code == 200:
            data = response.json()
            assert "error" in data or "column" in data
    
    def test_filtering_workflow_integration(self):
        """Test the complete filtering workflow."""
        dataset_name = "dev_instruments"
        
        # Step 1: Get available datasets
        datasets_response = requests.get(f"{self.base_url}/api/eda/datasets", timeout=10)
        assert datasets_response.status_code == 200
        datasets = datasets_response.json()
        assert isinstance(datasets, list)
        
        # Step 2: Get schema for dataset
        schema_response = requests.get(f"{self.base_url}/api/eda/datasets/{dataset_name}/schema", timeout=10)
        assert schema_response.status_code == 200
        schema = schema_response.json()
        assert "columns" in schema
        
        # Step 3: Get column values for filtering
        # Find a categorical column
        categorical_column = None
        for col in schema["columns"]:
            if col["data_type"].lower() in ["character varying", "text"]:
                categorical_column = col["column_name"]
                break
        
        if categorical_column:
            values_response = requests.get(
                f"{self.base_url}/api/eda/datasets/{dataset_name}/columns/{categorical_column}/values?limit=3",
                timeout=10
            )
            assert values_response.status_code == 200
            values_data = values_response.json()
            assert "values" in values_data
            
            # Step 4: Use values for filtering
            if values_data["values"]:
                test_values = [item["value"] for item in values_data["values"][:2]]
                
                filter_payload = {
                    "filters": {
                        categorical_column: {
                            "type": "values",
                            "values": test_values
                        }
                    },
                    "page": 1,
                    "page_size": 10
                }
                
                filtered_response = requests.post(
                    f"{self.base_url}/api/eda/datasets/{dataset_name}/data",
                    headers={"Content-Type": "application/json"},
                    json=filter_payload,
                    timeout=10
                )
                assert filtered_response.status_code == 200
                
                filtered_data = filtered_response.json()
                assert categorical_column in filtered_data["filters_applied"]
                assert filtered_data["filters_applied"][categorical_column]["values"] == test_values
        
        print(f"✅ Complete filtering workflow test passed for dataset '{dataset_name}'")


def test_manual_filtering_ui_verification():
    """Manual test instructions for UI verification."""
    print("\\n🧪 Manual UI Verification Steps:")
    print("1. Open http://localhost:3000/eda in your browser")
    print("2. Select a dataset from the list")
    print("3. Verify that filters section appears with column filters")
    print("4. Try applying categorical filters (checkboxes)")
    print("5. Try applying numeric range filters (min/max inputs)")
    print("6. Click 'Apply Filters' and then 'Load Filtered Data'")
    print("7. Verify filtered data table appears with pagination")
    print("8. Test pagination navigation (Next/Previous)")
    print("9. Try clearing filters and reloading data")


if __name__ == "__main__":
    # Run tests
    test_suite = TestEDAFilteringIntegration()
    test_suite.setup_class()
    
    try:
        print("🧪 Testing EDA Filtering Integration...")
        
        test_suite.test_health_endpoint()
        print("✅ Health endpoint test passed")
        
        test_suite.test_column_values_endpoint_categorical()
        print("✅ Column values (categorical) test passed")
        
        test_suite.test_column_values_endpoint_numeric()
        print("✅ Column values (numeric) test passed")
        
        test_suite.test_column_values_endpoint_with_different_limits()
        print("✅ Column values (limits) test passed")
        
        test_suite.test_filtered_data_endpoint_no_filters()
        print("✅ Filtered data (no filters) test passed")
        
        test_suite.test_filtered_data_endpoint_categorical_filter()
        print("✅ Filtered data (categorical filter) test passed")
        
        test_suite.test_filtered_data_endpoint_numeric_filter()
        print("✅ Filtered data (numeric filter) test passed")
        
        test_suite.test_filtered_data_endpoint_mixed_filters()
        print("✅ Filtered data (mixed filters) test passed")
        
        test_suite.test_filtered_data_endpoint_pagination()
        print("✅ Filtered data (pagination) test passed")
        
        test_suite.test_filtered_data_endpoint_invalid_requests()
        print("✅ Error handling test passed")
        
        test_suite.test_column_values_endpoint_invalid_requests()
        print("✅ Column values error handling test passed")
        
        test_suite.test_filtering_workflow_integration()
        print("✅ Complete workflow integration test passed")
        
        print("\\n🎉 All filtering integration tests passed!")
        
        test_manual_filtering_ui_verification()
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)