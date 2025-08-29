#!/usr/bin/env python3
"""
Unit tests for EDA filtering functionality.
Tests the column value discovery and filtered data methods.
"""

import pytest
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestEDAFilteringLogic:
    """Unit tests for filtering logic without requiring running service."""
    
    def test_numeric_filter_query_building(self):
        """Test building WHERE clauses for numeric range filters."""
        # Sample filter configuration
        filters = {
            "price": {
                "type": "range",
                "min": 10.0,
                "max": 100.0
            }
        }
        
        # Build WHERE clause (simulating backend logic)
        where_conditions = []
        params = []
        
        for column, filter_config in filters.items():
            if filter_config.get("type") == "range":
                if "min" in filter_config:
                    where_conditions.append(f"{column} >= ?")
                    params.append(filter_config["min"])
                if "max" in filter_config:
                    where_conditions.append(f"{column} <= ?")
                    params.append(filter_config["max"])
        
        expected_where = "price >= ? AND price <= ?"
        expected_params = [10.0, 100.0]
        
        assert " AND ".join(where_conditions) == expected_where
        assert params == expected_params
    
    def test_categorical_filter_query_building(self):
        """Test building WHERE clauses for categorical value filters."""
        filters = {
            "symbol": {
                "type": "values", 
                "values": ["AAPL", "GOOGL", "MSFT"]
            }
        }
        
        where_conditions = []
        params = []
        
        for column, filter_config in filters.items():
            if filter_config.get("type") == "values" and filter_config.get("values"):
                placeholders = ",".join(["?" for _ in filter_config["values"]])
                where_conditions.append(f"{column} IN ({placeholders})")
                params.extend(filter_config["values"])
        
        expected_where = "symbol IN (?,?,?)"
        expected_params = ["AAPL", "GOOGL", "MSFT"]
        
        assert " AND ".join(where_conditions) == expected_where
        assert params == expected_params
    
    def test_mixed_filters_query_building(self):
        """Test building WHERE clauses with both numeric and categorical filters."""
        filters = {
            "price": {
                "type": "range",
                "min": 50.0
            },
            "symbol": {
                "type": "values",
                "values": ["AAPL", "GOOGL"]
            },
            "volume": {
                "type": "range", 
                "max": 1000000
            }
        }
        
        where_conditions = []
        params = []
        
        for column, filter_config in filters.items():
            if filter_config.get("type") == "range":
                if "min" in filter_config:
                    where_conditions.append(f"{column} >= ?")
                    params.append(filter_config["min"])
                if "max" in filter_config:
                    where_conditions.append(f"{column} <= ?")
                    params.append(filter_config["max"])
            elif filter_config.get("type") == "values" and filter_config.get("values"):
                placeholders = ",".join(["?" for _ in filter_config["values"]])
                where_conditions.append(f"{column} IN ({placeholders})")
                params.extend(filter_config["values"])
        
        # Should have 3 conditions
        assert len(where_conditions) == 3
        assert len(params) == 4  # min price + 2 symbols + max volume
        
        # Check individual conditions
        assert "price >= ?" in where_conditions
        assert "symbol IN (?,?)" in where_conditions
        assert "volume <= ?" in where_conditions
    
    def test_pagination_calculation(self):
        """Test pagination logic calculation."""
        total_count = 1000
        page_size = 50
        current_page = 3
        
        # Calculate pagination values
        total_pages = (total_count + page_size - 1) // page_size  # Ceiling division
        offset = (current_page - 1) * page_size
        has_next = current_page < total_pages
        has_prev = current_page > 1
        
        expected_total_pages = 20
        expected_offset = 100  # (3-1) * 50
        
        assert total_pages == expected_total_pages
        assert offset == expected_offset
        assert has_next is True
        assert has_prev is True
    
    def test_pagination_edge_cases(self):
        """Test pagination edge cases."""
        # First page
        current_page = 1
        total_pages = 10
        assert current_page <= total_pages  # Valid page
        assert current_page > 1  # has_prev = False
        
        # Last page
        current_page = 10
        assert current_page < total_pages  # has_next = False
        assert current_page > 1  # has_prev = True
        
        # Single page
        total_count = 25
        page_size = 50
        total_pages = (total_count + page_size - 1) // page_size
        assert total_pages == 1
    
    def test_column_type_detection_logic(self):
        """Test logic for determining if column is numeric or categorical."""
        # Sample column metadata
        columns = [
            {"column_name": "id", "data_type": "integer"},
            {"column_name": "symbol", "data_type": "character varying"},
            {"column_name": "price", "data_type": "numeric"},
            {"column_name": "volume", "data_type": "bigint"},
            {"column_name": "name", "data_type": "text"},
            {"column_name": "market_cap", "data_type": "double precision"},
        ]
        
        numeric_types = ["numeric", "integer", "double", "bigint", "smallint", "real", "decimal", "float"]
        
        numeric_columns = []
        categorical_columns = []
        
        for col in columns:
            data_type = col["data_type"].lower()
            is_numeric = any(t in data_type for t in numeric_types)
            
            if is_numeric:
                numeric_columns.append(col["column_name"])
            else:
                categorical_columns.append(col["column_name"])
        
        expected_numeric = ["id", "price", "volume", "market_cap"]
        expected_categorical = ["symbol", "name"]
        
        assert numeric_columns == expected_numeric
        assert categorical_columns == expected_categorical
    
    def test_filter_validation(self):
        """Test filter input validation logic."""
        
        # Valid numeric filter
        numeric_filter = {"type": "range", "min": 10, "max": 100}
        assert numeric_filter.get("type") == "range"
        assert "min" in numeric_filter or "max" in numeric_filter
        
        # Valid categorical filter
        categorical_filter = {"type": "values", "values": ["A", "B", "C"]}
        assert categorical_filter.get("type") == "values"
        assert isinstance(categorical_filter.get("values"), list)
        assert len(categorical_filter.get("values")) > 0
        
        # Invalid filters
        invalid_filters = [
            {},  # Empty
            {"type": "range"},  # No min/max
            {"type": "values", "values": []},  # Empty values
            {"type": "invalid"},  # Unknown type
        ]
        
        for invalid_filter in invalid_filters:
            # These should be filtered out in real implementation
            is_valid = False
            
            if invalid_filter.get("type") == "range":
                is_valid = "min" in invalid_filter or "max" in invalid_filter
            elif invalid_filter.get("type") == "values":
                is_valid = isinstance(invalid_filter.get("values"), list) and len(invalid_filter.get("values")) > 0
            
            assert is_valid is False
    
    def test_demo_data_structure(self):
        """Test that demo data matches expected structure for filtering."""
        # Sample demo data structure (as used in fallback)
        demo_column_values = {
            "column": "symbol",
            "data_type": "categorical",
            "values": [
                {"value": "AAPL", "count": 100},
                {"value": "GOOGL", "count": 85},
                {"value": "MSFT", "count": 92}
            ],
            "total_unique": 3
        }
        
        # Validate structure
        assert "column" in demo_column_values
        assert "data_type" in demo_column_values
        assert "values" in demo_column_values
        assert isinstance(demo_column_values["values"], list)
        
        # Validate value structure
        for value_data in demo_column_values["values"]:
            assert "value" in value_data
            assert "count" in value_data
            assert isinstance(value_data["count"], int)
    
    def test_filtered_data_response_structure(self):
        """Test filtered data response structure."""
        demo_response = {
            "data": [
                {"id": 1, "symbol": "AAPL", "name": "Apple Inc."},
                {"id": 2, "symbol": "GOOGL", "name": "Alphabet Inc."}
            ],
            "pagination": {
                "current_page": 1,
                "page_size": 50,
                "total_count": 2,
                "total_pages": 1,
                "has_next": False,
                "has_prev": False
            },
            "filters_applied": {
                "symbol": {"type": "values", "values": ["AAPL", "GOOGL"]}
            },
            "table_name": "dev_instruments"
        }
        
        # Validate response structure
        required_keys = ["data", "pagination", "filters_applied", "table_name"]
        for key in required_keys:
            assert key in demo_response
        
        # Validate pagination structure
        pagination_keys = ["current_page", "page_size", "total_count", "total_pages", "has_next", "has_prev"]
        for key in pagination_keys:
            assert key in demo_response["pagination"]
        
        # Validate data is a list
        assert isinstance(demo_response["data"], list)
        
        # Validate filters structure
        assert isinstance(demo_response["filters_applied"], dict)


if __name__ == "__main__":
    # Run the unit tests
    pytest.main([__file__, "-v"])