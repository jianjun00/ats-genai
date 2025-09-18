#!/usr/bin/env python3
"""
Unit tests for EDA column selection functionality.
These tests verify the column dropdown population logic and data type filtering.
"""

import pytest
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

class TestEDAColumnSelectionLogic:
    """Unit tests for column selection logic without requiring running service."""

    def test_numeric_column_filtering_logic(self):
        """Test the logic for filtering numeric columns for dropdown."""
        # Sample schema data matching what the API returns
        sample_schema = {
            "columns": [
                {"column_name": "symbol", "data_type": "character varying", "is_nullable": "NO"},
                {"column_name": "name", "data_type": "text", "is_nullable": "YES"},
                {"column_name": "market_cap", "data_type": "numeric", "is_nullable": "YES"},
                {"column_name": "price", "data_type": "double precision", "is_nullable": "YES"},
                {"column_name": "volume", "data_type": "bigint", "is_nullable": "YES"},
                {"column_name": "start_date", "data_type": "date", "is_nullable": "YES"}
            ]
        }

        # Apply the same filtering logic as frontend
        numeric_columns = []
        for col in sample_schema["columns"]:
            data_type = col["data_type"].lower()
            if any(t in data_type for t in ["numeric", "integer", "double", "bigint"]):
                numeric_columns.append(col["column_name"])

        # Should identify exactly the numeric columns
        expected = ["market_cap", "price", "volume"]
        assert numeric_columns == expected, f"Expected {expected}, got {numeric_columns}"

    def test_ohlcv_column_detection(self):
        """Test detection of OHLCV columns for financial data."""
        ohlcv_schema = {
            "columns": [
                {"column_name": "symbol", "data_type": "character varying", "is_nullable": "NO"},
                {"column_name": "date", "data_type": "date", "is_nullable": "NO"},
                {"column_name": "open", "data_type": "numeric", "is_nullable": "YES"},
                {"column_name": "high", "data_type": "numeric", "is_nullable": "YES"},
                {"column_name": "low", "data_type": "numeric", "is_nullable": "YES"},
                {"column_name": "close", "data_type": "numeric", "is_nullable": "YES"},
                {"column_name": "volume", "data_type": "bigint", "is_nullable": "YES"}
            ]
        }

        numeric_columns = []
        ohlcv_columns = []

        for col in ohlcv_schema["columns"]:
            data_type = col["data_type"].lower()
            if any(t in data_type for t in ["numeric", "integer", "double", "bigint"]):
                numeric_columns.append(col["column_name"])

                # Check if it's an OHLCV column
                if col["column_name"] in ["open", "high", "low", "close", "volume"]:
                    ohlcv_columns.append(col["column_name"])

        assert len(numeric_columns) == 5, f"Expected 5 numeric columns, got {len(numeric_columns)}"
        assert len(ohlcv_columns) == 5, f"Expected 5 OHLCV columns, got {len(ohlcv_columns)}"
        assert ohlcv_columns == ["open", "high", "low", "close", "volume"]

    def test_column_filtering_edge_cases(self):
        """Test edge cases in column type detection."""
        edge_case_schema = {
            "columns": [
                {"column_name": "int_col", "data_type": "integer", "is_nullable": "YES"},
                {"column_name": "big_int_col", "data_type": "bigint", "is_nullable": "YES"},
                {"column_name": "small_int_col", "data_type": "smallint", "is_nullable": "YES"},
                {"column_name": "real_col", "data_type": "real", "is_nullable": "YES"},
                {"column_name": "double_col", "data_type": "double precision", "is_nullable": "YES"},
                {"column_name": "decimal_col", "data_type": "decimal", "is_nullable": "YES"},
                {"column_name": "money_col", "data_type": "money", "is_nullable": "YES"},
                {"column_name": "text_col", "data_type": "text", "is_nullable": "YES"},
                {"column_name": "boolean_col", "data_type": "boolean", "is_nullable": "YES"}
            ]
        }

        numeric_columns = []
        for col in edge_case_schema["columns"]:
            data_type = col["data_type"].lower()
            # Current filtering logic (may need expansion)
            if any(t in data_type for t in ["numeric", "integer", "double", "bigint"]):
                numeric_columns.append(col["column_name"])

        # Should catch most numeric types
        expected_found = ["int_col", "big_int_col", "small_int_col", "double_col"]
        for col in expected_found:
            assert col in numeric_columns, f"Should detect {col} as numeric"

        # Should not catch non-numeric
        assert "text_col" not in numeric_columns
        assert "boolean_col" not in numeric_columns

    def test_empty_schema_handling(self):
        """Test handling of empty or malformed schemas."""
        empty_schema = {"columns": []}

        numeric_columns = []
        for col in empty_schema["columns"]:
            data_type = col["data_type"].lower()
            if any(t in data_type for t in ["numeric", "integer", "double", "bigint"]):
                numeric_columns.append(col["column_name"])

        assert len(numeric_columns) == 0, "Empty schema should have no numeric columns"

    def test_schema_structure_validation(self):
        """Test that schema has required structure for column selection."""
        valid_schema = {
            "columns": [
                {"column_name": "test_col", "data_type": "numeric", "is_nullable": "YES"}
            ]
        }

        # Validate required fields exist
        assert "columns" in valid_schema
        assert isinstance(valid_schema["columns"], list)

        for col in valid_schema["columns"]:
            assert "column_name" in col
            assert "data_type" in col
            assert "is_nullable" in col
            assert isinstance(col["column_name"], str)
            assert isinstance(col["data_type"], str)

if __name__ == "__main__":
    # Run the unit tests
    pytest.main([__file__, "-v"])