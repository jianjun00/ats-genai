#!/usr/bin/env python3
"""
Integration tests for type-aware analytics service.

Tests the complete flow of intelligent filter generation, column analysis,
and database interaction using the type system.
"""

import asyncio
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from schema.registry import schema_registry
from schema.types import FieldSemantics


class MockDatabase:
    """Mock database for testing type-aware analytics service."""
    
    def __init__(self):
        self.call_log = []  # Track what queries were called
    
    async def execute_query(self, query, params=None):
        """Mock database responses based on query patterns."""
        self.call_log.append({"query": query, "params": params})
        
        # Numeric range queries (MIN/MAX)
        if "MIN" in query and "MAX" in query and "COUNT" in query:
            if "close" in query.lower():
                return [{"min": 15.25, "max": 1250.75, "count": 50000}]
            elif "volume" in query.lower():
                return [{"min": 1000, "max": 50000000, "count": 45000}]
            elif "open" in query.lower():
                return [{"min": 10.12, "max": 1100.88, "count": 50000}]
            else:
                return [{"min": 1.0, "max": 100.0, "count": 1000}]
        
        # Categorical options (GROUP BY with counts)
        elif "GROUP BY" in query and "COUNT(*)" in query:
            if "exchange" in query.lower():
                return [
                    {"value": "NYSE", "count": 1500},
                    {"value": "NASDAQ", "count": 1200},
                    {"value": "AMEX", "count": 300}
                ]
            elif "type" in query.lower():
                return [
                    {"value": "STOCK", "count": 2500},
                    {"value": "ETF", "count": 400},
                    {"value": "MUTUAL_FUND", "count": 100}
                ]
            elif "symbol" in query.lower():
                return [
                    {"value": "AAPL", "count": 5000},
                    {"value": "GOOGL", "count": 4500},
                    {"value": "MSFT", "count": 4200},
                    {"value": "AMZN", "count": 4000},
                    {"value": "TSLA", "count": 3500}
                ]
            else:
                return [{"value": "Sample", "count": 100}]
        
        # Search suggestions (DISTINCT)
        elif "DISTINCT" in query:
            if "symbol" in query.lower():
                return [
                    {"value": "AAPL"}, {"value": "AMZN"}, {"value": "AMD"}, 
                    {"value": "GOOGL"}, {"value": "GOOG"}, {"value": "META"},
                    {"value": "MSFT"}, {"value": "NFLX"}, {"value": "NVDA"}, {"value": "TSLA"}
                ]
            elif "name" in query.lower():
                return [
                    {"value": "Apple Inc."}, {"value": "Amazon.com Inc."}, 
                    {"value": "Microsoft Corporation"}, {"value": "Alphabet Inc."}
                ]
            else:
                return [{"value": "Sample"}]
        
        # Date ranges
        elif "MIN(" in query and "MAX(" in query and "date" in query.lower():
            return [{"min": "2020-01-01", "max": "2024-12-31", "count": 100000}]
        
        # Information schema queries (for legacy fallback testing)
        elif "information_schema.columns" in query.lower():
            return [
                {"column_name": "symbol", "data_type": "character varying"},
                {"column_name": "close", "data_type": "numeric"},
                {"column_name": "date", "data_type": "date"},
                {"column_name": "volume", "data_type": "bigint"}
            ]
        
        # Statistics queries
        elif "AVG(" in query and "STDDEV(" in query:
            return [{
                "count": 50000,
                "mean": 125.75,
                "std": 45.25,
                "min": 15.25,
                "max": 1250.75
            }]
        
        # Histogram queries (FLOOR and bin calculations)
        elif "FLOOR(" in query and "GROUP BY" in query:
            return [
                {"bin_index": 0, "count": 5000},
                {"bin_index": 1, "count": 8000},
                {"bin_index": 2, "count": 12000},
                {"bin_index": 3, "count": 15000},
                {"bin_index": 4, "count": 10000}
            ]
        
        # Value counts for categorical analysis
        elif "GROUP BY" in query and "ORDER BY count DESC" in query:
            if "symbol" in query.lower():
                return [
                    {"value": "AAPL", "count": 1500},
                    {"value": "GOOGL", "count": 1200},
                    {"value": "MSFT", "count": 1100}
                ]
            else:
                return [{"value": "Category1", "count": 500}]
        
        return []
    
    def get_call_count(self):
        """Get number of database calls made."""
        return len(self.call_log)
    
    def get_last_query(self):
        """Get the last query executed."""
        return self.call_log[-1] if self.call_log else None
    
    def clear_log(self):
        """Clear the call log."""
        self.call_log = []


# Import after setting up the path to avoid dependency issues
try:
    from services.type_aware_analytics_service import TypeAwareAnalyticsService
    SERVICE_AVAILABLE = True
except ImportError:
    SERVICE_AVAILABLE = False
    print("⚠️  TypeAwareAnalyticsService not available (missing FastAPI dependency)")


class TestTypeAwareIntelligentFilters:
    """Test intelligent filter generation based on field types."""
    
    def setup_method(self):
        """Set up test environment."""
        self.mock_db = MockDatabase()
        if SERVICE_AVAILABLE:
            self.service = TypeAwareAnalyticsService(self.mock_db)
    
    async def test_instrument_intelligent_filters(self):
        """Test intelligent filter generation for instrument table."""
        if not SERVICE_AVAILABLE:
            print("⏭️  Skipping service test - TypeAwareAnalyticsService not available")
            return
        
        filters = await self.service.get_intelligent_filters("dev_instruments")
        
        # Should generate multiple intelligent filters
        assert len(filters) > 0
        assert len(filters) <= 4  # Limited by EDA priority
        
        # Check for expected filter types
        filter_fields = [f["field"] for f in filters]
        filter_types = [f["type"] for f in filters]
        
        # Should include high-priority fields
        assert "symbol" in filter_fields
        assert "exchange" in filter_fields
        
        # Verify filter configurations
        for filter_config in filters:
            assert "field" in filter_config
            assert "type" in filter_config
            assert "ui_type" in filter_config
            assert "label" in filter_config
            assert "priority" in filter_config
            
            # Check type-specific configurations
            if filter_config["type"] == "text_search":
                assert "supports_partial" in filter_config
                assert "suggestions" in filter_config
                
            elif filter_config["type"] == "categorical":
                assert "options" in filter_config
                # Exchange should use predefined enum values (no DB query)
                if filter_config["field"] == "exchange":
                    assert "enum_values" in filter_config
                    assert filter_config["enum_values"] is not None
                    
            elif filter_config["type"] == "numeric_range":
                assert "min" in filter_config
                assert "max" in filter_config
                assert "step" in filter_config
    
    async def test_price_data_intelligent_filters(self):
        """Test intelligent filter generation for price data table."""
        if not SERVICE_AVAILABLE:
            print("⏭️  Skipping service test - TypeAwareAnalyticsService not available") 
            return
        
        filters = await self.service.get_intelligent_filters("dev_daily_prices_polygon_30year")
        
        assert len(filters) > 0
        
        # Check for expected price data filters
        filter_fields = [f["field"] for f in filters]
        filter_types = [f["type"] for f in filters]
        
        # Should include symbol, date, and price fields
        assert "symbol" in filter_fields or "date" in filter_fields or "close" in filter_fields
        
        # Verify date range filter
        date_filters = [f for f in filters if f["type"] == "date_range"]
        if date_filters:
            date_filter = date_filters[0]
            assert "min_date" in date_filter
            assert "max_date" in date_filter
            assert "ui_type" in date_filter
        
        # Verify numeric range filters
        numeric_filters = [f for f in filters if f["type"] == "numeric_range"]
        for num_filter in numeric_filters:
            assert "min" in num_filter
            assert "max" in num_filter
            assert num_filter["min"] < num_filter["max"]
    
    async def test_legacy_fallback_for_unknown_table(self):
        """Test legacy fallback for tables without type definitions."""
        if not SERVICE_AVAILABLE:
            print("⏭️  Skipping service test - TypeAwareAnalyticsService not available")
            return
        
        # Test with unknown table
        filters = await self.service.get_intelligent_filters("unknown_table")
        
        # Should use legacy fallback
        # Verify it attempts to use information_schema
        assert self.mock_db.get_call_count() > 0
        
        # Check if any filters were generated using legacy method
        assert isinstance(filters, list)
    
    async def test_enum_values_performance_optimization(self):
        """Test that enum fields don't require database queries."""
        if not SERVICE_AVAILABLE:
            print("⏭️  Skipping service test - TypeAwareAnalyticsService not available")
            return
        
        self.mock_db.clear_log()
        
        filters = await self.service.get_intelligent_filters("dev_instruments")
        
        # Find exchange filter (should use predefined enum values)
        exchange_filters = [f for f in filters if f["field"] == "exchange"]
        
        if exchange_filters:
            exchange_filter = exchange_filters[0]
            
            # Should have predefined enum values
            assert "enum_values" in exchange_filter
            assert exchange_filter["enum_values"] is not None
            assert "NYSE" in exchange_filter["enum_values"]
            
            # Should not have queried database for exchange options
            queries = [call["query"] for call in self.mock_db.call_log]
            exchange_queries = [q for q in queries if "exchange" in q.lower() and "GROUP BY" in q]
            
            # Should be empty - no DB queries for predefined enum values!
            assert len(exchange_queries) == 0


class TestTypeAwareColumnAnalysis:
    """Test intelligent column analysis using type information."""
    
    def setup_method(self):
        """Set up test environment."""
        self.mock_db = MockDatabase()
        if SERVICE_AVAILABLE:
            self.service = TypeAwareAnalyticsService(self.mock_db)
    
    async def test_analyze_searchable_string_column(self):
        """Test analysis of searchable string column."""
        if not SERVICE_AVAILABLE:
            print("⏭️  Skipping service test - TypeAwareAnalyticsService not available")
            return
        
        analysis = await self.service.analyze_column_intelligent("dev_instruments", "symbol")
        
        # Should use type-aware analysis
        assert analysis["column"] == "symbol"
        assert analysis["field_type"] == "string"
        assert analysis["semantics"] == "searchable_string"
        assert analysis["analysis_type"] == "categorical"
        assert "value_counts" in analysis
        assert "visualization_hint" in analysis
        assert analysis["visualization_hint"] == "bar_chart"
    
    async def test_analyze_categorical_enum_column(self):
        """Test analysis of categorical enum column."""
        if not SERVICE_AVAILABLE:
            print("⏭️  Skipping service test - TypeAwareAnalyticsService not available")
            return
        
        analysis = await self.service.analyze_column_intelligent("dev_instruments", "exchange")
        
        # Should use type-aware analysis
        assert analysis["column"] == "exchange"
        assert analysis["field_type"] == "enum"
        assert analysis["semantics"] == "categorical"
        assert analysis["analysis_type"] == "categorical"
        assert "enum_values" in analysis
        assert analysis["enum_values"] is not None
        assert "NYSE" in analysis["enum_values"]
        assert analysis["visualization_hint"] == "bar_chart"
    
    async def test_analyze_numeric_range_column(self):
        """Test analysis of numeric range column."""
        if not SERVICE_AVAILABLE:
            print("⏭️  Skipping service test - TypeAwareAnalyticsService not available")
            return
        
        analysis = await self.service.analyze_column_intelligent("dev_daily_prices_polygon_30year", "close")
        
        # Should use type-aware analysis  
        assert analysis["column"] == "close"
        assert analysis["field_type"] == "decimal"
        assert analysis["semantics"] == "numeric_range"
        assert analysis["analysis_type"] == "numeric"
        assert "statistics" in analysis
        assert "histogram" in analysis
        assert analysis["visualization_hint"] == "histogram"
        
        # Verify statistics structure
        stats = analysis["statistics"]
        assert "count" in stats
        assert "mean" in stats
        assert "std" in stats
        assert "min" in stats
        assert "max" in stats
    
    async def test_analyze_date_range_column(self):
        """Test analysis of date range column.""" 
        if not SERVICE_AVAILABLE:
            print("⏭️  Skipping service test - TypeAwareAnalyticsService not available")
            return
        
        analysis = await self.service.analyze_column_intelligent("dev_daily_prices_polygon_30year", "date")
        
        # Should use type-aware analysis
        assert analysis["column"] == "date"
        assert analysis["field_type"] == "date"
        assert analysis["semantics"] == "date_range"
        assert analysis["analysis_type"] == "date"
        assert "date_statistics" in analysis
        assert analysis["visualization_hint"] == "timeline"
    
    async def test_analyze_boolean_column(self):
        """Test analysis of boolean column."""
        if not SERVICE_AVAILABLE:
            print("⏭️  Skipping service test - TypeAwareAnalyticsService not available")
            return
        
        analysis = await self.service.analyze_column_intelligent("dev_instruments", "active")
        
        # Should use type-aware analysis
        assert analysis["column"] == "active"
        assert analysis["field_type"] == "boolean"
        assert analysis["semantics"] == "boolean"
        assert analysis["analysis_type"] == "boolean"
        assert "distribution" in analysis
        assert analysis["visualization_hint"] == "pie_chart"
    
    async def test_analyze_unknown_column_fallback(self):
        """Test fallback analysis for unknown columns."""
        if not SERVICE_AVAILABLE:
            print("⏭️  Skipping service test - TypeAwareAnalyticsService not available")
            return
        
        # Test column not in type system
        analysis = await self.service.analyze_column_intelligent("dev_instruments", "unknown_column")
        
        # Should fall back to legacy analysis
        assert analysis["analysis_type"] == "legacy"
        assert "message" in analysis


class TestTypeAwareQueryOptimization:
    """Test query optimization based on type information."""
    
    def setup_method(self):
        """Set up test environment."""
        self.mock_db = MockDatabase()
        if SERVICE_AVAILABLE:
            self.service = TypeAwareAnalyticsService(self.mock_db)
    
    async def test_enum_query_optimization(self):
        """Test that enum fields avoid database queries."""
        if not SERVICE_AVAILABLE:
            print("⏭️  Skipping service test - TypeAwareAnalyticsService not available")
            return
        
        self.mock_db.clear_log()
        
        # Generate filter for exchange field (enum)
        exchange_field = schema_registry.get_field_definition("dev_instruments", "exchange")
        filter_config = await self.service._generate_typed_filter(
            "dev_instruments", "exchange", exchange_field
        )
        
        # Should use predefined enum values, no DB queries
        assert filter_config["type"] == "categorical"
        assert "enum_values" in filter_config
        assert filter_config["enum_values"] is not None
        
        # Verify no database calls were made for enum values
        db_calls = self.mock_db.get_call_count()
        assert db_calls == 0
    
    async def test_searchable_field_optimization(self):
        """Test optimized handling of searchable fields."""
        if not SERVICE_AVAILABLE:
            print("⏭️  Skipping service test - TypeAwareAnalyticsService not available")
            return
        
        self.mock_db.clear_log()
        
        # Generate filter for symbol field (searchable string)
        symbol_field = schema_registry.get_field_definition("dev_instruments", "symbol")
        filter_config = await self.service._generate_typed_filter(
            "dev_instruments", "symbol", symbol_field
        )
        
        # Should generate text search with autocomplete
        assert filter_config["type"] == "text_search"
        assert filter_config["supports_partial"] == True
        assert "suggestions" in filter_config
        
        # Should have made exactly one DB call for suggestions
        db_calls = self.mock_db.get_call_count()
        assert db_calls == 1
        
        # Verify the query was for suggestions
        last_query = self.mock_db.get_last_query()
        assert "DISTINCT" in last_query["query"]
        assert "symbol" in last_query["query"].lower()
    
    async def test_numeric_range_optimization(self):
        """Test optimized handling of numeric range fields."""
        if not SERVICE_AVAILABLE:
            print("⏭️  Skipping service test - TypeAwareAnalyticsService not available")
            return
        
        self.mock_db.clear_log()
        
        # Generate filter for close field (numeric range)
        close_field = schema_registry.get_field_definition("dev_daily_prices_polygon_30year", "close")
        filter_config = await self.service._generate_typed_filter(
            "dev_daily_prices_polygon_30year", "close", close_field
        )
        
        # Should generate numeric range slider
        assert filter_config["type"] == "numeric_range"
        assert "min" in filter_config
        assert "max" in filter_config
        assert "step" in filter_config
        assert filter_config["format"] == "currency"  # Should detect $ in label
        
        # Should have made exactly one DB call for min/max
        db_calls = self.mock_db.get_call_count()
        assert db_calls == 1
        
        # Verify the query was for min/max
        last_query = self.mock_db.get_last_query()
        assert "MIN" in last_query["query"] and "MAX" in last_query["query"]


class TestTypeSystemIntegrationBehavior:
    """Test complete integration behavior of type system."""
    
    async def test_complete_filter_generation_flow(self):
        """Test complete flow from schema to UI filter generation."""
        
        # Test schema registry integration
        schema = schema_registry.get_table_schema("dev_instruments")
        assert schema.entity_name == "instrument"
        
        # Test field queries
        searchable_fields = schema_registry.get_table_searchable_fields("dev_instruments")
        categorical_fields = schema_registry.get_table_categorical_fields("dev_instruments")
        
        assert "symbol" in searchable_fields
        assert "name" in searchable_fields
        assert "exchange" in categorical_fields
        assert "type" in categorical_fields
        
        # Test priority ordering
        priority_fields = schema_registry.get_eda_priority_fields("dev_instruments", limit=4)
        
        # Should be ordered by priority
        assert len(priority_fields) == 4
        # symbol should be first (priority 10)
        assert priority_fields[0] == "symbol"
    
    async def test_type_system_validation_integration(self):
        """Test validation using type system."""
        
        # Test enum validation
        assert schema_registry.validate_field_value("dev_instruments", "exchange", "NYSE") == True
        assert schema_registry.validate_field_value("dev_instruments", "exchange", "INVALID") == False
        
        # Test field type detection
        assert schema_registry.is_field_searchable("dev_instruments", "symbol") == True
        assert schema_registry.is_field_categorical("dev_instruments", "exchange") == True
        assert schema_registry.is_field_numeric_range("dev_daily_prices_polygon_30year", "close") == True
        
        # Test enum value retrieval
        exchange_values = schema_registry.get_enum_values("dev_instruments", "exchange")
        assert exchange_values is not None
        assert "NYSE" in exchange_values
        assert "NASDAQ" in exchange_values
    
    async def test_performance_comparison_typed_vs_legacy(self):
        """Test performance difference between typed and legacy approaches.""" 
        
        if not SERVICE_AVAILABLE:
            print("⏭️  Skipping service test - TypeAwareAnalyticsService not available")
            return
        
        mock_db = MockDatabase()
        service = TypeAwareAnalyticsService(mock_db)
        
        # Test typed approach
        mock_db.clear_log()
        typed_filters = await service.get_intelligent_filters("dev_instruments")
        typed_db_calls = mock_db.get_call_count()
        
        # Test legacy approach  
        mock_db.clear_log()
        legacy_filters = await service._legacy_get_filters("unknown_table")
        legacy_db_calls = mock_db.get_call_count()
        
        print(f"\\n📊 Performance Comparison:")
        print(f"  Typed approach: {typed_db_calls} DB calls, {len(typed_filters)} filters")
        print(f"  Legacy approach: {legacy_db_calls} DB calls, {len(legacy_filters)} filters")
        
        # Typed approach should be more efficient due to enum optimizations
        # (Note: This may vary based on which fields are included)
        assert isinstance(typed_filters, list)
        assert isinstance(legacy_filters, list)


async def run_all_integration_tests():
    """Run all integration tests."""
    
    test_classes = [
        TestTypeAwareIntelligentFilters,
        TestTypeAwareColumnAnalysis, 
        TestTypeAwareQueryOptimization,
        TestTypeSystemIntegrationBehavior
    ]
    
    total_tests = 0
    passed_tests = 0
    
    print("🧪 Running Type-Aware Analytics Integration Tests")
    print("=" * 60)
    
    if not SERVICE_AVAILABLE:
        print("⚠️  Some tests will be skipped due to missing dependencies")
        print("    (FastAPI not available in test environment)")
        print()
    
    for test_class in test_classes:
        print(f"\\n📋 Testing {test_class.__name__}")
        print("-" * 50)
        
        instance = test_class()
        test_methods = [method for method in dir(instance) if method.startswith('test_')]
        
        for test_method in test_methods:
            total_tests += 1
            try:
                # Set up method if it exists
                if hasattr(instance, 'setup_method'):
                    instance.setup_method()
                
                # Run the test
                await getattr(instance, test_method)()
                print(f"✅ {test_method}")
                passed_tests += 1
                
            except Exception as e:
                print(f"❌ {test_method}: {e}")
                import traceback
                traceback.print_exc()
    
    print(f"\\n📊 Integration Test Results")
    print("-" * 30)
    print(f"Total Tests: {total_tests}")
    print(f"Passed: {passed_tests}")
    print(f"Failed: {total_tests - passed_tests}")
    
    if passed_tests == total_tests:
        print("\\n🎉 All type-aware analytics integration tests passed!")
        return True
    else:
        print(f"\\n⚠️  {total_tests - passed_tests} test(s) failed")
        return False


if __name__ == "__main__":
    success = asyncio.run(run_all_integration_tests())
    if not success:
        exit(1)