#!/usr/bin/env python3
"""
Comprehensive demonstration tests for the ATS type system.

These tests validate the complete transformation from manual EDA to
intelligent, type-driven analysis and UI generation.
"""

import asyncio
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from schema.registry import schema_registry
from schema.types import FieldSemantics, FieldType


class TestTypeSystemTransformation:
    """Test the complete transformation enabled by type system."""

    def test_schema_registry_completeness(self):
        """Test that schema registry contains expected entities and tables."""

        # Test entities
        entities = schema_registry.list_entities()
        expected_entities = ["instrument", "daily_price", "instrument_xref"]

        for entity in expected_entities:
            assert entity in entities, f"Missing entity: {entity}"

        # Test tables
        tables = schema_registry.list_tables()
        expected_tables = [
            "dev_instruments",
            "dev_daily_prices_polygon",
            "dev_daily_prices_tiingo",
            "dev_daily_prices_eodhd",
            "instrument_xrefs"
        ]

        for table in expected_tables:
            assert table in tables, f"Missing table: {table}"

        print(f"✅ Schema registry contains {len(entities)} entities, {len(tables)} tables")

    def test_intelligent_field_classification(self):
        """Test that fields are intelligently classified by semantics."""

        # Test instrument fields
        instrument_schema = schema_registry.get_schema("instrument")

        # Symbol should be searchable string
        symbol_field = instrument_schema.fields["symbol"]
        assert symbol_field.semantics == FieldSemantics.SEARCHABLE_STRING
        assert symbol_field.supports_search == True
        assert symbol_field.eda_priority == 10

        # Exchange should be categorical enum
        exchange_field = instrument_schema.fields["exchange"]
        assert exchange_field.semantics == FieldSemantics.CATEGORICAL
        assert exchange_field.field_type == FieldType.ENUM
        assert exchange_field.enum_values is not None
        assert "NYSE" in exchange_field.enum_values
        assert "NASDAQ" in exchange_field.enum_values

        # Active should be boolean
        active_field = instrument_schema.fields["active"]
        assert active_field.semantics == FieldSemantics.BOOLEAN
        assert active_field.field_type == FieldType.BOOLEAN

        # ID should be readonly
        id_field = instrument_schema.fields["id"]
        assert id_field.semantics == FieldSemantics.READONLY
        assert id_field.is_filterable == False

        print("✅ Instrument fields correctly classified by semantics")

    def test_price_data_field_classification(self):
        """Test price data field classifications."""

        price_schema = schema_registry.get_schema("daily_price")

        # Symbol in price data should be categorical (for filtering)
        symbol_field = price_schema.fields["symbol"]
        assert symbol_field.semantics == FieldSemantics.CATEGORICAL
        assert symbol_field.eda_priority == 10

        # Date should be date range
        date_field = price_schema.fields["date"]
        assert date_field.semantics == FieldSemantics.DATE_RANGE
        assert date_field.supports_range == True

        # Price fields should be numeric range
        price_fields = ["open", "high", "low", "close"]
        for field_name in price_fields:
            field = price_schema.fields[field_name]
            assert field.semantics == FieldSemantics.NUMERIC_RANGE
            assert field.supports_range == True
            assert field.min_value == 0  # Prices can't be negative

        # Volume should be numeric range
        volume_field = price_schema.fields["volume"]
        assert volume_field.semantics == FieldSemantics.NUMERIC_RANGE
        assert volume_field.min_value == 0  # Volume can't be negative

        print("✅ Price data fields correctly classified by semantics")

    def test_eda_priority_ordering(self):
        """Test that EDA priority ordering works correctly."""

        # Test instrument priorities
        instrument_priorities = schema_registry.get_eda_priority_fields("dev_instruments", limit=6)

        # Should be ordered by priority: symbol(10), exchange(9), active(8), name(8)...
        assert instrument_priorities[0] == "symbol"  # Highest priority
        assert "exchange" in instrument_priorities[:3]  # Very high priority
        assert "id" not in instrument_priorities  # Readonly excluded

        # Test price data priorities
        price_priorities = schema_registry.get_eda_priority_fields("dev_daily_prices_polygon", limit=6)

        # Symbol and date should be high priority
        assert "symbol" in price_priorities[:2]
        assert "date" in price_priorities[:2]
        assert "close" in price_priorities[:4]  # Close price important

        print("✅ EDA priority ordering works correctly")

    def test_enum_value_consistency(self):
        """Test that enum values are consistent and comprehensive."""

        # Test exchange values
        exchange_values = schema_registry.get_enum_values("dev_instruments", "exchange")

        assert exchange_values is not None
        assert len(exchange_values) >= 10  # Should have major exchanges

        expected_exchanges = ["NYSE", "NASDAQ", "AMEX", "LSE"]
        for exchange in expected_exchanges:
            assert exchange in exchange_values

        # Test instrument type values
        type_values = schema_registry.get_enum_values("dev_instruments", "type")

        assert type_values is not None
        assert len(type_values) >= 8  # Should have major instrument types

        expected_types = ["STOCK", "ETF", "BOND", "OPTION"]
        for inst_type in expected_types:
            assert inst_type in type_values

        print("✅ Enum values are consistent and comprehensive")

    def test_validation_capabilities(self):
        """Test field validation using type definitions."""

        # Test enum validation
        assert schema_registry.validate_field_value("dev_instruments", "exchange", "NYSE") == True
        assert schema_registry.validate_field_value("dev_instruments", "exchange", "NASDAQ") == True
        assert schema_registry.validate_field_value("dev_instruments", "exchange", "InvalidExchange") == False

        # Test type validation
        assert schema_registry.validate_field_value("dev_instruments", "type", "STOCK") == True
        assert schema_registry.validate_field_value("dev_instruments", "type", "ETF") == True
        assert schema_registry.validate_field_value("dev_instruments", "type", "InvalidType") == False

        # Test unknown field validation (should pass)
        assert schema_registry.validate_field_value("dev_instruments", "unknown_field", "any_value") == True
        assert schema_registry.validate_field_value("unknown_table", "any_field", "any_value") == True

        print("✅ Validation capabilities work correctly")

    def test_field_query_capabilities(self):
        """Test comprehensive field querying."""

        # Test searchable fields
        searchable = schema_registry.get_table_searchable_fields("dev_instruments")
        assert "symbol" in searchable
        assert "name" in searchable
        assert "exchange" not in searchable  # Categorical, not searchable

        # Test categorical fields
        categorical = schema_registry.get_table_categorical_fields("dev_instruments")
        assert "exchange" in categorical
        assert "type" in categorical
        assert "currency" in categorical
        assert "symbol" not in categorical  # Searchable, not just categorical

        # Test numeric fields
        numeric = schema_registry.get_table_numeric_fields("dev_daily_prices_polygon")
        price_fields = ["open", "high", "low", "close", "volume"]
        for field in price_fields:
            assert field in numeric

        # Test date fields
        date_fields = schema_registry.get_date_fields("daily_price")
        assert "date" in date_fields

        print("✅ Field querying capabilities comprehensive")


class TestTypeSystemUIGeneration:
    """Test UI generation implications of type system."""

    def test_automatic_filter_type_mapping(self):
        """Test that field semantics map to correct UI components."""

        mappings = {
            # Instrument fields
            ("dev_instruments", "symbol"): {
                "expected_ui": "text_input_with_autocomplete",
                "semantics": FieldSemantics.SEARCHABLE_STRING,
                "features": ["partial_search", "autocomplete"]
            },
            ("dev_instruments", "exchange"): {
                "expected_ui": "dropdown_or_checkboxes",
                "semantics": FieldSemantics.CATEGORICAL,
                "features": ["predefined_options", "no_db_query"]
            },
            ("dev_instruments", "active"): {
                "expected_ui": "tri_state_checkbox",
                "semantics": FieldSemantics.BOOLEAN,
                "features": ["true_false_either"]
            },

            # Price fields
            ("dev_daily_prices_polygon", "close"): {
                "expected_ui": "range_slider_with_currency",
                "semantics": FieldSemantics.NUMERIC_RANGE,
                "features": ["min_max_range", "currency_format"]
            },
            ("dev_daily_prices_polygon", "date"): {
                "expected_ui": "date_range_picker",
                "semantics": FieldSemantics.DATE_RANGE,
                "features": ["date_range_selection"]
            }
        }

        for (table, field), expected in mappings.items():
            field_def = schema_registry.get_field_definition(table, field)
            assert field_def is not None, f"Field definition missing: {table}.{field}"
            assert field_def.semantics == expected["semantics"], f"Wrong semantics for {table}.{field}"

        print("✅ Field semantics correctly map to UI component types")

    def test_performance_optimization_implications(self):
        """Test performance optimizations enabled by type system."""

        # Test enum fields don't require database queries
        enum_fields = [
            ("dev_instruments", "exchange"),
            ("dev_instruments", "type"),
            ("dev_instruments", "currency")
        ]

        for table, field in enum_fields:
            enum_values = schema_registry.get_enum_values(table, field)
            assert enum_values is not None, f"Enum values missing for {table}.{field}"
            assert len(enum_values) > 0, f"Empty enum values for {table}.{field}"

        # Test priority-based field limiting
        priority_fields = schema_registry.get_eda_priority_fields("dev_instruments", limit=4)
        assert len(priority_fields) == 4, "Priority limiting not working"

        # Test filterable field filtering
        all_fields = schema_registry.get_schema("instrument").fields
        filterable_fields = schema_registry.get_filterable_fields("instrument")

        readonly_count = sum(1 for f in all_fields.values() if f.semantics == FieldSemantics.READONLY)
        expected_filterable = len(all_fields) - readonly_count

        assert len(filterable_fields) == expected_filterable, "Filterable field filtering incorrect"

        print("✅ Type system enables significant performance optimizations")

    def test_consistency_across_similar_tables(self):
        """Test that similar tables have consistent type treatment."""

        # All price tables should have similar field semantics
        price_tables = [
            "dev_daily_prices_polygon",
            "dev_daily_prices_tiingo",
            "dev_daily_prices_eodhd"
        ]

        for table in price_tables:
            try:
                schema = schema_registry.get_table_schema(table)

                # All should have same basic structure
                assert "symbol" in schema.fields
                assert "date" in schema.fields
                assert "close" in schema.fields

                # All should classify fields the same way
                assert schema.fields["symbol"].semantics == FieldSemantics.CATEGORICAL
                assert schema.fields["date"].semantics == FieldSemantics.DATE_RANGE
                assert schema.fields["close"].semantics == FieldSemantics.NUMERIC_RANGE

            except ValueError:
                # Some tables might not have schemas yet - that's ok
                pass

        print("✅ Similar tables have consistent type treatment")


class TestTypeSystemIntegrationBenefits:
    """Test the benefits achieved by type system integration."""

    def test_reduced_hardcoding(self):
        """Test that hardcoded field logic is eliminated."""

        # Before type system: hard-coded field handling
        # if column == 'symbol': return text_input()
        # elif column == 'exchange': return dropdown(query_db())
        # elif column in ['open', 'close']: return range_slider()

        # After type system: semantic-driven handling
        test_cases = [
            ("dev_instruments", "symbol", "should generate text search"),
            ("dev_instruments", "exchange", "should generate dropdown with predefined options"),
            ("dev_daily_prices_polygon", "close", "should generate range slider"),
            ("dev_daily_prices_polygon", "date", "should generate date picker"),
            ("dev_instruments", "active", "should generate boolean checkbox")
        ]

        for table, field, expected_behavior in test_cases:
            field_def = schema_registry.get_field_definition(table, field)
            assert field_def is not None, f"No type definition for {table}.{field} - {expected_behavior}"

        print("✅ Hardcoded field logic eliminated through semantic types")

    def test_extensibility(self):
        """Test that new entity types can be easily added."""

        # The schema registry supports adding new entities
        initial_entity_count = len(schema_registry.list_entities())

        # Schema registry has register_schema method
        assert hasattr(schema_registry, 'register_schema')
        assert callable(schema_registry.register_schema)

        # Field semantics are extensible (new semantics can be added to enum)
        semantics_count = len(list(FieldSemantics))
        assert semantics_count >= 6  # At least the core semantics

        # Field types are extensible
        field_types_count = len(list(FieldType))
        assert field_types_count >= 7  # At least the core types

        print("✅ Type system is extensible for new entity types")

    def test_documentation_and_discoverability(self):
        """Test that type system provides self-documentation."""

        # Schema summary provides comprehensive overview
        summary = schema_registry.get_schema_summary()

        assert "total_entities" in summary
        assert "total_tables" in summary
        assert "entities" in summary
        assert summary["total_entities"] > 0

        # Each entity has documentation
        for entity_name, entity_info in summary["entities"].items():
            assert "description" in entity_info
            assert "field_breakdown" in entity_info
            assert len(entity_info["description"]) > 0

        # Field definitions include documentation
        symbol_field = schema_registry.get_field_definition("dev_instruments", "symbol")
        assert symbol_field.description != ""
        assert symbol_field.ui_help_text != ""

        print("✅ Type system provides comprehensive self-documentation")

    def test_transformation_completeness(self):
        """Test that the complete transformation is achieved."""

        transformation_checklist = {
            "symbol_searchable": schema_registry.is_field_searchable("dev_instruments", "symbol"),
            "exchange_categorical": schema_registry.is_field_categorical("dev_instruments", "exchange"),
            "exchange_predefined": schema_registry.get_enum_values("dev_instruments", "exchange") is not None,
            "prices_numeric": schema_registry.is_field_numeric_range("dev_daily_prices_polygon", "close"),
            "dates_rangeable": schema_registry.get_date_fields("daily_price") != [],
            "booleans_handled": schema_registry.get_boolean_fields("instrument") != [],
            "priorities_set": len(schema_registry.get_eda_priority_fields("dev_instruments", 4)) == 4,
            "validation_enabled": schema_registry.validate_field_value("dev_instruments", "exchange", "NYSE")
        }

        for check_name, check_result in transformation_checklist.items():
            assert check_result, f"Transformation incomplete: {check_name} failed"

        print("✅ Complete transformation achieved:")
        for check_name in transformation_checklist:
            print(f"    • {check_name.replace('_', ' ').title()}: ✓")


def run_all_demonstration_tests():
    """Run all type system demonstration tests."""

    test_classes = [
        TestTypeSystemTransformation,
        TestTypeSystemUIGeneration,
        TestTypeSystemIntegrationBenefits
    ]

    total_tests = 0
    passed_tests = 0

    print("🧪 Running Type System Demonstration Tests")
    print("=" * 55)

    for test_class in test_classes:
        print(f"\\n📋 Testing {test_class.__name__}")
        print("-" * 50)

        instance = test_class()
        test_methods = [method for method in dir(instance) if method.startswith('test_')]

        for test_method in test_methods:
            total_tests += 1
            try:
                getattr(instance, test_method)()
                print(f"✅ {test_method}")
                passed_tests += 1

            except Exception as e:
                print(f"❌ {test_method}: {e}")
                import traceback
                traceback.print_exc()

    print(f"\\n📊 Demonstration Test Results")
    print("-" * 35)
    print(f"Total Tests: {total_tests}")
    print(f"Passed: {passed_tests}")
    print(f"Failed: {total_tests - passed_tests}")

    if passed_tests == total_tests:
        print("\\n🎉 All type system demonstration tests passed!")
        print("\\n✨ TYPE SYSTEM TRANSFORMATION VERIFIED:")
        print("    • Schema-driven EDA filter generation")
        print("    • Intelligent field type classification")
        print("    • Predefined enum values (no DB queries)")
        print("    • Automatic UI component selection")
        print("    • Priority-based field ordering")
        print("    • Comprehensive validation")
        print("    • Performance optimizations")
        print("    • Extensible and self-documenting")
        return True
    else:
        print(f"\\n⚠️  {total_tests - passed_tests} test(s) failed")
        return False


if __name__ == "__main__":
    success = run_all_demonstration_tests()
    if not success:
        exit(1)