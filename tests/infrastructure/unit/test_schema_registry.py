#!/usr/bin/env python3
"""
Comprehensive unit tests for schema registry functionality.

Tests schema registration, querying, validation, and all registry
operations including field lookup and entity management.
"""

import pytest
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from domains.ml.schema.registry import SchemaRegistry, schema_registry
from domains.ml.schema.types import FieldType, FieldSemantics, FieldDefinition, EntitySchema
from domains.ml.schema.entities import INSTRUMENT_SCHEMA, PRICE_SCHEMA, INSTRUMENT_XREF_SCHEMA


class TestSchemaRegistry:
    """Test schema registry core functionality."""

    def create_test_registry(self):
        """Create a fresh registry for testing."""
        registry = SchemaRegistry()

        # Add a simple test schema
        test_schema = EntitySchema(
            entity_name="test_entity",
            table_name="test_table",
            description="Test schema for unit tests",
            fields={
                "id": FieldDefinition(
                    name="id",
                    field_type=FieldType.INTEGER,
                    semantics=FieldSemantics.READONLY,
                    nullable=False,
                    eda_priority=0
                ),
                "search_field": FieldDefinition(
                    name="search_field",
                    field_type=FieldType.STRING,
                    semantics=FieldSemantics.SEARCHABLE_STRING,
                    nullable=False,
                    eda_priority=8
                ),
                "category_field": FieldDefinition(
                    name="category_field",
                    field_type=FieldType.ENUM,
                    semantics=FieldSemantics.CATEGORICAL,
                    enum_values=["A", "B", "C"],
                    eda_priority=6
                ),
                "numeric_field": FieldDefinition(
                    name="numeric_field",
                    field_type=FieldType.DECIMAL,
                    semantics=FieldSemantics.NUMERIC_RANGE,
                    eda_priority=7
                ),
                "date_field": FieldDefinition(
                    name="date_field",
                    field_type=FieldType.DATE,
                    semantics=FieldSemantics.DATE_RANGE,
                    eda_priority=5
                ),
                "bool_field": FieldDefinition(
                    name="bool_field",
                    field_type=FieldType.BOOLEAN,
                    semantics=FieldSemantics.BOOLEAN,
                    eda_priority=4
                )
            }
        )

        registry.register_schema(test_schema)
        return registry

    def test_registry_initialization(self):
        """Test registry initializes with default schemas."""
        registry = SchemaRegistry()

        # Should have default schemas
        entities = registry.list_entities()
        assert "instrument" in entities
        assert "daily_price" in entities
        assert "instrument_xref" in entities

        tables = registry.list_tables()
        assert "dev_instrument" in tables
        assert "dev_daily_price_polygon" in tables
        assert "instrument_xrefs" in tables

    def test_schema_registration(self):
        """Test registering new schemas."""
        registry = SchemaRegistry()
        initial_count = len(registry.list_entities())

        test_schema = EntitySchema(
            entity_name="new_entity",
            table_name="new_table",
            description="New test entity",
            fields={
                "id": FieldDefinition(
                    name="id",
                    field_type=FieldType.INTEGER,
                    semantics=FieldSemantics.READONLY,
                    nullable=False
                )
            }
        )

        registry.register_schema(test_schema)

        # Should have one more entity
        assert len(registry.list_entities()) == initial_count + 1
        assert "new_entity" in registry.list_entities()
        assert "new_table" in registry.list_tables()

    def test_get_schema_by_entity_name(self):
        """Test retrieving schema by entity name."""
        registry = self.create_test_registry()

        schema = registry.get_schema("test_entity")
        assert schema.entity_name == "test_entity"
        assert schema.table_name == "test_table"
        assert len(schema.fields) == 6

        # Test unknown entity
        with pytest.raises(ValueError, match="Unknown entity: nonexistent"):
            registry.get_schema("nonexistent")

    def test_get_schema_by_table_name(self):
        """Test retrieving schema by table name."""
        registry = self.create_test_registry()

        schema = registry.get_table_schema("test_table")
        assert schema.entity_name == "test_entity"
        assert schema.table_name == "test_table"

        # Test unknown table
        with pytest.raises(ValueError, match="Unknown table: nonexistent"):
            registry.get_table_schema("nonexistent")

    def test_has_schema_checks(self):
        """Test schema existence checks."""
        registry = self.create_test_registry()

        # Entity checks
        assert registry.has_schema("test_entity") == True
        assert registry.has_schema("nonexistent") == False

        # Table checks
        assert registry.has_table_schema("test_table") == True
        assert registry.has_table_schema("nonexistent") == False


class TestSchemaRegistryFieldQueries:
    """Test field querying capabilities of schema registry."""

    def create_test_registry(self):
        """Create registry with test schema for field queries."""
        registry = SchemaRegistry()

        test_schema = EntitySchema(
            entity_name="field_test",
            table_name="field_test_table",
            description="Schema for field query testing",
            fields={
                "readonly_field": FieldDefinition(
                    name="readonly_field",
                    field_type=FieldType.INTEGER,
                    semantics=FieldSemantics.READONLY
                ),
                "search_field1": FieldDefinition(
                    name="search_field1",
                    field_type=FieldType.STRING,
                    semantics=FieldSemantics.SEARCHABLE_STRING,
                    eda_priority=8
                ),
                "search_field2": FieldDefinition(
                    name="search_field2",
                    field_type=FieldType.STRING,
                    semantics=FieldSemantics.SEARCHABLE_STRING,
                    eda_priority=6
                ),
                "category_field1": FieldDefinition(
                    name="category_field1",
                    field_type=FieldType.ENUM,
                    semantics=FieldSemantics.CATEGORICAL,
                    enum_values=["X", "Y", "Z"],
                    eda_priority=7
                ),
                "category_field2": FieldDefinition(
                    name="category_field2",
                    field_type=FieldType.STRING,
                    semantics=FieldSemantics.CATEGORICAL,
                    eda_priority=5
                ),
                "numeric_field1": FieldDefinition(
                    name="numeric_field1",
                    field_type=FieldType.DECIMAL,
                    semantics=FieldSemantics.NUMERIC_RANGE,
                    eda_priority=9
                ),
                "numeric_field2": FieldDefinition(
                    name="numeric_field2",
                    field_type=FieldType.INTEGER,
                    semantics=FieldSemantics.NUMERIC_RANGE,
                    eda_priority=4
                ),
                "date_field": FieldDefinition(
                    name="date_field",
                    field_type=FieldType.DATE,
                    semantics=FieldSemantics.DATE_RANGE,
                    eda_priority=3
                ),
                "bool_field": FieldDefinition(
                    name="bool_field",
                    field_type=FieldType.BOOLEAN,
                    semantics=FieldSemantics.BOOLEAN,
                    eda_priority=2
                )
            }
        )

        registry.register_schema(test_schema)
        return registry

    def test_get_searchable_fields(self):
        """Test getting searchable fields."""
        registry = self.create_test_registry()

        searchable = registry.get_searchable_fields("field_test")
        assert len(searchable) == 2
        assert "search_field1" in searchable
        assert "search_field2" in searchable

        # Test by table name
        searchable_table = registry.get_table_searchable_fields("field_test_table")
        assert searchable == searchable_table

    def test_get_categorical_fields(self):
        """Test getting categorical fields."""
        registry = self.create_test_registry()

        categorical = registry.get_categorical_fields("field_test")
        assert len(categorical) == 2
        assert "category_field1" in categorical
        assert "category_field2" in categorical

        # Test by table name
        categorical_table = registry.get_table_categorical_fields("field_test_table")
        assert categorical == categorical_table

    def test_get_numeric_fields(self):
        """Test getting numeric range fields."""
        registry = self.create_test_registry()

        numeric = registry.get_numeric_fields("field_test")
        assert len(numeric) == 2
        assert "numeric_field1" in numeric
        assert "numeric_field2" in numeric

        # Test by table name
        numeric_table = registry.get_table_numeric_fields("field_test_table")
        assert numeric == numeric_table

    def test_get_date_fields(self):
        """Test getting date range fields."""
        registry = self.create_test_registry()

        date_fields = registry.get_date_fields("field_test")
        assert len(date_fields) == 1
        assert "date_field" in date_fields

    def test_get_boolean_fields(self):
        """Test getting boolean fields."""
        registry = self.create_test_registry()

        boolean_fields = registry.get_boolean_fields("field_test")
        assert len(boolean_fields) == 1
        assert "bool_field" in boolean_fields

    def test_get_filterable_fields(self):
        """Test getting all filterable fields."""
        registry = self.create_test_registry()

        filterable = registry.get_filterable_fields("field_test")

        # Should exclude readonly field
        assert len(filterable) == 8  # All except readonly_field
        assert "readonly_field" not in filterable
        assert "search_field1" in filterable

        # Test by table name
        filterable_table = registry.get_table_filterable_fields("field_test_table")
        assert len(filterable) == len(filterable_table)

    def test_get_eda_priority_fields(self):
        """Test getting fields by EDA priority."""
        registry = self.create_test_registry()

        # Get top 3 priority fields
        priority_fields = registry.get_eda_priority_fields("field_test_table", limit=3)

        # Should be ordered by priority: numeric_field1(9), search_field1(8), category_field1(7)
        assert len(priority_fields) == 3
        assert priority_fields[0] == "numeric_field1"  # Priority 9
        assert priority_fields[1] == "search_field1"   # Priority 8
        assert priority_fields[2] == "category_field1" # Priority 7

        # Get more than available filterable fields
        all_priority = registry.get_eda_priority_fields("field_test_table", limit=20)
        assert len(all_priority) == 8  # All filterable fields
        assert "readonly_field" not in all_priority


class TestSchemaRegistryFieldDetails:
    """Test detailed field information retrieval."""

    def test_get_field_definition(self):
        """Test getting individual field definitions."""
        registry = schema_registry  # Use global registry

        # Test instrument schema fields
        symbol_field = registry.get_field_definition("dev_instrument", "symbol")
        assert symbol_field is not None
        assert symbol_field.name == "symbol"
        assert symbol_field.semantics == FieldSemantics.SEARCHABLE_STRING
        assert symbol_field.eda_priority == 10

        exchange_field = registry.get_field_definition("dev_instrument", "exchange")
        assert exchange_field is not None
        assert exchange_field.semantics == FieldSemantics.CATEGORICAL
        assert exchange_field.enum_values is not None
        assert "NYSE" in exchange_field.enum_values

        # Test nonexistent field
        nonexistent = registry.get_field_definition("dev_instrument", "nonexistent")
        assert nonexistent is None

        # Test nonexistent table
        nonexistent_table = registry.get_field_definition("nonexistent", "symbol")
        assert nonexistent_table is None

    def test_field_type_checks(self):
        """Test field type checking methods."""
        registry = schema_registry

        # Test searchable field
        assert registry.is_field_searchable("dev_instrument", "symbol") == True
        assert registry.is_field_searchable("dev_instrument", "exchange") == False

        # Test categorical field
        assert registry.is_field_categorical("dev_instrument", "exchange") == True
        assert registry.is_field_categorical("dev_instrument", "symbol") == False

        # Test numeric range field
        assert registry.is_field_numeric_range("dev_daily_price_polygon", "close") == True
        assert registry.is_field_numeric_range("dev_instrument", "symbol") == False

        # Test nonexistent fields
        assert registry.is_field_searchable("dev_instrument", "nonexistent") == False
        assert registry.is_field_categorical("nonexistent", "symbol") == False

    def test_get_enum_values(self):
        """Test getting enum values for fields."""
        registry = schema_registry

        # Test field with enum values
        exchange_values = registry.get_enum_values("dev_instrument", "exchange")
        assert exchange_values is not None
        assert isinstance(exchange_values, list)
        assert "NYSE" in exchange_values
        assert "NASDAQ" in exchange_values

        # Test field without enum values
        symbol_values = registry.get_enum_values("dev_instrument", "symbol")
        assert symbol_values is None

        # Test nonexistent field
        nonexistent_values = registry.get_enum_values("dev_instrument", "nonexistent")
        assert nonexistent_values is None


class TestSchemaRegistryValidation:
    """Test schema registry validation capabilities."""

    def test_validate_field_value_enum(self):
        """Test enum field value validation."""
        registry = schema_registry

        # Test valid enum values
        assert registry.validate_field_value("dev_instrument", "exchange", "NYSE") == True
        assert registry.validate_field_value("dev_instrument", "exchange", "NASDAQ") == True

        # Test invalid enum values
        assert registry.validate_field_value("dev_instrument", "exchange", "INVALID") == False
        assert registry.validate_field_value("dev_instrument", "exchange", "nyse") == False  # Case sensitive

        # Test None for nullable enum
        exchange_field = registry.get_field_definition("dev_instrument", "exchange")
        if exchange_field and exchange_field.nullable:
            assert registry.validate_field_value("dev_instrument", "exchange", None) == True

    def test_validate_field_value_string_length(self):
        """Test string length validation."""
        registry = schema_registry

        # Test symbol field with max_length
        symbol_field = registry.get_field_definition("dev_instrument", "symbol")
        if symbol_field and symbol_field.max_length:
            # Valid length
            assert registry.validate_field_value("dev_instrument", "symbol", "AAPL") == True

            # Too long
            long_symbol = "A" * (symbol_field.max_length + 1)
            assert registry.validate_field_value("dev_instrument", "symbol", long_symbol) == False

    def test_validate_field_value_numeric_range(self):
        """Test numeric range validation."""
        # Create a test registry with numeric constraints
        registry = SchemaRegistry()

        test_schema = EntitySchema(
            entity_name="validation_test",
            table_name="validation_test_table",
            description="Schema for validation testing",
            fields={
                "constrained_price": FieldDefinition(
                    name="constrained_price",
                    field_type=FieldType.DECIMAL,
                    semantics=FieldSemantics.NUMERIC_RANGE,
                    min_value=0,
                    max_value=1000
                )
            }
        )
        registry.register_schema(test_schema)

        # Test valid values
        assert registry.validate_field_value("validation_test_table", "constrained_price", 50.5) == True
        assert registry.validate_field_value("validation_test_table", "constrained_price", 0) == True
        assert registry.validate_field_value("validation_test_table", "constrained_price", 1000) == True

        # Test invalid values
        assert registry.validate_field_value("validation_test_table", "constrained_price", -1) == False
        assert registry.validate_field_value("validation_test_table", "constrained_price", 1001) == False

        # Test non-numeric values
        assert registry.validate_field_value("validation_test_table", "constrained_price", "invalid") == False

    def test_validate_field_value_nullable(self):
        """Test nullable field validation."""
        # Create test registry with nullable/non-nullable fields
        registry = SchemaRegistry()

        test_schema = EntitySchema(
            entity_name="nullable_test",
            table_name="nullable_test_table",
            description="Schema for nullable testing",
            fields={
                "required_field": FieldDefinition(
                    name="required_field",
                    field_type=FieldType.STRING,
                    semantics=FieldSemantics.SEARCHABLE_STRING,
                    nullable=False
                ),
                "optional_field": FieldDefinition(
                    name="optional_field",
                    field_type=FieldType.STRING,
                    semantics=FieldSemantics.SEARCHABLE_STRING,
                    nullable=True
                )
            }
        )
        registry.register_schema(test_schema)

        # Test required field
        assert registry.validate_field_value("nullable_test_table", "required_field", "value") == True
        assert registry.validate_field_value("nullable_test_table", "required_field", None) == False

        # Test optional field
        assert registry.validate_field_value("nullable_test_table", "optional_field", "value") == True
        assert registry.validate_field_value("nullable_test_table", "optional_field", None) == True

    def test_validate_field_value_unknown_field(self):
        """Test validation with unknown fields."""
        registry = schema_registry

        # Should return True for unknown fields (no validation)
        assert registry.validate_field_value("dev_instrument", "unknown_field", "any_value") == True
        assert registry.validate_field_value("unknown_table", "any_field", "any_value") == True


class TestSchemaRegistrySummary:
    """Test schema registry summary and utility methods."""

    def test_get_schema_summary(self):
        """Test getting schema summary."""
        registry = schema_registry

        summary = registry.get_schema_summary()

        # Check summary structure
        assert "total_entities" in summary
        assert "total_tables" in summary
        assert "entities" in summary

        # Check counts
        assert isinstance(summary["total_entities"], int)
        assert summary["total_entities"] > 0
        assert isinstance(summary["total_tables"], int)
        assert summary["total_tables"] >= summary["total_entities"]

        # Check entity details
        entities = summary["entities"]
        assert isinstance(entities, dict)

        if "instrument" in entities:
            instrument_info = entities["instrument"]
            assert "table_name" in instrument_info
            assert "total_fields" in instrument_info
            assert "field_breakdown" in instrument_info
            assert "description" in instrument_info

            # Check field breakdown
            field_breakdown = instrument_info["field_breakdown"]
            assert isinstance(field_breakdown, dict)

            # Should have various field types
            expected_semantics = ["searchable_string", "categorical", "readonly"]
            for semantic in expected_semantics:
                if semantic in field_breakdown:
                    assert isinstance(field_breakdown[semantic], int)
                    assert field_breakdown[semantic] > 0

    def test_list_entities_and_tables(self):
        """Test listing entities and tables."""
        registry = schema_registry

        entities = registry.list_entities()
        assert isinstance(entities, list)
        assert len(entities) > 0
        assert "instrument" in entities

        tables = registry.list_tables()
        assert isinstance(tables, list)
        assert len(tables) > 0
        assert "dev_instrument" in tables

        # Should have at least as many tables as entities (due to table mapping)
        assert len(tables) >= len(entities)


class TestGlobalRegistryFunctions:
    """Test global registry convenience functions."""

    def test_global_convenience_functions(self):
        """Test global convenience functions."""
        from domains.ml.schema.registry import get_field_definition, is_table_typed, get_table_entity_name

        # Test get_field_definition
        field_def = get_field_definition("dev_instrument", "symbol")
        assert field_def is not None
        assert field_def.name == "symbol"

        # Test is_table_typed
        assert is_table_typed("dev_instrument") == True
        assert is_table_typed("nonexistent_table") == False

        # Test get_table_entity_name
        entity_name = get_table_entity_name("dev_instrument")
        assert entity_name == "instrument"

        unknown_entity = get_table_entity_name("nonexistent_table")
        assert unknown_entity is None


if __name__ == "__main__":
    # Run tests manually without pytest
    pass

    test_classes = [
        TestSchemaRegistry,
        TestSchemaRegistryFieldQueries,
        TestSchemaRegistryFieldDetails,
        TestSchemaRegistryValidation,
        TestSchemaRegistrySummary,
        TestGlobalRegistryFunctions
    ]

    total_tests = 0
    passed_tests = 0

    print("🧪 Running Schema Registry Unit Tests")
    print("=" * 50)

    for test_class in test_classes:
        print(f"\\n📋 Testing {test_class.__name__}")
        print("-" * 40)

        instance = test_class()
        test_methods = [method for method in dir(instance) if method.startswith('test_')]

        for test_method in test_methods:
            total_tests += 1
            getattr(instance, test_method)()
            print(f"✅ {test_method}")
            passed_tests += 1

    print(f"\\n📊 Test Results")
    print("-" * 20)
    print(f"Total Tests: {total_tests}")
    print(f"Passed: {passed_tests}")
    print(f"Failed: {total_tests - passed_tests}")

    if passed_tests == total_tests:
        print("\\n🎉 All schema registry tests passed!")
    else:
        print(f"\\n⚠️  {total_tests - passed_tests} test(s) failed")
        exit(1)