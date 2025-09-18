#!/usr/bin/env python3
"""
Comprehensive unit tests for ATS type system core components.

Tests the fundamental type definitions, field semantics, validation,
and schema structure validation.
"""

import pytest
import sys
import os

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from schema.types import FieldType, FieldSemantics, FieldDefinition, EntitySchema
from schema.entities import EXCHANGE_VALUES, INSTRUMENT_TYPE_VALUES, CURRENCY_VALUES


class TestFieldDefinition:
    """Test FieldDefinition class and validation."""

    def test_field_definition_creation(self):
        """Test creating basic field definition."""
        field = FieldDefinition(
            name="test_field",
            field_type=FieldType.STRING,
            semantics=FieldSemantics.SEARCHABLE_STRING,
            nullable=False,
            max_length=50,
            description="Test field for validation"
        )

        assert field.name == "test_field"
        assert field.field_type == FieldType.STRING
        assert field.semantics == FieldSemantics.SEARCHABLE_STRING
        assert field.nullable == False
        assert field.max_length == 50
        assert field.description == "Test field for validation"
        assert field.ui_label == "Test Field"  # Auto-generated
        assert field.is_filterable == True
        assert field.supports_search == True
        assert field.supports_range == False

    def test_enum_field_validation(self):
        """Test that enum fields require enum_values."""
        # Should work with enum values
        field = FieldDefinition(
            name="status",
            field_type=FieldType.ENUM,
            semantics=FieldSemantics.CATEGORICAL,
            enum_values=["active", "inactive", "pending"]
        )
        assert field.enum_values == ["active", "inactive", "pending"]

        # Should raise error without enum values
        with pytest.raises(ValueError, match="ENUM field 'status' must have enum_values defined"):
            FieldDefinition(
                name="status",
                field_type=FieldType.ENUM,
                semantics=FieldSemantics.CATEGORICAL
            )

    def test_ui_label_auto_generation(self):
        """Test automatic UI label generation."""
        field = FieldDefinition(
            name="market_cap_usd",
            field_type=FieldType.DECIMAL,
            semantics=FieldSemantics.NUMERIC_RANGE
        )
        assert field.ui_label == "Market Cap Usd"

        # Custom label should override
        field_custom = FieldDefinition(
            name="market_cap_usd",
            field_type=FieldType.DECIMAL,
            semantics=FieldSemantics.NUMERIC_RANGE,
            ui_label="Market Cap (USD)"
        )
        assert field_custom.ui_label == "Market Cap (USD)"

    def test_field_capability_properties(self):
        """Test field capability detection properties."""
        # Searchable string
        searchable = FieldDefinition(
            name="symbol",
            field_type=FieldType.STRING,
            semantics=FieldSemantics.SEARCHABLE_STRING
        )
        assert searchable.is_filterable == True
        assert searchable.supports_search == True
        assert searchable.supports_range == False

        # Categorical
        categorical = FieldDefinition(
            name="exchange",
            field_type=FieldType.ENUM,
            semantics=FieldSemantics.CATEGORICAL,
            enum_values=["NYSE", "NASDAQ"]
        )
        assert categorical.is_filterable == True
        assert categorical.supports_search == False
        assert categorical.supports_range == False

        # Numeric range
        numeric = FieldDefinition(
            name="price",
            field_type=FieldType.DECIMAL,
            semantics=FieldSemantics.NUMERIC_RANGE
        )
        assert numeric.is_filterable == True
        assert numeric.supports_search == False
        assert numeric.supports_range == True

        # Date range
        date_field = FieldDefinition(
            name="trade_date",
            field_type=FieldType.DATE,
            semantics=FieldSemantics.DATE_RANGE
        )
        assert date_field.is_filterable == True
        assert date_field.supports_search == False
        assert date_field.supports_range == True

        # Boolean
        boolean = FieldDefinition(
            name="active",
            field_type=FieldType.BOOLEAN,
            semantics=FieldSemantics.BOOLEAN
        )
        assert boolean.is_filterable == True
        assert boolean.supports_search == False
        assert boolean.supports_range == False

        # Readonly
        readonly = FieldDefinition(
            name="id",
            field_type=FieldType.INTEGER,
            semantics=FieldSemantics.READONLY
        )
        assert readonly.is_filterable == False
        assert readonly.supports_search == False
        assert readonly.supports_range == False


class TestEntitySchema:
    """Test EntitySchema class functionality."""

    def create_test_schema(self):
        """Create a test schema for testing."""
        return EntitySchema(
            entity_name="test_entity",
            table_name="test_table",
            description="Test entity for validation",
            fields={
                "id": FieldDefinition(
                    name="id",
                    field_type=FieldType.INTEGER,
                    semantics=FieldSemantics.READONLY,
                    nullable=False
                ),
                "name": FieldDefinition(
                    name="name",
                    field_type=FieldType.STRING,
                    semantics=FieldSemantics.SEARCHABLE_STRING,
                    nullable=False,
                    eda_priority=8
                ),
                "category": FieldDefinition(
                    name="category",
                    field_type=FieldType.ENUM,
                    semantics=FieldSemantics.CATEGORICAL,
                    enum_values=["A", "B", "C"],
                    eda_priority=6
                ),
                "amount": FieldDefinition(
                    name="amount",
                    field_type=FieldType.DECIMAL,
                    semantics=FieldSemantics.NUMERIC_RANGE,
                    nullable=True,
                    eda_priority=7
                ),
                "is_active": FieldDefinition(
                    name="is_active",
                    field_type=FieldType.BOOLEAN,
                    semantics=FieldSemantics.BOOLEAN,
                    nullable=False,
                    eda_priority=5
                )
            }
        )

    def test_entity_schema_creation(self):
        """Test basic entity schema creation."""
        schema = self.create_test_schema()

        assert schema.entity_name == "test_entity"
        assert schema.table_name == "test_table"
        assert schema.description == "Test entity for validation"
        assert len(schema.fields) == 5
        assert schema.primary_key == ["id"]  # Default
        assert schema.indexes == []  # Default

    def test_get_filterable_fields(self):
        """Test getting filterable fields."""
        schema = self.create_test_schema()
        filterable = schema.get_filterable_fields()

        # Should exclude readonly 'id' field
        assert len(filterable) == 4
        assert "id" not in filterable
        assert "name" in filterable
        assert "category" in filterable
        assert "amount" in filterable
        assert "is_active" in filterable

    def test_get_searchable_fields(self):
        """Test getting searchable fields."""
        schema = self.create_test_schema()
        searchable = schema.get_searchable_fields()

        assert len(searchable) == 1
        assert "name" in searchable

    def test_get_fields_by_semantics(self):
        """Test getting fields by semantic type."""
        schema = self.create_test_schema()

        categorical = schema.get_fields_by_semantics(FieldSemantics.CATEGORICAL)
        assert len(categorical) == 1
        assert "category" in categorical

        numeric = schema.get_fields_by_semantics(FieldSemantics.NUMERIC_RANGE)
        assert len(numeric) == 1
        assert "amount" in numeric

        readonly = schema.get_fields_by_semantics(FieldSemantics.READONLY)
        assert len(readonly) == 1
        assert "id" in readonly

    def test_get_eda_priority_fields(self):
        """Test getting fields by EDA priority."""
        schema = self.create_test_schema()

        # Get top 3 priority fields
        priority_fields = schema.get_eda_priority_fields(limit=3)

        # Should be ordered by priority: name(8), amount(7), category(6)
        assert len(priority_fields) == 3
        assert priority_fields == ["name", "amount", "category"]

        # Get more than available
        all_priority = schema.get_eda_priority_fields(limit=10)
        assert len(all_priority) == 4  # Only filterable fields
        assert "id" not in all_priority  # Readonly excluded


class TestEnumDefinitions:
    """Test predefined enum value definitions."""

    def test_exchange_values(self):
        """Test exchange enum values."""
        assert isinstance(EXCHANGE_VALUES, list)
        assert len(EXCHANGE_VALUES) > 0

        # Check for expected major exchanges
        expected_exchanges = ["NYSE", "NASDAQ", "AMEX", "LSE"]
        for exchange in expected_exchanges:
            assert exchange in EXCHANGE_VALUES

        # All should be strings
        for exchange in EXCHANGE_VALUES:
            assert isinstance(exchange, str)
            assert len(exchange) > 0

    def test_instrument_type_values(self):
        """Test instrument type enum values."""
        assert isinstance(INSTRUMENT_TYPE_VALUES, list)
        assert len(INSTRUMENT_TYPE_VALUES) > 0

        # Check for expected types
        expected_types = ["STOCK", "ETF", "BOND", "OPTION"]
        for inst_type in expected_types:
            assert inst_type in INSTRUMENT_TYPE_VALUES

        # All should be strings
        for inst_type in INSTRUMENT_TYPE_VALUES:
            assert isinstance(inst_type, str)
            assert len(inst_type) > 0

    def test_currency_values(self):
        """Test currency enum values."""
        assert isinstance(CURRENCY_VALUES, list)
        assert len(CURRENCY_VALUES) > 0

        # Check for expected currencies
        expected_currencies = ["USD", "EUR", "GBP", "JPY"]
        for currency in expected_currencies:
            assert currency in CURRENCY_VALUES

        # All should be 3-character strings
        for currency in CURRENCY_VALUES:
            assert isinstance(currency, str)
            assert len(currency) == 3
            assert currency.isupper()


class TestFieldSemantics:
    """Test field semantic enumeration."""

    def test_all_semantics_present(self):
        """Test that all expected semantic types are defined."""
        expected_semantics = [
            "searchable_string",
            "categorical",
            "numeric_range",
            "boolean",
            "date_range",
            "readonly"
        ]

        semantic_values = [sem.value for sem in FieldSemantics]

        for expected in expected_semantics:
            assert expected in semantic_values

    def test_semantic_enum_properties(self):
        """Test semantic enum properties."""
        # Each semantic should have a string value
        for semantic in FieldSemantics:
            assert isinstance(semantic.value, str)
            assert len(semantic.value) > 0
            assert "_" in semantic.value or semantic.value in ["boolean", "readonly"]


class TestFieldType:
    """Test field type enumeration."""

    def test_all_field_types_present(self):
        """Test that all expected field types are defined."""
        expected_types = [
            "string",
            "integer",
            "decimal",
            "boolean",
            "date",
            "datetime",
            "enum"
        ]

        field_type_values = [ft.value for ft in FieldType]

        for expected in expected_types:
            assert expected in field_type_values

    def test_field_type_enum_properties(self):
        """Test field type enum properties."""
        for field_type in FieldType:
            assert isinstance(field_type.value, str)
            assert len(field_type.value) > 0


class TestFieldDefinitionValidation:
    """Test field definition validation and edge cases."""

    def test_nullable_validation(self):
        """Test nullable field validation."""
        # Nullable field should allow None conceptually
        field = FieldDefinition(
            name="optional_field",
            field_type=FieldType.STRING,
            semantics=FieldSemantics.SEARCHABLE_STRING,
            nullable=True
        )
        assert field.nullable == True

        # Non-nullable field
        field = FieldDefinition(
            name="required_field",
            field_type=FieldType.STRING,
            semantics=FieldSemantics.SEARCHABLE_STRING,
            nullable=False
        )
        assert field.nullable == False

    def test_max_length_validation(self):
        """Test max length constraint."""
        field = FieldDefinition(
            name="short_string",
            field_type=FieldType.STRING,
            semantics=FieldSemantics.SEARCHABLE_STRING,
            max_length=10
        )
        assert field.max_length == 10

    def test_numeric_constraints(self):
        """Test numeric field constraints."""
        field = FieldDefinition(
            name="price",
            field_type=FieldType.DECIMAL,
            semantics=FieldSemantics.NUMERIC_RANGE,
            min_value=0,
            max_value=10000
        )
        assert field.min_value == 0
        assert field.max_value == 10000

    def test_validation_regex(self):
        """Test regex validation pattern."""
        field = FieldDefinition(
            name="symbol",
            field_type=FieldType.STRING,
            semantics=FieldSemantics.SEARCHABLE_STRING,
            validation_regex=r"^[A-Z]{1,5}$"
        )
        assert field.validation_regex == r"^[A-Z]{1,5}$"

    def test_eda_priority_default(self):
        """Test EDA priority defaults."""
        field = FieldDefinition(
            name="test_field",
            field_type=FieldType.STRING,
            semantics=FieldSemantics.SEARCHABLE_STRING
        )
        assert field.eda_priority == 0  # Default

        field_with_priority = FieldDefinition(
            name="important_field",
            field_type=FieldType.STRING,
            semantics=FieldSemantics.SEARCHABLE_STRING,
            eda_priority=9
        )
        assert field_with_priority.eda_priority == 9

    def test_eda_default_visible(self):
        """Test EDA default visibility."""
        field = FieldDefinition(
            name="visible_field",
            field_type=FieldType.STRING,
            semantics=FieldSemantics.SEARCHABLE_STRING
        )
        assert field.eda_default_visible == True  # Default

        field_hidden = FieldDefinition(
            name="hidden_field",
            field_type=FieldType.STRING,
            semantics=FieldSemantics.READONLY,
            eda_default_visible=False
        )
        assert field_hidden.eda_default_visible == False


if __name__ == "__main__":
    # Run tests manually without pytest to avoid dependency issues
    import traceback

    test_classes = [
        TestFieldDefinition,
        TestEntitySchema,
        TestEnumDefinitions,
        TestFieldSemantics,
        TestFieldType,
        TestFieldDefinitionValidation
    ]

    total_tests = 0
    passed_tests = 0

    print("🧪 Running Type System Core Unit Tests")
    print("=" * 50)

    for test_class in test_classes:
        print(f"\\n📋 Testing {test_class.__name__}")
        print("-" * 30)

        instance = test_class()
        test_methods = [method for method in dir(instance) if method.startswith('test_')]

        for test_method in test_methods:
            total_tests += 1
            try:
                # Handle special methods that need setup
                if hasattr(instance, 'create_test_schema') and 'schema' in test_method:
                    # Methods that need test schema setup
                    getattr(instance, test_method)()
                else:
                    getattr(instance, test_method)()

                print(f"✅ {test_method}")
                passed_tests += 1

            except Exception as e:
                print(f"❌ {test_method}: {e}")
                traceback.print_exc()

    print(f"\\n📊 Test Results")
    print("-" * 20)
    print(f"Total Tests: {total_tests}")
    print(f"Passed: {passed_tests}")
    print(f"Failed: {total_tests - passed_tests}")

    if passed_tests == total_tests:
        print("\\n🎉 All type system core tests passed!")
    else:
        print(f"\\n⚠️  {total_tests - passed_tests} test(s) failed")
        exit(1)