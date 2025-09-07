"""
Schema registry for ATS platform type system.

Provides centralized access to all entity schemas and utility methods
for type-driven operations.
"""

from typing import Dict, List, Optional, Any
from .types import FieldSemantics, EntitySchema, FieldDefinition
from .entities import ALL_SCHEMAS, TABLE_SCHEMA_MAPPING


class SchemaRegistry:
    """Central registry for all entity schemas."""

    def __init__(self):
        self._schemas = ALL_SCHEMAS.copy()
        self._table_mapping = TABLE_SCHEMA_MAPPING.copy()

    def register_schema(self, schema: EntitySchema) -> None:
        """Register a new entity schema."""
        self._schemas[schema.entity_name] = schema
        self._table_mapping[schema.table_name] = schema

    def get_schema(self, entity_name: str) -> EntitySchema:
        """Get schema definition for entity."""
        if entity_name not in self._schemas:
            raise ValueError(f"Unknown entity: {entity_name}")
        return self._schemas[entity_name]

    def get_table_schema(self, table_name: str) -> EntitySchema:
        """Get schema by table name."""
        if table_name not in self._table_mapping:
            raise ValueError(f"Unknown table: {table_name}")
        return self._table_mapping[table_name]

    def has_schema(self, entity_name: str) -> bool:
        """Check if entity has a registered schema."""
        return entity_name in self._schemas

    def has_table_schema(self, table_name: str) -> bool:
        """Check if table has a registered schema."""
        return table_name in self._table_mapping

    def list_entities(self) -> List[str]:
        """Get list of all registered entity names."""
        return list(self._schemas.keys())

    def list_tables(self) -> List[str]:
        """Get list of all registered table names."""
        return list(self._table_mapping.keys())

    # =============================================================================
    # FIELD QUERY METHODS
    # =============================================================================

    def get_fields_by_semantics(self, entity_name: str, semantics: FieldSemantics) -> Dict[str, FieldDefinition]:
        """Get fields with specific semantic type."""
        schema = self.get_schema(entity_name)
        return schema.get_fields_by_semantics(semantics)

    def get_searchable_fields(self, entity_name: str) -> List[str]:
        """Get field names that support text search."""
        schema = self.get_schema(entity_name)
        searchable = schema.get_searchable_fields()
        return list(searchable.keys())

    def get_categorical_fields(self, entity_name: str) -> List[str]:
        """Get field names that are categorical (dropdowns)."""
        categorical = self.get_fields_by_semantics(entity_name, FieldSemantics.CATEGORICAL)
        return list(categorical.keys())

    def get_numeric_fields(self, entity_name: str) -> List[str]:
        """Get field names that support numeric range filtering."""
        numeric = self.get_fields_by_semantics(entity_name, FieldSemantics.NUMERIC_RANGE)
        return list(numeric.keys())

    def get_date_fields(self, entity_name: str) -> List[str]:
        """Get field names that support date range filtering."""
        date_fields = self.get_fields_by_semantics(entity_name, FieldSemantics.DATE_RANGE)
        return list(date_fields.keys())

    def get_boolean_fields(self, entity_name: str) -> List[str]:
        """Get field names that are boolean."""
        boolean_fields = self.get_fields_by_semantics(entity_name, FieldSemantics.BOOLEAN)
        return list(boolean_fields.keys())

    def get_filterable_fields(self, entity_name: str) -> Dict[str, FieldDefinition]:
        """Get all fields that can be used for filtering."""
        schema = self.get_schema(entity_name)
        return schema.get_filterable_fields()

    # =============================================================================
    # TABLE-BASED CONVENIENCE METHODS
    # =============================================================================

    def get_table_searchable_fields(self, table_name: str) -> List[str]:
        """Get searchable fields by table name."""
        schema = self.get_table_schema(table_name)
        return self.get_searchable_fields(schema.entity_name)

    def get_table_categorical_fields(self, table_name: str) -> List[str]:
        """Get categorical fields by table name."""
        schema = self.get_table_schema(table_name)
        return self.get_categorical_fields(schema.entity_name)

    def get_table_numeric_fields(self, table_name: str) -> List[str]:
        """Get numeric fields by table name."""
        schema = self.get_table_schema(table_name)
        return self.get_numeric_fields(schema.entity_name)

    def get_table_filterable_fields(self, table_name: str) -> Dict[str, FieldDefinition]:
        """Get filterable fields by table name."""
        schema = self.get_table_schema(table_name)
        return schema.get_filterable_fields()

    # =============================================================================
    # EDA-SPECIFIC METHODS
    # =============================================================================

    def get_eda_priority_fields(self, table_name: str, limit: int = 6) -> List[str]:
        """Get highest priority fields for EDA display."""
        schema = self.get_table_schema(table_name)
        return schema.get_eda_priority_fields(limit)

    def get_field_definition(self, table_name: str, field_name: str) -> Optional[FieldDefinition]:
        """Get field definition for specific table and field."""
        try:
            schema = self.get_table_schema(table_name)
            return schema.fields.get(field_name)
        except ValueError:
            return None

    def is_field_searchable(self, table_name: str, field_name: str) -> bool:
        """Check if field supports text search."""
        field_def = self.get_field_definition(table_name, field_name)
        return field_def is not None and field_def.supports_search

    def is_field_categorical(self, table_name: str, field_name: str) -> bool:
        """Check if field is categorical."""
        field_def = self.get_field_definition(table_name, field_name)
        return field_def is not None and field_def.semantics == FieldSemantics.CATEGORICAL

    def is_field_numeric_range(self, table_name: str, field_name: str) -> bool:
        """Check if field supports numeric range filtering."""
        field_def = self.get_field_definition(table_name, field_name)
        return field_def is not None and field_def.semantics == FieldSemantics.NUMERIC_RANGE

    def get_enum_values(self, table_name: str, field_name: str) -> Optional[List[str]]:
        """Get enum values for field if it's an enum."""
        field_def = self.get_field_definition(table_name, field_name)
        return field_def.enum_values if field_def else None

    # =============================================================================
    # VALIDATION METHODS
    # =============================================================================

    def validate_field_value(self, table_name: str, field_name: str, value: Any) -> bool:
        """Validate that a value is acceptable for a field."""
        field_def = self.get_field_definition(table_name, field_name)
        if not field_def:
            return True  # No validation if field not defined

        if value is None:
            return field_def.nullable

        # Type-specific validation
        if field_def.field_type.value == "enum" and field_def.enum_values:
            return str(value) in field_def.enum_values

        if field_def.field_type.value == "string" and field_def.max_length:
            return len(str(value)) <= field_def.max_length

        if field_def.field_type.value in ["decimal", "integer"] and field_def.min_value is not None:
            try:
                numeric_value = float(value)
                if field_def.min_value is not None and numeric_value < field_def.min_value:
                    return False
                if field_def.max_value is not None and numeric_value > field_def.max_value:
                    return False
            except (ValueError, TypeError):
                return False

        return True

    # =============================================================================
    # UTILITY METHODS
    # =============================================================================

    def get_schema_summary(self) -> Dict[str, Any]:
        """Get summary of all registered schemas."""
        summary = {
            "total_entities": len(self._schemas),
            "total_tables": len(self._table_mapping),
            "entities": {}
        }

        for entity_name, schema in self._schemas.items():
            field_counts = {}
            for semantics in FieldSemantics:
                fields = schema.get_fields_by_semantics(semantics)
                if fields:
                    field_counts[semantics.value] = len(fields)

            summary["entities"][entity_name] = {
                "table_name": schema.table_name,
                "total_fields": len(schema.fields),
                "field_breakdown": field_counts,
                "description": schema.description
            }

        return summary


# Global registry instance
schema_registry = SchemaRegistry()


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def get_field_definition(table_name: str, field_name: str) -> Optional[FieldDefinition]:
    """Get field definition - convenience function."""
    return schema_registry.get_field_definition(table_name, field_name)


def is_table_typed(table_name: str) -> bool:
    """Check if table has type definitions."""
    return schema_registry.has_table_schema(table_name)


def get_table_entity_name(table_name: str) -> Optional[str]:
    """Get entity name for table."""
    try:
        schema = schema_registry.get_table_schema(table_name)
        return schema.entity_name
    except ValueError:
        return None