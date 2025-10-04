"""
Core type system definitions for ATS platform.

Provides semantic type information that drives database schema generation,
UI component creation, and EDA filter generation.
"""

from enum import Enum
from typing import List, Optional
from dataclasses import dataclass


class FieldSemantics(Enum):
    """Semantic meaning of fields for UI generation and behavior."""
    SEARCHABLE_STRING = "searchable_string"    # Partial text search with autocomplete
    CATEGORICAL = "categorical"                 # Dropdown/checkbox selection
    NUMERIC_RANGE = "numeric_range"            # Range slider/input
    BOOLEAN = "boolean"                        # Checkbox
    DATE_RANGE = "date_range"                  # Date picker range
    READONLY = "readonly"                      # Display only, no filtering


class FieldType(Enum):
    """Database-level data types."""
    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    ENUM = "enum"


@dataclass
class FieldDefinition:
    """Complete field type definition with semantic information."""

    # Core properties
    name: str
    field_type: FieldType
    semantics: FieldSemantics
    nullable: bool = True

    # Validation constraints
    max_length: Optional[int] = None
    enum_values: Optional[List[str]] = None
    validation_regex: Optional[str] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None

    # Documentation
    description: str = ""

    # UI hints for frontend generation
    ui_label: str = ""
    ui_placeholder: str = ""
    ui_help_text: str = ""
    ui_group: Optional[str] = None  # For grouping related fields in UI

    # EDA hints
    eda_priority: int = 0  # Higher = more important for filtering (0-10)
    eda_default_visible: bool = True  # Show in EDA by default

    def __post_init__(self):
        """Validate field definition consistency."""
        if self.field_type == FieldType.ENUM and not self.enum_values:
            raise ValueError(f"ENUM field '{self.name}' must have enum_values defined")

        if self.semantics == FieldSemantics.CATEGORICAL and self.field_type != FieldType.ENUM:
            # Allow non-enum categoricals (queried from database)
            pass

        if not self.ui_label:
            self.ui_label = self.name.replace('_', ' ').title()

    @property
    def is_filterable(self) -> bool:
        """Whether this field can be used for filtering in EDA."""
        return self.semantics != FieldSemantics.READONLY

    @property
    def supports_search(self) -> bool:
        """Whether this field supports text-based search."""
        return self.semantics == FieldSemantics.SEARCHABLE_STRING

    @property
    def supports_range(self) -> bool:
        """Whether this field supports range filtering."""
        return self.semantics in [FieldSemantics.NUMERIC_RANGE, FieldSemantics.DATE_RANGE]


@dataclass
class EntitySchema:
    """Complete schema definition for a domain entity."""
    entity_name: str
    table_name: str
    description: str
    fields: dict[str, FieldDefinition]

    # Entity metadata
    primary_key: List[str] = None
    indexes: List[List[str]] = None

    def __post_init__(self):
        if self.primary_key is None:
            self.primary_key = ["id"]  # Default primary key
        if self.indexes is None:
            self.indexes = []

    def get_filterable_fields(self) -> dict[str, FieldDefinition]:
        """Get fields that can be used for filtering."""
        return {
            name: field_def for name, field_def in self.fields.items()
            if field_def.is_filterable
        }

    def get_searchable_fields(self) -> dict[str, FieldDefinition]:
        """Get fields that support text search."""
        return {
            name: field_def for name, field_def in self.fields.items()
            if field_def.supports_search
        }

    def get_fields_by_semantics(self, semantics: FieldSemantics) -> dict[str, FieldDefinition]:
        """Get fields with specific semantic type."""
        return {
            name: field_def for name, field_def in self.fields.items()
            if field_def.semantics == semantics
        }

    def get_eda_priority_fields(self, limit: int = 6) -> List[str]:
        """Get field names ordered by EDA priority for default display."""
        filterable = self.get_filterable_fields()
        sorted_fields = sorted(
            filterable.items(),
            key=lambda x: (-x[1].eda_priority, x[0])  # Sort by priority desc, name asc
        )
        return [name for name, _ in sorted_fields[:limit]]