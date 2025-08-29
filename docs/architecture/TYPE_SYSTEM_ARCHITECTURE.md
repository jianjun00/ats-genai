# ATS Type System Architecture

## Problem Statement

The ATS platform currently lacks a consistent type system, leading to:

- **Schema Inconsistencies**: `symbol TEXT` vs `exchange TEXT` - both treated as strings but with different semantic meanings
- **Manual UI Generation**: EDA filters hard-coded instead of generated from type definitions  
- **No Validation**: Database schema doesn't enforce categorical constraints
- **Type Confusion**: No distinction between searchable strings (symbol) and enums (exchange)
- **Maintenance Burden**: Changes require updates across database, backend, frontend, APIs

## Proposed Solution: Schema-First Type System

### Core Components

#### 1. **Central Type Registry** (`src/schema/types.py`)
```python
from enum import Enum
from typing import List, Optional, Union
from dataclasses import dataclass
from decimal import Decimal

class FieldSemantics(Enum):
    """Semantic meaning of fields for UI generation."""
    SEARCHABLE_STRING = "searchable_string"    # Partial text search
    CATEGORICAL = "categorical"                 # Dropdown selection
    NUMERIC_RANGE = "numeric_range"            # Range slider/input
    BOOLEAN = "boolean"                        # Checkbox
    DATE_RANGE = "date_range"                  # Date picker range
    READONLY = "readonly"                      # Display only
    
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
    """Complete field type definition."""
    name: str
    field_type: FieldType
    semantics: FieldSemantics
    nullable: bool = True
    max_length: Optional[int] = None
    enum_values: Optional[List[str]] = None
    validation_regex: Optional[str] = None
    description: str = ""
    
    # UI hints
    ui_label: str = ""
    ui_placeholder: str = ""
    ui_help_text: str = ""
```

#### 2. **Domain Entity Definitions** (`src/schema/entities.py`)
```python
from typing import Dict, List
from .types import FieldDefinition, FieldType, FieldSemantics

# Exchange enumeration - centrally defined
EXCHANGE_VALUES = [
    "NYSE", "NASDAQ", "AMEX", "LSE", "TSE", "XETRA", 
    "OTC", "PINK", "CBOE", "BATS"
]

INSTRUMENT_TYPE_VALUES = [
    "STOCK", "ETF", "MUTUAL_FUND", "BOND", "OPTION", 
    "FUTURE", "FOREX", "CRYPTO", "INDEX"
]

# Instrument entity schema
INSTRUMENT_SCHEMA = {
    "entity_name": "instrument",
    "table_name": "instruments", 
    "description": "Financial instrument master data",
    "fields": {
        "id": FieldDefinition(
            name="id",
            field_type=FieldType.INTEGER,
            semantics=FieldSemantics.READONLY,
            nullable=False,
            ui_label="ID"
        ),
        "symbol": FieldDefinition(
            name="symbol",
            field_type=FieldType.STRING,
            semantics=FieldSemantics.SEARCHABLE_STRING,
            nullable=False,
            max_length=20,
            validation_regex=r"^[A-Z0-9\.\-]{1,20}$",
            ui_label="Symbol",
            ui_placeholder="AAPL, BRK.A, etc.",
            ui_help_text="Ticker symbol - supports partial search"
        ),
        "name": FieldDefinition(
            name="name", 
            field_type=FieldType.STRING,
            semantics=FieldSemantics.SEARCHABLE_STRING,
            nullable=True,
            max_length=255,
            ui_label="Company Name",
            ui_placeholder="Apple Inc.",
            ui_help_text="Full company name - supports partial search"
        ),
        "exchange": FieldDefinition(
            name="exchange",
            field_type=FieldType.ENUM,
            semantics=FieldSemantics.CATEGORICAL,
            nullable=True,
            enum_values=EXCHANGE_VALUES,
            ui_label="Exchange",
            ui_help_text="Trading exchange - select from dropdown"
        ),
        "instrument_type": FieldDefinition(
            name="type",
            field_type=FieldType.ENUM, 
            semantics=FieldSemantics.CATEGORICAL,
            nullable=True,
            enum_values=INSTRUMENT_TYPE_VALUES,
            ui_label="Instrument Type",
            ui_help_text="Type of financial instrument"
        ),
        "active": FieldDefinition(
            name="active",
            field_type=FieldType.BOOLEAN,
            semantics=FieldSemantics.BOOLEAN,
            nullable=False,
            ui_label="Active",
            ui_help_text="Whether instrument is currently active"
        ),
        "list_date": FieldDefinition(
            name="list_date",
            field_type=FieldType.DATE,
            semantics=FieldSemantics.DATE_RANGE,
            nullable=True,
            ui_label="Listing Date",
            ui_help_text="Date when instrument started trading"
        ),
        "market_cap": FieldDefinition(
            name="market_cap",
            field_type=FieldType.DECIMAL,
            semantics=FieldSemantics.NUMERIC_RANGE,
            nullable=True,
            ui_label="Market Cap ($)",
            ui_help_text="Market capitalization in USD"
        )
    }
}

# Price data schema
PRICE_SCHEMA = {
    "entity_name": "daily_price",
    "table_name": "daily_prices",
    "description": "Daily price and volume data",
    "fields": {
        "symbol": FieldDefinition(
            name="symbol",
            field_type=FieldType.STRING,
            semantics=FieldSemantics.CATEGORICAL,  # For price data, symbol is categorical
            nullable=False,
            ui_label="Symbol"
        ),
        "date": FieldDefinition(
            name="date",
            field_type=FieldType.DATE,
            semantics=FieldSemantics.DATE_RANGE,
            nullable=False,
            ui_label="Date"
        ),
        "open": FieldDefinition(
            name="open",
            field_type=FieldType.DECIMAL,
            semantics=FieldSemantics.NUMERIC_RANGE,
            nullable=True,
            ui_label="Open Price ($)"
        ),
        "high": FieldDefinition(
            name="high", 
            field_type=FieldType.DECIMAL,
            semantics=FieldSemantics.NUMERIC_RANGE,
            nullable=True,
            ui_label="High Price ($)"
        ),
        "low": FieldDefinition(
            name="low",
            field_type=FieldType.DECIMAL, 
            semantics=FieldSemantics.NUMERIC_RANGE,
            nullable=True,
            ui_label="Low Price ($)"
        ),
        "close": FieldDefinition(
            name="close",
            field_type=FieldType.DECIMAL,
            semantics=FieldSemantics.NUMERIC_RANGE, 
            nullable=True,
            ui_label="Close Price ($)"
        ),
        "volume": FieldDefinition(
            name="volume",
            field_type=FieldType.INTEGER,
            semantics=FieldSemantics.NUMERIC_RANGE,
            nullable=True,
            ui_label="Volume"
        )
    }
}
```

#### 3. **Schema Registry** (`src/schema/registry.py`)
```python
from typing import Dict, List, Optional
from .entities import INSTRUMENT_SCHEMA, PRICE_SCHEMA

class SchemaRegistry:
    """Central registry for all entity schemas."""
    
    def __init__(self):
        self._schemas = {
            "instrument": INSTRUMENT_SCHEMA,
            "daily_price": PRICE_SCHEMA
        }
    
    def get_schema(self, entity_name: str) -> Dict:
        """Get schema definition for entity."""
        if entity_name not in self._schemas:
            raise ValueError(f"Unknown entity: {entity_name}")
        return self._schemas[entity_name]
    
    def get_table_schema(self, table_name: str) -> Dict:
        """Get schema by table name."""
        for schema in self._schemas.values():
            if schema["table_name"] == table_name:
                return schema
        raise ValueError(f"Unknown table: {table_name}")
    
    def get_searchable_fields(self, entity_name: str) -> List[str]:
        """Get fields that support text search."""
        schema = self.get_schema(entity_name)
        return [
            field_name for field_name, field_def in schema["fields"].items()
            if field_def.semantics == FieldSemantics.SEARCHABLE_STRING
        ]
    
    def get_categorical_fields(self, entity_name: str) -> List[str]:
        """Get fields that are categorical (dropdowns)."""
        schema = self.get_schema(entity_name)
        return [
            field_name for field_name, field_def in schema["fields"].items()
            if field_def.semantics == FieldSemantics.CATEGORICAL
        ]
    
    def get_numeric_fields(self, entity_name: str) -> List[str]:
        """Get fields that support numeric range filtering."""
        schema = self.get_schema(entity_name)
        return [
            field_name for field_name, field_def in schema["fields"].items()
            if field_def.semantics == FieldSemantics.NUMERIC_RANGE
        ]

# Global registry instance
schema_registry = SchemaRegistry()
```

#### 4. **Database Integration** (`src/schema/database.py`)
```python
from typing import List, Dict
from .registry import schema_registry
from .types import FieldType

class DatabaseSchemaGenerator:
    """Generate SQL DDL from schema definitions."""
    
    def generate_create_table(self, entity_name: str) -> str:
        """Generate CREATE TABLE SQL."""
        schema = schema_registry.get_schema(entity_name)
        table_name = schema["table_name"]
        
        columns = []
        for field_name, field_def in schema["fields"].items():
            column_sql = self._generate_column_sql(field_name, field_def)
            columns.append(column_sql)
        
        sql = f"""
CREATE TABLE {table_name} (
    {',\\n    '.join(columns)}
);"""
        
        # Add enum constraints
        constraints = self._generate_constraints(entity_name)
        if constraints:
            sql += "\\n\\n" + "\\n".join(constraints)
            
        return sql
    
    def _generate_column_sql(self, field_name: str, field_def) -> str:
        """Generate column definition SQL."""
        type_mapping = {
            FieldType.STRING: "TEXT",
            FieldType.INTEGER: "INTEGER", 
            FieldType.DECIMAL: "DECIMAL(15,4)",
            FieldType.BOOLEAN: "BOOLEAN",
            FieldType.DATE: "DATE",
            FieldType.DATETIME: "TIMESTAMPTZ",
            FieldType.ENUM: "TEXT"
        }
        
        sql_type = type_mapping[field_def.field_type]
        sql = f"{field_name} {sql_type}"
        
        if not field_def.nullable:
            sql += " NOT NULL"
            
        if field_def.max_length and field_def.field_type == FieldType.STRING:
            # Use VARCHAR instead of TEXT for length-limited strings
            sql = f"{field_name} VARCHAR({field_def.max_length})"
            if not field_def.nullable:
                sql += " NOT NULL"
        
        return sql
    
    def _generate_constraints(self, entity_name: str) -> List[str]:
        """Generate CHECK constraints for enums."""
        schema = schema_registry.get_schema(entity_name)
        table_name = schema["table_name"]
        constraints = []
        
        for field_name, field_def in schema["fields"].items():
            if field_def.field_type == FieldType.ENUM and field_def.enum_values:
                values = "', '".join(field_def.enum_values)
                constraint = f"ALTER TABLE {table_name} ADD CONSTRAINT chk_{table_name}_{field_name} CHECK ({field_name} IN ('{values}'));"
                constraints.append(constraint)
        
        return constraints
```

#### 5. **EDA Integration** (`src/services/type_aware_analytics_service.py`)
```python
from typing import Dict, List, Any
from ..schema.registry import schema_registry
from ..schema.types import FieldSemantics

class TypeAwareAnalyticsService:
    """Analytics service that uses type system for intelligent analysis."""
    
    def __init__(self, db_manager):
        self.db = db_manager
        
    async def get_intelligent_filters(self, table_name: str) -> List[Dict]:
        """Generate filters based on field semantics."""
        try:
            schema = schema_registry.get_table_schema(table_name)
        except ValueError:
            # Fallback to legacy behavior for unknown tables
            return await self._legacy_get_filters(table_name)
        
        filters = []
        
        for field_name, field_def in schema["fields"].items():
            filter_config = await self._generate_filter_config(
                table_name, field_name, field_def
            )
            if filter_config:
                filters.append(filter_config)
        
        return filters
    
    async def _generate_filter_config(self, table_name: str, field_name: str, field_def) -> Dict:
        """Generate filter configuration based on field semantics."""
        
        if field_def.semantics == FieldSemantics.SEARCHABLE_STRING:
            # Text search input with autocomplete
            return {
                "field": field_name,
                "type": "text_search",
                "ui_type": "text_input",
                "label": field_def.ui_label or field_name.title(),
                "placeholder": field_def.ui_placeholder or f"Search {field_name}...",
                "help_text": field_def.ui_help_text,
                "supports_partial": True,
                "autocomplete_endpoint": f"/api/eda/datasets/{table_name}/columns/{field_name}/suggest"
            }
            
        elif field_def.semantics == FieldSemantics.CATEGORICAL:
            # Dropdown/checkbox list
            if field_def.enum_values:
                # Use predefined enum values
                options = [{"value": v, "label": v} for v in field_def.enum_values]
            else:
                # Query database for actual values
                options = await self._get_categorical_options(table_name, field_name)
            
            return {
                "field": field_name,
                "type": "categorical", 
                "ui_type": "checkbox_list" if len(options) <= 10 else "searchable_dropdown",
                "label": field_def.ui_label or field_name.title(),
                "help_text": field_def.ui_help_text,
                "options": options
            }
            
        elif field_def.semantics == FieldSemantics.NUMERIC_RANGE:
            # Range slider/input
            min_max = await self._get_numeric_range(table_name, field_name)
            return {
                "field": field_name,
                "type": "numeric_range",
                "ui_type": "range_slider",
                "label": field_def.ui_label or field_name.title(),
                "help_text": field_def.ui_help_text,
                "min": min_max["min"],
                "max": min_max["max"],
                "step": self._calculate_step(min_max)
            }
            
        elif field_def.semantics == FieldSemantics.DATE_RANGE:
            # Date range picker
            date_range = await self._get_date_range(table_name, field_name)
            return {
                "field": field_name,
                "type": "date_range",
                "ui_type": "date_range_picker", 
                "label": field_def.ui_label or field_name.title(),
                "help_text": field_def.ui_help_text,
                "min_date": date_range["min"],
                "max_date": date_range["max"]
            }
            
        elif field_def.semantics == FieldSemantics.BOOLEAN:
            # Checkbox
            return {
                "field": field_name,
                "type": "boolean",
                "ui_type": "checkbox",
                "label": field_def.ui_label or field_name.title(),
                "help_text": field_def.ui_help_text
            }
            
        # Skip readonly fields
        return None
    
    async def analyze_column_intelligent(self, table_name: str, column: str) -> Dict:
        """Analyze column using type information."""
        try:
            schema = schema_registry.get_table_schema(table_name)
            field_def = schema["fields"].get(column)
            
            if field_def:
                return await self._analyze_typed_column(table_name, column, field_def)
        except ValueError:
            pass
        
        # Fallback to legacy analysis
        return await self._legacy_analyze_column(table_name, column)
    
    async def _analyze_typed_column(self, table_name: str, column: str, field_def) -> Dict:
        """Analyze column with known type information."""
        
        if field_def.semantics in [FieldSemantics.NUMERIC_RANGE]:
            # Generate histogram for numeric data
            return await self._analyze_numeric_column_typed(table_name, column, field_def)
            
        elif field_def.semantics in [FieldSemantics.CATEGORICAL, FieldSemantics.SEARCHABLE_STRING]:
            # Generate value counts for categorical/string data
            return await self._analyze_categorical_column_typed(table_name, column, field_def)
            
        elif field_def.semantics == FieldSemantics.DATE_RANGE:
            # Generate time series analysis
            return await self._analyze_date_column_typed(table_name, column, field_def)
            
        else:
            # Fallback analysis
            return await self._legacy_analyze_column(table_name, column)
```

#### 6. **Frontend Type Generation** (`scripts/generate_types.py`)
```python
#!/usr/bin/env python3
"""
Generate TypeScript interfaces and React components from schema definitions.
"""

from pathlib import Path
from ..src.schema.registry import schema_registry
from ..src.schema.types import FieldSemantics

class TypeScriptGenerator:
    """Generate TypeScript definitions from Python schemas."""
    
    def generate_interfaces(self) -> str:
        """Generate TypeScript interfaces for all entities."""
        interfaces = []
        
        for entity_name, schema in schema_registry._schemas.items():
            interface = self._generate_interface(entity_name, schema)
            interfaces.append(interface)
        
        return "\\n\\n".join(interfaces)
    
    def _generate_interface(self, entity_name: str, schema: Dict) -> str:
        """Generate single TypeScript interface."""
        interface_name = self._to_pascal_case(entity_name)
        
        fields = []
        for field_name, field_def in schema["fields"].items():
            ts_type = self._python_to_typescript_type(field_def)
            optional = "?" if field_def.nullable else ""
            field_doc = f"  /** {field_def.description} */" if field_def.description else ""
            
            field_line = f"  {field_name}{optional}: {ts_type};"
            if field_doc:
                fields.append(field_doc)
            fields.append(field_line)
        
        return f"""export interface {interface_name} {{
{chr(10).join(fields)}
}}"""

class ReactFormGenerator:
    """Generate React filter components from schemas."""
    
    def generate_filter_component(self, table_name: str) -> str:
        """Generate React component for table filters.""" 
        try:
            schema = schema_registry.get_table_schema(table_name)
        except ValueError:
            return self._generate_fallback_component(table_name)
        
        component_name = f"{self._to_pascal_case(schema['entity_name'])}Filters"
        
        filter_elements = []
        for field_name, field_def in schema["fields"].items():
            if field_def.semantics != FieldSemantics.READONLY:
                element = self._generate_filter_element(field_name, field_def)
                filter_elements.append(element)
        
        return f"""import React, {{ useState }} from 'react';
import {{ FilterComponent }} from './FilterComponent';

interface {component_name}Props {{
  onFiltersChange: (filters: Record<string, any>) => void;
}}

export const {component_name}: React.FC<{component_name}Props> = ({{ onFiltersChange }}) => {{
  const [filters, setFilters] = useState<Record<string, any>>({{}});
  
  const handleFilterChange = (field: string, value: any) => {{
    const newFilters = {{ ...filters, [field]: value }};
    setFilters(newFilters);
    onFiltersChange(newFilters);
  }};
  
  return (
    <div className="filters-container">
{chr(10).join(filter_elements)}
    </div>
  );
}};"""

# CLI to generate types
if __name__ == "__main__":
    ts_gen = TypeScriptGenerator()
    react_gen = ReactFormGenerator()
    
    # Generate TypeScript interfaces
    interfaces = ts_gen.generate_interfaces()
    Path("frontend/src/types/generated.ts").write_text(interfaces)
    
    # Generate React components for each table
    for entity_name in schema_registry._schemas:
        schema = schema_registry.get_schema(entity_name) 
        component = react_gen.generate_filter_component(schema["table_name"])
        
        component_path = f"frontend/src/components/filters/{entity_name}Filters.tsx"
        Path(component_path).write_text(component)
    
    print("✅ Generated TypeScript types and React components")
```

## Implementation Benefits

### 1. **Consistent Types Across Stack**
- Single source of truth for all field definitions
- Database constraints automatically generated
- TypeScript interfaces auto-generated  
- API validation automatically enforced

### 2. **Intelligent EDA**
- Filters automatically generated based on field semantics
- `symbol` gets text search, `exchange` gets dropdown
- Proper numeric ranges for price fields
- Date pickers for date fields

### 3. **Reduced Maintenance**
- Change schema once, updates propagate everywhere
- Database migrations can be auto-generated
- Frontend components auto-update when schema changes
- API documentation stays in sync

### 4. **Better UX**
- Appropriate input types based on data semantics
- Validation feedback based on field constraints
- Autocomplete for searchable fields
- Range sliders for numeric data

### 5. **Data Quality**
- Database constraints enforce valid values
- Runtime validation prevents bad data
- Schema evolution tracked and managed
- Type safety prevents bugs

## Migration Strategy

### Phase 1: Core Types
1. Implement base type system (`types.py`, `registry.py`)
2. Define schemas for main entities (instrument, prices)
3. Update analytics service to use type information

### Phase 2: Database Integration  
1. Generate DDL from schemas
2. Create migration scripts for existing tables
3. Add enum constraints for categorical fields

### Phase 3: Frontend Generation
1. Implement TypeScript generation
2. Create React component generators
3. Update EDA to use generated components

### Phase 4: API Integration
1. Add request/response validation using schemas
2. Generate OpenAPI specs from types
3. Create client SDKs from types

This type system would transform your EDA from manual UI coding to intelligent, schema-driven generation where `symbol` automatically gets search functionality and `exchange` automatically becomes a dropdown - exactly what you described!