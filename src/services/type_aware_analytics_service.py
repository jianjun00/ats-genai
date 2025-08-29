"""
Type-aware analytics service that uses the schema registry to generate
intelligent filters and analysis based on field semantics.

This demonstrates how the EDA system is transformed from manual filter
generation to automatic, schema-driven UI generation.
"""

import logging
from typing import Dict, List, Any, Optional
from fastapi import HTTPException
from ..schema.registry import schema_registry
from ..schema.types import FieldSemantics, FieldDefinition

logger = logging.getLogger(__name__)


class TypeAwareAnalyticsService:
    """Analytics service that uses type system for intelligent analysis."""
    
    def __init__(self, db_manager):
        self.db = db_manager
        logger.info("Type-aware analytics service initialized")
    
    # =============================================================================
    # INTELLIGENT FILTER GENERATION
    # =============================================================================
    
    async def get_intelligent_filters(self, table_name: str) -> List[Dict[str, Any]]:
        """Generate filters based on field semantics - the magic happens here!"""
        
        if not schema_registry.has_table_schema(table_name):
            logger.warning(f"No schema found for table {table_name}, falling back to legacy filters")
            return await self._legacy_get_filters(table_name)
        
        logger.info(f"Generating intelligent filters for {table_name} using type definitions")
        
        # Get high-priority filterable fields
        priority_fields = schema_registry.get_eda_priority_fields(table_name, limit=4)
        filterable_fields = schema_registry.get_table_filterable_fields(table_name)
        
        filters = []
        
        # Generate filters for priority fields first
        for field_name in priority_fields:
            if field_name in filterable_fields:
                filter_config = await self._generate_typed_filter(
                    table_name, field_name, filterable_fields[field_name]
                )
                if filter_config:
                    filters.append(filter_config)
        
        logger.info(f"Generated {len(filters)} intelligent filters for {table_name}")
        return filters
    
    async def _generate_typed_filter(
        self, 
        table_name: str, 
        field_name: str, 
        field_def: FieldDefinition
    ) -> Optional[Dict[str, Any]]:
        """Generate filter configuration based on field semantics."""
        
        logger.debug(f"Generating filter for {field_name} with semantics {field_def.semantics}")
        
        try:
            if field_def.semantics == FieldSemantics.SEARCHABLE_STRING:
                return await self._generate_searchable_filter(table_name, field_name, field_def)
                
            elif field_def.semantics == FieldSemantics.CATEGORICAL:
                return await self._generate_categorical_filter(table_name, field_name, field_def)
                
            elif field_def.semantics == FieldSemantics.NUMERIC_RANGE:
                return await self._generate_numeric_filter(table_name, field_name, field_def)
                
            elif field_def.semantics == FieldSemantics.DATE_RANGE:
                return await self._generate_date_filter(table_name, field_name, field_def)
                
            elif field_def.semantics == FieldSemantics.BOOLEAN:
                return await self._generate_boolean_filter(table_name, field_name, field_def)
            
            logger.debug(f"Skipping filter for {field_name} with semantics {field_def.semantics}")
            return None
            
        except Exception as e:
            logger.error(f"Error generating filter for {field_name}: {e}")
            return None
    
    async def _generate_searchable_filter(
        self, 
        table_name: str, 
        field_name: str, 
        field_def: FieldDefinition
    ) -> Dict[str, Any]:
        """Generate text search input with autocomplete."""
        
        # Get sample values for autocomplete suggestions
        suggestions = await self._get_search_suggestions(table_name, field_name, limit=20)
        
        return {
            "field": field_name,
            "type": "text_search",
            "ui_type": "text_input_with_autocomplete",
            "label": field_def.ui_label,
            "placeholder": field_def.ui_placeholder or f"Search {field_name}...",
            "help_text": field_def.ui_help_text,
            "supports_partial": True,
            "suggestions": suggestions,
            "validation_regex": field_def.validation_regex,
            "max_length": field_def.max_length,
            "priority": field_def.eda_priority
        }
    
    async def _generate_categorical_filter(
        self, 
        table_name: str, 
        field_name: str, 
        field_def: FieldDefinition
    ) -> Dict[str, Any]:
        """Generate dropdown/checkbox list for categorical data."""
        
        if field_def.enum_values:
            # Use predefined enum values - no database query needed!
            options = [{"value": v, "label": v, "count": None} for v in field_def.enum_values]
            logger.debug(f"Using predefined enum values for {field_name}: {len(options)} options")
        else:
            # Query database for actual values
            options = await self._get_categorical_options(table_name, field_name, limit=50)
            logger.debug(f"Queried database for {field_name}: {len(options)} options")
        
        # Choose UI type based on number of options
        ui_type = "checkbox_list" if len(options) <= 8 else "searchable_dropdown"
        
        return {
            "field": field_name,
            "type": "categorical",
            "ui_type": ui_type,
            "label": field_def.ui_label,
            "help_text": field_def.ui_help_text,
            "options": options[:10],  # Limit for performance
            "total_options": len(options),
            "enum_values": field_def.enum_values,
            "priority": field_def.eda_priority
        }
    
    async def _generate_numeric_filter(
        self, 
        table_name: str, 
        field_name: str, 
        field_def: FieldDefinition
    ) -> Dict[str, Any]:
        """Generate range slider/input for numeric data."""
        
        # Get actual min/max from database
        min_max = await self._get_numeric_range(table_name, field_name)
        
        if not min_max:
            logger.warning(f"Could not determine numeric range for {field_name}")
            return None
        
        return {
            "field": field_name,
            "type": "numeric_range",
            "ui_type": "range_slider_with_input",
            "label": field_def.ui_label,
            "help_text": field_def.ui_help_text,
            "min": min_max["min"],
            "max": min_max["max"],
            "step": self._calculate_step(min_max),
            "format": "currency" if "$" in field_def.ui_label else "number",
            "constraint_min": field_def.min_value,
            "constraint_max": field_def.max_value,
            "priority": field_def.eda_priority
        }
    
    async def _generate_date_filter(
        self, 
        table_name: str, 
        field_name: str, 
        field_def: FieldDefinition
    ) -> Dict[str, Any]:
        """Generate date range picker."""
        
        date_range = await self._get_date_range(table_name, field_name)
        
        if not date_range:
            logger.warning(f"Could not determine date range for {field_name}")
            return None
        
        return {
            "field": field_name,
            "type": "date_range",
            "ui_type": "date_range_picker",
            "label": field_def.ui_label,
            "help_text": field_def.ui_help_text,
            "min_date": date_range["min"],
            "max_date": date_range["max"],
            "default_range": "last_year",  # Could be configurable
            "priority": field_def.eda_priority
        }
    
    async def _generate_boolean_filter(
        self, 
        table_name: str, 
        field_name: str, 
        field_def: FieldDefinition
    ) -> Dict[str, Any]:
        """Generate checkbox for boolean fields."""
        
        return {
            "field": field_name,
            "type": "boolean",
            "ui_type": "tri_state_checkbox",  # True/False/Either
            "label": field_def.ui_label,
            "help_text": field_def.ui_help_text,
            "options": [
                {"value": True, "label": "Yes"},
                {"value": False, "label": "No"},
                {"value": None, "label": "Either"}
            ],
            "default": None,  # Either
            "priority": field_def.eda_priority
        }
    
    # =============================================================================
    # INTELLIGENT COLUMN ANALYSIS 
    # =============================================================================
    
    async def analyze_column_intelligent(self, table_name: str, column: str) -> Dict[str, Any]:
        """Analyze column using type information for better insights."""
        
        field_def = schema_registry.get_field_definition(table_name, column)
        
        if field_def:
            logger.info(f"Analyzing {column} using type definition: {field_def.semantics}")
            return await self._analyze_typed_column(table_name, column, field_def)
        
        # Fallback to legacy analysis
        logger.warning(f"No type definition for {table_name}.{column}, using legacy analysis")
        return await self._legacy_analyze_column(table_name, column)
    
    async def _analyze_typed_column(
        self, 
        table_name: str, 
        column: str, 
        field_def: FieldDefinition
    ) -> Dict[str, Any]:
        """Analyze column with known type information."""
        
        base_analysis = {
            "column": column,
            "field_type": field_def.field_type.value,
            "semantics": field_def.semantics.value,
            "description": field_def.description,
            "ui_label": field_def.ui_label,
            "nullable": field_def.nullable
        }
        
        if field_def.semantics == FieldSemantics.NUMERIC_RANGE:
            stats = await self._get_numeric_statistics(table_name, column)
            return {
                **base_analysis,
                "analysis_type": "numeric",
                "statistics": stats,
                "histogram": await self._get_histogram_data(table_name, column),
                "visualization_hint": "histogram"
            }
            
        elif field_def.semantics in [FieldSemantics.CATEGORICAL, FieldSemantics.SEARCHABLE_STRING]:
            value_counts = await self._get_value_counts(table_name, column, limit=20)
            return {
                **base_analysis,
                "analysis_type": "categorical",
                "value_counts": value_counts,
                "total_unique": len(value_counts) if value_counts else 0,
                "enum_values": field_def.enum_values,
                "visualization_hint": "bar_chart"
            }
            
        elif field_def.semantics == FieldSemantics.DATE_RANGE:
            date_stats = await self._get_date_statistics(table_name, column)
            return {
                **base_analysis,
                "analysis_type": "date",
                "date_statistics": date_stats,
                "visualization_hint": "timeline"
            }
            
        elif field_def.semantics == FieldSemantics.BOOLEAN:
            bool_counts = await self._get_boolean_distribution(table_name, column)
            return {
                **base_analysis,
                "analysis_type": "boolean",
                "distribution": bool_counts,
                "visualization_hint": "pie_chart"
            }
        
        return base_analysis
    
    # =============================================================================
    # DATABASE QUERY METHODS
    # =============================================================================
    
    async def _get_search_suggestions(self, table_name: str, field_name: str, limit: int = 20) -> List[str]:
        """Get autocomplete suggestions for searchable fields."""
        query = f"""
            SELECT DISTINCT {field_name} as value
            FROM {table_name}
            WHERE {field_name} IS NOT NULL
            ORDER BY {field_name}
            LIMIT %s
        """
        
        try:
            result = await self.db.execute_query(query, (limit,))
            return [row["value"] for row in result if row["value"]]
        except Exception as e:
            logger.error(f"Error getting suggestions for {field_name}: {e}")
            return []
    
    async def _get_categorical_options(self, table_name: str, field_name: str, limit: int = 50) -> List[Dict]:
        """Get options for categorical fields with counts."""
        query = f"""
            SELECT {field_name} as value, COUNT(*) as count
            FROM {table_name}
            WHERE {field_name} IS NOT NULL
            GROUP BY {field_name}
            ORDER BY count DESC, {field_name}
            LIMIT %s
        """
        
        try:
            result = await self.db.execute_query(query, (limit,))
            return [
                {"value": row["value"], "label": str(row["value"]), "count": row["count"]}
                for row in result
            ]
        except Exception as e:
            logger.error(f"Error getting categorical options for {field_name}: {e}")
            return []
    
    async def _get_numeric_range(self, table_name: str, field_name: str) -> Optional[Dict]:
        """Get min/max for numeric fields."""
        query = f"""
            SELECT 
                MIN({field_name}::numeric) as min,
                MAX({field_name}::numeric) as max,
                COUNT({field_name}) as count
            FROM {table_name}
            WHERE {field_name} IS NOT NULL
        """
        
        try:
            result = await self.db.execute_query(query)
            if result and result[0]["count"] > 0:
                return {
                    "min": float(result[0]["min"]),
                    "max": float(result[0]["max"]),
                    "count": result[0]["count"]
                }
        except Exception as e:
            logger.error(f"Error getting numeric range for {field_name}: {e}")
        
        return None
    
    async def _get_date_range(self, table_name: str, field_name: str) -> Optional[Dict]:
        """Get date range for date fields."""
        query = f"""
            SELECT 
                MIN({field_name}) as min,
                MAX({field_name}) as max,
                COUNT({field_name}) as count
            FROM {table_name}
            WHERE {field_name} IS NOT NULL
        """
        
        try:
            result = await self.db.execute_query(query)
            if result and result[0]["count"] > 0:
                return {
                    "min": result[0]["min"].isoformat() if result[0]["min"] else None,
                    "max": result[0]["max"].isoformat() if result[0]["max"] else None,
                    "count": result[0]["count"]
                }
        except Exception as e:
            logger.error(f"Error getting date range for {field_name}: {e}")
        
        return None
    
    def _calculate_step(self, min_max: Dict) -> float:
        """Calculate appropriate step size for numeric range."""
        range_size = min_max["max"] - min_max["min"]
        
        if range_size == 0:
            return 1
        
        # Calculate step as 1% of range, rounded to nice number
        step = range_size / 100
        
        if step < 0.01:
            return 0.01
        elif step < 0.1:
            return 0.1
        elif step < 1:
            return 1
        elif step < 10:
            return 10
        else:
            return round(step, -1)  # Round to nearest 10
    
    # =============================================================================
    # LEGACY FALLBACK METHODS
    # =============================================================================
    
    async def _legacy_get_filters(self, table_name: str) -> List[Dict]:
        """Fallback filter generation for tables without schema definitions."""
        logger.info(f"Using legacy filter generation for {table_name}")
        
        # This would contain the old logic from analytics_service.py
        # Simplified for example
        try:
            query = """
                SELECT column_name, data_type
                FROM information_schema.columns 
                WHERE table_name = %s 
                ORDER BY ordinal_position
                LIMIT 4
            """
            result = await self.db.execute_query(query, (table_name,))
            
            filters = []
            for row in result:
                column_name = row["column_name"]
                data_type = row["data_type"].lower()
                
                if any(t in data_type for t in ['numeric', 'integer', 'double', 'decimal']):
                    # Legacy numeric filter
                    min_max = await self._get_numeric_range(table_name, column_name)
                    if min_max:
                        filters.append({
                            "field": column_name,
                            "type": "numeric_range",
                            "ui_type": "range_input",
                            "label": column_name.title(),
                            "min": min_max["min"],
                            "max": min_max["max"]
                        })
                else:
                    # Legacy categorical filter
                    options = await self._get_categorical_options(table_name, column_name, limit=10)
                    if options:
                        filters.append({
                            "field": column_name,
                            "type": "categorical",
                            "ui_type": "checkbox_list",
                            "label": column_name.title(),
                            "options": options
                        })
            
            return filters
            
        except Exception as e:
            logger.error(f"Legacy filter generation failed for {table_name}: {e}")
            return []
    
    async def _legacy_analyze_column(self, table_name: str, column: str) -> Dict:
        """Fallback column analysis."""
        # This would be the old analyze_column logic
        logger.info(f"Using legacy column analysis for {table_name}.{column}")
        # Simplified implementation
        return {
            "column": column,
            "analysis_type": "legacy",
            "message": "No type definition available"
        }


# =============================================================================
# DEMO/EXAMPLE USAGE
# =============================================================================

async def demo_type_aware_analytics():
    """Demonstrate the difference between typed and untyped analysis."""
    
    print("🎯 TYPE-AWARE ANALYTICS DEMO")
    print("=" * 50)
    
    # Mock database for demo
    class MockDB:
        async def execute_query(self, query, params=None):
            if "MIN" in query and "MAX" in query:
                return [{"min": 10.5, "max": 450.75, "count": 1000}]
            elif "GROUP BY" in query:
                return [
                    {"value": "NYSE", "count": 500},
                    {"value": "NASDAQ", "count": 300},
                    {"value": "AMEX", "count": 200}
                ]
            return []
    
    service = TypeAwareAnalyticsService(MockDB())
    
    # Example 1: Instrument table (typed)
    print("\\n1. TYPED TABLE: dev_instruments")
    print("-" * 30)
    
    filters = await service.get_intelligent_filters("dev_instruments")
    for f in filters:
        print(f"• {f['field']}: {f['type']} ({f['ui_type']}) - Priority {f.get('priority', 0)}")
        if f['type'] == 'categorical' and 'enum_values' in f and f['enum_values']:
            print(f"  Predefined values: {f['enum_values'][:3]}...")
    
    # Example 2: Unknown table (untyped)
    print("\\n2. UNTYPED TABLE: some_unknown_table")
    print("-" * 35)
    
    try:
        filters = await service.get_intelligent_filters("some_unknown_table")
        print(f"Generated {len(filters)} legacy filters")
    except:
        print("Would fall back to legacy filter generation")
    
    print("\\n✨ KEY BENEFITS:")
    print("• symbol: Automatic text search with autocomplete")
    print("• exchange: Automatic dropdown with predefined NYSE/NASDAQ/etc options")
    print("• prices: Automatic range sliders with currency formatting")
    print("• dates: Automatic date range pickers")
    print("• No manual UI coding required!")


if __name__ == "__main__":
    import asyncio
    asyncio.run(demo_type_aware_analytics())