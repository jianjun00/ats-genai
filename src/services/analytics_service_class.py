#!/usr/bin/env python3
"""
Type-aware analytics service for dataset analysis and intelligent EDA.
Fixed version with proper syntax and imports.
"""

import logging
from typing import Dict, Any
from fastapi import HTTPException

# Import type system components
from schema.registry import schema_registry
from schema.types import FieldSemantics

logger = logging.getLogger(__name__)


class AnalyticsService:
    """Type-aware analytics service for dataset analysis and intelligent EDA."""
    
    def __init__(self, db_manager):
        self.db = db_manager
        logger.info("Type-aware analytics service initialized with schema registry")
        logger.info(f"Available schemas: {list(schema_registry.get_schema_summary()['entities'].keys())}")

    async def get_intelligent_filters(self, table_name: str) -> Dict[str, Any]:
        """Generate intelligent filter definitions using type system."""
        try:
            # Get field definitions from schema registry
            filterable_fields = {}
            
            # Try to get schema for this table
            try:
                schema = schema_registry.get_table_schema(table_name)
                entity_name = schema.entity_name
                
                # Get all filterable fields from schema
                for field_name, field_def in schema.fields.items():
                    if field_def.is_filterable:
                        filter_config = {
                            "field_name": field_name,
                            "display_name": field_def.ui_label,
                            "field_type": field_def.field_type.value,
                            "semantics": field_def.semantics.value,
                            "description": field_def.description,
                            "help_text": field_def.ui_help_text,
                            "placeholder": field_def.ui_placeholder,
                            "nullable": field_def.nullable,
                            "eda_priority": field_def.eda_priority
                        }
                        
                        # Add semantic-specific configuration
                        if field_def.semantics == FieldSemantics.SEARCHABLE_STRING:
                            filter_config["ui_component"] = "text_input_with_autocomplete"
                            filter_config["supports_search"] = True
                            
                        elif field_def.semantics == FieldSemantics.CATEGORICAL:
                            filter_config["ui_component"] = "dropdown_or_checkboxes"
                            if field_def.enum_values:
                                filter_config["enum_values"] = field_def.enum_values
                                filter_config["requires_db_query"] = False
                            else:
                                filter_config["requires_db_query"] = True
                                
                        elif field_def.semantics == FieldSemantics.NUMERIC_RANGE:
                            filter_config["ui_component"] = "range_slider"
                            filter_config["supports_range"] = True
                            if field_def.min_value is not None:
                                filter_config["min_value"] = field_def.min_value
                            if field_def.max_value is not None:
                                filter_config["max_value"] = field_def.max_value
                                
                        elif field_def.semantics == FieldSemantics.DATE_RANGE:
                            filter_config["ui_component"] = "date_range_picker"
                            filter_config["supports_range"] = True
                            
                        elif field_def.semantics == FieldSemantics.BOOLEAN:
                            filter_config["ui_component"] = "tri_state_checkbox"
                            filter_config["enum_values"] = [True, False]
                            
                        filterable_fields[field_name] = filter_config
                        
            except ValueError:
                # Table not in schema registry, fall back to database inspection
                logger.info(f"Table {table_name} not in schema registry, using database inspection")
                return await self._get_legacy_filters(table_name)
                
            # Sort by EDA priority
            sorted_fields = dict(sorted(
                filterable_fields.items(), 
                key=lambda x: x[1]["eda_priority"], 
                reverse=True
            ))
            
            return {
                "table_name": table_name,
                "entity_name": entity_name if 'entity_name' in locals() else None,
                "total_filterable_fields": len(sorted_fields),
                "filters": sorted_fields,
                "type_system_enabled": True,
                "performance_optimized": sum(1 for f in sorted_fields.values() if not f.get("requires_db_query", True))
            }
            
        except Exception as e:
            logger.error(f"Error generating intelligent filters for {table_name}: {e}")
            raise HTTPException(500, f"Failed to generate filters: {str(e)}")

    async def _get_legacy_filters(self, table_name: str) -> Dict[str, Any]:
        """Legacy filter generation for tables not in schema registry."""
        try:
            query = """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = %s 
                ORDER BY ordinal_position
            """
            result = await self.db.execute_query(query, (table_name,))
            
            if not result:
                raise HTTPException(404, f"Table {table_name} not found")
                
            legacy_filters = {}
            for row in result:
                column = row["column_name"]
                data_type = row["data_type"].lower()
                
                # Simple type-based classification
                if any(t in data_type for t in ['text', 'character', 'varchar']):
                    ui_component = "text_input"
                elif any(t in data_type for t in ['numeric', 'integer', 'double', 'decimal', 'float']):
                    ui_component = "range_input"
                elif 'boolean' in data_type:
                    ui_component = "checkbox"
                elif any(t in data_type for t in ['date', 'timestamp']):
                    ui_component = "date_picker"
                else:
                    ui_component = "text_input"
                    
                legacy_filters[column] = {
                    "field_name": column,
                    "display_name": column.replace('_', ' ').title(),
                    "field_type": data_type,
                    "ui_component": ui_component,
                    "nullable": row["is_nullable"] == 'YES',
                    "eda_priority": 0,
                    "requires_db_query": True
                }
                
            return {
                "table_name": table_name,
                "entity_name": None,
                "total_filterable_fields": len(legacy_filters),
                "filters": legacy_filters,
                "type_system_enabled": False,
                "performance_optimized": 0
            }
            
        except Exception as e:
            logger.error(f"Error getting legacy filters for {table_name}: {e}")
            raise HTTPException(500, f"Database inspection failed: {str(e)}")

    async def analyze_column(self, table_name: str, column: str, filters: Dict = None) -> Dict:
        """Type-aware analysis of a specific column with optional filters."""
        if filters is None:
            filters = {}
            
        logger.info(f"Type-aware analyzing column {column} in table {table_name} with filters: {filters}")
        
        try:
            # Try to get field definition from schema registry first
            field_def = schema_registry.get_field_definition(table_name, column)
            
            if field_def:
                # Use type system for analysis
                logger.info(f"Using type-aware analysis for {table_name}.{column} (semantics: {field_def.semantics.value})")
                
                if field_def.semantics == FieldSemantics.NUMERIC_RANGE:
                    return await self._analyze_numeric_column(table_name, column, filters)
                elif field_def.semantics in [FieldSemantics.CATEGORICAL, FieldSemantics.SEARCHABLE_STRING, FieldSemantics.BOOLEAN]:
                    return await self._analyze_categorical_column(table_name, column, filters)
                elif field_def.semantics == FieldSemantics.DATE_RANGE:
                    return await self._analyze_date_column(table_name, column, filters)
                else:
                    # Readonly or other - treat as categorical
                    return await self._analyze_categorical_column(table_name, column, filters)
            else:
                # Fall back to legacy database inspection
                logger.info(f"Field {table_name}.{column} not in schema registry, using legacy analysis")
                query = """
                    SELECT data_type 
                    FROM information_schema.columns 
                    WHERE table_name = %s AND column_name = %s
                """
                result = await self.db.execute_query(query, (table_name, column))
                
                if not result:
                    raise HTTPException(404, f"Column {column} not found in table {table_name}")
                    
                data_type = result[0]['data_type'].lower()
                is_numeric = any(t in data_type for t in ['numeric', 'integer', 'double', 'bigint', 'smallint', 'real', 'decimal', 'float'])
                
                if is_numeric:
                    return await self._analyze_numeric_column(table_name, column, filters)
                else:
                    return await self._analyze_categorical_column(table_name, column, filters)
                    
        except Exception as e:
            logger.error(f"Analysis query failed for {table_name}.{column}: {e}")
            raise HTTPException(500, f"Database query failed for {table_name}.{column}: {str(e)}")

    async def _analyze_numeric_column(self, table_name: str, column: str, filters: Dict) -> Dict:
        """Analyze numeric column - returns statistics and histogram."""
        
        # Build WHERE clause for filters
        where_conditions = []
        params = []
        
        for filter_col, filter_values in filters.items():
            if filter_values:
                placeholders = ','.join(['%s'] * len(filter_values))
                where_conditions.append(f"{filter_col} IN ({placeholders})")
                params.extend(filter_values)
        
        where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
        
        # Get basic statistics
        stats_query = f"""
            SELECT 
                COUNT(*) as count,
                AVG({column}::numeric) as mean,
                STDDEV({column}::numeric) as std,
                MIN({column}::numeric) as min,
                MAX({column}::numeric) as max
            FROM {table_name}
            {where_clause}
        """
        
        stats_result = await self.db.execute_query(stats_query, params)
        if not stats_result or not stats_result[0]['count']:
            raise HTTPException(404, f"No data found for column {column} with given filters")
            
        stats = stats_result[0]
        
        # Create histogram with 10 bins
        min_val = float(stats['min'])
        max_val = float(stats['max'])
        
        if min_val == max_val:
            # All values are the same
            return {
                'statistics': {
                    'count': stats['count'],
                    'mean': float(stats['mean']) if stats['mean'] else 0,
                    'std': float(stats['std']) if stats['std'] else 0,
                    'min': min_val,
                    'max': max_val
                },
                'histogram': {
                    'bin_centers': [str(min_val)],
                    'counts': [stats['count']]
                }
            }
        
        bin_width = (max_val - min_val) / 10
        
        histogram_query = f"""
            SELECT 
                FLOOR(({column}::numeric - %s) / %s) as bin_index,
                COUNT(*) as count
            FROM {table_name}
            {where_clause}
            GROUP BY bin_index
            ORDER BY bin_index
        """
        
        hist_params = [min_val, bin_width] + params
        hist_result = await self.db.execute_query(histogram_query, hist_params)
        
        # Create bin labels and counts
        bin_centers = []
        counts = []
        
        for row in hist_result:
            bin_idx = int(row['bin_index'])
            bin_start = min_val + (bin_idx * bin_width)
            bin_end = bin_start + bin_width
            bin_centers.append(f"{bin_start:.1f}-{bin_end:.1f}")
            counts.append(row['count'])
        
        return {
            'statistics': {
                'count': stats['count'],
                'mean': float(stats['mean']) if stats['mean'] else 0,
                'std': float(stats['std']) if stats['std'] else 0,
                'min': min_val,
                'max': max_val
            },
            'histogram': {
                'bin_centers': bin_centers,
                'counts': counts
            }
        }

    async def _analyze_categorical_column(self, table_name: str, column: str, filters: Dict) -> Dict:
        """Analyze categorical column - returns value counts."""
        
        # Build WHERE clause for filters  
        where_conditions = []
        params = []
        
        for filter_col, filter_values in filters.items():
            if filter_values and filter_col != column:  # Don't filter on the column we're analyzing
                placeholders = ','.join(['%s'] * len(filter_values))
                where_conditions.append(f"{filter_col} IN ({placeholders})")
                params.extend(filter_values)
        
        where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
        
        query = f"""
            SELECT {column} as value, COUNT(*) as count
            FROM {table_name}
            {where_clause}
            GROUP BY {column}
            ORDER BY count DESC
            LIMIT 50
        """
        
        result = await self.db.execute_query(query, params)
        
        if not result:
            raise HTTPException(404, f"No data found for column {column} with given filters")
        
        values = [str(row['value']) if row['value'] is not None else 'NULL' for row in result]
        counts = [row['count'] for row in result]
        
        return {
            'value_counts': {
                'values': values,
                'counts': counts
            },
            'total_unique_values': len(values),
            'most_frequent': values[0] if values else None,
            'most_frequent_count': counts[0] if counts else 0
        }

    async def _analyze_date_column(self, table_name: str, column: str, filters: Dict) -> Dict:
        """Analyze date column - returns date range and distribution."""
        
        # Build WHERE clause for filters
        where_conditions = []
        params = []
        
        for filter_col, filter_values in filters.items():
            if filter_values and filter_col != column:
                placeholders = ','.join(['%s'] * len(filter_values))
                where_conditions.append(f"{filter_col} IN ({placeholders})")
                params.extend(filter_values)
        
        where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
        
        # Get date statistics
        stats_query = f"""
            SELECT 
                COUNT(*) as count,
                MIN({column}) as min_date,
                MAX({column}) as max_date
            FROM {table_name}
            {where_clause}
        """
        
        result = await self.db.execute_query(stats_query, params)
        
        if not result or not result[0]['count']:
            raise HTTPException(404, f"No data found for column {column} with given filters")
        
        stats = result[0]
        
        return {
            'date_range': {
                'min_date': str(stats['min_date']) if stats['min_date'] else None,
                'max_date': str(stats['max_date']) if stats['max_date'] else None,
                'total_records': stats['count']
            }
        }