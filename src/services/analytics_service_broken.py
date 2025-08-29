#!/usr/bin/env python3
"""
Analytics service - TYPE-AWARE VERSION
Provides dataset analysis and EDA functionality with intelligent type-driven filtering.
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any
from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse
from urllib.parse import urlparse, parse_qs
from decimal import Decimal

# Import type system components
from schema.registry import schema_registry
from schema.types import FieldSemantics, FieldType

logger = logging.getLogger(__name__)

class AnalyticsService:
    """Type-aware analytics service for dataset analysis and intelligent EDA."""
    
    def __init__(self, db_manager):
        self.db = db_manager
        logger.info("Type-aware analytics service initialized with schema registry")
        logger.info(f"Available schemas: {list(schema_registry.get_schema_summary()['entities'].keys())}")

    async def get_intelligent_filters(self, table_name: str) -> Dict[str, Any]:
        \"\"\"Generate intelligent filter definitions using type system.\"\"\"
        try:\n            # Get field definitions from schema registry\n            filterable_fields = {}\n            \n            # Try to get schema for this table\n            try:\n                schema = schema_registry.get_table_schema(table_name)\n                entity_name = schema.entity_name\n                \n                # Get all filterable fields from schema\n                for field_name, field_def in schema.fields.items():\n                    if field_def.is_filterable:\n                        filter_config = {\n                            \"field_name\": field_name,\n                            \"display_name\": field_def.ui_label,\n                            \"field_type\": field_def.field_type.value,\n                            \"semantics\": field_def.semantics.value,\n                            \"description\": field_def.description,\n                            \"help_text\": field_def.ui_help_text,\n                            \"placeholder\": field_def.ui_placeholder,\n                            \"nullable\": field_def.nullable,\n                            \"eda_priority\": field_def.eda_priority\n                        }\n                        \n                        # Add semantic-specific configuration\n                        if field_def.semantics == FieldSemantics.SEARCHABLE_STRING:\n                            filter_config[\"ui_component\"] = \"text_input_with_autocomplete\"\n                            filter_config[\"supports_search\"] = True\n                            \n                        elif field_def.semantics == FieldSemantics.CATEGORICAL:\n                            filter_config[\"ui_component\"] = \"dropdown_or_checkboxes\"\n                            if field_def.enum_values:\n                                filter_config[\"enum_values\"] = field_def.enum_values\n                                filter_config[\"requires_db_query\"] = False\n                            else:\n                                filter_config[\"requires_db_query\"] = True\n                                \n                        elif field_def.semantics == FieldSemantics.NUMERIC_RANGE:\n                            filter_config[\"ui_component\"] = \"range_slider\"\n                            filter_config[\"supports_range\"] = True\n                            if field_def.min_value is not None:\n                                filter_config[\"min_value\"] = field_def.min_value\n                            if field_def.max_value is not None:\n                                filter_config[\"max_value\"] = field_def.max_value\n                                \n                        elif field_def.semantics == FieldSemantics.DATE_RANGE:\n                            filter_config[\"ui_component\"] = \"date_range_picker\"\n                            filter_config[\"supports_range\"] = True\n                            \n                        elif field_def.semantics == FieldSemantics.BOOLEAN:\n                            filter_config[\"ui_component\"] = \"tri_state_checkbox\"\n                            filter_config[\"enum_values\"] = [True, False]\n                            \n                        filterable_fields[field_name] = filter_config\n                        \n            except ValueError:\n                # Table not in schema registry, fall back to database inspection\n                logger.info(f\"Table {table_name} not in schema registry, using database inspection\")\n                return await self._get_legacy_filters(table_name)\n                \n            # Sort by EDA priority\n            sorted_fields = dict(sorted(\n                filterable_fields.items(), \n                key=lambda x: x[1][\"eda_priority\"], \n                reverse=True\n            ))\n            \n            return {\n                \"table_name\": table_name,\n                \"entity_name\": entity_name if 'entity_name' in locals() else None,\n                \"total_filterable_fields\": len(sorted_fields),\n                \"filters\": sorted_fields,\n                \"type_system_enabled\": True,\n                \"performance_optimized\": sum(1 for f in sorted_fields.values() if not f.get(\"requires_db_query\", True))\n            }\n            \n        except Exception as e:\n            logger.error(f\"Error generating intelligent filters for {table_name}: {e}\")\n            raise HTTPException(500, f\"Failed to generate filters: {str(e)}\")\n\n    async def _get_legacy_filters(self, table_name: str) -> Dict[str, Any]:\n        \"\"\"Legacy filter generation for tables not in schema registry.\"\"\"\n        try:\n            query = \"\"\"\n                SELECT column_name, data_type, is_nullable\n                FROM information_schema.columns \n                WHERE table_name = %s \n                ORDER BY ordinal_position\n            \"\"\"\n            result = await self.db.execute_query(query, (table_name,))\n            \n            if not result:\n                raise HTTPException(404, f\"Table {table_name} not found\")\n                \n            legacy_filters = {}\n            for row in result:\n                column = row[\"column_name\"]\n                data_type = row[\"data_type\"].lower()\n                \n                # Simple type-based classification\n                if any(t in data_type for t in ['text', 'character', 'varchar']):\n                    ui_component = \"text_input\"\n                elif any(t in data_type for t in ['numeric', 'integer', 'double', 'decimal', 'float']):\n                    ui_component = \"range_input\"\n                elif 'boolean' in data_type:\n                    ui_component = \"checkbox\"\n                elif any(t in data_type for t in ['date', 'timestamp']):\n                    ui_component = \"date_picker\"\n                else:\n                    ui_component = \"text_input\"\n                    \n                legacy_filters[column] = {\n                    \"field_name\": column,\n                    \"display_name\": column.replace('_', ' ').title(),\n                    \"field_type\": data_type,\n                    \"ui_component\": ui_component,\n                    \"nullable\": row[\"is_nullable\"] == 'YES',\n                    \"eda_priority\": 0,\n                    \"requires_db_query\": True\n                }\n                \n            return {\n                \"table_name\": table_name,\n                \"entity_name\": None,\n                \"total_filterable_fields\": len(legacy_filters),\n                \"filters\": legacy_filters,\n                \"type_system_enabled\": False,\n                \"performance_optimized\": 0\n            }\n            \n        except Exception as e:\n            logger.error(f\"Error getting legacy filters for {table_name}: {e}\")\n            raise HTTPException(500, f\"Database inspection failed: {str(e)}\")\n\n    async def analyze_column(self, table_name: str, column: str, filters: Dict = None) -> Dict:
        """Analyze a specific column with optional filters."""
        if filters is None:
            filters = {}
            
        logger.info(f"Analyzing column {column} in table {table_name} with filters: {filters}")
        
        try:
            # Get column info to determine if it's numeric
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
            LIMIT 20
        """
        
        result = await self.db.execute_query(query, params)
        if not result:
            raise HTTPException(404, f"No data found for column {column} with given filters")
        
        values = [row['value'] for row in result]
        counts = [row['count'] for row in result]
        
        return {
            'value_counts': {
                'values': values,
                'counts': counts
            }
        }

    async def get_column_values(self, table_name: str, column: str, limit: int = 100) -> Dict:
        """Get unique values for a categorical column or min/max for numeric columns."""
        
        try:
            # Get column type
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
                # For numeric columns, return min/max for range filtering
                query = f"SELECT MIN({column}::numeric) as min_val, MAX({column}::numeric) as max_val FROM {table_name}"
                result = await self.db.execute_query(query)
                
                if not result or result[0]['min_val'] is None:
                    raise HTTPException(404, f"No data found in column {column}")
                
                return {
                    "column": column,
                    "data_type": "numeric",
                    "min_value": float(result[0]['min_val']),
                    "max_value": float(result[0]['max_val'])
                }
            else:
                # For categorical columns, return unique values with counts
                query = f"""
                    SELECT {column} as value, COUNT(*) as count
                    FROM {table_name} 
                    WHERE {column} IS NOT NULL
                    GROUP BY {column}
                    ORDER BY count DESC
                    LIMIT %s
                """
                result = await self.db.execute_query(query, (limit,))
                
                if not result:
                    raise HTTPException(404, f"No data found in column {column}")
                
                return {
                    "column": column,
                    "data_type": "categorical",
                    "values": [{"value": row['value'], "count": row['count']} for row in result]
                }
                
        except Exception as e:
            logger.error(f"Failed to get column values for {table_name}.{column}: {e}")
            raise HTTPException(500, f"Failed to get column values for {table_name}.{column}: {str(e)}")

    async def get_filtered_data(self, table_name: str, filters: Dict = None, page: int = 1, page_size: int = 50) -> Dict:
        """Get paginated data with applied filters."""
        
        if filters is None:
            filters = {}
            
        try:
            # Build WHERE clause
            where_conditions = []
            params = []
            
            for column, values in filters.items():
                if values:
                    if isinstance(values, dict) and 'min' in values and 'max' in values:
                        # Numeric range filter
                        where_conditions.append(f"{column} BETWEEN %s AND %s")
                        params.extend([values['min'], values['max']])
                    elif isinstance(values, list):
                        # Categorical filter
                        placeholders = ','.join(['%s'] * len(values))
                        where_conditions.append(f"{column} IN ({placeholders})")
                        params.extend(values)
            
            where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
            
            # Get total count
            count_query = f"SELECT COUNT(*) as total FROM {table_name} {where_clause}"
            count_result = await self.db.execute_query(count_query, params)
            total_count = count_result[0]['total'] if count_result else 0
            
            if total_count == 0:
                return {
                    "data": [],
                    "pagination": {
                        "page": page,
                        "page_size": page_size,
                        "total_count": 0,
                        "total_pages": 0
                    }
                }
            
            # Get paginated data
            offset = (page - 1) * page_size
            data_query = f"""
                SELECT * FROM {table_name} 
                {where_clause}
                ORDER BY 1
                LIMIT %s OFFSET %s
            """
            
            data_params = params + [page_size, offset]
            data_result = await self.db.execute_query(data_query, data_params)
            
            # Convert Decimal objects to floats for JSON serialization
            cleaned_data = []
            for row in data_result:
                cleaned_row = {}
                for key, value in row.items():
                    if isinstance(value, Decimal):
                        cleaned_row[key] = float(value)
                    else:
                        cleaned_row[key] = value
                cleaned_data.append(cleaned_row)
            
            total_pages = (total_count + page_size - 1) // page_size
            
            return {
                "data": cleaned_data,
                "pagination": {
                    "page": page,
                    "page_size": page_size,
                    "total_count": total_count,
                    "total_pages": total_pages
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting filtered data: {e}")
            raise HTTPException(500, f"Database query failed for filtered data from {table_name}: {str(e)}")

    def get_available_datasets(self) -> List[Dict]:
        """Get list of available datasets for EDA."""
        return [
            {
                'name': 'dev_daily_prices_polygon_30year',
                'display_name': '📊 Polygon Daily Prices (Best for Analysis)', 
                'row_count': 6845978,
                'column_count': 7,
                'vendor': 'Polygon', 
                'data_type': 'prices'
            },
            {
                'name': 'dev_daily_prices_tiingo',
                'display_name': '📊 Tiingo Daily Prices (Best for Analysis)',
                'row_count': 6559540,
                'column_count': 7,
                'vendor': 'Tiingo',
                'data_type': 'prices'
            },
            {
                'name': 'dev_instruments',
                'display_name': 'All Instruments (Consolidated) - Metadata Only',
                'row_count': 69796,
                'column_count': 16,
                'vendor': 'ATS',
                'data_type': 'instruments'
            },
            {
                'name': 'dev_instrument_tiingo',
                'display_name': 'Tiingo Instruments - Metadata Only',
                'row_count': 28080,
                'column_count': 11,
                'vendor': 'Tiingo',
                'data_type': 'instruments'
            },
            {
                'name': 'dev_daily_prices_eodhd',
                'display_name': '📊 EODHD Daily Prices (Best for Analysis)',
                'row_count': 727905,
                'column_count': 7,
                'vendor': 'EODHD',
                'data_type': 'prices'
            },
            {
                'name': 'dev_instrument_polygon',
                'display_name': 'Polygon Instruments - Metadata Only', 
                'row_count': 15000,
                'column_count': 16,
                'vendor': 'Polygon',
                'data_type': 'instruments'
            }
        ]
    
    async def get_dataset_schema(self, table_name: str) -> Dict:
        """Get schema for a specific dataset from database."""
        try:
            query = """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns 
                WHERE table_name = %s 
                ORDER BY ordinal_position
            """
            result = await self.db.execute_query(query, (table_name,))
            
            if not result:
                raise HTTPException(404, f"Table {table_name} not found or has no accessible columns")
            
            return {
                "columns": [
                    {
                        "column_name": row["column_name"],
                        "data_type": row["data_type"],
                        "is_nullable": row["is_nullable"]
                    }
                    for row in result
                ]
            }
            
        except Exception as e:
            logger.error(f"Error getting schema for {table_name}: {e}")
            raise HTTPException(500, f"Failed to get schema for {table_name}: {str(e)}")

    async def get_eda_page(self, request: Request) -> HTMLResponse:
        """Serve the EDA interface page with proper error handling."""
        
        html_content = '''
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Dataset EDA - Exploratory Data Analysis</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                body {
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                    margin: 0;
                    padding: 20px;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: #333;
                    min-height: 100vh;
                }
                
                .container {
                    max-width: 1400px;
                    margin: 0 auto;
                    background: white;
                    border-radius: 15px;
                    padding: 30px;
                    box-shadow: 0 20px 40px rgba(0,0,0,0.1);
                }
                
                h1 {
                    color: #2c3e50;
                    text-align: center;
                    margin-bottom: 30px;
                    font-size: 2.5em;
                    text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
                }
                
                .controls {
                    background: #f8f9fa;
                    padding: 25px;
                    border-radius: 10px;
                    margin-bottom: 30px;
                    border: 2px solid #e9ecef;
                }
                
                .control-group {
                    margin-bottom: 20px;
                }
                
                label {
                    display: block;
                    margin-bottom: 8px;
                    font-weight: 600;
                    color: #495057;
                }
                
                select {
                    width: 100%;
                    padding: 12px;
                    border: 2px solid #ced4da;
                    border-radius: 6px;
                    font-size: 16px;
                    background: white;
                    transition: border-color 0.3s;
                }
                
                select:focus {
                    outline: none;
                    border-color: #007bff;
                    box-shadow: 0 0 0 3px rgba(0,123,255,0.25);
                }
                
                .analysis-container {
                    display: grid;
                    grid-template-columns: 1fr 2fr;
                    gap: 30px;
                    margin-top: 30px;
                }
                
                .filters-container {
                    background: #f8f9fa;
                    padding: 25px;
                    border-radius: 10px;
                    border: 2px solid #e9ecef;
                    height: fit-content;
                }
                
                .distributions-container {
                    display: grid;
                    gap: 25px;
                }
                
                .column-distribution {
                    background: white;
                    padding: 20px;
                    border-radius: 10px;
                    border: 1px solid #dee2e6;
                    box-shadow: 0 4px 6px rgba(0,0,0,0.05);
                }
                
                .distribution-chart {
                    height: 300px;
                    margin-bottom: 15px;
                }
                
                .distribution-stats {
                    display: flex;
                    justify-content: space-around;
                    flex-wrap: wrap;
                    background: #f8f9fa;
                    padding: 15px;
                    border-radius: 8px;
                    border: 1px solid #e9ecef;
                }
                
                .distribution-stat {
                    text-align: center;
                    margin: 5px;
                }
                
                .stat-value-small {
                    font-size: 1.4em;
                    font-weight: bold;
                    color: #007bff;
                }
                
                .stat-label-small {
                    font-size: 0.9em;
                    color: #6c757d;
                    margin-top: 2px;
                }
                
                .filter-group {
                    margin-bottom: 20px;
                    padding: 15px;
                    background: white;
                    border-radius: 8px;
                    border: 1px solid #dee2e6;
                }
                
                .checkbox-list {
                    max-height: 200px;
                    overflow-y: auto;
                    border: 1px solid #ced4da;
                    border-radius: 4px;
                    padding: 10px;
                    background: white;
                }
                
                .checkbox-list label {
                    margin-bottom: 8px;
                    font-weight: normal;
                }
                
                .data-table-container {
                    margin-top: 30px;
                    background: white;
                    border-radius: 10px;
                    border: 1px solid #dee2e6;
                    overflow: hidden;
                }
                
                .data-table {
                    width: 100%;
                    border-collapse: collapse;
                }
                
                .data-table th,
                .data-table td {
                    padding: 12px;
                    text-align: left;
                    border-bottom: 1px solid #dee2e6;
                }
                
                .data-table th {
                    background: #f8f9fa;
                    font-weight: 600;
                    color: #495057;
                }
                
                .pagination {
                    padding: 20px;
                    text-align: center;
                    background: #f8f9fa;
                    border-top: 1px solid #dee2e6;
                }
                
                .pagination button {
                    margin: 0 5px;
                    padding: 8px 16px;
                    border: 1px solid #ced4da;
                    background: white;
                    border-radius: 4px;
                    cursor: pointer;
                    transition: all 0.3s;
                }
                
                .pagination button:hover:not(:disabled) {
                    background: #007bff;
                    color: white;
                    border-color: #007bff;
                }
                
                .pagination button:disabled {
                    opacity: 0.6;
                    cursor: not-allowed;
                }
                
                .error-message {
                    background: #f8d7da;
                    color: #721c24;
                    padding: 15px;
                    border-radius: 8px;
                    border: 1px solid #f5c6cb;
                    margin: 15px 0;
                }
                
                .loading-message {
                    background: #d4edda;
                    color: #155724;
                    padding: 15px;
                    border-radius: 8px;
                    border: 1px solid #c3e6cb;
                    margin: 15px 0;
                    text-align: center;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🔍 Dataset Explorer & Analysis</h1>
                
                <div class="controls">
                    <div class="control-group">
                        <label for="dataset-select">Select Dataset:</label>
                        <select id="dataset-select">
                            <option value="">Choose a dataset...</option>
                        </select>
                    </div>
                </div>
                
                <div class="analysis-container">
                    <div class="filters-container">
                        <h3>Filters</h3>
                        <div id="filters-controls">
                            <p>Select a dataset to see available filters</p>
                        </div>
                        <button onclick="applyFilters()" style="margin-top: 15px; padding: 10px 20px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer;">Apply Filters</button>
                        <button onclick="clearFilters()" style="margin-top: 15px; margin-left: 10px; padding: 10px 20px; background: #6c757d; color: white; border: none; border-radius: 4px; cursor: pointer;">Clear Filters</button>
                    </div>
                    
                    <div class="distributions-container" id="distributions-container">
                        <p>Select a dataset to see column distributions</p>
                    </div>
                </div>
                
                <div id="data-table-container" class="data-table-container" style="display: none;">
                    <table class="data-table" id="data-table">
                        <thead id="table-headers"></thead>
                        <tbody id="table-body"></tbody>
                    </table>
                    <div class="pagination" id="pagination-controls"></div>
                </div>
            </div>

            <script>
                let currentFilters = {};
                let currentDataset = '';
                let currentPage = 1;
                const pageSize = 10;

                // Load datasets on page load
                document.addEventListener('DOMContentLoaded', async function() {
                    await loadDatasets();
                });

                async function loadDatasets() {
                    try {
                        const response = await fetch('/api/eda/datasets');
                        const datasets = await response.json();
                        
                        const select = document.getElementById('dataset-select');
                        select.innerHTML = '<option value="">Choose a dataset...</option>';
                        
                        datasets.forEach(dataset => {
                            const option = document.createElement('option');
                            option.value = dataset.name;
                            option.textContent = `${dataset.display_name} (${dataset.row_count.toLocaleString()} rows)`;
                            select.appendChild(option);
                        });
                        
                        select.addEventListener('change', async function() {
                            if (this.value) {
                                currentDataset = this.value;
                                await loadDatasetAnalysis();
                            } else {
                                clearAnalysis();
                            }
                        });
                        
                    } catch (error) {
                        console.error('Error loading datasets:', error);
                        document.getElementById('distributions-container').innerHTML = 
                            '<div class="error-message">Failed to load datasets. Please check database connection.</div>';
                    }
                }

                async function loadDatasetAnalysis() {
                    if (!currentDataset) return;
                    
                    document.getElementById('distributions-container').innerHTML = 
                        '<div class="loading-message">Loading column analysis...</div>';
                    document.getElementById('filters-controls').innerHTML = 
                        '<div class="loading-message">Loading filters...</div>';
                    
                    try {
                        // Get schema first
                        const schemaResponse = await fetch(`/api/eda/datasets/${currentDataset}/schema`);
                        if (!schemaResponse.ok) {
                            throw new Error(`Failed to load schema: ${schemaResponse.status}`);
                        }
                        const schema = await schemaResponse.json();
                        
                        // Load filters and distributions in parallel for better performance
                        const filterPromise = loadFiltersForDataset(currentDataset, schema.columns);
                        const distributionPromise = loadAllColumnDistributions(currentDataset, schema.columns);
                        
                        await Promise.allSettled([filterPromise, distributionPromise]);
                        
                    } catch (error) {
                        console.error('Error loading dataset analysis:', error);
                        document.getElementById('distributions-container').innerHTML = 
                            `<div class="error-message">Failed to load analysis: ${error.message}</div>`;
                        document.getElementById('filters-controls').innerHTML = 
                            `<div class="error-message">Failed to load filters: ${error.message}</div>`;
                    }
                }

                async function loadFiltersForDataset(datasetName, columns) {
                    const filterControls = document.getElementById('filters-controls');
                    let combinedFilterHtml = '';
                    
                    // Limit to first 4 columns for better performance
                    const columnsForFilters = columns.slice(0, 4);
                    
                    for (const col of columnsForFilters) {
                        const dataType = col.data_type.toLowerCase();
                        const isNumeric = dataType.includes('numeric') || dataType.includes('integer') || 
                            dataType.includes('double') || dataType.includes('bigint') ||
                            dataType.includes('smallint') || dataType.includes('real') ||
                            dataType.includes('decimal') || dataType.includes('float');
                        
                        try {
                            const response = await fetch(`/api/eda/datasets/${datasetName}/columns/${col.column_name}/values?limit=10`);
                            const columnData = await response.json();
                            
                            if (columnData.error) {
                                console.warn(`Error loading filter for ${col.column_name}:`, columnData.error);
                                continue;
                            }
                            
                            if (isNumeric && columnData.min_value !== undefined) {
                                combinedFilterHtml += `
                                    <div class="filter-group">
                                        <label>${col.column_name} (numeric):</label>
                                        <input type="range" id="filter-${col.column_name}-min" 
                                               min="${columnData.min_value}" max="${columnData.max_value}" 
                                               value="${columnData.min_value}" step="0.01">
                                        <input type="range" id="filter-${col.column_name}-max" 
                                               min="${columnData.min_value}" max="${columnData.max_value}" 
                                               value="${columnData.max_value}" step="0.01">
                                        <span id="filter-${col.column_name}-display">${columnData.min_value} - ${columnData.max_value}</span>
                                    </div>
                                `;
                            } else if (columnData.values && columnData.values.length > 0) {
                                let checkboxHtml = '';
                                columnData.values.slice(0, 8).forEach(item => {
                                    checkboxHtml += `<label><input type="checkbox" name="filter-${col.column_name}" value="${item.value}"> ${item.value} (${item.count})</label><br>`;
                                });
                                
                                combinedFilterHtml += `
                                    <div class="filter-group">
                                        <label>${col.column_name} (categorical):</label>
                                        <div class="checkbox-list">
                                            ${checkboxHtml}
                                        </div>
                                    </div>
                                `;
                            }
                        } catch (error) {
                            console.error(`Error loading filter for ${col.column_name}:`, error);
                        }
                    }
                    
                    if (!combinedFilterHtml) {
                        combinedFilterHtml = '<div class="error-message">No filters could be loaded. Database may be unavailable.</div>';
                    }
                    
                    filterControls.innerHTML = combinedFilterHtml;
                }
                
                async function loadAllColumnDistributions(datasetName, columns) {
                    const distributionsContainer = document.getElementById('distributions-container');
                    distributionsContainer.innerHTML = '';
                    
                    // Limit to first 6 columns for better performance
                    const columnsToAnalyze = columns.slice(0, 6);
                    
                    // Create all containers first for immediate feedback
                    const distributionPromises = [];
                    
                    for (const col of columnsToAnalyze) {
                        const dataType = col.data_type.toLowerCase();
                        const isNumeric = dataType.includes('numeric') || dataType.includes('integer') || 
                            dataType.includes('double') || dataType.includes('bigint') ||
                            dataType.includes('smallint') || dataType.includes('real') ||
                            dataType.includes('decimal') || dataType.includes('float');
                        
                        // Create container for this column's distribution
                        const colDiv = document.createElement('div');
                        colDiv.className = 'column-distribution';
                        colDiv.innerHTML = `
                            <h4>${col.column_name} (${isNumeric ? 'Numeric' : 'Categorical'})</h4>
                            <div id="chart-${col.column_name}" class="distribution-chart">Loading...</div>
                            <div id="stats-${col.column_name}" class="distribution-stats"></div>
                        `;
                        distributionsContainer.appendChild(colDiv);
                        
                        // Add to parallel loading promises
                        if (isNumeric) {
                            distributionPromises.push(
                                loadNumericDistribution(datasetName, col.column_name).catch(error => {
                                    console.error(`Error loading numeric distribution for ${col.column_name}:`, error);
                                    document.getElementById(`chart-${col.column_name}`).innerHTML = 
                                        `<div class="error-message">Failed to load distribution: ${error.message}</div>`;
                                    return null;
                                })
                            );
                        } else {
                            distributionPromises.push(
                                loadCategoricalDistribution(datasetName, col.column_name).catch(error => {
                                    console.error(`Error loading categorical distribution for ${col.column_name}:`, error);
                                    document.getElementById(`chart-${col.column_name}`).innerHTML = 
                                        `<div class="error-message">Failed to load distribution: ${error.message}</div>`;
                                    return null;
                                })
                            );
                        }
                    }
                    
                    if (columns.length > 6) {
                        const moreDiv = document.createElement('div');
                        moreDiv.innerHTML = `<p style="text-align: center; color: #666; font-style: italic;">
                            Showing first 6 columns (${columns.length - 6} more columns available) - Loading in parallel...
                        </p>`;
                        distributionsContainer.appendChild(moreDiv);
                    }
                    
                    // Load all distributions in parallel
                    await Promise.allSettled(distributionPromises);
                    
                    // Update status when done
                    if (columns.length > 6) {
                        const statusDiv = distributionsContainer.querySelector('p');
                        if (statusDiv) {
                            statusDiv.innerHTML = `<p style="text-align: center; color: #666; font-style: italic;">
                                Showing first 6 columns (${columns.length - 6} more columns available)
                            </p>`;
                        }
                    }
                }
                
                async function loadNumericDistribution(datasetName, columnName) {
                    const statsContainer = document.getElementById(`stats-${columnName}`);
                    const chartContainer = document.getElementById(`chart-${columnName}`);
                    
                    try {
                        const response = await fetch('/api/eda/analyze', {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({
                                dataset_name: datasetName,
                                column: columnName,
                                filters: {}
                            })
                        });
                        
                        const analysis = await response.json();
                        
                        if (analysis.error) {
                            throw new Error(analysis.error);
                        }
                        
                        // Update statistics
                        if (analysis.statistics) {
                            statsContainer.innerHTML = `
                                <div class="distribution-stat">
                                    <div class="stat-value-small">${analysis.statistics.count.toLocaleString()}</div>
                                    <div class="stat-label-small">Count</div>
                                </div>
                                <div class="distribution-stat">
                                    <div class="stat-value-small">${analysis.statistics.mean.toFixed(2)}</div>
                                    <div class="stat-label-small">Mean</div>
                                </div>
                                <div class="distribution-stat">
                                    <div class="stat-value-small">${analysis.statistics.std.toFixed(2)}</div>
                                    <div class="stat-label-small">Std Dev</div>
                                </div>
                                <div class="distribution-stat">
                                    <div class="stat-value-small">${analysis.statistics.min.toFixed(2)}</div>
                                    <div class="stat-label-small">Min</div>
                                </div>
                                <div class="distribution-stat">
                                    <div class="stat-value-small">${analysis.statistics.max.toFixed(2)}</div>
                                    <div class="stat-label-small">Max</div>
                                </div>
                            `;
                        }
                        
                        // Update chart
                        if (analysis.histogram) {
                            const trace = {
                                x: analysis.histogram.bin_centers,
                                y: analysis.histogram.counts,
                                type: 'bar',
                                name: columnName,
                                marker: { color: '#3498db' }
                            };
                            
                            const layout = {
                                title: `Distribution: ${columnName}`,
                                xaxis: { title: columnName },
                                yaxis: { title: 'Frequency' },
                                bargap: 0.1,
                                margin: { l: 60, r: 20, t: 40, b: 60 }
                            };
                            
                            Plotly.newPlot(`chart-${columnName}`, [trace], layout, {responsive: true});
                        }
                        
                    } catch (error) {
                        console.error(`Error loading numeric distribution for ${columnName}:`, error);
                        chartContainer.innerHTML = `<div class="error-message">Error: ${error.message}</div>`;
                        statsContainer.innerHTML = `<div class="error-message">Statistics unavailable</div>`;
                        throw error;
                    }
                }
                
                async function loadCategoricalDistribution(datasetName, columnName) {
                    const chartContainer = document.getElementById(`chart-${columnName}`);
                    
                    try {
                        const response = await fetch('/api/eda/analyze', {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({
                                dataset_name: datasetName,
                                column: columnName,
                                filters: {}
                            })
                        });
                        
                        const analysis = await response.json();
                        
                        if (analysis.error) {
                            throw new Error(analysis.error);
                        }
                        
                        if (analysis.value_counts) {
                            const trace = {
                                x: analysis.value_counts.values,
                                y: analysis.value_counts.counts,
                                type: 'bar',
                                name: columnName,
                                marker: { color: '#e74c3c' }
                            };
                            
                            const layout = {
                                title: `Distribution: ${columnName}`,
                                xaxis: { title: columnName },
                                yaxis: { title: 'Count' },
                                bargap: 0.1,
                                margin: { l: 60, r: 20, t: 40, b: 60 }
                            };
                            
                            Plotly.newPlot(`chart-${columnName}`, [trace], layout, {responsive: true});
                        }
                        
                    } catch (error) {
                        console.error(`Error loading categorical distribution for ${columnName}:`, error);
                        chartContainer.innerHTML = `<div class="error-message">Error: ${error.message}</div>`;
                        throw error;
                    }
                }

                function clearAnalysis() {
                    document.getElementById('distributions-container').innerHTML = '<p>Select a dataset to see column distributions</p>';
                    document.getElementById('filters-controls').innerHTML = '<p>Select a dataset to see available filters</p>';
                    document.getElementById('data-table-container').style.display = 'none';
                    currentFilters = {};
                    currentDataset = '';
                }

                async function applyFilters() {
                    if (!currentDataset) {
                        alert('Please select a dataset first');
                        return;
                    }
                    
                    // Collect filter values
                    currentFilters = {};
                    
                    // Get all filter inputs
                    const filterInputs = document.querySelectorAll('[name^="filter-"], [id^="filter-"]');
                    
                    filterInputs.forEach(input => {
                        const columnName = input.name ? input.name.replace('filter-', '') : input.id.split('-')[1];
                        
                        if (input.type === 'checkbox' && input.checked) {
                            if (!currentFilters[columnName]) {
                                currentFilters[columnName] = [];
                            }
                            currentFilters[columnName].push(input.value);
                        } else if (input.type === 'range') {
                            // Handle numeric range filters
                            const isMin = input.id.includes('-min');
                            if (!currentFilters[columnName]) {
                                currentFilters[columnName] = {};
                            }
                            if (isMin) {
                                currentFilters[columnName].min = parseFloat(input.value);
                            } else {
                                currentFilters[columnName].max = parseFloat(input.value);
                            }
                        }
                    });
                    
                    // Load filtered data
                    currentPage = 1;
                    await loadFilteredData();
                }

                async function loadFilteredData() {
                    try {
                        const response = await fetch(`/api/eda/datasets/${currentDataset}/data`, {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({
                                filters: currentFilters,
                                page: currentPage,
                                page_size: pageSize
                            })
                        });
                        
                        const result = await response.json();
                        
                        if (result.error) {
                            throw new Error(result.error);
                        }
                        
                        displayDataTable(result.data, result.pagination);
                        
                    } catch (error) {
                        console.error('Error loading filtered data:', error);
                        document.getElementById('data-table-container').innerHTML = 
                            `<div class="error-message">Failed to load data: ${error.message}</div>`;
                        document.getElementById('data-table-container').style.display = 'block';
                    }
                }

                function displayDataTable(data, pagination) {
                    const container = document.getElementById('data-table-container');
                    const headers = document.getElementById('table-headers');
                    const tbody = document.getElementById('table-body');
                    const paginationControls = document.getElementById('pagination-controls');
                    
                    if (!data || data.length === 0) {
                        container.innerHTML = '<div class="error-message">No data matches the selected filters</div>';
                        container.style.display = 'block';
                        return;
                    }
                    
                    // Create headers
                    const headerRow = document.createElement('tr');
                    Object.keys(data[0]).forEach(key => {
                        const th = document.createElement('th');
                        th.textContent = key;
                        headerRow.appendChild(th);
                    });
                    headers.innerHTML = '';
                    headers.appendChild(headerRow);
                    
                    // Create data rows
                    tbody.innerHTML = '';
                    data.forEach(row => {
                        const tr = document.createElement('tr');
                        Object.values(row).forEach(value => {
                            const td = document.createElement('td');
                            td.textContent = value !== null && value !== undefined ? value : '';
                            tr.appendChild(td);
                        });
                        tbody.appendChild(tr);
                    });
                    
                    // Create pagination controls
                    let paginationHtml = `
                        <span>Page ${pagination.page} of ${pagination.total_pages} (${pagination.total_count.toLocaleString()} total records)</span><br><br>
                    `;
                    
                    if (pagination.page > 1) {
                        paginationHtml += '<button onclick="changePage(1)">First</button>';
                        paginationHtml += `<button onclick="changePage(${pagination.page - 1})">Previous</button>`;
                    }
                    
                    if (pagination.page < pagination.total_pages) {
                        paginationHtml += `<button onclick="changePage(${pagination.page + 1})">Next</button>`;
                        paginationHtml += `<button onclick="changePage(${pagination.total_pages})">Last</button>`;
                    }
                    
                    paginationControls.innerHTML = paginationHtml;
                    container.style.display = 'block';
                }

                async function changePage(newPage) {
                    currentPage = newPage;
                    await loadFilteredData();
                }

                function clearFilters() {
                    // Clear all checkboxes
                    document.querySelectorAll('[name^="filter-"]').forEach(input => {
                        if (input.type === 'checkbox') {
                            input.checked = false;
                        }
                    });
                    
                    // Reset range inputs to their min/max values
                    document.querySelectorAll('[id^="filter-"]').forEach(input => {
                        if (input.type === 'range') {
                            if (input.id.includes('-min')) {
                                input.value = input.min;
                            } else {
                                input.value = input.max;
                            }
                        }
                    });
                    
                    currentFilters = {};
                    document.getElementById('data-table-container').style.display = 'none';
                }
            </script>
        </body>
        </html>
        '''
        
        return HTMLResponse(content=html_content)