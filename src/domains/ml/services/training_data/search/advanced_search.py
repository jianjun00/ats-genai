#!/usr/bin/env python3
"""
Advanced Search and Filtering for Monthly Training Data
Provides sophisticated search capabilities across training datasets.
"""

from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any
from enum import Enum
import json

class SearchOperator(Enum):
    """Search operators for filtering."""
    EQUALS = "eq"
    NOT_EQUALS = "ne"
    GREATER_THAN = "gt"
    GREATER_EQUAL = "ge"
    LESS_THAN = "lt"
    LESS_EQUAL = "le"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"
    IN = "in"
    NOT_IN = "not_in"
    BETWEEN = "between"
    IS_NULL = "is_null"
    IS_NOT_NULL = "is_not_null"

@dataclass
class SearchFilter:
    """Individual search filter."""
    field: str
    operator: SearchOperator
    value: Any
    case_sensitive: bool = False

@dataclass
class AdvancedSearchQuery:
    """Advanced search query with multiple filters and options."""
    filters: List[SearchFilter]
    logical_operator: str = "AND"  # AND/OR
    sort_by: Optional[str] = None
    sort_direction: str = "DESC"
    limit: int = 100
    offset: int = 0
    include_metadata: bool = True
    include_quality_metrics: bool = True

class TrainingDataSearchEngine:
    """
    Advanced search engine for monthly training data.
    Provides sophisticated filtering, sorting, and aggregation capabilities.
    """

    def __init__(self, environment_type: str = "dev"):
        self.environment_type = environment_type
        self.table_name = f"{environment_type}_monthly_training_data"

        # Field mappings for search
        self.searchable_fields = {
            'symbol': {'type': 'string', 'operators': ['eq', 'ne', 'in', 'not_in', 'contains', 'starts_with']},
            'year_month': {'type': 'date', 'operators': ['eq', 'ne', 'gt', 'ge', 'lt', 'le', 'between']},
            'total_records': {'type': 'int', 'operators': ['eq', 'ne', 'gt', 'ge', 'lt', 'le', 'between']},
            'file_size_mb': {'type': 'float', 'operators': ['eq', 'ne', 'gt', 'ge', 'lt', 'le', 'between']},
            'data_quality_score': {'type': 'float', 'operators': ['eq', 'ne', 'gt', 'ge', 'lt', 'le', 'between']},
            'status': {'type': 'string', 'operators': ['eq', 'ne', 'in', 'not_in']},
            'created_at': {'type': 'timestamp', 'operators': ['eq', 'ne', 'gt', 'ge', 'lt', 'le', 'between']},
            'run_id': {'type': 'int', 'operators': ['eq', 'ne', 'in', 'not_in']},
            'instrument_id': {'type': 'int', 'operators': ['eq', 'ne', 'in', 'not_in', 'is_null', 'is_not_null']},
        }

        # Predefined common searches
        self.common_searches = {
            'high_quality': {
                'name': 'High Quality Datasets',
                'description': 'Datasets with quality score >= 0.9',
                'filters': [SearchFilter('data_quality_score', SearchOperator.GREATER_EQUAL, 0.9)]
            },
            'recent': {
                'name': 'Recent Datasets',
                'description': 'Datasets created in the last 7 days',
                'filters': [SearchFilter('created_at', SearchOperator.GREATER_EQUAL,
                                       datetime.now() - timedelta(days=7))]
            },
            'large_datasets': {
                'name': 'Large Datasets',
                'description': 'Datasets with > 50MB file size',
                'filters': [SearchFilter('file_size_mb', SearchOperator.GREATER_THAN, 50.0)]
            },
            'current_year': {
                'name': 'Current Year',
                'description': 'Datasets from current year',
                'filters': [SearchFilter('year_month', SearchOperator.GREATER_EQUAL,
                                       date(datetime.now().year, 1, 1))]
            },
            'failed_or_processing': {
                'name': 'Failed or Processing',
                'description': 'Datasets that failed or are still processing',
                'filters': [SearchFilter('status', SearchOperator.IN, ['failed', 'processing'])]
            }
        }

    def build_sql_query(self, search_query: AdvancedSearchQuery) -> tuple[str, list]:
        """
        Build SQL query from advanced search parameters.

        Returns:
            Tuple of (SQL query string, parameter values)
        """
        where_clauses = []
        params = []
        param_count = 0

        # Build WHERE clause from filters
        for filter_item in search_query.filters:
            clause, filter_params = self._build_filter_clause(filter_item, param_count)
            if clause:
                where_clauses.append(clause)
                params.extend(filter_params)
                param_count += len(filter_params)

        # Base query
        base_query = f"""
        SELECT
            id, run_id, symbol, instrument_id, year_month,
            timeframe_paths, total_records, file_size_mb,
            data_quality_score, status, error_message,
            created_at, updated_at
        FROM {self.table_name}
        """

        # Add WHERE clause
        if where_clauses:
            logical_op = f" {search_query.logical_operator} "
            where_clause = "WHERE " + logical_op.join(where_clauses)
            base_query += where_clause

        # Add ORDER BY
        if search_query.sort_by:
            base_query += f" ORDER BY {search_query.sort_by} {search_query.sort_direction}"
        else:
            base_query += " ORDER BY created_at DESC"

        # Add LIMIT and OFFSET
        base_query += f" LIMIT ${param_count + 1} OFFSET ${param_count + 2}"
        params.extend([search_query.limit, search_query.offset])

        return base_query, params

    def _build_filter_clause(self, filter_item: SearchFilter, param_offset: int) -> tuple[str, list]:
        """Build SQL clause for a single filter."""
        field = filter_item.field
        operator = filter_item.operator
        value = filter_item.value

        if field not in self.searchable_fields:
            return "", []

        param_num = param_offset + 1
        params = []

        if operator == SearchOperator.EQUALS:
            clause = f"{field} = ${param_num}"
            params = [value]

        elif operator == SearchOperator.NOT_EQUALS:
            clause = f"{field} != ${param_num}"
            params = [value]

        elif operator == SearchOperator.GREATER_THAN:
            clause = f"{field} > ${param_num}"
            params = [value]

        elif operator == SearchOperator.GREATER_EQUAL:
            clause = f"{field} >= ${param_num}"
            params = [value]

        elif operator == SearchOperator.LESS_THAN:
            clause = f"{field} < ${param_num}"
            params = [value]

        elif operator == SearchOperator.LESS_EQUAL:
            clause = f"{field} <= ${param_num}"
            params = [value]

        elif operator == SearchOperator.CONTAINS:
            if filter_item.case_sensitive:
                clause = f"{field} LIKE ${param_num}"
            else:
                clause = f"LOWER({field}) LIKE LOWER(${param_num})"
            params = [f"%{value}%"]

        elif operator == SearchOperator.STARTS_WITH:
            if filter_item.case_sensitive:
                clause = f"{field} LIKE ${param_num}"
            else:
                clause = f"LOWER({field}) LIKE LOWER(${param_num})"
            params = [f"{value}%"]

        elif operator == SearchOperator.ENDS_WITH:
            if filter_item.case_sensitive:
                clause = f"{field} LIKE ${param_num}"
            else:
                clause = f"LOWER({field}) LIKE LOWER(${param_num})"
            params = [f"%{value}"]

        elif operator == SearchOperator.IN:
            if isinstance(value, list):
                placeholders = ",".join([f"${param_num + i}" for i in range(len(value))])
                clause = f"{field} IN ({placeholders})"
                params = value
            else:
                clause = f"{field} = ${param_num}"
                params = [value]

        elif operator == SearchOperator.NOT_IN:
            if isinstance(value, list):
                placeholders = ",".join([f"${param_num + i}" for i in range(len(value))])
                clause = f"{field} NOT IN ({placeholders})"
                params = value
            else:
                clause = f"{field} != ${param_num}"
                params = [value]

        elif operator == SearchOperator.BETWEEN:
            if isinstance(value, list) and len(value) == 2:
                clause = f"{field} BETWEEN ${param_num} AND ${param_num + 1}"
                params = value
            else:
                return "", []

        elif operator == SearchOperator.IS_NULL:
            clause = f"{field} IS NULL"
            params = []

        elif operator == SearchOperator.IS_NOT_NULL:
            clause = f"{field} IS NOT NULL"
            params = []

        else:
            return "", []

        return clause, params

    def build_aggregation_query(self, group_by: str, aggregation_type: str = "count") -> str:
        """
        Build aggregation query for analytics.

        Args:
            group_by: Field to group by (symbol, year_month, status, etc.)
            aggregation_type: Type of aggregation (count, sum, avg, min, max)
        """
        if group_by not in self.searchable_fields:
            raise ValueError(f"Invalid group_by field: {group_by}")

        agg_fields = {
            'count': 'COUNT(*) as count',
            'sum_records': 'SUM(total_records) as total_records',
            'avg_quality': 'AVG(data_quality_score) as avg_quality_score',
            'sum_size': 'SUM(file_size_mb) as total_size_mb',
            'min_date': 'MIN(created_at) as earliest_created',
            'max_date': 'MAX(created_at) as latest_created'
        }

        if aggregation_type == "all":
            select_fields = ", ".join(agg_fields.values())
        else:
            select_fields = agg_fields.get(aggregation_type, agg_fields['count'])

        query = f"""
        SELECT
            {group_by},
            {select_fields}
        FROM {self.table_name}
        GROUP BY {group_by}
        ORDER BY {group_by}
        """

        return query

    def get_search_suggestions(self, partial_query: str, field: str) -> List[str]:
        """
        Get search suggestions for autocomplete.

        Args:
            partial_query: Partial search text
            field: Field to search in

        Returns:
            List of matching suggestions
        """
        if field not in self.searchable_fields:
            return []

        # Mock suggestions - in real implementation would query database
        suggestions_map = {
            'symbol': ['AAPL', 'TSLA', 'MSFT', 'GOOGL', 'AMZN', 'NVDA'],
            'status': ['created', 'processing', 'completed', 'failed'],
        }

        suggestions = suggestions_map.get(field, [])

        if partial_query:
            # Filter suggestions based on partial query
            filtered = [s for s in suggestions if partial_query.upper() in s.upper()]
            return filtered[:10]  # Limit to 10 suggestions

        return suggestions[:10]

    def get_field_statistics(self, field: str) -> Dict[str, Any]:
        """
        Get statistical information about a field for search assistance.

        Returns min/max values, common values, etc.
        """
        if field not in self.searchable_fields:
            return {}

        # Mock statistics - in real implementation would query database
        field_stats = {
            'total_records': {
                'min': 100,
                'max': 50000,
                'avg': 15000,
                'common_ranges': ['1k-5k', '5k-10k', '10k-25k', '25k+']
            },
            'file_size_mb': {
                'min': 0.5,
                'max': 250.0,
                'avg': 45.0,
                'common_ranges': ['0-10MB', '10-50MB', '50-100MB', '100MB+']
            },
            'data_quality_score': {
                'min': 0.0,
                'max': 1.0,
                'avg': 0.87,
                'common_ranges': ['0.9-1.0 (Excellent)', '0.8-0.9 (Good)', '0.7-0.8 (Fair)', '<0.7 (Poor)']
            }
        }

        return field_stats.get(field, {})

    def export_search_results(self, search_query: AdvancedSearchQuery, format: str = 'json') -> str:
        """
        Export search results in various formats.

        Args:
            search_query: Search query to execute
            format: Export format ('json', 'csv', 'sql')
        """
        # Build query
        sql_query, params = self.build_sql_query(search_query)

        if format == 'sql':
            # Return the SQL query for manual execution
            return f"-- Generated SQL Query\n{sql_query}\n-- Parameters: {params}"

        elif format == 'json':
            # Return JSON structure for API consumption
            export_config = {
                'query': {
                    'sql': sql_query,
                    'parameters': params
                },
                'filters': [
                    {
                        'field': f.field,
                        'operator': f.operator.value,
                        'value': f.value,
                        'case_sensitive': f.case_sensitive
                    }
                    for f in search_query.filters
                ],
                'options': {
                    'logical_operator': search_query.logical_operator,
                    'sort_by': search_query.sort_by,
                    'sort_direction': search_query.sort_direction,
                    'limit': search_query.limit,
                    'offset': search_query.offset
                }
            }
            return json.dumps(export_config, indent=2, default=str)

        elif format == 'csv':
            # Return CSV headers for result export
            headers = [
                'id', 'run_id', 'symbol', 'instrument_id', 'year_month',
                'total_records', 'file_size_mb', 'data_quality_score',
                'status', 'created_at', 'updated_at'
            ]
            return f"# CSV Export Configuration\nHeaders: {','.join(headers)}\nSQL: {sql_query}"

        return "Unsupported format"

def demo_advanced_search():
    """Demonstrate advanced search capabilities."""
    search_engine = TrainingDataSearchEngine()

    print("🔍 Advanced Training Data Search Engine Demo")
    print("=" * 50)

    # Demo 1: Complex search query
    search_query = AdvancedSearchQuery(
        filters=[
            SearchFilter('symbol', SearchOperator.IN, ['AAPL', 'TSLA', 'MSFT']),
            SearchFilter('data_quality_score', SearchOperator.GREATER_EQUAL, 0.85),
            SearchFilter('total_records', SearchOperator.BETWEEN, [5000, 50000]),
            SearchFilter('status', SearchOperator.EQUALS, 'completed')
        ],
        logical_operator="AND",
        sort_by="data_quality_score",
        sort_direction="DESC",
        limit=50
    )

    sql_query, params = search_engine.build_sql_query(search_query)
    print(f"📋 Generated SQL Query:")
    print(sql_query)
    print(f"\n📊 Parameters: {params}")

    # Demo 2: Common searches
    print(f"\n🔖 Common Searches Available:")
    for key, search_info in search_engine.common_searches.items():
        print(f"  • {search_info['name']}: {search_info['description']}")

    # Demo 3: Field statistics
    print(f"\n📈 Field Statistics:")
    for field in ['total_records', 'file_size_mb', 'data_quality_score']:
        stats = search_engine.get_field_statistics(field)
        if stats:
            print(f"  • {field}: min={stats.get('min')}, max={stats.get('max')}, avg={stats.get('avg')}")

    # Demo 4: Search suggestions
    print(f"\n💡 Search Suggestions:")
    for field in ['symbol', 'status']:
        suggestions = search_engine.get_search_suggestions('', field)
        print(f"  • {field}: {suggestions}")

    # Demo 5: Export options
    print(f"\n📤 Export Example (JSON):")
    export_json = search_engine.export_search_results(search_query, 'json')
    print(export_json[:300] + "..." if len(export_json) > 300 else export_json)

if __name__ == "__main__":
    demo_advanced_search()