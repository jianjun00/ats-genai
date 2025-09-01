#!/usr/bin/env python3
"""
ATS Analytics Service - External Script for Kubernetes
Provides web-based analytics dashboard for 30-year price database
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler, ThreadingHTTPServer
import sys
import os
from typing import Dict, List, Optional
import numpy as np
import time

# Configure logging first
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add the src directory to the path for imports
sys.path.insert(0, '/workspace/src')
from core.database.connection_manager import get_connection_manager
from core.config.settings import get_settings

# Ray EDA integration for massive dataset analysis
try:
    from services.ray_eda_engine import get_ray_eda_service, RayEDAService
    RAY_AVAILABLE = True
    logger.info("✅ Ray EDA engine loaded - distributed computing enabled")
except ImportError as e:
    RAY_AVAILABLE = False
    logger.warning(f"⚠️ Ray EDA engine not available: {e}. Falling back to traditional methods")

# Dataset metadata cache - expires after 4 hours
DATASET_CACHE = {
    'data': None,
    'timestamp': 0,
    'ttl': 4 * 60 * 60  # 4 hours in seconds
}

def get_cached_datasets(job_manager):
    """Get cached dataset metadata or refresh if expired."""
    current_time = time.time()
    
    # Check if cache is valid
    if (DATASET_CACHE['data'] is not None and 
        current_time - DATASET_CACHE['timestamp'] < DATASET_CACHE['ttl']):
        logger.debug("📋 Using cached dataset metadata")
        return DATASET_CACHE['data']
    
    # Cache miss or expired - refresh data
    logger.info("🔄 Refreshing dataset metadata cache...")
    try:
        datasets = job_manager.get_datasets()
        DATASET_CACHE['data'] = datasets
        DATASET_CACHE['timestamp'] = current_time
        logger.info(f"✅ Cached {len(datasets)} datasets (expires in {DATASET_CACHE['ttl']//3600}h)")
        return datasets
    except Exception as e:
        logger.error(f"❌ Failed to refresh dataset cache: {e}")
        # Return stale cache if available
        if DATASET_CACHE['data'] is not None:
            logger.warning("⚠️ Using stale dataset cache due to refresh failure")
            return DATASET_CACHE['data']
        raise

class JobManager:
    """Job management functionality for analytics service using centralized connection manager."""
    
    def __init__(self):
        self.db_manager = get_connection_manager()
        self.settings = get_settings()
        
    def initialize(self):
        """Initialize database connection using centralized manager."""
        try:
            # Test the centralized connection
            if self.db_manager.check_connection():
                logger.info("✅ Database connection established via centralized manager")
            else:
                logger.warning("⚠️ Database connection check failed")
        except Exception as e:
            logger.warning(f"Database initialization failed: {e}")
    
    def get_job_stats(self) -> Dict:
        """Get job statistics using centralized connection manager."""
        try:
            from core.database.connection_manager import get_raw_connection
            
            with get_raw_connection() as conn:
                with conn.cursor() as cursor:
                    table_name = self.settings.get_table_name("runs")
                    cursor.execute(f"""
                        SELECT 
                            COUNT(*) as total_jobs,
                            COUNT(CASE WHEN status = 'running' THEN 1 END) as running_jobs,
                            COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_jobs,
                            COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_jobs,
                            COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending_jobs
                        FROM {table_name}
                    """)
                    
                    result = cursor.fetchone()
                    
                    return {
                        "total_jobs": result['total_jobs'],
                        "running_jobs": result['running_jobs'],
                        "completed_jobs": result['completed_jobs'],
                        "failed_jobs": result['failed_jobs'],
                        "pending_jobs": result['pending_jobs']
                    }
                
        except Exception as e:
            logger.error(f"Database error getting job stats: {e}")
            return {"error": str(e)}
    
    def get_recent_jobs(self, limit: int = 10) -> List[Dict]:
        """Get recent jobs using centralized connection manager."""
        try:
            from core.database.connection_manager import get_raw_connection
            
            with get_raw_connection() as conn:
                with conn.cursor() as cursor:
                    table_name = self.settings.get_table_name("runs")
                    cursor.execute(f"""
                        SELECT id, run_type, status, start_time, end_time, created_by, 
                               created_at, error_message, parameters
                        FROM {table_name}
                        ORDER BY created_at DESC
                        LIMIT %s
                    """, (limit,))
                    
                    jobs = cursor.fetchall()
                    
                    result = []
                    for job in jobs:
                        duration = None
                        if job['start_time'] and job['end_time']:
                            duration = int((job['end_time'] - job['start_time']).total_seconds())
                        
                        result.append({
                            "job_id": str(job['id']),
                            "job_type": job['run_type'],
                            "status": job['status'],
                            "user_id": job['created_by'] or 'system',
                            "start_time": job['start_time'].isoformat() if job['start_time'] else None,
                            "end_time": job['end_time'].isoformat() if job['end_time'] else None,
                            "duration_seconds": duration,
                            "created_at": job['created_at'].isoformat() if job['created_at'] else None,
                            "error_message": job['error_message'],
                            "parameters": job['parameters'] or {}
                        })
                    
                    return result
                
        except Exception as e:
            logger.error(f"Database error getting recent jobs: {e}")
            return []
    
    def get_collection_status(self) -> Dict:
        """Get REAL collection job status from actual running processes."""
        import subprocess
        import os
        
        def check_real_process_status(log_path: str, process_name: str) -> Dict:
            """Check if a real process is running based on log activity."""
            status = {
                "status": "inactive",
                "last_activity": None,
                "records": 0
            }
            
            try:
                if os.path.exists(log_path):
                    # Check log modification time
                    log_mtime = os.path.getmtime(log_path)
                    last_activity = datetime.fromtimestamp(log_mtime)
                    status['last_activity'] = last_activity.isoformat()
                    
                    # Consider active if modified within last 5 minutes
                    minutes_ago = (datetime.now() - last_activity).total_seconds() / 60
                    if minutes_ago < 5:
                        status['status'] = 'running'
                    elif minutes_ago < 60:
                        status['status'] = 'recent'
                    
                    # Try to extract record count from logs
                    try:
                        result = subprocess.run(['tail', '-50', log_path], 
                                              capture_output=True, text=True, timeout=3)
                        log_lines = result.stdout
                        
                        # Look for record counts
                        for line in reversed(log_lines.split('\n')):
                            if 'records' in line.lower():
                                # Extract numbers from line
                                import re
                                numbers = re.findall(r'(\d+(?:,\d+)*)', line)
                                if numbers:
                                    # Take the largest number found
                                    record_count = max(int(n.replace(',', '')) for n in numbers)
                                    status['records'] = record_count
                                    break
                    except:
                        pass
                        
            except Exception as e:
                logger.debug(f"Error checking {process_name}: {e}")
            
            return status
        
        # Check REAL collection processes
        real_jobs = {
            "price_backfills": {
                "polygon_30y": check_real_process_status("/tmp/polygon_30year_daily_backfill.log", "Polygon 30Y"),
                "tiingo_30y": check_real_process_status("/tmp/tiingo_30year_backfill.log", "Tiingo 30Y"),
                "eodhd_30y": check_real_process_status("/tmp/eodhd_30year_backfill.log", "EODHD 30Y"),
            },
            "events_collection": {
                "polygon": check_real_process_status("/tmp/polygon_earnings_fixed.log", "Polygon Events"),
                "eodhd": check_real_process_status("/tmp/eodhd_events.log", "EODHD Events"),
                "tiingo": check_real_process_status("/tmp/tiingo_events.log", "Tiingo Events"),
            },
            "minute_data": {
                "polygon": check_real_process_status("/tmp/polygon_minute_backfill.log", "Polygon Minutes"),
            },
            "last_updated": datetime.now().isoformat()
        }
        
        return real_jobs
    
    def get_datasets(self) -> List[Dict]:
        """Get available datasets for EDA analysis from real database."""
        try:
            from core.database.connection_manager import get_raw_connection
            
            with get_raw_connection() as conn:
                with conn.cursor() as cursor:
                    # Get table prefix based on environment
                    import os
                    environment = os.getenv('ENVIRONMENT', 'dev')
                    table_prefix = f"{environment}_%"
                    
                    # Get actual table information from database
                    cursor.execute("""
                        SELECT 
                            schemaname, tablename,
                            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
                        FROM pg_tables 
                        WHERE schemaname = 'public' 
                        AND tablename LIKE %s
                        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
                    """, (table_prefix,))
                    
                    tables = cursor.fetchall()
                    datasets = []
                    
                    for table in tables:
                        table_name = table['tablename']
                        # Get row count for each table
                        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                        row_count = cursor.fetchone()['count']
                        
                        # Get column count
                        cursor.execute(f"""
                            SELECT COUNT(*) 
                            FROM information_schema.columns 
                            WHERE table_name = %s AND table_schema = 'public'
                        """, (table_name,))
                        column_count = cursor.fetchone()['count']
                        
                        datasets.append({
                            'name': table_name,
                            'display_name': table_name.replace('_', ' ').title(),
                            'row_count': row_count,
                            'column_count': column_count,
                            'size': table['size']
                        })
                    
                    return datasets
                    
        except Exception as e:
            logger.error(f"Failed to get datasets from database: {e}")
            raise Exception(f"Database error getting datasets: {e}")
    
    def get_dataset_schema(self, table_name: str) -> Dict:
        """Get schema for a specific dataset from real database."""
        try:
            from core.database.connection_manager import get_raw_connection
            
            with get_raw_connection() as conn:
                with conn.cursor() as cursor:
                    # Get actual column information from database
                    cursor.execute("""
                        SELECT column_name, data_type, is_nullable, column_default
                        FROM information_schema.columns 
                        WHERE table_name = %s AND table_schema = 'public'
                        ORDER BY ordinal_position
                    """, (table_name,))
                    
                    columns = cursor.fetchall()
                    if not columns:
                        raise Exception(f"Table '{table_name}' not found")
                    
                    schema = {
                        'table_name': table_name,
                        'columns': []
                    }
                    
                    for col in columns:
                        schema['columns'].append({
                            'name': col['column_name'],
                            'type': col['data_type'],
                            'nullable': col['is_nullable'] == 'YES',
                            'default': col['column_default']
                        })
                    
                    return schema
                    
        except Exception as e:
            logger.error(f"Failed to get schema for table {table_name}: {e}")
            raise Exception(f"Database error getting schema for {table_name}: {e}")
    
    def analyze_column_distribution(self, table_name: str, column: str, filters: dict = {}) -> Dict:
        """Analyze distribution of a column with optional filters."""
        try:
            from core.database.connection_manager import get_raw_connection
            
            with get_raw_connection() as conn:
                with conn.cursor() as cursor:
                    # Build query
                    query = f"SELECT {column} FROM {table_name} WHERE {column} IS NOT NULL"
                    params = []
                    
                    # Add filters if provided
                    if filters:
                        for key, value in filters.items():
                            if isinstance(value, str):
                                query += f" AND {key} = %s"
                                params.append(value)
                            else:
                                query += f" AND {key} = %s"
                                params.append(value)
                    
                    # Limit for performance
                    query += " LIMIT 10000"
                    
                    cursor.execute(query, params)
                    results = cursor.fetchall()
                    
                    if not results:
                        return {'error': 'No data found'}
                    
                    values = [row[column] for row in results if row[column] is not None]
                    
                    if not values:
                        return {'error': 'No valid values found'}
                    
                    # Calculate statistics
                    values_array = np.array(values)
                    stats = {
                        'count': len(values),
                        'mean': float(np.mean(values_array)),
                        'median': float(np.median(values_array)),
                        'std': float(np.std(values_array)),
                        'min': float(np.min(values_array)),
                        'max': float(np.max(values_array)),
                        'q25': float(np.percentile(values_array, 25)),
                        'q75': float(np.percentile(values_array, 75))
                    }
                    
                    # Create histogram
                    hist, bin_edges = np.histogram(values_array, bins=20)
                    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
                    
                    return {
                        'statistics': stats,
                        'histogram': {
                            'counts': hist.tolist(),
                            'bin_centers': bin_centers.tolist(),
                            'bin_edges': bin_edges.tolist()
                        },
                        'column': column,
                        'table': table_name
                    }
                    
        except Exception as e:
            logger.error(f"Analysis query failed for {table_name}.{column}: {e}")
            raise Exception(f"Database error analyzing column {column} in table {table_name}: {e}")
    
    def get_column_values(self, table_name: str, column: str, limit: int = 100) -> Dict:
        """Get unique values for a categorical column or min/max for numeric columns."""
        try:
            from core.database.connection_manager import get_raw_connection
            
            with get_raw_connection() as conn:
                with conn.cursor() as cursor:
                    # First check if column is numeric
                    schema = self.get_dataset_schema(table_name)
                    if "error" in schema:
                        return {"error": schema["error"]}
                    
                    column_info = next((col for col in schema["columns"] if col["name"] == column), None)
                    if not column_info:
                        return {"error": f"Column {column} not found in {table_name}"}
                    
                    data_type = column_info["type"].lower()
                    is_numeric = any(t in data_type for t in ["numeric", "integer", "double", "bigint", "smallint", "real", "decimal", "float"])
                    
                    if is_numeric:
                        # Get min/max for numeric columns
                        cursor.execute(f"""
                            SELECT 
                                MIN({column}) as min_value,
                                MAX({column}) as max_value,
                                COUNT(DISTINCT {column}) as distinct_count,
                                COUNT({column}) as total_count
                            FROM {table_name} 
                            WHERE {column} IS NOT NULL
                        """)
                        result = cursor.fetchone()
                        
                        return {
                            "column": column,
                            "data_type": "numeric",
                            "min_value": float(result['min_value']) if result['min_value'] is not None else None,
                            "max_value": float(result['max_value']) if result['max_value'] is not None else None,
                            "distinct_count": result['distinct_count'],
                            "total_count": result['total_count']
                        }
                    else:
                        # Get unique values for categorical columns  
                        # Handle ENUM types by only checking for NOT NULL
                        
                        # For symbol columns, ensure we include popular symbols like TSLA
                        if column.lower() == 'symbol':
                            # Prioritize popular symbols using priority field
                            popular_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NFLX', 'NVDA', 'UBER', 'SPOT']
                            
                            # Get symbols with priority indicator
                            popular_placeholders = ', '.join(['%s'] * len(popular_symbols))
                            cursor.execute(f"""
                                WITH prioritized_symbols AS (
                                    SELECT 
                                        {column} as value, 
                                        COUNT(*) as count,
                                        CASE WHEN {column} IN ({popular_placeholders}) THEN 1 ELSE 0 END as priority
                                    FROM {table_name}
                                    WHERE {column} IS NOT NULL
                                    GROUP BY {column}
                                )
                                SELECT value, count
                                FROM prioritized_symbols
                                ORDER BY priority DESC, count DESC
                                LIMIT %s
                            """, popular_symbols + [limit])
                        else:
                            cursor.execute(f"""
                                SELECT {column} as value, COUNT(*) as count
                                FROM {table_name}
                                WHERE {column} IS NOT NULL
                                GROUP BY {column}
                                ORDER BY count DESC
                                LIMIT %s
                            """, (limit,))
                        
                        results = cursor.fetchall()
                        values = [{"value": row['value'], "count": row['count']} for row in results]
                        
                        return {
                            "column": column,
                            "data_type": "categorical",
                            "values": values,
                            "total_unique": len(values)
                        }
                        
        except Exception as e:
            logger.error(f"Failed to get column values for {table_name}.{column}: {e}")
            raise Exception(f"Database error getting column values for {column} in table {table_name}: {e}")
    
    def get_filtered_data(self, table_name: str, filters: Dict = {}, page: int = 1, page_size: int = 50) -> Dict:
        """Get paginated data with applied filters."""
        try:
            from core.database.connection_manager import get_raw_connection
            
            with get_raw_connection() as conn:
                with conn.cursor() as cursor:
                    # Build WHERE clause from filters
                    where_conditions = []
                    params = []
                    
                    for column, filter_config in filters.items():
                        if filter_config.get('type') in ['categorical', 'values'] and filter_config.get('values'):
                            # Categorical filter - IN clause
                            placeholders = ', '.join(['%s'] * len(filter_config['values']))
                            where_conditions.append(f"{column} IN ({placeholders})")
                            params.extend(filter_config['values'])
                            
                        elif filter_config.get('type') == 'numeric':
                            # Numeric range filter
                            if 'min' in filter_config and filter_config['min'] is not None:
                                where_conditions.append(f"{column} >= %s")
                                params.append(filter_config['min'])
                            if 'max' in filter_config and filter_config['max'] is not None:
                                where_conditions.append(f"{column} <= %s")
                                params.append(filter_config['max'])
                    
                    # Build base query
                    where_clause = ""
                    if where_conditions:
                        where_clause = "WHERE " + " AND ".join(where_conditions)
                    
                    # Get total count
                    count_query = f"SELECT COUNT(*) as total FROM {table_name} {where_clause}"
                    cursor.execute(count_query, params)
                    total_count = cursor.fetchone()['total']
                    
                    # Get paginated data
                    offset = (page - 1) * page_size
                    data_query = f"""
                        SELECT * FROM {table_name} 
                        {where_clause}
                        ORDER BY 1
                        LIMIT %s OFFSET %s
                    """
                    
                    cursor.execute(data_query, params + [page_size, offset])
                    rows = cursor.fetchall()
                    
                    # Convert rows to list of dicts
                    data = []
                    for row in rows:
                        row_dict = {}
                        for column_name in row.keys():
                            value = row[column_name]
                            # Format values for display
                            if isinstance(value, (int, float)):
                                row_dict[column_name] = value
                            elif value is not None:
                                row_dict[column_name] = str(value)
                            else:
                                row_dict[column_name] = None
                        data.append(row_dict)
                    
                    total_pages = (total_count + page_size - 1) // page_size
                    
                    return {
                        "data": data,
                        "pagination": {
                            "current_page": page,
                            "page_size": page_size,
                            "total_count": total_count,
                            "total_pages": total_pages,
                            "has_next": page < total_pages,
                            "has_prev": page > 1
                        },
                        "filters_applied": filters,
                        "table_name": table_name
                    }
                    
        except Exception as e:
            logger.error(f"Failed to get filtered data for {table_name}: {e}")
            raise Exception(f"Database error getting filtered data for table {table_name}: {e}")
    
    def format_display_name(self, table_name: str) -> str:
        """Format table name for display."""
        parts = table_name.replace('dev_', '').split('_')
        return ' '.join(word.title() for word in parts)
    
    def extract_vendor(self, table_name: str) -> str:
        """Extract vendor from table name."""
        if 'tiingo' in table_name:
            return 'Tiingo'
        elif 'polygon' in table_name:
            return 'Polygon'
        elif 'eodhd' in table_name:
            return 'EODHD'
        return 'Unknown'
    
    def get_timeseries_data(self, table_name: str, y_column: str, x_column: str) -> Dict:
        """Get time-series data for charting."""
        try:
            from core.database.connection_manager import get_raw_connection
            
            with get_raw_connection() as conn:
                with conn.cursor() as cursor:
                    # Check column types to determine aggregation strategy
                    cursor.execute("""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_name = %s AND column_name IN (%s, %s)
                    """, (table_name, y_column, x_column))
                    
                    column_info = {row[0]: row[1] for row in cursor.fetchall()}
                    
                    # Determine Y-axis aggregation based on column type
                    y_data_type = column_info.get(y_column, '').lower()
                    is_numeric = any(t in y_data_type for t in ['numeric', 'integer', 'double', 'bigint', 'real', 'decimal', 'float'])
                    
                    if is_numeric:
                        # For numeric columns, aggregate with AVG
                        cursor.execute(f"""
                            SELECT DATE({x_column}) as date_val, AVG({y_column}) as avg_val
                            FROM {table_name} 
                            WHERE {x_column} IS NOT NULL AND {y_column} IS NOT NULL
                            GROUP BY DATE({x_column})
                            ORDER BY DATE({x_column})
                            LIMIT 1000
                        """)
                        
                        data = cursor.fetchall()
                        return {
                            'type': 'numeric',
                            'x_column': x_column,
                            'y_column': y_column,
                            'data': [{'x': str(row[0]), 'y': float(row[1])} for row in data],
                            'y_label': f'Average {y_column}',
                            'chart_type': 'line'
                        }
                    else:
                        # For categorical columns, count occurrences
                        cursor.execute(f"""
                            SELECT DATE({x_column}) as date_val, {y_column}, COUNT(*) as count_val
                            FROM {table_name} 
                            WHERE {x_column} IS NOT NULL AND {y_column} IS NOT NULL
                            GROUP BY DATE({x_column}), {y_column}
                            ORDER BY DATE({x_column}), COUNT(*) DESC
                            LIMIT 1000
                        """)
                        
                        data = cursor.fetchall()
                        
                        # Group by date and aggregate counts
                        date_totals = {}
                        for row in data:
                            date_str = str(row[0])
                            if date_str not in date_totals:
                                date_totals[date_str] = 0
                            date_totals[date_str] += int(row[2])
                        
                        return {
                            'type': 'categorical',
                            'x_column': x_column,
                            'y_column': y_column,
                            'data': [{'x': date, 'y': total} for date, total in sorted(date_totals.items())],
                            'y_label': f'Count of {y_column}',
                            'chart_type': 'bar'
                        }
                        
        except Exception as e:
            logger.error(f"Error getting time-series data: {e}")
            return {"error": str(e)}

# Initialize global job manager
job_manager = JobManager()

class AnalyticsHandler(BaseHTTPRequestHandler):
    
    def should_use_ray_for_table(self, table_name: str) -> bool:
        """Determine if Ray should be used for large tables"""
        large_tables = [
            'dev_daily_prices_eodhd',     # 4.4GB
            'dev_daily_prices_tiingo',    # 3.6GB  
            'dev_daily_prices_polygon',   # 250MB
            'dev_financial_events',       # 359MB
            'dev_news_polygon'            # 236MB
        ]
        return table_name in large_tables
    
    async def analyze_column_with_ray(self, dataset_name: str, column: str, filters: dict) -> dict:
        """Analyze column using Ray distributed computing"""
        try:
            ray_service = get_ray_eda_service()
            
            # Get column metadata
            column_info = await self.get_column_metadata(dataset_name, column)
            if not column_info:
                return {"error": f"Column {column} not found in {dataset_name}"}
            
            # Use Ray to analyze the column
            columns_list = [{'column_name': column, 'data_type': column_info['data_type']}]
            
            async for result in ray_service.analyze_dataset_columns(dataset_name, columns_list, max_columns=1):
                if result['result'].statistics.get('error'):
                    return {"error": result['result'].statistics['error']}
                
                # Convert Ray result to expected API format
                ray_result = result['result']
                
                response = {
                    "column": column,
                    "data_type": ray_result.data_type,
                    "sample_size": ray_result.sample_size,
                    "computation_time": ray_result.computation_time,
                    "distributed_analysis": True
                }
                
                if ray_result.statistics:
                    response["statistics"] = ray_result.statistics
                
                if ray_result.histogram:
                    # Convert Ray histogram format to expected format
                    bins = ray_result.histogram.get('bins', [])
                    if bins and isinstance(bins, list) and len(bins) > 0:
                        # Handle different histogram formats
                        if isinstance(bins[0], dict):
                            # Bin is a dictionary with bucket and count
                            response["histogram"] = {
                                "bin_centers": [b.get('bucket', 0) for b in bins],
                                "counts": [b.get('count', 0) for b in bins]
                            }
                        else:
                            # Bin is a simple value or tuple
                            response["histogram"] = {
                                "bin_centers": list(range(len(bins))),
                                "counts": [int(b) if isinstance(b, (int, float)) else 0 for b in bins]
                            }
                
                if ray_result.top_values:
                    response["top_values"] = ray_result.top_values[:10]  # Limit to top 10
                
                return response
        
        except Exception as e:
            logger.error(f"Ray analysis failed for {column}: {e}")
            return {"error": f"Ray analysis failed: {str(e)}"}
    
    async def get_column_metadata(self, table_name: str, column_name: str) -> dict:
        """Get column metadata from database"""
        try:
            # Use centralized raw connection
            from core.database.connection_manager import get_raw_connection
            
            with get_raw_connection() as conn:
                from psycopg2.extras import RealDictCursor
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT column_name, data_type, is_nullable
                        FROM information_schema.columns 
                        WHERE table_name = %s AND column_name = %s
                    """, (table_name, column_name))
                    
                    result = cursor.fetchone()
                    if result:
                        return {
                            'column_name': result['column_name'],
                            'data_type': result['data_type'], 
                            'is_nullable': result['is_nullable']
                        }
        except Exception as e:
            logger.error(f"Error getting column metadata: {e}")
        
        return None
    
    async def get_column_values_with_ray(self, table_name: str, column_name: str, limit: int) -> dict:
        """Get column values using Ray for massive datasets"""
        try:
            ray_service = get_ray_eda_service()
            
            # Skip metadata lookup - infer data type from column name or assume mixed
            # Common numeric columns in financial data
            numeric_columns = ['close', 'open', 'high', 'low', 'volume', 'adjclose', 'price', 'amount']
            is_numeric = any(col in column_name.lower() for col in numeric_columns)
            
            # Use Ray to analyze the column for values - let Ray determine the data type
            data_type = 'double precision' if is_numeric else 'text'
            columns_list = [{'column_name': column_name, 'data_type': data_type}]
            
            async for result in ray_service.analyze_dataset_columns(table_name, columns_list, max_columns=1):
                ray_result = result['result']
                
                if ray_result.statistics.get('error'):
                    return {"error": ray_result.statistics['error']}
                
                if is_numeric:
                    # Return numeric range info
                    stats = ray_result.statistics
                    return {
                        "min_value": stats.get('min_val', 0),
                        "max_value": stats.get('max_val', 1), 
                        "distinct_count": stats.get('count', 0),
                        "total_count": stats.get('count', 0),
                        "data_type": "numeric",
                        "ray_powered": True
                    }
                else:
                    # Return categorical values
                    values = []
                    if ray_result.top_values:
                        values = ray_result.top_values[:limit]  # Limit results
                    
                    stats = ray_result.statistics
                    return {
                        "values": values,
                        "total_unique": stats.get('unique_count', len(values)),
                        "data_type": "categorical", 
                        "ray_powered": True
                    }
        
        except Exception as e:
            logger.error(f"Ray column values failed for {column_name}: {e}")
            return {"error": f"Ray analysis failed: {str(e)}"}
    
    def get_training_datasets(self):
        """Get training datasets from database for dual-tab functionality"""
        try:
            from core.database.connection_manager import get_raw_connection
            
            # Determine table name based on environment (plural form)
            import os
            environment = os.getenv('ENVIRONMENT', 'dev')
            table_name = f"{environment}_training_datasets"
            
            with get_raw_connection() as conn:
                from psycopg2.extras import RealDictCursor
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(f"""
                        SELECT id, dataset_name, total_sequences, feature_count, label_count,
                               data_quality_score, feature_completeness, label_completeness,
                               file_size_mb, technical_indicators, symbols, 
                               date_range_start, date_range_end, created_at
                        FROM {table_name} 
                        ORDER BY created_at DESC 
                        LIMIT 20
                    """)
                    
                    results = cursor.fetchall()
                    
                    datasets = []
                    for row in results:
                        dataset = {
                            'id': row['id'],
                            'dataset_name': row['dataset_name'],
                            'total_sequences': row['total_sequences'] or 0,
                            'feature_count': row['feature_count'] or 0,
                            'label_count': row['label_count'] or 0,
                            'data_quality_score': float(row['data_quality_score']) if row['data_quality_score'] else 0.0,
                            'feature_completeness': float(row['feature_completeness']) if row['feature_completeness'] else 0.0,
                            'label_completeness': float(row['label_completeness']) if row['label_completeness'] else 0.0,
                            'file_size_mb': float(row['file_size_mb']) if row['file_size_mb'] else 0.0,
                            'technical_indicators': row['technical_indicators'] or "",
                            'symbols': list(row['symbols']) if row['symbols'] else [],
                            'date_range_start': row['date_range_start'].isoformat() if row['date_range_start'] else None,
                            'date_range_end': row['date_range_end'].isoformat() if row['date_range_end'] else None,
                            'created_at': row['created_at'].isoformat() if row['created_at'] else None
                        }
                        datasets.append(dataset)
                    
                    return {
                        'datasets': datasets,
                        'total_count': len(datasets)
                    }
                    
        except Exception as e:
            logger.error(f"Error getting training datasets: {e}")
            return {
                'datasets': [],
                'total_count': 0,
                'error': str(e)
            }
    
    def get_training_dataset_distributions(self, dataset_id):
        """Get distributions and TFDV data for a specific training dataset"""
        try:
            from core.database.connection_manager import get_raw_connection
            import json
            
            # Determine table name based on environment (plural form)
            import os
            environment = os.getenv('ENVIRONMENT', 'dev')
            table_name = f"{environment}_training_datasets"
            
            with get_raw_connection() as conn:
                from psycopg2.extras import RealDictCursor
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(f"""
                        SELECT data_quality_score, feature_completeness, label_completeness,
                               tfdv_statistics, feature_distributions, label_distributions, 
                               tfdv_anomalies, tfdv_histogram_path
                        FROM {table_name} 
                        WHERE id = %s
                    """, (dataset_id,))
                    
                    result = cursor.fetchone()
                    
                    if not result:
                        return {
                            'error': f'Training dataset {dataset_id} not found',
                            'data_quality_score': 0.0,
                            'feature_completeness': 0.0,
                            'label_completeness': 0.0,
                            'feature_distributions': {},
                            'label_distributions': {},
                            'tfdv_statistics': {},
                            'tfdv_anomalies': {}
                        }
                    
                    # Parse JSON fields safely
                    def safe_json_parse(field_value, default=None):
                        if field_value is None:
                            return default or {}
                        if isinstance(field_value, (dict, list)):
                            return field_value
                        try:
                            return json.loads(field_value) if field_value else (default or {})
                        except (json.JSONDecodeError, TypeError):
                            logger.warning(f"Failed to parse JSON field: {field_value}")
                            return default or {}
                    
                    distributions = {
                        'data_quality_score': float(result['data_quality_score']) if result['data_quality_score'] else 0.0,
                        'feature_completeness': float(result['feature_completeness']) if result['feature_completeness'] else 0.0,
                        'label_completeness': float(result['label_completeness']) if result['label_completeness'] else 0.0,
                        'feature_distributions': safe_json_parse(result['feature_distributions']),
                        'label_distributions': safe_json_parse(result['label_distributions']),
                        'tfdv_statistics': safe_json_parse(result['tfdv_statistics']),
                        'tfdv_anomalies': safe_json_parse(result['tfdv_anomalies']),
                        'tfdv_histogram_path': result['tfdv_histogram_path'] or ""
                    }
                    
                    logger.info(f"Retrieved distributions for dataset {dataset_id}: {len(distributions['feature_distributions'])} features, {len(distributions['label_distributions'])} labels")
                    return distributions
                    
        except Exception as e:
            logger.error(f"Error getting training dataset distributions for ID {dataset_id}: {e}")
            return {
                'error': f'Database error: {str(e)}',
                'data_quality_score': 0.0,
                'feature_completeness': 0.0,
                'label_completeness': 0.0,
                'feature_distributions': {},
                'label_distributions': {},
                'tfdv_statistics': {},
                'tfdv_anomalies': {}
            }
    
    def get_training_dataset_data(self, dataset_id, page=1, limit=50, use_ray=False):
        """Get paginated training data for a specific dataset with optional Ray parallelization"""
        try:
            from core.database.connection_manager import get_raw_connection
            import numpy as np
            import json
            import os
            
            # Determine table name based on environment
            environment = os.getenv('ENVIRONMENT', 'dev')
            table_name = f"{environment}_training_datasets"
            
            offset = (page - 1) * limit
            
            with get_raw_connection() as conn:
                from psycopg2.extras import RealDictCursor
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Get dataset info and file paths
                    cursor.execute(f"""
                        SELECT features_file_path, labels_file_path, metadata_file_path,
                               date_range_start, date_range_end, total_sequences
                        FROM {table_name} 
                        WHERE id = %s
                    """, (dataset_id,))
                    
                    dataset_info = cursor.fetchone()
                    
                    if not dataset_info:
                        return {
                            'data': [],
                            'total_count': 0,
                            'date_range': None
                        }
                    
                    # Load numpy arrays from files with optimizations
                    feature_data = []
                    label_data = []
                    
                    try:
                        # Use column names that exist in the database
                        feature_path = dataset_info.get('features_file_path') or dataset_info.get('feature_file_path')
                        label_path = dataset_info.get('labels_file_path') or dataset_info.get('label_file_path')
                        metadata_path = dataset_info.get('metadata_file_path')
                        
                        # Load feature and label names from metadata
                        feature_names = ['open', 'high', 'low', 'close', 'volume', 'envelope_top', 'envelope_bot', 'pldot', 'oneonedot']  # Default
                        label_names = ['label_0', 'label_1', 'label_2', 'label_3', 'label_4']  # Default
                        
                        if metadata_path:
                            try:
                                import json
                                with open(metadata_path, 'r') as f:
                                    metadata = json.load(f)
                                    feature_names = metadata.get('feature_names', feature_names)
                                    label_names = metadata.get('label_names', label_names)
                                    logger.info(f"Loaded feature names: {feature_names}")
                            except Exception as e:
                                logger.warning(f"Could not load metadata: {e}")
                        
                        # OPTIMIZATION 1: Memory-mapped loading (don't load entire file)
                        if feature_path:
                            logger.info(f"Loading features from: {feature_path}")
                            with np.load(feature_path, mmap_mode='r') as features_mmap:
                                feature_data = features_mmap[offset:offset+limit].copy()
                            
                        if label_path:
                            logger.info(f"Loading labels from: {label_path}")  
                            with np.load(label_path, mmap_mode='r') as labels_mmap:
                                label_data = labels_mmap[offset:offset+limit].copy()
                                
                    except Exception as e:
                        logger.warning(f"Error loading numpy files: {e}")
                        # Fallback to traditional loading if mmap fails
                        try:
                            if feature_path:
                                features = np.load(feature_path)
                                feature_data = features[offset:offset+limit]
                                del features  # Free memory immediately
                                
                            if label_path:
                                labels = np.load(label_path)
                                label_data = labels[offset:offset+limit]
                                del labels  # Free memory immediately
                        except Exception as e2:
                            logger.error(f"Both optimized and fallback loading failed: {e2}")
                    
                    # OPTIMIZATION 2: Parallel processing with Ray if enabled
                    total_count = dataset_info['total_sequences'] or 0
                    num_rows = min(limit, len(feature_data) if len(feature_data) > 0 else len(label_data))
                    
                    if use_ray and num_rows > 20:  # Use Ray for larger datasets
                        try:
                            import ray
                            if not ray.is_initialized():
                                ray.init(ignore_reinit_error=True, num_cpus=min(4, os.cpu_count()))
                            
                            # Ray remote function for parallel processing
                            @ray.remote
                            def process_data_row(i, feature_row, label_row, offset, feat_names, lbl_names):
                                row = {'sequence_id': offset + i + 1}
                                
                                # Process features with real names
                                if feature_row is not None and len(feature_row) > 0:
                                    features = feature_row
                                    if features.ndim == 2:  # (timesteps, features) - take last timestep
                                        features = features[-1]
                                    # Use actual feature names
                                    for j in range(min(len(feat_names), len(features))):
                                        row[feat_names[j]] = float(features[j])
                                
                                # Process labels with real names
                                if label_row is not None and len(label_row) > 0:
                                    labels = label_row
                                    if labels.ndim == 1 and len(labels) > 1:
                                        for j in range(min(len(lbl_names), len(labels))):
                                            row[lbl_names[j] if j < len(lbl_names) else f'label_{j}'] = float(labels[j])
                                    else:
                                        row['label'] = float(labels.item() if hasattr(labels, 'item') else labels)
                                
                                return row
                            
                            # Submit parallel tasks
                            tasks = []
                            for i in range(num_rows):
                                feature_row = feature_data[i] if i < len(feature_data) and len(feature_data) > 0 else None
                                label_row = label_data[i] if i < len(label_data) and len(label_data) > 0 else None
                                tasks.append(process_data_row.remote(i, feature_row, label_row, offset, feature_names, label_names))
                            
                            # Get results
                            data_rows = ray.get(tasks)
                            logger.info(f"✅ Ray parallel processing completed for {num_rows} rows")
                            
                        except Exception as ray_error:
                            logger.warning(f"Ray processing failed, falling back to sequential: {ray_error}")
                            use_ray = False
                    
                    if not use_ray:
                        # OPTIMIZATION 2 (Fallback): Sequential processing
                        data_rows = []
                        
                        for i in range(num_rows):
                            row = {'sequence_id': offset + i + 1}
                            
                            # OPTIMIZATION 3: Vectorized feature processing with real names
                            if i < len(feature_data) and len(feature_data) > 0:
                                features = feature_data[i]
                                if features.ndim == 2:  # (timesteps, features) - take last timestep
                                    features = features[-1]
                                # Use actual feature names
                                for j in range(min(len(feature_names), len(features))):
                                    row[feature_names[j]] = float(features[j])
                            
                            # OPTIMIZATION 4: Vectorized label processing with real names
                            if i < len(label_data) and len(label_data) > 0:
                                labels = label_data[i]
                                if labels.ndim == 1 and len(labels) > 1:
                                    for j in range(min(len(label_names), len(labels))):
                                        row[label_names[j] if j < len(label_names) else f'label_{j}'] = float(labels[j])
                                else:
                                    row['label'] = float(labels.item() if hasattr(labels, 'item') else labels)
                            
                            data_rows.append(row)
                    
                    date_range = None
                    if dataset_info['date_range_start'] and dataset_info['date_range_end']:
                        date_range = {
                            'start': dataset_info['date_range_start'].isoformat() if dataset_info['date_range_start'] else None,
                            'end': dataset_info['date_range_end'].isoformat() if dataset_info['date_range_end'] else None
                        }
                    
                    return {
                        'data': data_rows,
                        'total_count': total_count,
                        'date_range': date_range
                    }
                    
        except Exception as e:
            logger.error(f"Error loading training dataset data: {e}")
            return {
                'data': [],
                'total_count': 0,
                'date_range': None,
                'error': str(e)
            }
    
    def do_GET(self):
        logger.info(f"📍 GET request: {self.path}")
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            response = {"status": "healthy", "service": "ats-analytics", "timestamp": datetime.now().isoformat()}
            self.wfile.write(json.dumps(response).encode('utf-8'))
            
        elif self.path == '/dataset-detail':
            # Serve the enhanced dataset detail page
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.end_headers()
            
            try:
                # Read the HTML file
                html_file_path = '/workspace/dataset_detail_page_frontend.html'
                with open(html_file_path, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                self.wfile.write(html_content.encode('utf-8'))
            except Exception as e:
                logger.error(f"Error serving dataset detail page: {e}")
                error_html = f"""
                <!DOCTYPE html>
                <html><head><title>Error</title></head>
                <body><h1>Error Loading Dataset Detail Page</h1><p>{e}</p></body></html>
                """
                self.wfile.write(error_html.encode('utf-8'))
        
        elif self.path == '/dual_axis_ohlc_chart.js':
            # Serve the chart JavaScript file
            self.send_response(200)
            self.send_header('Content-type', 'application/javascript; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            try:
                js_file_path = '/workspace/dual_axis_ohlc_chart.js'
                with open(js_file_path, 'r', encoding='utf-8') as f:
                    js_content = f.read()
                self.wfile.write(js_content.encode('utf-8'))
            except Exception as e:
                logger.error(f"Error serving chart JS file: {e}")
                error_js = f"// Error loading chart file: {e}"
                self.wfile.write(error_js.encode('utf-8'))
        
        elif self.path == '/' or self.path == '/dashboard':
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.end_headers()
            html = """
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>ATS Analytics Dashboard</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
                    .header { background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
                    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; }
                    .card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
                    .metric { font-size: 2em; font-weight: bold; color: #2c3e50; }
                    .btn { background: #3498db; color: white; padding: 10px 20px; border: none; border-radius: 4px; text-decoration: none; display: inline-block; margin: 5px; }
                    .job-item { padding: 8px; margin: 4px 0; border-radius: 4px; font-size: 0.9em; border-left: 3px solid #ddd; }
                    .job-running { border-left-color: #3498db; background: #e8f4fd; }
                    .job-completed { border-left-color: #27ae60; background: #eafaf1; }
                    .job-failed { border-left-color: #e74c3c; background: #fdf2f2; }
                    .job-pending { border-left-color: #f39c12; background: #fef9e7; }
                    .job-stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(80px, 1fr)); gap: 10px; margin-bottom: 10px; }
                    .stat-item { text-align: center; }
                    .stat-value { font-size: 1.2em; font-weight: bold; }
                    .stat-label { font-size: 0.8em; color: #666; }
                </style>
            </head>
            <body>
                <div class="header">
                    <h1>ATS Analytics Platform</h1>
                    <p>30-Year Price History Database - Development Environment</p>
                </div>
                
                <div class="grid">
                    
                    <div class="card">
                        <h3>Job Management</h3>
                        <div id="job-stats">Loading job statistics...</div>
                        <div id="recent-jobs" style="margin-top: 15px; max-height: 150px; overflow-y: auto;">
                            <p>Loading recent jobs...</p>
                        </div>
                        <a href="/api/jobs/stats" class="btn">Job Stats</a>
                        <a href="/api/jobs/recent" class="btn">Recent Jobs</a>
                    </div>
                    
                    <div class="card">
                        <h3>Analytics Tools</h3>
                        <a href="/eda" class="btn" style="background: #27ae60; margin-bottom: 10px; display: block; text-align: center;">🔍 Exploratory Data Analysis</a>
                        <p style="font-size: 0.9em; color: #666; margin-bottom: 15px;">Interactive histograms, cross-filtering, and dataset comparison</p>
                        
                        <a href="/training-eda" class="btn" style="background: #667eea; margin-bottom: 10px; display: block; text-align: center;">🤖 Training Dataset EDA</a>
                        <p style="font-size: 0.9em; color: #666; margin-bottom: 15px;">Analyze ML training datasets with TFDV statistics and distributions</p>
                        
                        <h4>API Endpoints</h4>
                        <a href="/health" class="btn">Health Check</a>
                        <a href="/api/summary" class="btn">Data Summary</a>
                        <a href="/api/jobs/stats" class="btn">Job Stats</a>
                        <a href="/api/collections/status" class="btn">Collection Status</a>
                    </div>
                </div>
                
                <div style="margin-top: 20px; text-align: center; color: #7f8c8d;">
                    <p>ATS Analytics Service | Development Environment | External Access Available</p>
                </div>
                
                <script>
                    async function loadJobStats() {
                        try {
                            const response = await fetch('/api/jobs/stats');
                            const stats = await response.json();
                            
                            if (stats.error) {
                                document.getElementById('job-stats').innerHTML = '<p style="color: #e74c3c;">Database unavailable</p>';
                                return;
                            }
                            
                            document.getElementById('job-stats').innerHTML = `
                                <div class="job-stats">
                                    <div class="stat-item">
                                        <div class="stat-value">${stats.total_jobs || 0}</div>
                                        <div class="stat-label">Total</div>
                                    </div>
                                    <div class="stat-item">
                                        <div class="stat-value" style="color: #3498db;">${stats.running_jobs || 0}</div>
                                        <div class="stat-label">Running</div>
                                    </div>
                                    <div class="stat-item">
                                        <div class="stat-value" style="color: #27ae60;">${stats.completed_jobs || 0}</div>
                                        <div class="stat-label">Completed</div>
                                    </div>
                                    <div class="stat-item">
                                        <div class="stat-value" style="color: #e74c3c;">${stats.failed_jobs || 0}</div>
                                        <div class="stat-label">Failed</div>
                                    </div>
                                </div>
                            `;
                        } catch (error) {
                            console.error('Failed to load job stats:', error);
                            document.getElementById('job-stats').innerHTML = '<p style="color: #e74c3c;">Failed to load stats</p>';
                        }
                    }
                    
                    async function loadRecentJobs() {
                        try {
                            const response = await fetch('/api/jobs/recent');
                            const data = await response.json();
                            
                            if (!data.jobs || data.jobs.length === 0) {
                                document.getElementById('recent-jobs').innerHTML = '<p style="color: #666;">No recent jobs found</p>';
                                return;
                            }
                            
                            const jobsHtml = data.jobs.slice(0, 5).map(job => {
                                const statusClass = `job-${job.status}`;
                                const duration = job.duration_seconds ? `${Math.round(job.duration_seconds/60)}m` : 'N/A';
                                const timeAgo = job.created_at ? new Date(job.created_at).toLocaleString() : 'Unknown';
                                
                                return `
                                    <div class="job-item ${statusClass}">
                                        <div style="font-weight: bold;">${job.job_type}</div>
                                        <div style="font-size: 0.8em; color: #666;">
                                            ${job.status.toUpperCase()} | ${job.user_id} | ${duration}
                                        </div>
                                        ${job.error_message ? `<div style="color: #e74c3c; font-size: 0.8em;">${job.error_message}</div>` : ''}
                                    </div>
                                `;
                            }).join('');
                            
                            document.getElementById('recent-jobs').innerHTML = jobsHtml;
                        } catch (error) {
                            console.error('Failed to load recent jobs:', error);
                            document.getElementById('recent-jobs').innerHTML = '<p style="color: #e74c3c;">Failed to load jobs</p>';
                        }
                    }
                    
                    // Load data on page load
                    document.addEventListener('DOMContentLoaded', function() {
                        loadJobStats();
                        loadRecentJobs();
                        
                        // Refresh every 30 seconds
                        setInterval(() => {
                            loadJobStats();
                            loadRecentJobs();
                        }, 30000);
                    });
                </script>
            </body>
            </html>
            """
            self.wfile.write(html.encode('utf-8'))
            
        elif self.path == '/api/summary':
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            summary = {
                "environment": "development",
                "database": "dev_db",
                "total_records": "7,953,657",
                "vendors": {
                    "tiingo": {"records": "6,559,540", "symbols": 2355},
                    "eodhd": {"records": "727,905", "symbols": 268}, 
                    "polygon": {"records": "666,212", "symbols": 849}
                },
                "instruments": "17,700",
                "etfs": "23",
                "date_range": "1995-2025",
                "status": "operational",
                "timestamp": datetime.now().isoformat()
            }
            self.wfile.write(json.dumps(summary, indent=2).encode('utf-8'))
            
        elif self.path == '/api/vendors':
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            vendors = {
                "vendors": [
                    {
                        "name": "tiingo",
                        "records": 6559540,
                        "symbols": 2355,
                        "coverage": "1995-2025",
                        "status": "active"
                    },
                    {
                        "name": "eodhd", 
                        "records": 727905,
                        "symbols": 268,
                        "coverage": "1995-2025",
                        "status": "active"
                    },
                    {
                        "name": "polygon",
                        "records": 666212,
                        "symbols": 849,
                        "coverage": "2015-2025",
                        "status": "active"
                    }
                ],
                "total_records": 7953657,
                "timestamp": datetime.now().isoformat()
            }
            self.wfile.write(json.dumps(vendors, indent=2).encode('utf-8'))
        
        elif self.path == '/api/jobs/stats':
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            # Need to run async function in sync context
            try:
                stats = job_manager.get_job_stats()
                self.wfile.write(json.dumps(stats, indent=2).encode('utf-8'))
            except Exception as e:
                logger.error(f"Error getting job stats: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode('utf-8'))
        
        elif self.path == '/api/jobs/recent':
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            try:
                jobs = job_manager.get_recent_jobs(15)
                response = {"jobs": jobs, "total": len(jobs), "timestamp": datetime.now().isoformat()}
                self.wfile.write(json.dumps(response, indent=2).encode('utf-8'))
            except Exception as e:
                logger.error(f"Error getting recent jobs: {e}")
                error_response = {"jobs": [], "total": 0, "error": str(e)}
                self.wfile.write(json.dumps(error_response).encode('utf-8'))
        
        elif self.path == '/api/collections/status':
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                status = loop.run_until_complete(job_manager.get_collection_status())
                self.wfile.write(json.dumps(status, indent=2).encode('utf-8'))
            except Exception as e:
                logger.error(f"Error getting collection status: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode('utf-8'))
        
        elif self.path == '/api/eda/datasets':
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')  # Disable browser cache
            self.send_header('Pragma', 'no-cache')
            self.send_header('Expires', '0')
            self.end_headers()
            
            try:
                datasets = get_cached_datasets(job_manager)
                self.wfile.write(json.dumps(datasets, indent=2).encode('utf-8'))
            except Exception as e:
                logger.error(f"Error getting datasets: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode('utf-8'))
        
        elif self.path == '/api/v1/training-datasets':
            # Training dataset API endpoint for dual-tab functionality
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            try:
                training_datasets = self.get_training_datasets()
                self.wfile.write(json.dumps(training_datasets, indent=2).encode('utf-8'))
            except Exception as e:
                logger.error(f"Error getting training datasets: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode('utf-8'))
        
        elif self.path.startswith('/api/v1/training-datasets/') and '/data' in self.path and '/distributions' not in self.path:
            # Training dataset data API endpoint: /api/v1/training-datasets/{id}/data
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            try:
                # Extract dataset ID from path: /api/v1/training-datasets/{id}/data
                path_parts = self.path.split('/')
                dataset_id = path_parts[4]  # Get the ID part
                
                # Parse query parameters
                from urllib.parse import urlparse, parse_qs
                parsed = urlparse(self.path)
                query_params = parse_qs(parsed.query)
                
                page = int(query_params.get('page', ['1'])[0])
                limit = int(query_params.get('limit', ['50'])[0])
                use_ray = query_params.get('ray', ['false'])[0].lower() == 'true'
                
                logger.info(f"Loading training data for dataset ID: {dataset_id}, page: {page}, limit: {limit}, ray: {use_ray}")
                
                # Get training data from database
                training_data = self.get_training_dataset_data(dataset_id, page, limit, use_ray)
                
                response_data = {
                    "data": training_data.get("data", []),
                    "total_count": training_data.get("total_count", 0),
                    "current_page": page,
                    "total_pages": (training_data.get("total_count", 0) + limit - 1) // limit,
                    "dataset_id": dataset_id,
                    "date_range": training_data.get("date_range")
                }
                
                self.wfile.write(json.dumps(response_data).encode('utf-8'))
                
            except Exception as e:
                logger.error(f"Error loading training dataset data: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode('utf-8'))
                
        elif self.path.startswith('/api/v1/training-datasets/') and self.path.endswith('/distributions'):
            # Training dataset distributions API endpoint: /api/v1/training-datasets/{id}/distributions
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            try:
                # Extract dataset ID from path: /api/v1/training-datasets/{id}/distributions
                path_parts = self.path.split('/')
                dataset_id = path_parts[4]  # Get the ID part
                
                logger.info(f"Loading distributions for training dataset ID: {dataset_id}")
                distributions = self.get_training_dataset_distributions(dataset_id)
                self.wfile.write(json.dumps(distributions, indent=2).encode('utf-8'))
            except Exception as e:
                logger.error(f"Error getting training dataset distributions: {e}")
                error_response = {"error": str(e), "dataset_id": dataset_id if 'dataset_id' in locals() else 'unknown'}
                self.wfile.write(json.dumps(error_response).encode('utf-8'))
        
        elif self.path.startswith('/api/eda/datasets/') and self.path.endswith('/schema'):
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            try:
                # Extract dataset name from path
                parts = self.path.split('/')
                dataset_name = parts[4]  # /api/eda/datasets/{name}/schema
                
                schema = job_manager.get_dataset_schema(dataset_name)
                self.wfile.write(json.dumps(schema, indent=2).encode('utf-8'))
            except Exception as e:
                logger.error(f"Error getting schema: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode('utf-8'))
        
        elif self.path.startswith('/api/eda/datasets/') and '/columns/' in self.path and '/values' in self.path:
            # GET /api/eda/datasets/{table_name}/columns/{column_name}/values
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            try:
                # Parse URL to handle query parameters
                from urllib.parse import urlparse, parse_qs
                parsed_url = urlparse(self.path)
                path_parts = parsed_url.path.split('/')
                
                # Extract dataset name and column name from path
                # Path: /api/eda/datasets/{table_name}/columns/{column_name}/values
                table_name = path_parts[4]  # table name
                column_name = path_parts[6]  # column name
                
                # Parse query parameters for limit
                query_params = parse_qs(parsed_url.query)
                limit = int(query_params.get('limit', [100])[0])
                
                # Use Ray EDA for massive datasets
                if RAY_AVAILABLE and self.should_use_ray_for_table(table_name):
                    # Use Ray to get column values for large tables
                    import concurrent.futures
                    import asyncio
                    
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        future = executor.submit(
                            lambda: asyncio.run(self.get_column_values_with_ray(table_name, column_name, limit))
                        )
                        column_values = future.result(timeout=15)  # 15 second timeout
                else:
                    column_values = job_manager.get_column_values(table_name, column_name, limit)
                
                self.wfile.write(json.dumps(column_values, indent=2).encode('utf-8'))
            except Exception as e:
                logger.error(f"Error getting column values: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode('utf-8'))
        
        elif self.path.startswith('/api/eda/datasets/') and '/precompute' in self.path:
            # GET /api/eda/datasets/{table_name}/precompute - Trigger pre-computation
            # GET /api/eda/datasets/{table_name}/precompute/status - Check status
            logger.info(f"🚀 Pre-compute endpoint accessed: {self.path}")
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            try:
                path_parts = self.path.split('/')
                table_name = path_parts[4]
                
                if '/status' in self.path:
                    # Check pre-computation status (synchronous fallback)
                    try:
                        from services.ray_eda_engine import get_ray_eda_service
                        ray_service = get_ray_eda_service()
                        
                        # For now, assume no pre-computation available (will be enhanced)
                        status_response = {
                            'table': table_name,
                            'precomputed': False,
                            'message': 'Pre-computation status check not yet implemented',
                            'precomputed_tables': []
                        }
                        
                        self.wfile.write(json.dumps(status_response, indent=2).encode('utf-8'))
                        
                    except Exception as e:
                        error_response = {'error': f'Status check failed: {str(e)}'}
                        self.wfile.write(json.dumps(error_response).encode('utf-8'))
                else:
                    # Trigger pre-computation (synchronous fallback)
                    try:
                        from services.ray_eda_engine import get_ray_eda_service
                        ray_service = get_ray_eda_service()
                        
                        # For now, return a placeholder response (will be enhanced)
                        result = {
                            'table': table_name,
                            'success': True,
                            'message': 'Pre-computation scheduled (implementation in progress)',
                            'columns_computed': 'placeholder'
                        }
                        logger.info(f"📋 Pre-computation requested for {table_name}")
                        self.wfile.write(json.dumps(result, indent=2).encode('utf-8'))
                            
                    except Exception as e:
                        logger.error(f"Pre-computation failed for {table_name}: {e}")
                        error_response = {'error': f'Pre-computation failed: {str(e)}'}
                        self.wfile.write(json.dumps(error_response).encode('utf-8'))
                
            except Exception as e:
                logger.error(f"Error in pre-compute endpoint: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode('utf-8'))

        elif self.path.startswith('/api/eda/datasets/') and '/timeseries/' in self.path:
            # GET /api/eda/datasets/{table_name}/timeseries/{y_column}/{x_column}
            logger.info(f"🎯 Timeseries endpoint accessed: {self.path}")
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            try:
                # Parse URL: /api/eda/datasets/{table_name}/timeseries/{y_column}/{x_column}
                path_parts = self.path.split('/')
                logger.info(f"Timeseries request - Path parts: {path_parts}")
                table_name = path_parts[4]  
                y_column = path_parts[6]  
                x_column = path_parts[7] if len(path_parts) > 7 else None
                logger.info(f"Timeseries params - table: {table_name}, y: {y_column}, x: {x_column}")
                
                if not x_column:
                    raise ValueError("X-axis column required for time-series")
                
                # Get time-series data
                timeseries_data = job_manager.get_timeseries_data(table_name, y_column, x_column)
                self.wfile.write(json.dumps(timeseries_data, indent=2).encode('utf-8'))
                
            except Exception as e:
                logger.error(f"Error getting time-series data: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode('utf-8'))
        
        elif self.path == '/training-eda':
            # Training Dataset EDA Dashboard - Separate from table EDA
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.end_headers()
            training_eda_html = self.get_training_eda_dashboard_html()
            self.wfile.write(training_eda_html.encode('utf-8'))
            
        elif self.path == '/eda':
            # EDA Dashboard page
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.end_headers()
            eda_html = self.get_eda_dashboard_html()
            self.wfile.write(eda_html.encode('utf-8'))
            
        else:
            self.send_response(404)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            error = {"error": "Not found", "path": self.path}
            self.wfile.write(json.dumps(error).encode('utf-8'))

    def do_POST(self):
        if self.path == '/api/eda/analyze':
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            try:
                # Read POST data
                content_length = int(self.headers.get('Content-Length', 0))
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode('utf-8'))
                
                dataset_name = data.get('dataset_name')
                column = data.get('column')
                filters = data.get('filters', {})
                
                if not dataset_name or not column:
                    error_response = {"error": "Missing dataset_name or column"}
                    self.wfile.write(json.dumps(error_response).encode('utf-8'))
                    return
                
                # Use Ray EDA for massive dataset analysis if available
                if RAY_AVAILABLE and self.should_use_ray_for_table(dataset_name):
                    # Run async Ray analysis in thread pool
                    import asyncio
                    import concurrent.futures
                    
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        future = executor.submit(
                            lambda: asyncio.run(self.analyze_column_with_ray(dataset_name, column, filters))
                        )
                        analysis = future.result(timeout=30)  # 30 second timeout for Ray analysis
                else:
                    analysis = job_manager.analyze_column_distribution(dataset_name, column, filters)
                
                self.wfile.write(json.dumps(analysis, indent=2).encode('utf-8'))
                
            except Exception as e:
                logger.error(f"Error analyzing distribution: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode('utf-8'))
        
        elif self.path.startswith('/api/eda/datasets/') and self.path.endswith('/data'):
            # POST /api/eda/datasets/{table_name}/data
            self.send_response(200)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            try:
                # Extract table name from path
                parts = self.path.split('/')
                table_name = parts[4]  # /api/eda/datasets/{table_name}/data
                
                # Read POST data
                content_length = int(self.headers.get('Content-Length', 0))
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data.decode('utf-8'))
                
                filters = data.get('filters', {})
                page = data.get('page', 1)
                page_size = data.get('page_size', 50)
                
                filtered_data = job_manager.get_filtered_data(table_name, filters, page, page_size)
                self.wfile.write(json.dumps(filtered_data, indent=2).encode('utf-8'))
                
            except Exception as e:
                logger.error(f"Error getting filtered data: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode('utf-8'))
        
        else:
            self.send_response(404)
            self.send_header('Content-type', 'application/json; charset=utf-8')
            self.end_headers()
            error = {"error": "Not found", "path": self.path}
            self.wfile.write(json.dumps(error).encode('utf-8'))

    def get_eda_dashboard_html(self):
        """Generate the EDA dashboard HTML."""
        return """
        <!DOCTYPE html>
        <html>
        <head>
            <title>ATS EDA - Exploratory Data Analysis</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
                .header { background: #2c3e50; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
                .main-layout { display: flex; gap: 20px; height: calc(100vh - 200px); }
                .nav-panel { width: 300px; flex-shrink: 0; }
                .content-panel { flex: 1; display: flex; flex-direction: column; gap: 20px; }
                .nav-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); height: fit-content; }
                .content-card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
                .distributions-scroll { max-height: 60vh; overflow-y: auto; overflow-x: hidden; }
                .table-scroll { max-height: 400px; overflow: auto; border: 1px solid #ddd; border-radius: 4px; }
                .card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
                .dataset-card { border: 1px solid #ddd; padding: 15px; margin: 10px 0; border-radius: 5px; cursor: pointer; }
                .dataset-card:hover { background: #f0f8ff; }
                .dataset-card.selected { background: #e8f4fd; border-color: #3498db; }
                .chart-container { width: 100%; height: 400px; margin: 20px 0; }
                button, select { padding: 10px 15px; margin: 5px; cursor: pointer; border: 1px solid #ddd; border-radius: 4px; }
                button { background: #3498db; color: white; border: none; }
                button:hover { background: #2980b9; }
                .controls { margin: 20px 0; padding: 20px; background: white; border-radius: 8px; }
                .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 10px; margin: 15px 0; }
                .stat-item { text-align: center; padding: 10px; background: #f8f9fa; border-radius: 4px; }
                .stat-value { font-size: 1.2em; font-weight: bold; color: #2c3e50; }
                .stat-label { font-size: 0.9em; color: #666; }
                .filter-group { margin: 15px 0; padding: 10px; border: 1px solid #ddd; border-radius: 4px; background: #f9f9f9; }
                .filter-group label { display: block; margin-bottom: 5px; font-weight: bold; }
                .filter-input { width: 100%; padding: 5px; margin: 2px 0; }
                .checkbox-list { max-height: 150px; overflow-y: auto; border: 1px solid #ddd; padding: 10px; background: white; }
                .checkbox-list label { font-weight: normal; }
                #data-table { border: 1px solid #ddd; }
                #data-table th, #data-table td { border: 1px solid #ddd; padding: 8px; text-align: left; }
                #data-table th { background: #f8f9fa; font-weight: bold; cursor: pointer; user-select: none; position: relative; }
                #data-table th:hover { background: #e9ecef; }
                #data-table th.sortable::after { content: ' ⇅'; color: #999; font-size: 12px; }
                #data-table th.sort-asc::after { content: ' ↑'; color: #3498db; }
                #data-table th.sort-desc::after { content: ' ↓'; color: #3498db; }
                #data-table tbody tr:nth-child(even) { background: #f9f9f9; }
                .pagination-btn { padding: 5px 10px; margin: 0 2px; cursor: pointer; border: 1px solid #ddd; background: white; }
                .pagination-btn.active { background: #3498db; color: white; }
                .pagination-btn:disabled { opacity: 0.5; cursor: not-allowed; }
                .column-distribution { margin: 20px 0; padding: 15px; border: 1px solid #ddd; border-radius: 8px; background: #fafafa; }
                .column-distribution h4 { margin: 0 0 10px 0; color: #2c3e50; }
                .visualization-controls { margin: 10px 0; }
                .visualization-controls select { width: 100%; padding: 5px; border: 1px solid #ddd; border-radius: 4px; }
                .distribution-chart { width: 100%; height: 300px; margin: 10px 0; }
                .distribution-stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(100px, 1fr)); gap: 10px; margin: 10px 0; }
                .distribution-stat { text-align: center; padding: 8px; background: white; border-radius: 4px; }
                .stat-value-small { font-size: 1em; font-weight: bold; color: #2c3e50; }
                .stat-label-small { font-size: 0.8em; color: #666; }
            </style>
        </head>
        <body>
            <div class="header">
                <h1>ATS Exploratory Data Analysis</h1>
                <p>Comprehensive dataset analysis with automatic column distributions and filtering</p>
                <a href="/" style="color: #3498db; margin-right: 15px;">&lt; Back to Analytics Dashboard</a>
                
                <!-- Dual-Tab Interface -->
                <div class="tab-container" style="margin-top: 20px;">
                    <button class="tab-button active" onclick="switchTab('database-tab')">Database Tables</button>
                    <button class="tab-button" onclick="switchTab('training-tab')">Training Datasets</button>
                </div>
            </div>
            
            <style>
                .tab-container { display: flex; gap: 5px; }
                .tab-button { 
                    padding: 10px 20px; 
                    background: #34495e; 
                    color: white; 
                    border: none; 
                    border-radius: 5px 5px 0 0; 
                    cursor: pointer; 
                    font-size: 14px; 
                }
                .tab-button.active { background: #3498db; }
                .tab-button:hover { background: #2980b9; }
                .tab-content { display: none; }
                .tab-content.active { display: block; }
            </style>
            
            <!-- Database Tables Tab -->
            <div id="database-tab" class="tab-content active">
            <div class="main-layout">
                <!-- Left Navigation Panel -->
                <div class="nav-panel">
                    <div class="nav-card">
                        <h3>Dataset Selection</h3>
                        <div class="controls">
                            <div>
                                <label>Choose Dataset:</label>
                                <select id="dataset-select" onchange="loadDatasetAnalysis()">
                                    <option value="">Select dataset...</option>
                                </select>
                            </div>
                            <div style="margin-top: 10px;">
                                <button id="precompute-btn" onclick="precomputeStatistics()" 
                                        style="background: #e74c3c; color: white; padding: 8px 15px; border: none; border-radius: 4px; cursor: pointer;" 
                                        disabled>
                                    ⚡ Pre-compute Statistics
                                </button>
                                <button id="precompute-status-btn" onclick="checkPrecomputeStatus()" 
                                        style="background: #3498db; color: white; padding: 8px 15px; border: none; border-radius: 4px; cursor: pointer; margin-left: 5px;" 
                                        disabled>
                                    📊 Check Status
                                </button>
                            </div>
                            <div id="precompute-info" style="margin-top: 10px; padding: 10px; border-radius: 4px; display: none;"></div>
                            <div id="dataset-info" style="display: none; margin-top: 15px;">
                            </div>
                        </div>
                        
                        <!-- Global X-Axis Control -->
                        <div id="global-axis-section" class="controls" style="display: none;">
                            <h4>📊 Chart Configuration</h4>
                            <div style="margin: 10px 0;">
                                <label for="global-x-axis" style="display: block; margin-bottom: 5px; font-weight: bold;">X-Axis for All Charts:</label>
                                <select id="global-x-axis" onchange="updateGlobalXAxis()" style="width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                                    <option value="">Default (Value-based)</option>
                                    <option value="date">Date</option>
                                    <option value="sequence_step">Sequence Step</option>
                                    <option value="trading_day">Trading Day</option>
                                    <option value="relative_time">Relative Time</option>
                                </select>
                            </div>
                        </div>
                        
                        <!-- Filters Section -->
                        <div id="filters-section" class="controls" style="display: none;">
                            <h4>Data Filters</h4>
                            <div id="filter-controls"></div>
                            <div style="margin-top: 15px;">
                                <button onclick="applyFilters()" class="btn btn-primary">Apply Filters</button>
                                <button onclick="clearFilters()" class="btn btn-secondary">Clear</button>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Right Content Panel -->
                <div class="content-panel">
                    <!-- Column Distributions (Top) -->
                    <div id="distribution-analysis" class="content-card">
                        <h3>All Column Distributions</h3>
                        <div id="distributions-container" class="distributions-scroll">
                            <p style="text-align: center; color: #666; padding: 40px;">Select a dataset to view all column distributions</p>
                        </div>
                    </div>
                    
                    <!-- Data Table (Bottom) -->
                    <div id="data-table-section" class="content-card" style="display: none;">
                        <h3>Dataset Preview</h3>
                        <div id="table-info" style="margin-bottom: 15px; color: #666;"></div>
                        <div class="table-pagination">
                            <button id="prev-page" onclick="previousPage()" disabled>← Previous</button>
                            <span id="page-info">Page 1</span>
                            <button id="next-page" onclick="nextPage()">Next →</button>
                        </div>
                        <div id="data-table-container" class="table-scroll">
                            <table id="data-table">
                                <thead id="table-head"></thead>
                        <tbody id="table-body"></tbody>
                    </table>
                </div>
                <div id="pagination-controls" style="margin-top: 15px; text-align: center;"></div>
            </div>
            </div>
            </div>
            <!-- End Database Tables Tab -->
            
            <!-- Training Datasets Tab -->
            <div id="training-tab" class="tab-content">
                <div class="main-layout">
                    <!-- Left Navigation Panel -->
                    <div class="nav-panel">
                        <div class="nav-card">
                            <h3>Training Dataset Selection</h3>
                            <div class="controls">
                                <div>
                                    <label>Choose Training Dataset:</label>
                                    <select id="training-dataset-select" onchange="loadTrainingDatasetAnalysis()">
                                        <option value="">Select training dataset...</option>
                                    </select>
                                </div>
                                <div id="training-dataset-info" style="display: none; margin-top: 15px;">
                                    <div class="stats-grid">
                                        <div class="stat-item">
                                            <div class="stat-value" id="training-total-sequences">0</div>
                                            <div class="stat-label">Total Sequences</div>
                                        </div>
                                        <div class="stat-item">
                                            <div class="stat-value" id="training-feature-count">0</div>
                                            <div class="stat-label">Features</div>
                                        </div>
                                        <div class="stat-item">
                                            <div class="stat-value" id="training-label-count">0</div>
                                            <div class="stat-label">Labels</div>
                                        </div>
                                        <div class="stat-item">
                                            <div class="stat-value" id="training-quality-score">0.0</div>
                                            <div class="stat-label">Quality Score</div>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                    
                    <!-- Main Content Panel -->
                    <div class="content-panel">
                        <div class="content-card">
                            <h3>Training Dataset Analysis</h3>
                            <div id="training-analysis-content">
                                <p style="color: #666; text-align: center; margin-top: 50px;">Select a training dataset to view analysis</p>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
            <!-- End Training Datasets Tab -->
            
            <script>
                let datasets = [];
                let trainingDatasets = [];
                let currentAnalysis = null;
                let currentFilters = {};
                let currentPage = 1;
                let totalPages = 1;
                
                // Table sorting variables
                let currentSortColumn = null;
                let currentSortDirection = 'asc';
                let currentTableData = [];
                
                // Tab switching functionality
                function switchTab(tabId) {
                    // Remove active class from all tabs and buttons
                    document.querySelectorAll('.tab-content').forEach(tab => {
                        tab.classList.remove('active');
                    });
                    document.querySelectorAll('.tab-button').forEach(btn => {
                        btn.classList.remove('active');
                    });
                    
                    // Activate selected tab
                    document.getElementById(tabId).classList.add('active');
                    event.target.classList.add('active');
                    
                    // Load appropriate data
                    if (tabId === 'database-tab') {
                        loadDatasets();
                    } else if (tabId === 'training-tab') {
                        loadTrainingDatasets();
                    }
                }
                
                // Training dataset functions
                async function loadTrainingDatasets() {
                    try {
                        console.log('Loading training datasets...');
                        const response = await fetch('/api/v1/training-datasets');
                        const data = await response.json();
                        
                        trainingDatasets = data.datasets || [];
                        
                        const select = document.getElementById('training-dataset-select');
                        select.innerHTML = '<option value="">Select training dataset...</option>';
                        
                        trainingDatasets.forEach(dataset => {
                            const option = document.createElement('option');
                            option.value = dataset.id;
                            option.textContent = `${dataset.dataset_name} (${dataset.total_sequences} sequences)`;
                            select.appendChild(option);
                        });
                        
                        console.log(`Loaded ${trainingDatasets.length} training datasets`);
                        
                        // Force DOM refresh to ensure dropdown options are visible
                        select.dispatchEvent(new Event('change'));
                        select.style.display = 'none';
                        select.offsetHeight; // Force reflow
                        select.style.display = '';
                        
                        console.log(`DOM refresh applied to training dataset dropdown`);
                    } catch (error) {
                        console.error('Error loading training datasets:', error);
                        document.getElementById('training-analysis-content').innerHTML = 
                            `<p style="color: #e74c3c;">Error loading training datasets: ${error.message}</p>`;
                    }
                }
                
                function loadTrainingDatasetAnalysis() {
                    const select = document.getElementById('training-dataset-select');
                    const datasetId = select.value;
                    
                    if (!datasetId) {
                        document.getElementById('training-dataset-info').style.display = 'none';
                        document.getElementById('training-analysis-content').innerHTML = 
                            '<p style="color: #666; text-align: center; margin-top: 50px;">Select a training dataset to view analysis</p>';
                        return;
                    }
                    
                    const dataset = trainingDatasets.find(d => d.id == datasetId);
                    if (!dataset) return;
                    
                    // Update dataset info
                    document.getElementById('training-total-sequences').textContent = dataset.total_sequences;
                    document.getElementById('training-feature-count').textContent = dataset.feature_count;
                    document.getElementById('training-label-count').textContent = dataset.label_count;
                    document.getElementById('training-quality-score').textContent = dataset.data_quality_score.toFixed(2);
                    
                    document.getElementById('training-dataset-info').style.display = 'block';
                    
                    // Display training dataset analysis
                    const analysisContent = `
                        <div style="padding: 20px;">
                            <h4>Dataset: ${dataset.dataset_name}</h4>
                            <div class="stats-grid">
                                <div class="stat-item">
                                    <div class="stat-value">${dataset.file_size_mb.toFixed(1)} MB</div>
                                    <div class="stat-label">File Size</div>
                                </div>
                                <div class="stat-item">
                                    <div class="stat-value">${dataset.feature_completeness.toFixed(1)}%</div>
                                    <div class="stat-label">Feature Completeness</div>
                                </div>
                                <div class="stat-item">
                                    <div class="stat-value">${dataset.label_completeness.toFixed(1)}%</div>
                                    <div class="stat-label">Label Completeness</div>
                                </div>
                                <div class="stat-item">
                                    <div class="stat-value">${dataset.symbols.length}</div>
                                    <div class="stat-label">Symbols</div>
                                </div>
                            </div>
                            
                            <div style="margin-top: 20px;">
                                <h5>Technical Indicators:</h5>
                                <p>${dataset.technical_indicators || 'None specified'}</p>
                            </div>
                            
                            <div style="margin-top: 20px;">
                                <h5>Symbols:</h5>
                                <p>${dataset.symbols.join(', ') || 'None'}</p>
                            </div>
                            
                            <div style="margin-top: 20px;">
                                <h5>Date Range:</h5>
                                <p>${dataset.date_range_start} to ${dataset.date_range_end}</p>
                            </div>
                            
                            <div style="margin-top: 20px;">
                                <h5>Created:</h5>
                                <p>${new Date(dataset.created_at).toLocaleString()}</p>
                            </div>
                        </div>
                    `;
                    
                    document.getElementById('training-analysis-content').innerHTML = analysisContent;
                }
                
                // Frontend cache for datasets - 1 hour cache
                let datasetsCache = {
                    data: null,
                    timestamp: 0,
                    ttl: 60 * 60 * 1000  // 1 hour in milliseconds
                };
                
                async function loadDatasets() {
                    try {
                        const currentTime = Date.now();
                        
                        // Check frontend cache first
                        if (datasetsCache.data && 
                            currentTime - datasetsCache.timestamp < datasetsCache.ttl) {
                            console.log('📋 Using cached datasets (frontend cache)');
                            datasets = datasetsCache.data;
                            populateDatasetDropdown();
                            return;
                        }
                        
                        console.log('🚀 Loading datasets from API...');
                        const response = await fetch('/api/eda/datasets');
                        console.log('✅ Response status:', response.status);
                        
                        if (!response.ok) {
                            console.error('❌ API request failed:', response.status, response.statusText);
                            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                        }
                        
                        const data = await response.json();
                        datasets = Array.isArray(data) ? data : data.datasets || [];
                        console.log('Datasets received:', datasets.length);
                        
                        // Cache the results
                        datasetsCache.data = datasets;
                        datasetsCache.timestamp = currentTime;
                        console.log('💾 Datasets cached for 1 hour');
                        
                        populateDatasetDropdown();
                        
                    } catch (error) {
                        console.error('Error loading datasets:', error);
                        document.getElementById('dataset-select').innerHTML = '<option value="">Error loading datasets</option>';
                    }
                }
                
                function populateDatasetDropdown() {
                    if (!Array.isArray(datasets) || datasets.length === 0) {
                        document.getElementById('dataset-select').innerHTML = '<option value="">No datasets found</option>';
                        return;
                    }
                    
                    const select = document.getElementById('dataset-select');
                    select.innerHTML = '<option value="">Select dataset...</option>';
                    
                    datasets.forEach((dataset, index) => {
                        console.log(`Dataset ${index}:`, dataset.display_name);
                        
                        // Add dataset size information to dropdown
                        const sizeInfo = formatDatasetSize(dataset.row_count, dataset.column_count);
                        select.innerHTML += `<option value="${dataset.name}">${dataset.display_name} (${sizeInfo})</option>`;
                    });
                    
                    console.log('✅ Datasets populated successfully');
                }
                
                function formatDatasetSize(rowCount, columnCount) {
                    if (rowCount >= 1000000) {
                        return `${(rowCount/1000000).toFixed(1)}M rows, ${columnCount} cols`;
                    } else if (rowCount >= 1000) {
                        return `${(rowCount/1000).toFixed(1)}k rows, ${columnCount} cols`;
                    } else {
                        return `${rowCount} rows, ${columnCount} cols`;
                    }
                }
                
                function selectDataset(datasetName) {
                    document.getElementById('dataset-select').value = datasetName;
                    loadDatasetAnalysis();
                    
                    // Visual selection
                    document.querySelectorAll('.dataset-card').forEach(card => {
                        card.classList.remove('selected');
                    });
                    event.target.closest('.dataset-card').classList.add('selected');
                }
                
                async function loadDatasetAnalysis() {
                    const datasetName = document.getElementById('dataset-select').value;
                    if (!datasetName) {
                        document.getElementById('global-axis-section').style.display = 'none';
                        document.getElementById('filters-section').style.display = 'none';
                        document.getElementById('distribution-analysis').style.display = 'none';
                        document.getElementById('dataset-info').style.display = 'none';
                        // Disable pre-compute buttons
                        document.getElementById('precompute-btn').disabled = true;
                        document.getElementById('precompute-status-btn').disabled = true;
                        document.getElementById('precompute-info').style.display = 'none';
                        return;
                    }
                    
                    // Enable pre-compute buttons
                    document.getElementById('precompute-btn').disabled = false;
                    document.getElementById('precompute-status-btn').disabled = false;
                    
                    try {
                        // Show loading state
                        document.getElementById('distributions-container').innerHTML = '<p style="text-align: center;">Loading distributions...</p>';
                        document.getElementById('distribution-analysis').style.display = 'block';
                        
                        // Load schema
                        const response = await fetch(`/api/eda/datasets/${datasetName}/schema`);
                        const schema = await response.json();
                        
                        // Show dataset info
                        const datasetInfo = datasets.find(d => d.name === datasetName);
                        if (datasetInfo) {
                            document.getElementById('dataset-info').style.display = 'block';
                        }
                        
                        // Show global axis and filters sections
                        document.getElementById('global-axis-section').style.display = 'block';
                        document.getElementById('filters-section').style.display = 'block';
                        
                        // Load filters, distributions, and data table in parallel for speed
                        const filterPromise = loadFiltersForDataset(datasetName, schema.columns);
                        const distributionPromise = loadAllColumnDistributions(datasetName, schema.columns);
                        const dataTablePromise = loadDataTable(datasetName, schema.columns);
                        
                        // Wait for all to complete
                        await Promise.allSettled([filterPromise, distributionPromise, dataTablePromise]);
                        
                    } catch (error) {
                        console.error('Error loading dataset analysis:', error);
                        document.getElementById('distributions-container').innerHTML = `<p style="color: red;">Error loading distributions: ${error.message}</p>`;
                    }
                }
                
                async function loadFiltersForDataset(datasetName, columns) {
                    const filterControls = document.getElementById('filter-controls');
                    filterControls.innerHTML = '<p>Loading filters...</p>';
                    
                    // Load only first 4 columns for fast filter loading
                    const importantColumns = columns.slice(0, 4);
                    
                    // Create all filter requests in parallel
                    const filterPromises = importantColumns.map(async (col) => {
                        const dataType = col.type.toLowerCase();
                        const isNumeric = dataType.includes('numeric') || dataType.includes('integer') || 
                            dataType.includes('double') || dataType.includes('bigint') ||
                            dataType.includes('smallint') || dataType.includes('real') ||
                            dataType.includes('decimal') || dataType.includes('float');
                        
                        // Identify date columns for special handling
                        const isDateType = dataType.includes('date') || dataType.includes('timestamp') || 
                            col.name.toLowerCase().includes('date') || col.name.toLowerCase().includes('time');
                        
                        // Identify string columns that should use text search (not categorical checkboxes)
                        // Categorical columns like symbol, type, exchange should use checkboxes if they have distinct values
                        const isStringType = (dataType.includes('varchar') || dataType.includes('text') || 
                            dataType.includes('character')) && 
                            (col.name.toLowerCase().includes('name') || col.name.toLowerCase().includes('title') || 
                             col.name.toLowerCase().includes('url') || col.name.toLowerCase().includes('description') ||
                             col.name.toLowerCase().includes('id'));
                        
                        // Prefer categorical checkboxes for columns like symbol, type, exchange
                        const preferCategorical = col.name.toLowerCase().includes('symbol') || 
                                                 col.name.toLowerCase().includes('type') ||
                                                 col.name.toLowerCase().includes('exchange');
                        
                        try {
                            // Use higher limit for symbol-like columns to ensure we get TSLA and other symbols
                            const symbolLimit = preferCategorical ? 1000 : 10;
                            const response = await fetch(`/api/eda/datasets/${datasetName}/columns/${col.name}/values?limit=${symbolLimit}`, {timeout: 3000});
                            const columnData = await response.json();
                            
                            if (columnData.error) return null; // Skip if error loading values
                            
                            let filterHtml = `<div class="filter-group">`;
                            const typeLabel = isNumeric ? 'numeric' : (isStringType ? 'string' : (isDateType ? 'date' : 'categorical'));
                            filterHtml += `<label>${col.name} (${typeLabel}):</label>`;
                            
                            // Prioritize categorical checkboxes for symbol-like columns
                            if (preferCategorical && columnData.values && Array.isArray(columnData.values)) {
                                // Categorical checkbox filter (prioritized) with search
                                const searchId = `search-${col.name}`;
                                const listId = `list-${col.name}`;
                                
                                // Add search box for symbols
                                filterHtml += `<div style="margin-bottom: 8px;">
                                    <input type="text" id="${searchId}" placeholder="Search symbols..." 
                                           style="width: 100%; padding: 4px 8px; border: 1px solid #ddd; border-radius: 4px; font-size: 12px;"
                                           oninput="filterSymbolList('${searchId}', '${listId}')">
                                </div>`;
                                
                                // Show more symbols (up to 100 to ensure TSLA is visible) and make them searchable
                                filterHtml += `<div class="checkbox-list" id="${listId}" style="max-height: 200px; overflow-y: auto;">`;
                                columnData.values.slice(0, 100).forEach(valueData => { // Show up to 100 values
                                    const value = typeof valueData === 'object' ? valueData.value : valueData;
                                    const count = typeof valueData === 'object' ? valueData.count : '';
                                    const countText = count ? ` (${count})` : '';
                                    filterHtml += `
                                        <label style="display: block;">
                                            <input type="checkbox" name="filter-${col.name}" value="${value}"> ${value}${countText}
                                        </label>
                                    `;
                                });
                                if (columnData.values.length > 100) {
                                    filterHtml += `<small style="color: #666; display: block; margin-top: 4px;">(${columnData.values.length - 100} more symbols in dataset)</small>`;
                                }
                                filterHtml += `</div>`;
                            } else if (isNumeric && columnData.min_value !== undefined && columnData.max_value !== undefined) {
                                // Numeric range filter
                                filterHtml += `
                                    <div>
                                        <label>Min: <input type="number" class="filter-input" id="filter-${col.name}-min" placeholder="Min value (${columnData.min_value})"></label>
                                        <label>Max: <input type="number" class="filter-input" id="filter-${col.name}-max" placeholder="Max value (${columnData.max_value})"></label>
                                        <small>Range: ${columnData.min_value} - ${columnData.max_value}</small>
                                    </div>
                                `;
                            } else if (isDateType) {
                                // Date range filter with calendar inputs
                                const minDate = columnData.min_value ? columnData.min_value.split('T')[0] : '';
                                const maxDate = columnData.max_value ? columnData.max_value.split('T')[0] : '';
                                filterHtml += `
                                    <div>
                                        <label>Start Date: <input type="date" class="filter-input" id="filter-${col.name}-start" ${minDate ? `min="${minDate}"` : ''}></label>
                                        <label>End Date: <input type="date" class="filter-input" id="filter-${col.name}-end" ${maxDate ? `max="${maxDate}"` : ''}></label>
                                        <small>Available: ${minDate || 'N/A'} to ${maxDate || 'N/A'}</small>
                                    </div>
                                `;
                            } else if (isStringType) {
                                // String partial match filter
                                filterHtml += `
                                    <div>
                                        <input type="text" class="filter-input" id="filter-${col.name}-search" placeholder="Enter text to search..." 
                                               onkeyup="debounceStringFilter('${col.name}', this.value)">
                                        <small>Searches for partial matches in ${col.name}</small>
                                    </div>
                                `;
                            } else if (columnData.values && Array.isArray(columnData.values)) {
                                // Categorical checkbox filter with search (fallback for non-symbol columns)
                                const searchId = `search-${col.name}`;
                                const listId = `list-${col.name}`;
                                
                                // Add search box for categorical values (if more than 10 values)
                                if (columnData.values.length > 10) {
                                    filterHtml += `<div style="margin-bottom: 8px;">
                                        <input type="text" id="${searchId}" placeholder="Search values..." 
                                               style="width: 100%; padding: 4px 8px; border: 1px solid #ddd; border-radius: 4px; font-size: 12px;"
                                               oninput="filterSymbolList('${searchId}', '${listId}')">
                                    </div>`;
                                }
                                
                                filterHtml += `<div class="checkbox-list" id="${listId}" style="max-height: 200px; overflow-y: auto;">`;
                                columnData.values.slice(0, 30).forEach(valueData => { // Show up to 30 values for non-symbol columns
                                    const value = typeof valueData === 'object' ? valueData.value : valueData;
                                    const count = typeof valueData === 'object' ? valueData.count : '';
                                    const countText = count ? ` (${count})` : '';
                                    filterHtml += `
                                        <label style="display: block;">
                                            <input type="checkbox" name="filter-${col.name}" value="${value}"> ${value}${countText}
                                        </label>
                                    `;
                                });
                                if (columnData.values.length > 30) {
                                    filterHtml += `<small style="color: #666; display: block; margin-top: 4px;">(${columnData.values.length - 30} more values in dataset)</small>`;
                                }
                                filterHtml += `</div>`;
                            }
                            
                            filterHtml += `</div>`;
                            return filterHtml;
                            
                        } catch (error) {
                            console.error(`Error loading values for column ${col.name}:`, error);
                            return null;
                        }
                    });
                    
                    // Wait for all filter promises to resolve
                    const filterResults = await Promise.allSettled(filterPromises);
                    
                    // Combine successful results
                    let combinedFilterHtml = '';
                    filterResults.forEach(result => {
                        if (result.status === 'fulfilled' && result.value) {
                            combinedFilterHtml += result.value;
                        }
                    });
                    
                    if (!combinedFilterHtml) {
                        combinedFilterHtml = '<p style="color: red; text-align: center;">No filter data available - database connection required</p>';
                    }
                    
                    filterControls.innerHTML = combinedFilterHtml;
                }
                
                async function loadAllColumnDistributions(datasetName, columns) {
                    const distributionsContainer = document.getElementById('distributions-container');
                    distributionsContainer.innerHTML = '';
                    
                    // Show ALL columns as requested by user - do not hide any columns
                    const columnsToAnalyze = columns;
                    
                    // Create all containers first (immediate UI feedback)
                    const distributionPromises = [];
                    
                    for (const col of columnsToAnalyze) {
                        const dataType = col.type.toLowerCase();
                        const isNumeric = dataType.includes('numeric') || dataType.includes('integer') || 
                            dataType.includes('double') || dataType.includes('bigint') ||
                            dataType.includes('smallint') || dataType.includes('real') ||
                            dataType.includes('decimal') || dataType.includes('float');
                        
                        // Identify date columns for special handling
                        const isDateType = dataType.includes('date') || dataType.includes('timestamp') || 
                            col.name.toLowerCase().includes('date') || col.name.toLowerCase().includes('time');
                        
                        // Identify string columns to completely exclude from visualization
                        // Note: 'type' and 'exchange' are categorical, not string
                        const isStringType = (dataType.includes('varchar') || dataType.includes('text') || 
                            dataType.includes('character')) && !col.name.toLowerCase().includes('type') &&
                            !col.name.toLowerCase().includes('exchange') || 
                            col.name.toLowerCase().includes('id') || col.name.toLowerCase().includes('symbol') || 
                            col.name.toLowerCase().includes('name') || col.name.toLowerCase().includes('title') || 
                            col.name.toLowerCase().includes('url') || col.name.toLowerCase().includes('description');
                        
                        // Completely skip string columns from visualization
                        if (isStringType) {
                            continue;
                        }
                        
                        // Create container for this column's distribution
                        const colDiv = document.createElement('div');
                        colDiv.className = 'column-distribution';
                        const typeLabel = isNumeric ? 'Numeric' : (isDateType ? 'Date' : 'Categorical');
                        colDiv.innerHTML = `
                            <h4>${col.name} (${typeLabel})</h4>
                            <div id="chart-${col.name}" class="distribution-chart">Loading...</div>
                            <div id="stats-${col.name}" class="distribution-stats"></div>
                        `;
                        distributionsContainer.appendChild(colDiv);
                        
                        // Add to parallel loading promises (no await here!)
                        if (isNumeric) {
                            distributionPromises.push(
                                loadNumericDistribution(datasetName, col.name).catch(error => {
                                    console.error(`Error loading numeric distribution for ${col.name}:`, error);
                                    document.getElementById(`chart-${col.name}`).innerHTML = 
                                        `<p style="color: red; text-align: center;">Error loading distribution</p>`;
                                })
                            );
                        } else {
                            distributionPromises.push(
                                loadCategoricalDistribution(datasetName, col.name).catch(error => {
                                    console.error(`Error loading categorical distribution for ${col.name}:`, error);
                                    document.getElementById(`chart-${col.name}`).innerHTML = 
                                        `<p style="color: red; text-align: center;">Error loading distribution</p>`;
                                })
                            );
                        }
                    }
                    
                    // Show loading status for all columns
                    const statusDiv = document.createElement('div');
                    statusDiv.innerHTML = `<p style="text-align: center; color: #666; font-style: italic;">
                        Loading all ${columns.length} column distributions in parallel...
                    </p>`;
                    distributionsContainer.appendChild(statusDiv);
                    
                    // Load all distributions in parallel
                    await Promise.allSettled(distributionPromises);
                    
                    // Populate X-axis dropdowns with date columns
                    populateXAxisOptions(columns);
                    
                    // Update status when done
                    const completionStatusDiv = distributionsContainer.querySelector('p');
                    if (completionStatusDiv) {
                        completionStatusDiv.parentElement.innerHTML = `<p style="text-align: center; color: #666; font-style: italic;">
                            ✅ All ${columns.length} column distributions loaded successfully
                        </p>`;
                    }
                }
                
                async function loadNumericDistribution(datasetName, columnName) {
                    const statsContainer = document.getElementById(`stats-${columnName}`);
                    const chartContainer = document.getElementById(`chart-${columnName}`);
                    
                    // Show loading state
                    statsContainer.innerHTML = '<p style="text-align: center;">Loading...</p>';
                    chartContainer.innerHTML = '<p style="text-align: center;">Loading distribution...</p>';
                    
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
                        
                        // Update with real data
                        if (analysis.statistics) {
                            statsContainer.innerHTML = `
                                <div class="distribution-stat">
                                    <div class="stat-value-small">${analysis.statistics.count.toLocaleString()}</div>
                                    <div class="stat-label-small">Count</div>
                                </div>
                                <div class="distribution-stat">
                                    <div class="stat-value-small">${analysis.statistics.mean ? analysis.statistics.mean.toFixed(2) : 'N/A'}</div>
                                    <div class="stat-label-small">Mean</div>
                                </div>
                                <div class="distribution-stat">
                                    <div class="stat-value-small">${analysis.statistics.std ? analysis.statistics.std.toFixed(2) : 'N/A'}</div>
                                    <div class="stat-label-small">Std Dev</div>
                                </div>
                                <div class="distribution-stat">
                                    <div class="stat-value-small">${analysis.statistics.min ? analysis.statistics.min.toFixed(2) : 'N/A'}</div>
                                    <div class="stat-label-small">Min</div>
                                </div>
                                <div class="distribution-stat">
                                    <div class="stat-value-small">${analysis.statistics.max ? analysis.statistics.max.toFixed(2) : 'N/A'}</div>
                                    <div class="stat-label-small">Max</div>
                                </div>
                            `;
                        }
                        
                        if (analysis.histogram) {
                            const realTrace = {
                                x: analysis.histogram.bin_centers,
                                y: analysis.histogram.counts,
                                type: 'bar',
                                name: columnName,
                                marker: { color: '#3498db' }
                            };
                            
                            const realLayout = {
                                title: `Distribution: ${columnName}`,
                                xaxis: { title: columnName },
                                yaxis: { title: 'Frequency' },
                                bargap: 0.1,
                                margin: { l: 60, r: 20, t: 40, b: 60 }
                            };
                            
                            Plotly.newPlot(`chart-${columnName}`, [realTrace], realLayout, {responsive: true});
                        }
                        
                    } catch (error) {
                        console.error(`Error loading real data for ${columnName}:`, error);
                        statsContainer.innerHTML = `<p style="color: red; text-align: center;">Error loading stats: ${error.message}</p>`;
                        chartContainer.innerHTML = `<p style="color: red; text-align: center;">Error loading distribution: ${error.message}</p>`;
                    }
                }
                
                async function loadCategoricalDistribution(datasetName, columnName) {
                    const statsContainer = document.getElementById(`stats-${columnName}`);
                    const chartContainer = document.getElementById(`chart-${columnName}`);
                    
                    // Show loading state
                    statsContainer.innerHTML = '<p style="text-align: center;">Loading...</p>';
                    chartContainer.innerHTML = '<p style="text-align: center;">Loading distribution...</p>';
                    
                    try {
                        const response = await fetch(`/api/eda/datasets/${datasetName}/columns/${columnName}/values?limit=10`);
                        const data = await response.json();
                        
                        if (data.error || !data.values) {
                            throw new Error(data.error || 'No data available');
                        }
                        
                        // Update with real data
                        statsContainer.innerHTML = `
                            <div class="distribution-stat">
                                <div class="stat-value-small">${data.total_unique}</div>
                                <div class="stat-label-small">Unique</div>
                            </div>
                            <div class="distribution-stat">
                                <div class="stat-value-small">${data.values.length}</div>
                                <div class="stat-label-small">Showing</div>
                            </div>
                        `;
                        
                        if (data.values && data.values.length > 0) {
                            const values = data.values.slice(0, 8); // Show top 8 values for speed
                            
                            const realTrace = {
                                x: values.map(v => v.value),
                                y: values.map(v => v.count || 1),
                                type: 'bar',
                                name: columnName,
                                marker: { color: '#e74c3c' }
                            };
                            
                            const realLayout = {
                                title: `Distribution: ${columnName}`,
                                xaxis: { 
                                    title: columnName,
                                    tickangle: -45
                                },
                                yaxis: { title: 'Count' },
                                bargap: 0.2,
                                margin: { l: 60, r: 20, t: 40, b: 100 }
                            };
                            
                            Plotly.newPlot(`chart-${columnName}`, [realTrace], realLayout, {responsive: true});
                        }
                        
                    } catch (error) {
                        console.error(`Error loading real data for ${columnName}:`, error);
                        statsContainer.innerHTML = `<p style="color: red; text-align: center;">Error loading stats: ${error.message}</p>`;
                        chartContainer.innerHTML = `<p style="color: red; text-align: center;">Error loading distribution: ${error.message}</p>`;
                    }
                }
                
                function applyFilters() {
                    const datasetName = document.getElementById('dataset-select').value;
                    if (!datasetName) return;
                    
                    currentFilters = {};
                    
                    // Collect numeric filters
                    document.querySelectorAll('[id^="filter-"][id$="-min"], [id^="filter-"][id$="-max"]').forEach(input => {
                        if (input.value) {
                            const match = input.id.match(/filter-(.+)-(min|max)/);
                            if (match) {
                                const columnName = match[1];
                                const type = match[2];
                                
                                if (!currentFilters[columnName]) {
                                    currentFilters[columnName] = { type: 'range' };
                                }
                                currentFilters[columnName][type] = parseFloat(input.value);
                            }
                        }
                    });
                    
                    // Collect date range filters
                    const dateInputs = {};
                    document.querySelectorAll('[id$="-start"], [id$="-end"]').forEach(input => {
                        if (input.value) {
                            const match = input.id.match(/filter-(.+)-(start|end)/);
                            if (match) {
                                const columnName = match[1];
                                const dateType = match[2];
                                
                                if (!dateInputs[columnName]) {
                                    dateInputs[columnName] = {};
                                }
                                dateInputs[columnName][dateType] = input.value;
                            }
                        }
                    });
                    
                    // Convert date inputs to filters
                    Object.keys(dateInputs).forEach(columnName => {
                        const dates = dateInputs[columnName];
                        if (dates.start || dates.end) {
                            currentFilters[columnName] = { 
                                type: 'date_range',
                                start: dates.start,
                                end: dates.end
                            };
                        }
                    });
                    
                    // Collect string search filters
                    document.querySelectorAll('[id$="-search"]').forEach(input => {
                        if (input.value && input.value.trim()) {
                            const columnName = input.id.replace('filter-', '').replace('-search', '');
                            currentFilters[columnName] = { type: 'string_search', value: input.value.trim() };
                        }
                    });
                    
                    // Collect categorical filters
                    document.querySelectorAll('[name^="filter-"]:checked').forEach(checkbox => {
                        const columnName = checkbox.name.replace('filter-', '');
                        if (!currentFilters[columnName]) {
                            currentFilters[columnName] = { type: 'values', values: [] };
                        }
                        currentFilters[columnName].values.push(checkbox.value);
                    });
                    
                    console.log('Applied filters:', currentFilters);
                    
                    // Actually load the filtered data instead of just showing alert
                    if (Object.keys(currentFilters).length > 0) {
                        loadFilteredData(1); // Load first page of filtered data
                        
                        // Show success message briefly
                        const filterButton = document.querySelector('button[onclick="applyFilters()"]');
                        if (filterButton) {
                            const originalText = filterButton.textContent;
                            filterButton.textContent = '✅ Filters Applied';
                            filterButton.style.background = '#28a745';
                            setTimeout(() => {
                                filterButton.textContent = originalText;
                                filterButton.style.background = '';
                            }, 2000);
                        }
                    } else {
                        alert('No filters selected. Please set some filter values first.');
                    }
                }
                
                function clearFilters() {
                    currentFilters = {};
                    
                    // Clear all filter inputs
                    document.querySelectorAll('[id^="filter-"]').forEach(input => {
                        if (input.type === 'checkbox') {
                            input.checked = false;
                        } else {
                            input.value = '';
                        }
                    });
                    
                    // Clear string search filters specifically
                    document.querySelectorAll('[id$="-search"]').forEach(input => {
                        input.value = '';
                    });
                    
                    // Clear date range filters
                    document.querySelectorAll('[id$="-start"], [id$="-end"]').forEach(input => {
                        input.value = '';
                    });
                    
                    // Hide data table
                    document.getElementById('data-table-section').style.display = 'none';
                    console.log('Filters cleared');
                }
                
                async function loadFilteredData(page = 1) {
                    const datasetName = document.getElementById('dataset-select').value;
                    if (!datasetName) return;
                    
                    try {
                        const response = await fetch(`/api/eda/datasets/${datasetName}/data`, {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({
                                filters: currentFilters,
                                page: page,
                                page_size: 50
                            })
                        });
                        
                        const data = await response.json();
                        
                        if (data.error) {
                            alert('Error loading data: ' + data.error);
                            return;
                        }
                        
                        displayDataTable(data);
                        currentPage = data.pagination?.current_page || 1;
                        totalPages = data.pagination?.total_pages || 1;
                        
                    } catch (error) {
                        console.error('Error loading filtered data:', error);
                        alert('Error loading filtered data');
                    }
                }
                
                function sortTable(column, direction = null) {
                    if (!currentTableData.length) return;
                    
                    // Toggle direction if same column clicked
                    if (direction === null) {
                        if (currentSortColumn === column) {
                            currentSortDirection = currentSortDirection === 'asc' ? 'desc' : 'asc';
                        } else {
                            currentSortDirection = 'asc';
                        }
                    } else {
                        currentSortDirection = direction;
                    }
                    
                    currentSortColumn = column;
                    
                    // Sort the data
                    currentTableData.sort((a, b) => {
                        let aVal = a[column];
                        let bVal = b[column];
                        
                        // Handle null/undefined values
                        if (aVal === null || aVal === undefined) aVal = '';
                        if (bVal === null || bVal === undefined) bVal = '';
                        
                        // Convert to strings for comparison
                        aVal = String(aVal);
                        bVal = String(bVal);
                        
                        // Try to parse as numbers if they look numeric
                        const aNum = parseFloat(aVal);
                        const bNum = parseFloat(bVal);
                        
                        if (!isNaN(aNum) && !isNaN(bNum)) {
                            // Numeric comparison
                            return currentSortDirection === 'asc' ? aNum - bNum : bNum - aNum;
                        } else {
                            // String comparison
                            return currentSortDirection === 'asc' 
                                ? aVal.localeCompare(bVal) 
                                : bVal.localeCompare(aVal);
                        }
                    });
                    
                    // Re-render table body
                    renderTableBody();
                    updateSortIndicators();
                }
                
                function renderTableBody() {
                    const tableBody = document.getElementById('table-body');
                    if (currentTableData.length > 0) {
                        const headers = Object.keys(currentTableData[0]);
                        tableBody.innerHTML = currentTableData.map(row => 
                            '<tr>' + headers.map(h => `<td>${row[h] || ''}</td>`).join('') + '</tr>'
                        ).join('');
                    }
                }
                
                function updateSortIndicators() {
                    // Remove all sort classes
                    document.querySelectorAll('#data-table th').forEach(th => {
                        th.classList.remove('sort-asc', 'sort-desc');
                        th.classList.add('sortable');
                    });
                    
                    // Add current sort class
                    if (currentSortColumn) {
                        const headers = Array.from(document.querySelectorAll('#data-table th'));
                        const headerIndex = headers.findIndex(th => 
                            th.textContent.replace(/ [⇅↑↓]/, '') === currentSortColumn
                        );
                        
                        if (headerIndex >= 0) {
                            headers[headerIndex].classList.add(`sort-${currentSortDirection}`);
                        }
                    }
                }
                
                function displayDataTable(data) {
                    const tableSection = document.getElementById('data-table-section');
                    const tableInfo = document.getElementById('table-info');
                    const tableHead = document.getElementById('table-head');
                    const tableBody = document.getElementById('table-body');
                    
                    // Store current data for sorting
                    currentTableData = [...data.data];
                    
                    // Show table section
                    tableSection.style.display = 'block';
                    
                    // Update info with sorting hint
                    const filterCount = Object.keys(currentFilters).length;
                    tableInfo.innerHTML = `
                        Showing ${data.data.length} of ${data.pagination.total_count} records 
                        (Page ${data.pagination.current_page} of ${data.pagination.total_pages})
                        ${filterCount > 0 ? `with ${filterCount} filter(s) applied` : ''}
                        <span style="color: #666; margin-left: 15px; font-style: italic;">📊 Click column headers to sort</span>
                    `;
                    
                    // Create sortable table header
                    if (data.data.length > 0) {
                        const headers = Object.keys(data.data[0]);
                        tableHead.innerHTML = '<tr>' + headers.map(h => 
                            `<th class="sortable" onclick="sortTable('${h}')" title="Click to sort by ${h}">${h}</th>`
                        ).join('') + '</tr>';
                        
                        // Render table body
                        renderTableBody();
                        
                        // Apply current sort if any, otherwise just update indicators
                        if (currentSortColumn && headers.includes(currentSortColumn)) {
                            sortTable(currentSortColumn, currentSortDirection);
                        } else {
                            updateSortIndicators();
                        }
                    } else {
                        tableHead.innerHTML = '<tr><th>No Data</th></tr>';
                        tableBody.innerHTML = '<tr><td>No data matches the current filters</td></tr>';
                        currentTableData = [];
                    }
                    
                    // Update pagination
                    updatePagination(data);
                }
                
                function updatePagination(data) {
                    const paginationControls = document.getElementById('pagination-controls');
                    
                    if (data.total_pages <= 1) {
                        paginationControls.innerHTML = '';
                        return;
                    }
                    
                    let paginationHtml = '';
                    
                    // Previous button
                    paginationHtml += `<button class="pagination-btn" ${data.has_previous ? '' : 'disabled'} onclick="loadFilteredData(${data.current_page - 1})">← Previous</button>`;
                    
                    // Page numbers (show a few around current page)
                    const startPage = Math.max(1, data.current_page - 2);
                    const endPage = Math.min(data.total_pages, data.current_page + 2);
                    
                    if (startPage > 1) {
                        paginationHtml += `<button class="pagination-btn" onclick="loadFilteredData(1)">1</button>`;
                        if (startPage > 2) paginationHtml += '<span>...</span>';
                    }
                    
                    for (let i = startPage; i <= endPage; i++) {
                        const activeClass = i === data.current_page ? ' active' : '';
                        paginationHtml += `<button class="pagination-btn${activeClass}" onclick="loadFilteredData(${i})">${i}</button>`;
                    }
                    
                    if (endPage < data.total_pages) {
                        if (endPage < data.total_pages - 1) paginationHtml += '<span>...</span>';
                        paginationHtml += `<button class="pagination-btn" onclick="loadFilteredData(${data.total_pages})">${data.total_pages}</button>`;
                    }
                    
                    // Next button
                    paginationHtml += `<button class="pagination-btn" ${data.has_next ? '' : 'disabled'} onclick="loadFilteredData(${data.current_page + 1})">Next →</button>`;
                    
                    paginationControls.innerHTML = paginationHtml;
                }
                
                function populateXAxisOptions(columns) {
                    // Find all date columns
                    const dateColumns = columns.filter(col => {
                        const dataType = col.type.toLowerCase();
                        return dataType.includes('date') || dataType.includes('timestamp') || 
                               col.name.toLowerCase().includes('date') || col.name.toLowerCase().includes('time');
                    });
                    
                    // Populate all X-axis dropdowns
                    document.querySelectorAll('[id^="xaxis-"]').forEach(select => {
                        dateColumns.forEach(dateCol => {
                            const option = document.createElement('option');
                            option.value = dateCol.name;
                            option.textContent = `${dateCol.name} (Date)`;
                            select.appendChild(option);
                        });
                    });
                    
                    console.log(`📅 Added ${dateColumns.length} date columns as X-axis options`);
                }
                
                function updateGlobalXAxis() {
                    const globalXAxis = document.getElementById('global-x-axis').value;
                    console.log('Global X-axis updated to:', globalXAxis);
                    
                    // Re-render all column distributions with new x-axis setting
                    const datasetName = document.getElementById('dataset-select').value;
                    if (datasetName) {
                        // Get current schema columns
                        const distributionsContainer = document.getElementById('distributions-container');
                        const columnDivs = distributionsContainer.querySelectorAll('.column-distribution');
                        
                        columnDivs.forEach(colDiv => {
                            const h4 = colDiv.querySelector('h4');
                            if (h4) {
                                const columnName = h4.textContent.split(' (')[0]; // Extract column name from "name (type)"
                                const isNumeric = h4.textContent.includes('(Numeric)');
                                
                                // Reload distribution with global x-axis
                                if (isNumeric) {
                                    loadNumericDistribution(datasetName, columnName, globalXAxis);
                                } else {
                                    loadCategoricalDistribution(datasetName, columnName, globalXAxis);
                                }
                            }
                        });
                    }
                }
                
                // Symbol search filter function
                function filterSymbolList(searchInputId, listId) {
                    const searchInput = document.getElementById(searchInputId);
                    const symbolList = document.getElementById(listId);
                    const searchTerm = searchInput.value.toLowerCase();
                    
                    const labels = symbolList.querySelectorAll('label');
                    let visibleCount = 0;
                    
                    labels.forEach(label => {
                        const symbolText = label.textContent.toLowerCase();
                        const shouldShow = symbolText.includes(searchTerm);
                        label.style.display = shouldShow ? 'block' : 'none';
                        if (shouldShow) visibleCount++;
                    });
                    
                    // Show message if no results
                    let noResultsMsg = symbolList.querySelector('.no-results');
                    if (visibleCount === 0 && searchTerm.length > 0) {
                        if (!noResultsMsg) {
                            noResultsMsg = document.createElement('div');
                            noResultsMsg.className = 'no-results';
                            noResultsMsg.style.cssText = 'color: #666; font-style: italic; padding: 8px; text-align: center;';
                            noResultsMsg.textContent = 'No symbols found matching "' + searchTerm + '"';
                            symbolList.appendChild(noResultsMsg);
                        } else {
                            noResultsMsg.textContent = 'No symbols found matching "' + searchTerm + '"';
                        }
                    } else if (noResultsMsg) {
                        noResultsMsg.remove();
                    }
                }
                
                // Legacy function - now using global x-axis control instead
                async function updateVisualization(columnName) {
                    console.log('updateVisualization called but deprecated - using updateGlobalXAxis() instead');
                    updateGlobalXAxis(); // Use the new global function instead
                }
                
                function generateMockTimeSeriesData(columnName) {
                    // Generate 30 days of mock data
                    const data = [];
                    const today = new Date();
                    
                    for (let i = 29; i >= 0; i--) {
                        const date = new Date(today);
                        date.setDate(today.getDate() - i);
                        const dateStr = date.toISOString().split('T')[0];
                        
                        let value;
                        if (columnName.includes('price') || columnName.includes('close') || columnName.includes('open')) {
                            value = 100 + Math.random() * 50 + Math.sin(i / 5) * 20; // Price-like data
                        } else if (columnName.includes('volume')) {
                            value = 1000000 + Math.random() * 500000; // Volume-like data
                        } else {
                            value = Math.floor(10 + Math.random() * 20); // Count data
                        }
                        
                        data.push({ x: dateStr, y: value });
                    }
                    
                    return data;
                }
                
                // Debounce function for string filters
                let stringFilterTimeout = null;
                function debounceStringFilter(columnName, searchValue) {
                    clearTimeout(stringFilterTimeout);
                    stringFilterTimeout = setTimeout(() => {
                        if (searchValue.trim().length > 0) {
                            currentFilters[columnName] = { type: 'string_search', value: searchValue.trim() };
                        } else {
                            delete currentFilters[columnName];
                        }
                        console.log(`String filter for ${columnName}:`, searchValue);
                    }, 500); // 500ms delay
                }
                
                async function loadDataTable(datasetName, columns) {
                    try {
                        // Load initial data table without filters (first page)
                        const response = await fetch(`/api/eda/datasets/${datasetName}/data`, {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({
                                filters: {},
                                page: 1,
                                page_size: 50
                            })
                        });
                        
                        const data = await response.json();
                        
                        if (data.error) {
                            console.error('Error loading data table:', data.error);
                            return;
                        }
                        
                        displayDataTable(data);
                        currentPage = data.pagination?.current_page || 1;
                        totalPages = data.pagination?.total_pages || 1;
                        
                        // Show data table section
                        document.getElementById('data-table-section').style.display = 'block';
                        
                    } catch (error) {
                        console.error('Error loading data table:', error);
                    }
                }
                
                function nextPage() {
                    if (currentPage < totalPages) {
                        loadFilteredData(currentPage + 1);
                    }
                }
                
                function previousPage() {
                    if (currentPage > 1) {
                        loadFilteredData(currentPage - 1);
                    }
                }
                
                
                // Pre-computation functions
                async function precomputeStatistics() {
                    const datasetName = document.getElementById('dataset-select').value;
                    if (!datasetName) {
                        alert('Please select a dataset first');
                        return;
                    }
                    
                    const precomputeInfo = document.getElementById('precompute-info');
                    const precomputeBtn = document.getElementById('precompute-btn');
                    
                    try {
                        // Show loading state
                        precomputeInfo.style.display = 'block';
                        precomputeInfo.style.backgroundColor = '#fff3cd';
                        precomputeInfo.style.borderColor = '#ffeaa7';
                        precomputeInfo.innerHTML = '🔄 Starting pre-computation for ' + datasetName + '...';
                        precomputeBtn.disabled = true;
                        precomputeBtn.textContent = 'Computing...';
                        
                        const response = await fetch(`/api/eda/datasets/${datasetName}/precompute`);
                        const result = await response.json();
                        
                        if (result.error) {
                            throw new Error(result.error);
                        }
                        
                        if (result.success) {
                            precomputeInfo.style.backgroundColor = '#d4edda';
                            precomputeInfo.style.borderColor = '#c3e6cb';
                            precomputeInfo.innerHTML = `✅ Pre-computation completed for ${datasetName}!<br/>` +
                                `📊 Statistics computed for ${result.columns_computed || 'all'} columns<br/>` +
                                `⚡ Visualization performance should be significantly improved`;
                        } else {
                            // Still running - start polling for status
                            precomputeInfo.innerHTML = '🔄 Pre-computation in progress... checking status...';
                            setTimeout(() => checkPrecomputeStatus(), 2000);
                        }
                        
                    } catch (error) {
                        console.error('Error starting pre-computation:', error);
                        precomputeInfo.style.backgroundColor = '#f8d7da';
                        precomputeInfo.style.borderColor = '#f5c6cb';
                        precomputeInfo.innerHTML = `❌ Error: ${error.message}`;
                    } finally {
                        precomputeBtn.disabled = false;
                        precomputeBtn.textContent = '⚡ Pre-compute All Statistics';
                    }
                }
                
                async function checkPrecomputeStatus() {
                    const datasetName = document.getElementById('dataset-select').value;
                    if (!datasetName) {
                        alert('Please select a dataset first');
                        return;
                    }
                    
                    const precomputeInfo = document.getElementById('precompute-info');
                    const statusBtn = document.getElementById('precompute-status-btn');
                    
                    try {
                        precomputeInfo.style.display = 'block';
                        precomputeInfo.style.backgroundColor = '#fff3cd';
                        precomputeInfo.style.borderColor = '#ffeaa7';
                        precomputeInfo.innerHTML = '🔍 Checking pre-computation status...';
                        statusBtn.disabled = true;
                        
                        const response = await fetch(`/api/eda/datasets/${datasetName}/precompute/status`);
                        const result = await response.json();
                        
                        if (result.error) {
                            throw new Error(result.error);
                        }
                        
                        if (result.precomputed) {
                            precomputeInfo.style.backgroundColor = '#d4edda';
                            precomputeInfo.style.borderColor = '#c3e6cb';
                            precomputeInfo.innerHTML = `✅ ${result.message}<br/>` +
                                `📋 Pre-computed tables: ${(result.precomputed_tables || []).join(', ')}<br/>` +
                                `⚡ Fast histogram generation enabled!`;
                        } else {
                            precomputeInfo.style.backgroundColor = '#f8d7da';
                            precomputeInfo.style.borderColor = '#f5c6cb';
                            precomputeInfo.innerHTML = `ℹ️ ${result.message}<br/>` +
                                'Run pre-computation to speed up visualization loading times by 20-100x';
                        }
                        
                    } catch (error) {
                        console.error('Error checking pre-computation status:', error);
                        precomputeInfo.style.backgroundColor = '#f8d7da';
                        precomputeInfo.style.borderColor = '#f5c6cb';
                        precomputeInfo.innerHTML = `❌ Error checking status: ${error.message}`;
                    } finally {
                        statusBtn.disabled = false;
                    }
                }
                
                // Load data on page load
                document.addEventListener('DOMContentLoaded', function() {
                    console.log('DOM loaded, calling loadDatasets...');
                    loadDatasets();
                });
                
                // Also try loading after a short delay as backup
                setTimeout(function() {
                    console.log('Backup load attempt...');
                    if (datasets.length === 0) {
                        loadDatasets();
                    }
                }, 1000);
            </script>
        </body>
        </html>
        """

    def get_training_eda_dashboard_html(self):
        """Generate the Training Dataset EDA dashboard HTML."""
        return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Training Dataset EDA Dashboard</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }
        
        .header {
            text-align: center;
            color: white;
            margin-bottom: 30px;
        }
        
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        
        .header p {
            font-size: 1.2em;
            opacity: 0.9;
        }
        
        .main-card {
            background: white;
            border-radius: 15px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
            padding: 30px;
            margin-bottom: 30px;
        }
        
        .dataset-selection {
            background: #f8f9fa;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 30px;
            border-left: 5px solid #667eea;
        }
        
        .dataset-selection h3 {
            color: #2c3e50;
            margin-bottom: 15px;
            font-size: 1.3em;
        }
        
        .controls {
            display: flex;
            align-items: center;
            gap: 15px;
            flex-wrap: wrap;
        }
        
        .controls label {
            font-weight: 600;
            color: #495057;
        }
        
        select {
            padding: 12px 16px;
            border: 2px solid #e9ecef;
            border-radius: 8px;
            font-size: 16px;
            min-width: 300px;
            transition: all 0.3s ease;
        }
        
        select:focus {
            outline: none;
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }
        
        .dataset-info {
            display: none;
            margin-top: 20px;
            padding: 20px;
            background: white;
            border-radius: 10px;
            box-shadow: 0 4px 8px rgba(0,0,0,0.05);
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        
        .stat-card {
            text-align: center;
            padding: 20px;
            background: linear-gradient(45deg, #667eea, #764ba2);
            color: white;
            border-radius: 10px;
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        }
        
        .stat-value {
            font-size: 2.2em;
            font-weight: bold;
            margin-bottom: 5px;
        }
        
        .stat-label {
            font-size: 0.9em;
            opacity: 0.9;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        .analysis-content {
            margin-top: 30px;
        }
        
        .features-section, .distributions-section {
            margin-top: 30px;
            padding: 25px;
            background: #f8f9fa;
            border-radius: 10px;
        }
        
        .features-section h4, .distributions-section h4 {
            color: #2c3e50;
            margin-bottom: 20px;
            font-size: 1.4em;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }
        
        .feature-list {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
            gap: 15px;
        }
        
        .feature-item {
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            border-left: 4px solid #667eea;
        }
        
        .feature-name {
            font-weight: 600;
            color: #2c3e50;
            margin-bottom: 5px;
        }
        
        .feature-stats {
            font-size: 0.9em;
            color: #6c757d;
        }
        
        .chart-container {
            background: white;
            padding: 20px;
            border-radius: 10px;
            margin-top: 20px;
            box-shadow: 0 4px 8px rgba(0,0,0,0.05);
        }
        
        .loading {
            text-align: center;
            padding: 40px;
            color: #6c757d;
            font-size: 1.1em;
        }
        
        .error {
            color: #e74c3c;
            background: #fdf2f2;
            padding: 15px;
            border-radius: 8px;
            border-left: 4px solid #e74c3c;
        }
        
        /* Table Styles */
        .table-scroll {
            overflow-x: auto;
            margin: 15px 0;
        }
        
        .table-pagination {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
            padding: 10px;
            background: #f8f9fa;
            border-radius: 8px;
        }
        
        .table-pagination button {
            padding: 8px 16px;
            border: 1px solid #dee2e6;
            background: white;
            border-radius: 5px;
            cursor: pointer;
            transition: all 0.2s ease;
        }
        
        .table-pagination button:hover:not(:disabled) {
            background: #667eea;
            color: white;
            border-color: #667eea;
        }
        
        .table-pagination button:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }
        
        #training-data-table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }
        
        #training-data-table th,
        #training-data-table td {
            padding: 12px 8px;
            text-align: left;
            border-bottom: 1px solid #dee2e6;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            max-width: 150px;
        }
        
        #training-data-table th {
            background: linear-gradient(45deg, #667eea, #764ba2);
            color: white;
            font-weight: 600;
            position: sticky;
            top: 0;
            z-index: 10;
        }
        
        #training-data-table tbody tr:hover {
            background: #f8f9fa;
        }
        
        #training-data-table tbody tr:nth-child(even) {
            background: #fdfdfd;
        }
        
        .success {
            color: #27ae60;
            background: #f0f8f4;
            padding: 15px;
            border-radius: 8px;
            border-left: 4px solid #27ae60;
        }
        
        .nav-links {
            text-align: center;
            margin-bottom: 20px;
        }
        
        .nav-links a {
            color: white;
            text-decoration: none;
            padding: 10px 20px;
            margin: 0 10px;
            border-radius: 20px;
            background: rgba(255,255,255,0.2);
            transition: all 0.3s ease;
        }
        
        .nav-links a:hover {
            background: rgba(255,255,255,0.3);
            transform: translateY(-2px);
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🤖 Training Dataset EDA Dashboard</h1>
            <p>Explore and analyze your machine learning training datasets</p>
        </div>
        
        <div class="nav-links">
            <a href="/eda">📊 Table EDA Dashboard</a>
            <a href="/health">🏥 System Health</a>
        </div>
        
        <div class="main-card">
            <div class="dataset-selection">
                <h3>📂 Select Training Dataset</h3>
                <div class="controls">
                    <label for="training-dataset-select">Choose Dataset:</label>
                    <select id="training-dataset-select" onchange="loadTrainingDatasetAnalysis()">
                        <option value="">Loading training datasets...</option>
                    </select>
                </div>
                
                <div id="dataset-info" class="dataset-info">
                    <div class="stats-grid">
                        <div class="stat-card">
                            <div class="stat-value" id="total-sequences">0</div>
                            <div class="stat-label">Total Sequences</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value" id="feature-count">0</div>
                            <div class="stat-label">Features</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value" id="label-count">0</div>
                            <div class="stat-label">Labels</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value" id="quality-score">0.0</div>
                            <div class="stat-label">Quality Score</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value" id="file-size">0 MB</div>
                            <div class="stat-label">File Size</div>
                        </div>
                        <div class="stat-card">
                            <div class="stat-value" id="date-range">-</div>
                            <div class="stat-label">Date Range</div>
                        </div>
                    </div>
                </div>
            </div>
            
            <div id="analysis-content" class="analysis-content">
                <div class="loading">
                    👆 Select a training dataset above to view detailed analysis and visualizations
                </div>
            </div>
            
            <!-- Training Data Table Section -->
            <div id="training-data-table-section" class="main-card" style="display: none;">
                <h3>🗂️ Training Data Preview</h3>
                <div id="training-table-info" style="margin-bottom: 15px; color: #666;"></div>
                <div class="table-pagination">
                    <button id="training-prev-page" onclick="previousTrainingPage()" disabled>← Previous</button>
                    <span id="training-page-info">Page 1</span>
                    <button id="training-next-page" onclick="nextTrainingPage()">Next →</button>
                    <label style="margin-left: 20px;">
                        <input type="checkbox" id="use-ray-processing" style="margin-right: 5px;"> 
                        ⚡ Use Ray Parallel Processing
                    </label>
                </div>
                <div id="training-data-table-container" class="table-scroll">
                    <table id="training-data-table">
                        <thead id="training-table-head"></thead>
                        <tbody id="training-table-body"></tbody>
                    </table>
                </div>
                <div id="training-pagination-controls" style="margin-top: 15px; text-align: center;"></div>
            </div>
        </div>
    </div>

    <script>
        let trainingDatasets = [];
        let currentDataset = null;
        let trainingCurrentPage = 1;
        let trainingTotalPages = 1;
        let trainingTableData = [];
        
        // Load training datasets on page load
        document.addEventListener('DOMContentLoaded', function() {
            console.log('Training Dataset EDA Dashboard loaded');
            loadTrainingDatasets();
        });
        
        async function loadTrainingDatasets() {
            console.log('🔄 Loading training datasets...');
            const select = document.getElementById('training-dataset-select');
            
            try {
                const response = await fetch('/api/v1/training-datasets');
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }
                
                const data = await response.json();
                trainingDatasets = data.datasets || [];
                
                console.log(`✅ Loaded ${trainingDatasets.length} training datasets:`, trainingDatasets);
                
                // Clear and populate select
                select.innerHTML = '<option value="">Select a training dataset...</option>';
                
                if (trainingDatasets.length === 0) {
                    select.innerHTML = '<option value="">No training datasets found</option>';
                    return;
                }
                
                trainingDatasets.forEach(dataset => {
                    const option = document.createElement('option');
                    option.value = dataset.id;
                    option.textContent = `${dataset.dataset_name} (${dataset.total_sequences} sequences)`;
                    select.appendChild(option);
                    console.log(`➕ Added option: ${dataset.dataset_name} (ID: ${dataset.id})`);
                });
                
                console.log(`✅ Successfully populated dropdown with ${trainingDatasets.length} datasets`);
                console.log(`📋 Final dropdown state: ${select.options.length} total options`);
                
            } catch (error) {
                console.error('❌ Error loading training datasets:', error);
                select.innerHTML = '<option value="">Error loading datasets</option>';
                
                document.getElementById('analysis-content').innerHTML = `
                    <div class="error">
                        <h4>❌ Failed to Load Training Datasets</h4>
                        <p><strong>Error:</strong> ${error.message}</p>
                        <p>Please check that the analytics service is running and the database connection is working.</p>
                        <p><strong>Debug info:</strong> Trying to load from /api/v1/training-datasets</p>
                    </div>
                `;
            }
        }
        
        function loadTrainingDatasetAnalysis() {
            const select = document.getElementById('training-dataset-select');
            const datasetId = select.value;
            const datasetInfo = document.getElementById('dataset-info');
            const analysisContent = document.getElementById('analysis-content');
            
            console.log(`🔍 Selected dataset ID: ${datasetId}`);
            
            if (!datasetId) {
                datasetInfo.style.display = 'none';
                analysisContent.innerHTML = '<div class="loading">👆 Select a training dataset above to view detailed analysis and visualizations</div>';
                return;
            }
            
            currentDataset = trainingDatasets.find(d => d.id == datasetId);
            if (!currentDataset) {
                analysisContent.innerHTML = '<div class="error">Dataset not found</div>';
                return;
            }
            
            console.log('📊 Loading analysis for dataset:', currentDataset);
            
            // Update basic stats
            document.getElementById('total-sequences').textContent = currentDataset.total_sequences.toLocaleString();
            document.getElementById('feature-count').textContent = currentDataset.feature_count;
            document.getElementById('label-count').textContent = currentDataset.label_count;
            document.getElementById('quality-score').textContent = currentDataset.data_quality_score.toFixed(2);
            document.getElementById('file-size').textContent = `${currentDataset.file_size_mb.toFixed(1)} MB`;
            
            const startDate = currentDataset.date_range_start ? new Date(currentDataset.date_range_start).getFullYear() : 'N/A';
            const endDate = currentDataset.date_range_end ? new Date(currentDataset.date_range_end).getFullYear() : 'N/A';
            document.getElementById('date-range').textContent = `${startDate} - ${endDate}`;
            
            datasetInfo.style.display = 'block';
            
            // Load detailed analysis
            loadDatasetDistributions(datasetId);
            
            // Load training data table
            loadTrainingDataTable(datasetId);
        }
        
        async function loadDatasetDistributions(datasetId) {
            const analysisContent = document.getElementById('analysis-content');
            
            analysisContent.innerHTML = '<div class="loading">📊 Loading dataset distributions and TFDV analysis...</div>';
            
            try {
                console.log('📈 Loading distributions for dataset ID:', datasetId);
                const response = await fetch(`/api/v1/training-datasets/${datasetId}/distributions`);
                
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }
                
                const data = await response.json();
                console.log('📊 Received distributions data:', data);
                
                // Create analysis content
                let html = `
                    <div class="success">
                        <h4>✅ Dataset Analysis Complete</h4>
                        <p>Successfully loaded statistical distributions and data validation results.</p>
                    </div>
                `;
                
                // Feature distributions
                if (data.feature_distributions && Object.keys(data.feature_distributions).length > 0) {
                    html += `
                        <div class="features-section">
                            <h4>📊 Feature Distributions</h4>
                            <div class="feature-list">
                    `;
                    
                    Object.entries(data.feature_distributions).forEach(([featureName, stats]) => {
                        html += `
                            <div class="feature-item">
                                <div class="feature-name">${featureName}</div>
                                <div class="feature-stats">
                                    ${typeof stats === 'object' ? Object.entries(stats).map(([k,v]) => `${k}: ${v}`).join(', ') : stats}
                                </div>
                            </div>
                        `;
                    });
                    
                    html += `
                            </div>
                        </div>
                    `;
                }
                
                // Label distributions
                if (data.label_distributions && Object.keys(data.label_distributions).length > 0) {
                    html += `
                        <div class="features-section">
                            <h4>🎯 Label Distributions</h4>
                            <div class="feature-list">
                    `;
                    
                    Object.entries(data.label_distributions).forEach(([labelName, stats]) => {
                        html += `
                            <div class="feature-item">
                                <div class="feature-name">${labelName}</div>
                                <div class="feature-stats">
                                    ${typeof stats === 'object' ? Object.entries(stats).map(([k,v]) => `${k}: ${v}`).join(', ') : stats}
                                </div>
                            </div>
                        `;
                    });
                    
                    html += `
                            </div>
                        </div>
                    `;
                }
                
                // TFDV Statistics
                if (data.tfdv_statistics && Object.keys(data.tfdv_statistics).length > 0) {
                    html += `
                        <div class="distributions-section">
                            <h4>🔍 TensorFlow Data Validation Statistics</h4>
                            <div id="tfdv-charts"></div>
                        </div>
                    `;
                }
                
                // TFDV Anomalies
                if (data.tfdv_anomalies && Object.keys(data.tfdv_anomalies).length > 0) {
                    html += `
                        <div class="distributions-section">
                            <h4>⚠️ Data Anomalies Detected</h4>
                            <div class="error">
                                <pre>${JSON.stringify(data.tfdv_anomalies, null, 2)}</pre>
                            </div>
                        </div>
                    `;
                }
                
                analysisContent.innerHTML = html;
                
                // Create TFDV visualizations if available
                if (data.tfdv_statistics && Object.keys(data.tfdv_statistics).length > 0) {
                    createTFDVCharts(data.tfdv_statistics);
                }
                
            } catch (error) {
                console.error('❌ Error loading distributions:', error);
                analysisContent.innerHTML = `
                    <div class="error">
                        <h4>❌ Failed to Load Dataset Analysis</h4>
                        <p><strong>Error:</strong> ${error.message}</p>
                        <p>This could be due to missing TFDV data or database connection issues.</p>
                        <p><strong>Dataset ID:</strong> ${datasetId}</p>
                        <p><strong>API Endpoint:</strong> /api/v1/training-datasets/${datasetId}/distributions</p>
                    </div>
                `;
            }
        }
        
        function createTFDVCharts(tfdvStats) {
            console.log('📈 Creating TFDV charts with data:', tfdvStats);
            const container = document.getElementById('tfdv-charts');
            
            if (!container) {
                console.error('❌ TFDV charts container not found');
                return;
            }
            
            let chartsHtml = '';
            
            Object.entries(tfdvStats).forEach(([fieldName, stats], index) => {
                const chartId = `tfdv-chart-${index}`;
                chartsHtml += `<div id="${chartId}" class="chart-container"></div>`;
            });
            
            container.innerHTML = chartsHtml;
            
            // Create individual charts
            Object.entries(tfdvStats).forEach(([fieldName, stats], index) => {
                const chartId = `tfdv-chart-${index}`;
                
                // Create a simple bar chart for numeric stats
                if (typeof stats === 'object' && stats.count) {
                    const trace = {
                        x: Object.keys(stats),
                        y: Object.values(stats),
                        type: 'bar',
                        marker: { color: '#667eea' }
                    };
                    
                    const layout = {
                        title: `Distribution: ${fieldName}`,
                        xaxis: { title: 'Property' },
                        yaxis: { title: 'Value' }
                    };
                    
                    Plotly.newPlot(chartId, [trace], layout, {responsive: true});
                }
            });
        }
        
        // Training Data Table Functions
        async function loadTrainingDataTable(datasetId, page = 1) {
            console.log(`📊 Loading training data table for dataset ${datasetId}, page ${page}`);
            
            const tableSection = document.getElementById('training-data-table-section');
            const tableInfo = document.getElementById('training-table-info');
            const tableHead = document.getElementById('training-table-head');
            const tableBody = document.getElementById('training-table-body');
            
            try {
                // Show loading state
                tableInfo.innerHTML = '🔄 Loading training data...';
                tableSection.style.display = 'block';
                
                // Check if Ray processing is enabled
                const useRay = document.getElementById('use-ray-processing').checked;
                const rayParam = useRay ? '&ray=true' : '';
                
                // Fetch training data from API
                const response = await fetch(`/api/v1/training-datasets/${datasetId}/data?page=${page}&limit=50${rayParam}`);
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }
                
                const data = await response.json();
                console.log('📊 Training data loaded:', data);
                
                if (!data.data || data.data.length === 0) {
                    tableInfo.innerHTML = '📄 No training data available for this dataset';
                    tableBody.innerHTML = '<tr><td colspan="100%">No data available</td></tr>';
                    return;
                }
                
                // Store data for reference
                trainingTableData = data.data;
                trainingCurrentPage = page;
                trainingTotalPages = data.total_pages || 1;
                
                // Update pagination info
                document.getElementById('training-page-info').textContent = `Page ${trainingCurrentPage} of ${trainingTotalPages}`;
                document.getElementById('training-prev-page').disabled = trainingCurrentPage <= 1;
                document.getElementById('training-next-page').disabled = trainingCurrentPage >= trainingTotalPages;
                
                // Update table info
                tableInfo.innerHTML = `
                    📊 Showing ${data.data.length} of ${data.total_count || 0} training sequences
                    ${data.date_range ? `| Date range: ${data.date_range.start} to ${data.date_range.end}` : ''}
                `;
                
                // Build table headers
                if (data.data.length > 0) {
                    const headers = Object.keys(data.data[0]);
                    tableHead.innerHTML = `
                        <tr>
                            ${headers.map(header => `<th title="${header}">${header}</th>`).join('')}
                        </tr>
                    `;
                    
                    // Build table rows
                    const rows = data.data.map((row, index) => `
                        <tr>
                            ${headers.map(header => {
                                let cellValue = row[header];
                                if (cellValue === null || cellValue === undefined) {
                                    cellValue = '-';
                                } else if (typeof cellValue === 'number') {
                                    cellValue = cellValue.toFixed(4);
                                } else if (typeof cellValue === 'string' && cellValue.length > 20) {
                                    cellValue = cellValue.substring(0, 20) + '...';
                                }
                                return `<td title="${row[header]}">${cellValue}</td>`;
                            }).join('')}
                        </tr>
                    `).join('');
                    
                    tableBody.innerHTML = rows;
                }
                
            } catch (error) {
                console.error('❌ Error loading training data table:', error);
                tableInfo.innerHTML = `❌ Error loading data: ${error.message}`;
                tableBody.innerHTML = '<tr><td colspan="100%">Failed to load training data</td></tr>';
            }
        }
        
        function previousTrainingPage() {
            if (trainingCurrentPage > 1 && currentDataset) {
                loadTrainingDataTable(currentDataset.id, trainingCurrentPage - 1);
            }
        }
        
        function nextTrainingPage() {
            if (trainingCurrentPage < trainingTotalPages && currentDataset) {
                loadTrainingDataTable(currentDataset.id, trainingCurrentPage + 1);
            }
        }
    </script>
</body>
</html>
        """

def initialize_job_manager():
    """Initialize the job manager."""
    try:
        job_manager.initialize()
        logger.info("✅ Job manager initialized")
    except Exception as e:
        logger.warning(f"⚠️  Job manager initialization failed: {e}")

def main():
    """Main entry point for analytics service"""
    port = int(os.getenv('PORT', 3000))
    
    try:
        # Initialize job manager
        logger.info("🔧 Initializing job manager...")
        initialize_job_manager()
        
        server = ThreadingHTTPServer(('0.0.0.0', port), AnalyticsHandler)
        logger.info(f"🚀 ATS Analytics Service starting on port {port}")
        logger.info("📊 Serving 30-year price database analytics with job management")
        logger.info(f"🌐 External access available")
        logger.info("🔧 Job management endpoints: /api/jobs/stats, /api/jobs/recent")
        
        server.serve_forever()
        
    except KeyboardInterrupt:
        logger.info("📊 Analytics service stopped")
        server.server_close()
    except Exception as e:
        logger.error(f"❌ Analytics service error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()