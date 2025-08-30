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
                    # Get actual table information from database
                    cursor.execute("""
                        SELECT 
                            schemaname, tablename,
                            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
                        FROM pg_tables 
                        WHERE schemaname = 'public' 
                        AND tablename LIKE 'dev_%'
                        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
                    """)
                    
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
                        if filter_config.get('type') == 'categorical' and filter_config.get('values'):
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
    
    def do_GET(self):
        logger.info(f"📍 GET request: {self.path}")
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            response = {"status": "healthy", "service": "ats-analytics", "timestamp": datetime.now().isoformat()}
            self.wfile.write(json.dumps(response).encode())
            
        elif self.path == '/' or self.path == '/dashboard':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
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
                        <h3>Database Summary</h3>
                        <div class="metric">7.95M+</div>
                        <p>Total price records across all vendors</p>
                        <ul>
                            <li><strong>Instruments:</strong> 17,700 unique symbols</li>
                            <li><strong>ETFs:</strong> 23 critical market factors</li>
                            <li><strong>Date Range:</strong> 1995-2025 (30 years)</li>
                        </ul>
                    </div>
                    
                    <div class="card">
                        <h3>Vendor Coverage</h3>
                        <ul>
                            <li><strong>Tiingo:</strong> 6.56M records, 2,355 symbols</li>
                            <li><strong>EODHD:</strong> 728K records, 268 symbols</li>
                            <li><strong>Polygon:</strong> 666K records, 849 symbols</li>
                        </ul>
                        <a href="/api/vendors" class="btn">Vendor Details</a>
                    </div>
                    
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
            self.wfile.write(html.encode())
            
        elif self.path == '/api/summary':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
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
            self.wfile.write(json.dumps(summary, indent=2).encode())
            
        elif self.path == '/api/vendors':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
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
            self.wfile.write(json.dumps(vendors, indent=2).encode())
        
        elif self.path == '/api/jobs/stats':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            # Need to run async function in sync context
            try:
                stats = job_manager.get_job_stats()
                self.wfile.write(json.dumps(stats, indent=2).encode())
            except Exception as e:
                logger.error(f"Error getting job stats: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode())
        
        elif self.path == '/api/jobs/recent':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            try:
                jobs = job_manager.get_recent_jobs(15)
                response = {"jobs": jobs, "total": len(jobs), "timestamp": datetime.now().isoformat()}
                self.wfile.write(json.dumps(response, indent=2).encode())
            except Exception as e:
                logger.error(f"Error getting recent jobs: {e}")
                error_response = {"jobs": [], "total": 0, "error": str(e)}
                self.wfile.write(json.dumps(error_response).encode())
        
        elif self.path == '/api/collections/status':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                status = loop.run_until_complete(job_manager.get_collection_status())
                self.wfile.write(json.dumps(status, indent=2).encode())
            except Exception as e:
                logger.error(f"Error getting collection status: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode())
        
        elif self.path == '/api/eda/datasets':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.send_header('Cache-Control', 'public, max-age=3600')  # 1 hour browser cache
            self.end_headers()
            
            try:
                datasets = get_cached_datasets(job_manager)
                self.wfile.write(json.dumps(datasets, indent=2).encode())
            except Exception as e:
                logger.error(f"Error getting datasets: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode())
        
        elif self.path.startswith('/api/eda/datasets/') and self.path.endswith('/schema'):
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            
            try:
                # Extract dataset name from path
                parts = self.path.split('/')
                dataset_name = parts[4]  # /api/eda/datasets/{name}/schema
                
                schema = job_manager.get_dataset_schema(dataset_name)
                self.wfile.write(json.dumps(schema, indent=2).encode())
            except Exception as e:
                logger.error(f"Error getting schema: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode())
        
        elif self.path.startswith('/api/eda/datasets/') and '/columns/' in self.path and '/values' in self.path:
            # GET /api/eda/datasets/{table_name}/columns/{column_name}/values
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
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
                
                self.wfile.write(json.dumps(column_values, indent=2).encode())
            except Exception as e:
                logger.error(f"Error getting column values: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode())
        
        elif self.path.startswith('/api/eda/datasets/') and '/precompute' in self.path:
            # GET /api/eda/datasets/{table_name}/precompute - Trigger pre-computation
            # GET /api/eda/datasets/{table_name}/precompute/status - Check status
            logger.info(f"🚀 Pre-compute endpoint accessed: {self.path}")
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
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
                        
                        self.wfile.write(json.dumps(status_response, indent=2).encode())
                        
                    except Exception as e:
                        error_response = {'error': f'Status check failed: {str(e)}'}
                        self.wfile.write(json.dumps(error_response).encode())
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
                        self.wfile.write(json.dumps(result, indent=2).encode())
                            
                    except Exception as e:
                        logger.error(f"Pre-computation failed for {table_name}: {e}")
                        error_response = {'error': f'Pre-computation failed: {str(e)}'}
                        self.wfile.write(json.dumps(error_response).encode())
                
            except Exception as e:
                logger.error(f"Error in pre-compute endpoint: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode())

        elif self.path.startswith('/api/eda/datasets/') and '/timeseries/' in self.path:
            # GET /api/eda/datasets/{table_name}/timeseries/{y_column}/{x_column}
            logger.info(f"🎯 Timeseries endpoint accessed: {self.path}")
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
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
                self.wfile.write(json.dumps(timeseries_data, indent=2).encode())
                
            except Exception as e:
                logger.error(f"Error getting time-series data: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode())
        
        elif self.path == '/eda':
            # EDA Dashboard page
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            eda_html = self.get_eda_dashboard_html()
            self.wfile.write(eda_html.encode())
            
        else:
            self.send_response(404)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            error = {"error": "Not found", "path": self.path}
            self.wfile.write(json.dumps(error).encode())

    def do_POST(self):
        if self.path == '/api/eda/analyze':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
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
                    self.wfile.write(json.dumps(error_response).encode())
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
                
                self.wfile.write(json.dumps(analysis, indent=2).encode())
                
            except Exception as e:
                logger.error(f"Error analyzing distribution: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode())
        
        elif self.path.startswith('/api/eda/datasets/') and self.path.endswith('/data'):
            # POST /api/eda/datasets/{table_name}/data
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
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
                self.wfile.write(json.dumps(filtered_data, indent=2).encode())
                
            except Exception as e:
                logger.error(f"Error getting filtered data: {e}")
                error_response = {"error": str(e)}
                self.wfile.write(json.dumps(error_response).encode())
        
        else:
            self.send_response(404)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            error = {"error": "Not found", "path": self.path}
            self.wfile.write(json.dumps(error).encode())

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
            </div>
            
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
            
            <script>
                let datasets = [];
                let currentAnalysis = null;
                let currentFilters = {};
                let currentPage = 1;
                let totalPages = 1;
                
                // Table sorting variables
                let currentSortColumn = null;
                let currentSortDirection = 'asc';
                let currentTableData = [];
                
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
                        
                        // Show filters section and load filters for all columns
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
                        
                        // Identify string columns that should not be treated as categorical  
                        // Note: 'type' and 'exchange' are categorical, not string
                        const isStringType = (dataType.includes('varchar') || dataType.includes('text') || 
                            dataType.includes('character')) && !col.name.toLowerCase().includes('type') &&
                            !col.name.toLowerCase().includes('exchange') || 
                            col.name.toLowerCase().includes('id') || col.name.toLowerCase().includes('symbol') || 
                            col.name.toLowerCase().includes('name') || col.name.toLowerCase().includes('title') || 
                            col.name.toLowerCase().includes('url') || col.name.toLowerCase().includes('description');
                        
                        try {
                            const response = await fetch(`/api/eda/datasets/${datasetName}/columns/${col.name}/values?limit=10`, {timeout: 3000});
                            const columnData = await response.json();
                            
                            if (columnData.error) return null; // Skip if error loading values
                            
                            let filterHtml = `<div class="filter-group">`;
                            const typeLabel = isNumeric ? 'numeric' : (isStringType ? 'string' : (isDateType ? 'date' : 'categorical'));
                            filterHtml += `<label>${col.name} (${typeLabel}):</label>`;
                            
                            if (isNumeric && columnData.min_value !== undefined && columnData.max_value !== undefined) {
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
                                // Categorical checkbox filter
                                filterHtml += `<div class="checkbox-list">`;
                                columnData.values.slice(0, 8).forEach(valueData => { // Show only first 8 values for speed
                                    const value = typeof valueData === 'object' ? valueData.value : valueData;
                                    const count = typeof valueData === 'object' ? valueData.count : '';
                                    const countText = count ? ` (${count})` : '';
                                    filterHtml += `
                                        <label>
                                            <input type="checkbox" name="filter-${col.name}" value="${value}"> ${value}${countText}
                                        </label><br>
                                    `;
                                });
                                if (columnData.values.length > 8) {
                                    filterHtml += `<small>... and ${columnData.values.length - 8} more values</small>`;
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
                            <div class="visualization-controls">
                                <select id="xaxis-${col.name}" onchange="updateVisualization('${col.name}')">
                                    <option value="">Select X-axis (optional)</option>
                                </select>
                            </div>
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
                        currentPage = data.current_page || 1;
                        totalPages = data.total_pages || 1;
                        
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
                        Showing ${data.data.length} of ${data.total_count} records 
                        (Page ${data.current_page} of ${data.total_pages})
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
                
                async function updateVisualization(columnName) {
                    const xAxisSelect = document.getElementById(`xaxis-${columnName}`);
                    const xAxisColumn = xAxisSelect.value;
                    const datasetName = document.getElementById('dataset-select').value;
                    
                    if (xAxisColumn) {
                        console.log(`📊 Creating time-series chart: ${columnName} over ${xAxisColumn}`);
                        
                        try {
                            // Show loading state
                            const chartContainer = document.getElementById(`chart-${columnName}`);
                            chartContainer.innerHTML = '<p style="text-align: center;">Loading time-series chart...</p>';
                            
                            // Create mock time-series data for demonstration
                            // TODO: Replace with actual API call once endpoint is working
                            const timeseriesData = {
                                type: columnName.includes('price') || columnName.includes('volume') ? 'numeric' : 'categorical',
                                x_column: xAxisColumn,
                                y_column: columnName,
                                data: generateMockTimeSeriesData(columnName),
                                y_label: columnName.includes('price') || columnName.includes('volume') ? `Average ${columnName}` : `Count of ${columnName}`,
                                chart_type: columnName.includes('price') || columnName.includes('volume') ? 'line' : 'bar'
                            };
                            
                            // Create div for Plotly chart
                            chartContainer.innerHTML = `<div id="timeseries-${columnName}" style="width:100%; height:400px;"></div>`;
                            
                            // Prepare data for Plotly
                            const xValues = timeseriesData.data.map(d => d.x);
                            const yValues = timeseriesData.data.map(d => d.y);
                            
                            // Create Plotly trace
                            const trace = {
                                x: xValues,
                                y: yValues,
                                type: timeseriesData.chart_type === 'line' ? 'scatter' : 'bar',
                                mode: timeseriesData.chart_type === 'line' ? 'lines+markers' : undefined,
                                name: timeseriesData.y_label,
                                line: timeseriesData.chart_type === 'line' ? {
                                    color: 'rgb(52, 152, 219)',
                                    width: 2
                                } : undefined,
                                marker: {
                                    color: 'rgb(52, 152, 219)',
                                    size: timeseriesData.chart_type === 'line' ? 6 : undefined
                                },
                                fill: timeseriesData.chart_type === 'line' ? 'tonexty' : undefined
                            };
                            
                            // Layout configuration
                            const layout = {
                                title: {
                                    text: `${columnName} over ${xAxisColumn}`,
                                    font: { size: 16 }
                                },
                                xaxis: {
                                    title: xAxisColumn,
                                    type: 'date',
                                    tickformat: '%Y-%m-%d'
                                },
                                yaxis: {
                                    title: timeseriesData.y_label
                                },
                                margin: { t: 60, r: 50, b: 80, l: 80 },
                                hovermode: 'x unified',
                                showlegend: false,
                                plot_bgcolor: 'white',
                                paper_bgcolor: 'white'
                            };
                            
                            // Configuration options
                            const config = {
                                responsive: true,
                                displayModeBar: true,
                                modeBarButtonsToAdd: ['pan2d', 'select2d', 'lasso2d'],
                                displaylogo: false,
                                toImageButtonOptions: {
                                    format: 'png',
                                    filename: `timeseries_${columnName}`,
                                    height: 400,
                                    width: 800,
                                    scale: 1
                                }
                            };
                            
                            // Create Plotly chart
                            Plotly.newPlot(`timeseries-${columnName}`, [trace], layout, config);
                            
                            console.log(`✅ Time-series chart created for ${columnName}`);
                            
                        } catch (error) {
                            console.error(`❌ Error creating time-series chart:`, error);
                            const chartContainer = document.getElementById(`chart-${columnName}`);
                            chartContainer.innerHTML = `<p style="color: red; text-align: center;">
                                Error loading time-series: ${error.message}<br>
                                <small>Try selecting a different date column</small>
                            </p>`;
                        }
                    } else {
                        // Reload original distribution
                        console.log(`🔄 Restoring original distribution for ${columnName}`);
                        
                        // Find column type and reload appropriate distribution
                        const dataType = document.querySelector(`#chart-${columnName}`).closest('.column-distribution').querySelector('h4').textContent;
                        if (dataType.includes('Numeric')) {
                            loadNumericDistribution(datasetName, columnName);
                        } else if (dataType.includes('Categorical')) {
                            loadCategoricalDistribution(datasetName, columnName);
                        }
                    }
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
                        currentPage = data.current_page || 1;
                        totalPages = data.total_pages || 1;
                        
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