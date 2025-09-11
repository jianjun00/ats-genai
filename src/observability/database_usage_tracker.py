"""
ATS Database Usage Tracker - PostgreSQL table access monitoring

This module provides comprehensive tracking of PostgreSQL table access patterns
for identifying unused tables and optimizing database performance.
"""

import json
import re
import time
import threading
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set, Any, Optional, Tuple
import psycopg2
from psycopg2.extensions import cursor as PgCursor


class DatabaseUsageTracker:
    """
    Tracks PostgreSQL table usage across the ATS platform.
    
    Captures:
    - Table access frequency by operation (SELECT, INSERT, UPDATE, DELETE)
    - Query patterns and complexity
    - Table join relationships
    - Database connection patterns
    - Query performance metrics
    """
    
    def __init__(self, output_dir: str = "/tmp/ats-usage-data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Thread-safe counters
        self._lock = threading.Lock()
        self.table_access = defaultdict(lambda: defaultdict(int))  # {table: {operation: count}}
        self.query_patterns = defaultdict(int)
        self.table_joins = defaultdict(set)
        self.query_timings = defaultdict(list)
        self.daily_access = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        self.connection_stats = defaultdict(int)
        
        # SQL parsing patterns
        self.sql_patterns = {
            'SELECT': [
                r'SELECT\s+.*?\s+FROM\s+(\w+)',
                r'FROM\s+(\w+)',
                r'JOIN\s+(\w+)',
                r'LEFT\s+JOIN\s+(\w+)',
                r'RIGHT\s+JOIN\s+(\w+)',
                r'INNER\s+JOIN\s+(\w+)',
                r'OUTER\s+JOIN\s+(\w+)'
            ],
            'INSERT': [r'INSERT\s+INTO\s+(\w+)'],
            'UPDATE': [r'UPDATE\s+(\w+)'],
            'DELETE': [r'DELETE\s+FROM\s+(\w+)'],
            'TRUNCATE': [r'TRUNCATE\s+TABLE\s+(\w+)'],
            'CREATE': [r'CREATE\s+TABLE\s+(\w+)'],
            'DROP': [r'DROP\s+TABLE\s+(\w+)'],
            'ALTER': [r'ALTER\s+TABLE\s+(\w+)']
        }
        
        # Track startup time
        self.start_time = datetime.now()
        self.last_flush = self.start_time
        
        # Auto-flush every 5 minutes
        self._start_auto_flush()
    
    def extract_tables_from_query(self, query: str) -> Dict[str, Set[str]]:
        """
        Extract table names from SQL query, organized by operation type
        """
        query_clean = re.sub(r'--.*$', '', query, flags=re.MULTILINE)  # Remove comments
        query_clean = re.sub(r'/\*.*?\*/', '', query_clean, flags=re.DOTALL)  # Remove block comments
        query_upper = query_clean.upper()
        
        tables_by_operation = defaultdict(set)
        
        for operation, patterns in self.sql_patterns.items():
            for pattern in patterns:
                matches = re.findall(pattern, query_upper, re.IGNORECASE | re.MULTILINE)
                for match in matches:
                    table_name = match.lower().strip()
                    # Filter out common non-table keywords
                    if table_name not in ['select', 'from', 'where', 'order', 'group', 'having', 'limit']:
                        tables_by_operation[operation].add(table_name)
        
        return dict(tables_by_operation)
    
    def track_query(self, query: str, execution_time: float = None, 
                   connection_info: str = "unknown", database: str = "dev_db"):
        """
        Track a database query with comprehensive metrics
        """
        start_time = time.time()
        
        # Extract tables and operations
        tables_by_op = self.extract_tables_from_query(query)
        
        # Track join relationships for SELECT queries
        if 'SELECT' in tables_by_op:
            select_tables = list(tables_by_op['SELECT'])
            if len(select_tables) > 1:
                # Record table join relationships
                for i, table1 in enumerate(select_tables):
                    for table2 in select_tables[i+1:]:
                        with self._lock:
                            self.table_joins[table1].add(table2)
                            self.table_joins[table2].add(table1)
        
        # Track table access patterns
        with self._lock:
            for operation, tables in tables_by_op.items():
                for table in tables:
                    self.table_access[table][operation] += 1
                    
                    # Daily tracking
                    today = datetime.now().strftime('%Y-%m-%d')
                    self.daily_access[today][table][operation] += 1
            
            # Track query patterns
            query_signature = self._get_query_signature(query)
            self.query_patterns[query_signature] += 1
            
            # Track execution time if provided
            if execution_time:
                for operation, tables in tables_by_op.items():
                    for table in tables:
                        self.query_timings[f"{table}.{operation}"].append(execution_time)
            
            # Connection stats
            self.connection_stats[connection_info] += 1
    
    def _get_query_signature(self, query: str) -> str:
        """
        Create a normalized signature for query pattern analysis
        """
        # Remove specific values and normalize
        signature = re.sub(r"'[^']*'", "'?'", query)  # Replace string literals
        signature = re.sub(r'\b\d+\b', '?', signature)  # Replace numbers
        signature = re.sub(r'\s+', ' ', signature)  # Normalize whitespace
        
        # Extract basic pattern
        query_upper = signature.upper().strip()
        if query_upper.startswith('SELECT'):
            return 'SELECT_PATTERN'
        elif query_upper.startswith('INSERT'):
            return 'INSERT_PATTERN'
        elif query_upper.startswith('UPDATE'):
            return 'UPDATE_PATTERN'
        elif query_upper.startswith('DELETE'):
            return 'DELETE_PATTERN'
        else:
            return 'OTHER_PATTERN'
    
    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive database usage statistics
        """
        with self._lock:
            # Calculate total operations per table
            table_totals = {}
            for table, operations in self.table_access.items():
                table_totals[table] = sum(operations.values())
            
            # Calculate average query times
            avg_query_times = {}
            for table_op, times in self.query_timings.items():
                if times:
                    avg_query_times[table_op] = sum(times) / len(times)
            
            stats = {
                'tracking_duration_hours': (datetime.now() - self.start_time).total_seconds() / 3600,
                'total_table_accesses': sum(table_totals.values()),
                'unique_tables_accessed': len(self.table_access),
                'total_queries_tracked': sum(self.query_patterns.values()),
                
                # Table access patterns
                'table_access_frequency': dict(self.table_access),
                'table_totals': table_totals,
                
                # Most accessed tables
                'hot_tables': dict(Counter(table_totals).most_common(20)),
                
                # Query patterns
                'query_pattern_distribution': dict(self.query_patterns),
                
                # Performance metrics
                'average_query_times': avg_query_times,
                
                # Table relationships
                'table_join_relationships': {
                    table: list(joined_tables) 
                    for table, joined_tables in self.table_joins.items()
                },
                
                # Daily patterns
                'daily_access_patterns': dict(self.daily_access),
                
                # Connection patterns
                'connection_distribution': dict(self.connection_stats),
                
                # Operation breakdown
                'operations_by_table': {
                    table: dict(operations) 
                    for table, operations in self.table_access.items()
                }
            }
        
        return stats
    
    def get_unused_tables(self, all_tables: Set[str], min_days_unused: int = 30) -> Dict[str, Any]:
        """
        Identify tables that haven't been accessed recently
        """
        with self._lock:
            accessed_tables = set(self.table_access.keys())
            never_accessed = all_tables - accessed_tables
            
            # Check for tables not accessed in recent days
            cutoff_date = (datetime.now() - timedelta(days=min_days_unused)).strftime('%Y-%m-%d')
            recently_accessed = set()
            
            for date, tables_data in self.daily_access.items():
                if date >= cutoff_date:
                    recently_accessed.update(tables_data.keys())
            
            stale_tables = accessed_tables - recently_accessed
            
            return {
                'never_accessed': {
                    'count': len(never_accessed),
                    'tables': list(never_accessed)
                },
                'stale_tables': {
                    'count': len(stale_tables),
                    'tables': list(stale_tables),
                    'last_access': 'more_than_30_days_ago'
                },
                'recently_active': {
                    'count': len(recently_accessed),
                    'tables': list(recently_accessed)
                },
                'total_tables': len(all_tables),
                'usage_percentage': len(recently_accessed) / len(all_tables) * 100 if all_tables else 0
            }
    
    def discover_all_tables(self, connection_config: Dict[str, str]) -> Set[str]:
        """
        Connect to database and discover all tables
        """
        tables = set()
        try:
            conn = psycopg2.connect(**connection_config)
            cur = conn.cursor()
            
            # Get all user tables
            cur.execute("""
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                ORDER BY tablename
            """)
            
            tables = {row[0] for row in cur.fetchall()}
            
            cur.close()
            conn.close()
            
            print(f"✅ Discovered {len(tables)} tables in database")
            
        except Exception as e:
            print(f"❌ Failed to discover tables: {e}")
        
        return tables
    
    def get_table_sizes(self, connection_config: Dict[str, str]) -> Dict[str, Dict]:
        """
        Get table sizes and statistics from database
        """
        table_stats = {}
        try:
            conn = psycopg2.connect(**connection_config)
            cur = conn.cursor()
            
            # Get table sizes and row counts
            cur.execute("""
                SELECT 
                    tablename,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                    pg_total_relation_size(schemaname||'.'||tablename) as size_bytes,
                    (SELECT reltuples::bigint 
                     FROM pg_class 
                     WHERE relname = tablename) as estimated_rows
                FROM pg_tables 
                WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
            """)
            
            for row in cur.fetchall():
                table_stats[row[0]] = {
                    'size_human': row[1],
                    'size_bytes': row[2],
                    'estimated_rows': row[3] or 0
                }
            
            cur.close()
            conn.close()
            
        except Exception as e:
            print(f"❌ Failed to get table sizes: {e}")
        
        return table_stats
    
    def generate_cleanup_candidates(self, connection_config: Dict[str, str], 
                                  unused_threshold_days: int = 30) -> List[Dict]:
        """
        Generate prioritized list of table cleanup candidates
        """
        # Discover all tables
        all_tables = self.discover_all_tables(connection_config)
        table_sizes = self.get_table_sizes(connection_config)
        
        # Get unused tables
        unused_analysis = self.get_unused_tables(all_tables, unused_threshold_days)
        
        cleanup_candidates = []
        
        # Process never accessed tables
        for table in unused_analysis['never_accessed']['tables']:
            size_info = table_sizes.get(table, {})
            cleanup_candidates.append({
                'table': table,
                'cleanup_priority': 'high',
                'reason': 'never_accessed',
                'size_bytes': size_info.get('size_bytes', 0),
                'size_human': size_info.get('size_human', 'unknown'),
                'estimated_rows': size_info.get('estimated_rows', 0),
                'dependencies': list(self.table_joins.get(table, set()))
            })
        
        # Process stale tables
        for table in unused_analysis['stale_tables']['tables']:
            size_info = table_sizes.get(table, {})
            total_historical_access = sum(self.table_access[table].values())
            
            cleanup_candidates.append({
                'table': table,
                'cleanup_priority': 'medium',
                'reason': 'stale_access',
                'historical_access_count': total_historical_access,
                'size_bytes': size_info.get('size_bytes', 0),
                'size_human': size_info.get('size_human', 'unknown'),
                'estimated_rows': size_info.get('estimated_rows', 0),
                'dependencies': list(self.table_joins.get(table, set()))
            })
        
        # Sort by size (largest first for maximum cleanup impact)
        cleanup_candidates.sort(key=lambda x: x['size_bytes'], reverse=True)
        
        return cleanup_candidates
    
    def flush_to_disk(self):
        """
        Write current database usage data to disk
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        stats_file = self.output_dir / f"db_usage_{timestamp}.json"
        
        try:
            stats = self.get_database_stats()
            with open(stats_file, 'w') as f:
                json.dump(stats, f, indent=2, default=str)
            
            self.last_flush = datetime.now()
            print(f"✅ Database usage data flushed to {stats_file}")
            return stats_file
            
        except Exception as e:
            print(f"❌ Failed to flush database usage data: {e}")
            return None
    
    def _start_auto_flush(self):
        """
        Start background thread for periodic data flushing
        """
        def auto_flush():
            while True:
                time.sleep(300)  # 5 minutes
                self.flush_to_disk()
        
        flush_thread = threading.Thread(target=auto_flush, daemon=True)
        flush_thread.start()


class DatabaseQueryInterceptor:
    """
    Intercepts database queries for automatic tracking
    """
    
    def __init__(self, usage_tracker: DatabaseUsageTracker):
        self.tracker = usage_tracker
    
    def intercept_cursor_execute(self, original_execute):
        """
        Wrap cursor.execute to track all queries
        """
        def tracked_execute(self, query, vars=None):
            start_time = time.time()
            try:
                result = original_execute(query, vars)
                execution_time = time.time() - start_time
                
                # Track the query
                self.tracker.track_query(
                    query=query,
                    execution_time=execution_time,
                    connection_info=f"{self.connection.get_dsn_parameters().get('host', 'localhost')}:{self.connection.get_dsn_parameters().get('port', '5432')}"
                )
                
                return result
                
            except Exception as e:
                # Track failed queries too
                execution_time = time.time() - start_time
                self.tracker.track_query(
                    query=f"FAILED: {query}",
                    execution_time=execution_time
                )
                raise
        
        return tracked_execute


# Global tracker instance
_db_tracker = None

def get_database_tracker() -> DatabaseUsageTracker:
    """Get or create global database tracker instance"""
    global _db_tracker
    if _db_tracker is None:
        _db_tracker = DatabaseUsageTracker()
    return _db_tracker

def install_db_tracking():
    """
    Install automatic database query tracking
    """
    tracker = get_database_tracker()
    interceptor = DatabaseQueryInterceptor(tracker)
    
    # Monkey patch psycopg2 cursor
    original_execute = psycopg2.extensions.cursor.execute
    psycopg2.extensions.cursor.execute = interceptor.intercept_cursor_execute(original_execute)
    
    print("✅ Database query tracking installed")
    return tracker