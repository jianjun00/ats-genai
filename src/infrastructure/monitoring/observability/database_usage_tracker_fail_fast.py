"""
Database Usage Tracker with Fail-Fast Exception Handling

This refactored version eliminates exception masking to reveal real database issues.
Exceptions are handled specifically with actionable error messages.

BEFORE: Generic exception handling masked real database issues
AFTER: Specific exception handling with clear error propagation and actionable messages
"""

import psycopg2
import psycopg2.sql
from typing import Dict, List, Set, Optional
from datetime import datetime, timedelta
import logging

# Custom exceptions for specific database issues
class DatabaseConnectionError(Exception):
    """Specific exception for database connection issues"""
    pass

class DatabaseQueryError(Exception):
    """Specific exception for database query issues"""
    pass

class DatabasePermissionError(Exception):
    """Specific exception for database permission issues"""
    pass

class DatabaseSchemaError(Exception):
    """Specific exception for database schema issues"""
    pass


class DatabaseUsageTrackerFailFast:
    """Database usage tracker with fail-fast exception handling"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.supported_schemas = {'public', 'information_schema', 'pg_catalog'}
    
    def discover_all_tables(self, connection_config: Dict[str, str]) -> Set[str]:
        """
        Discover all tables in database - FAIL FAST ON DATABASE ISSUES
        """
        try:
            conn = psycopg2.connect(**connection_config)
            
        except psycopg2.OperationalError as e:
            # Specific connection error with actionable message
            raise DatabaseConnectionError(
                f"Cannot connect to database: {e}. "
                f"Check database server status, connection parameters, and network connectivity."
            )
        except psycopg2.DatabaseError as e:
            # Database-specific error
            raise DatabaseConnectionError(
                f"Database error during connection: {e}. "
                f"Check database configuration and credentials."
            )
        
        tables = set()
        
        try:
            cur = conn.cursor()
            
            # Query system tables for table discovery
            cur.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_type = 'BASE TABLE'
                AND table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY table_schema, table_name
            """)
            
            rows = cur.fetchall()
            
            for schema, table_name in rows:
                full_table_name = f"{schema}.{table_name}" if schema != 'public' else table_name
                tables.add(full_table_name)
            
            cur.close()
            
        except psycopg2.ProgrammingError as e:
            # SQL programming error
            raise DatabaseQueryError(
                f"Failed to query system tables for table discovery: {e}. "
                f"Check database permissions for information_schema access."
            )
        except psycopg2.DataError as e:
            # Data-related error
            raise DatabaseQueryError(
                f"Data error during table discovery: {e}. "
                f"Database system tables may be corrupted."
            )
        except psycopg2.Error as e:
            # Other PostgreSQL errors
            raise DatabaseQueryError(
                f"PostgreSQL error during table discovery: {e}"
            )
        finally:
            try:
                conn.close()
            except Exception:
                # Connection cleanup failure - log but don't mask original error
                self.logger.warning("Failed to close database connection during cleanup")
        
        if not tables:
            # No tables found could indicate permission issues
            raise DatabasePermissionError(
                "No tables discovered. This could indicate insufficient database permissions "
                "or an empty database. Check user permissions for schema access."
            )
        
        self.logger.info(f"✅ Discovered {len(tables)} tables in database")
        return tables

    def get_table_sizes(self, connection_config: Dict[str, str]) -> Dict[str, Dict]:
        """
        Get table sizes and statistics - FAIL FAST ON QUERY ISSUES
        """
        try:
            conn = psycopg2.connect(**connection_config)
            
        except psycopg2.OperationalError as e:
            raise DatabaseConnectionError(
                f"Cannot connect to database for table size analysis: {e}"
            )
        
        table_stats = {}
        
        try:
            cur = conn.cursor()
            
            # Get table sizes and row counts with proper error handling
            size_query = """
                SELECT 
                    schemaname,
                    tablename,
                    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                    pg_total_relation_size(schemaname||'.'||tablename) as size_bytes,
                    n_tup_ins as inserts,
                    n_tup_upd as updates,
                    n_tup_del as deletes,
                    n_live_tup as live_tuples,
                    n_dead_tup as dead_tuples,
                    last_vacuum,
                    last_autovacuum,
                    last_analyze,
                    last_autoanalyze
                FROM pg_stat_user_tables
                ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
            """
            
            cur.execute(size_query)
            rows = cur.fetchall()
            
            for row in rows:
                schema, table_name = row[0], row[1]
                full_table_name = f"{schema}.{table_name}" if schema != 'public' else table_name
                
                table_stats[full_table_name] = {
                    'size_pretty': row[2],
                    'size_bytes': row[3],
                    'inserts': row[4] or 0,
                    'updates': row[5] or 0,
                    'deletes': row[6] or 0,
                    'live_tuples': row[7] or 0,
                    'dead_tuples': row[8] or 0,
                    'last_vacuum': row[9],
                    'last_autovacuum': row[10],
                    'last_analyze': row[11],
                    'last_autoanalyze': row[12]
                }
            
            cur.close()
            
        except psycopg2.ProgrammingError as e:
            # SQL programming error - likely permission issue
            raise DatabasePermissionError(
                f"Failed to query table statistics: {e}. "
                f"User may need permissions to access pg_stat_user_tables view."
            )
        except psycopg2.DataError as e:
            # Data conversion or constraint error
            raise DatabaseQueryError(
                f"Data error while retrieving table sizes: {e}. "
                f"Database statistics may be inconsistent."
            )
        except psycopg2.Error as e:
            # Other PostgreSQL errors
            raise DatabaseQueryError(
                f"PostgreSQL error during table size analysis: {e}"
            )
        finally:
            try:
                conn.close()
            except Exception:
                self.logger.warning("Failed to close database connection during table size analysis")
        
        if not table_stats:
            self.logger.warning("No table statistics retrieved - database may be empty or permissions insufficient")
        
        return table_stats

    def get_unused_tables(
        self, 
        all_tables: Set[str], 
        unused_threshold_days: int = 30
    ) -> Dict[str, Dict]:
        """
        Identify unused tables - FAIL FAST ON ANALYSIS ISSUES
        """
        if not all_tables:
            raise ValueError(
                f"No tables provided for unused table analysis. "
                f"Run table discovery first."
            )
        
        if unused_threshold_days <= 0:
            raise ValueError(
                f"Invalid unused threshold: {unused_threshold_days} days. "
                f"Threshold must be positive."
            )
        
        threshold_date = datetime.now() - timedelta(days=unused_threshold_days)
        unused_tables = {}
        
        # Analyze each table for usage patterns
        for table in all_tables:
            try:
                usage_analysis = self._analyze_table_usage(table, threshold_date)
                
                if usage_analysis['is_unused']:
                    unused_tables[table] = usage_analysis
                    
            except DatabaseQueryError as e:
                # Re-raise with table context
                raise DatabaseQueryError(
                    f"Failed to analyze usage for table '{table}': {e}"
                )
            except Exception as e:
                # Unexpected error during table analysis
                raise DatabaseQueryError(
                    f"Unexpected error analyzing table '{table}': {e}"
                )
        
        self.logger.info(f"Found {len(unused_tables)} unused tables out of {len(all_tables)} total")
        return unused_tables

    def _analyze_table_usage(self, table_name: str, threshold_date: datetime) -> Dict:
        """
        Analyze individual table usage patterns - SPECIFIC ERROR HANDLING
        """
        # This would typically query pg_stat_user_tables for activity
        # For demonstration, showing the pattern of specific error handling
        
        usage_analysis = {
            'table_name': table_name,
            'is_unused': False,
            'last_activity': None,
            'activity_score': 0.0,
            'recommendations': []
        }
        
        try:
            # Simulate table usage analysis logic
            # In real implementation, this would query actual table statistics
            
            # Check for recent DML operations
            recent_activity = self._check_recent_dml_activity(table_name, threshold_date)
            
            # Check for recent query activity
            query_activity = self._check_recent_query_activity(table_name, threshold_date)
            
            # Determine if table is unused
            if not recent_activity and not query_activity:
                usage_analysis['is_unused'] = True
                usage_analysis['recommendations'].append('Consider archiving or dropping table')
            
            return usage_analysis
            
        except Exception as e:
            raise DatabaseQueryError(
                f"Failed to analyze usage patterns for table '{table_name}': {e}"
            )

    def _check_recent_dml_activity(self, table_name: str, threshold_date: datetime) -> bool:
        """
        Check for recent DML activity - SPECIFIC DATABASE ERROR HANDLING
        """
        try:
            # In real implementation, query pg_stat_user_tables for DML statistics
            # This is a placeholder showing error handling pattern
            return False
            
        except psycopg2.Error as e:
            raise DatabaseQueryError(
                f"Failed to check DML activity for table '{table_name}': {e}"
            )

    def _check_recent_query_activity(self, table_name: str, threshold_date: datetime) -> bool:
        """
        Check for recent query activity - SPECIFIC DATABASE ERROR HANDLING
        """
        try:
            # In real implementation, query pg_stat_statements if available
            # This is a placeholder showing error handling pattern
            return False
            
        except psycopg2.Error as e:
            raise DatabaseQueryError(
                f"Failed to check query activity for table '{table_name}': {e}"
            )

    def generate_cleanup_candidates(
        self, 
        connection_config: Dict[str, str],
        unused_threshold_days: int = 30
    ) -> List[Dict]:
        """
        Generate cleanup candidates - FAIL FAST WITH CLEAR ERROR CONTEXT
        """
        try:
            # Discover all tables - let discovery errors propagate
            all_tables = self.discover_all_tables(connection_config)
            
        except DatabaseConnectionError as e:
            # Add context for cleanup candidate generation
            raise DatabaseConnectionError(
                f"Cannot generate cleanup candidates due to connection error: {e}"
            )
        except DatabasePermissionError as e:
            # Add context for cleanup candidate generation
            raise DatabasePermissionError(
                f"Cannot generate cleanup candidates due to permission error: {e}"
            )
        
        try:
            # Get table sizes - let size analysis errors propagate
            table_sizes = self.get_table_sizes(connection_config)
            
        except DatabaseQueryError as e:
            # Add context for cleanup candidate generation
            raise DatabaseQueryError(
                f"Cannot generate cleanup candidates due to table size analysis error: {e}"
            )
        
        try:
            # Get unused tables - let usage analysis errors propagate
            unused_analysis = self.get_unused_tables(all_tables, unused_threshold_days)
            
        except ValueError as e:
            # Configuration error
            raise ValueError(
                f"Invalid configuration for cleanup candidate generation: {e}"
            )
        except DatabaseQueryError as e:
            # Add context for cleanup candidate generation
            raise DatabaseQueryError(
                f"Cannot generate cleanup candidates due to usage analysis error: {e}"
            )
        
        # Generate prioritized cleanup candidates
        cleanup_candidates = []
        
        for table_name, usage_info in unused_analysis.items():
            size_info = table_sizes.get(table_name, {})
            
            candidate = {
                'table_name': table_name,
                'size_bytes': size_info.get('size_bytes', 0),
                'size_pretty': size_info.get('size_pretty', 'Unknown'),
                'last_activity': usage_info.get('last_activity'),
                'unused_days': unused_threshold_days,
                'recommendations': usage_info.get('recommendations', []),
                'priority_score': self._calculate_cleanup_priority(size_info, usage_info)
            }
            
            cleanup_candidates.append(candidate)
        
        # Sort by priority score (highest priority first)
        cleanup_candidates.sort(key=lambda x: x['priority_score'], reverse=True)
        
        self.logger.info(f"Generated {len(cleanup_candidates)} cleanup candidates")
        return cleanup_candidates

    def _calculate_cleanup_priority(self, size_info: Dict, usage_info: Dict) -> float:
        """
        Calculate cleanup priority score - FAIL FAST ON CALCULATION ERRORS
        """
        try:
            # Priority factors:
            # 1. Size (larger tables = higher priority)
            # 2. Unused duration (longer unused = higher priority)
            # 3. Dead tuple ratio (more dead tuples = higher priority)
            
            size_bytes = size_info.get('size_bytes', 0)
            live_tuples = size_info.get('live_tuples', 1)  # Avoid division by zero
            dead_tuples = size_info.get('dead_tuples', 0)
            
            # Size component (normalized)
            size_score = min(size_bytes / (1024 * 1024 * 1024), 10.0)  # Cap at 10GB = score 10
            
            # Dead tuple ratio component
            dead_ratio = dead_tuples / max(live_tuples, 1)
            dead_score = min(dead_ratio * 5, 5.0)  # Cap at score 5
            
            # Base unused score
            unused_score = 5.0  # Base score for being unused
            
            total_score = size_score + dead_score + unused_score
            return round(total_score, 2)
            
        except (TypeError, ValueError, ZeroDivisionError) as e:
            # Calculation error with specific context
            raise ValueError(
                f"Failed to calculate cleanup priority score: {e}. "
                f"Size info: {size_info}, Usage info: {usage_info}"
            )

    def validate_connection_config(self, connection_config: Dict[str, str]) -> None:
        """
        Validate database connection configuration - FAIL FAST ON CONFIG ISSUES
        """
        required_params = {'host', 'database', 'user', 'password'}
        missing_params = required_params - set(connection_config.keys())
        
        if missing_params:
            raise ValueError(
                f"Missing required connection parameters: {missing_params}. "
                f"Required: {required_params}"
            )
        
        # Validate parameter values
        for param, value in connection_config.items():
            if not value or not str(value).strip():
                raise ValueError(
                    f"Invalid value for connection parameter '{param}': "
                    f"'{value}'. Parameter cannot be empty."
                )
        
        # Test connection
        try:
            test_conn = psycopg2.connect(**connection_config)
            test_conn.close()
            
        except psycopg2.OperationalError as e:
            raise DatabaseConnectionError(
                f"Connection validation failed: {e}. "
                f"Check database server status and connection parameters."
            )
        except psycopg2.Error as e:
            raise DatabaseConnectionError(
                f"Database error during connection validation: {e}"
            )

    def get_database_health_summary(self, connection_config: Dict[str, str]) -> Dict:
        """
        Get database health summary - FAIL FAST ON HEALTH CHECK ISSUES
        """
        try:
            # Validate configuration first
            self.validate_connection_config(connection_config)
            
            # Get basic database metrics
            all_tables = self.discover_all_tables(connection_config)
            table_sizes = self.get_table_sizes(connection_config)
            
            # Calculate health metrics
            total_tables = len(all_tables)
            total_size_bytes = sum(stats.get('size_bytes', 0) for stats in table_sizes.values())
            total_dead_tuples = sum(stats.get('dead_tuples', 0) for stats in table_sizes.values())
            total_live_tuples = sum(stats.get('live_tuples', 0) for stats in table_sizes.values())
            
            health_summary = {
                'database_name': connection_config['database'],
                'total_tables': total_tables,
                'total_size_bytes': total_size_bytes,
                'total_size_pretty': self._format_bytes(total_size_bytes),
                'dead_tuple_ratio': total_dead_tuples / max(total_live_tuples, 1),
                'health_score': self._calculate_health_score(table_sizes),
                'timestamp': datetime.now().isoformat(),
                'recommendations': self._generate_health_recommendations(table_sizes)
            }
            
            return health_summary
            
        except (DatabaseConnectionError, DatabasePermissionError, DatabaseQueryError) as e:
            # Re-raise database-specific errors with health check context
            raise type(e)(f"Database health check failed: {e}")
        except Exception as e:
            # Unexpected error during health check
            raise DatabaseQueryError(
                f"Unexpected error during database health check: {e}"
            )

    def _format_bytes(self, bytes_value: int) -> str:
        """Format bytes to human readable string"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f} PB"

    def _calculate_health_score(self, table_sizes: Dict[str, Dict]) -> float:
        """Calculate database health score (0-100)"""
        if not table_sizes:
            return 100.0  # Empty database is "healthy"
        
        # Health factors: low dead tuple ratio, recent vacuum/analyze
        total_score = 0.0
        scored_tables = 0
        
        for table_stats in table_sizes.values():
            live_tuples = table_stats.get('live_tuples', 1)
            dead_tuples = table_stats.get('dead_tuples', 0)
            
            # Dead tuple ratio (lower is better)
            dead_ratio = dead_tuples / max(live_tuples, 1)
            dead_score = max(0, 100 - (dead_ratio * 200))  # 50% dead = 0 score
            
            total_score += dead_score
            scored_tables += 1
        
        return round(total_score / max(scored_tables, 1), 1)

    def _generate_health_recommendations(self, table_sizes: Dict[str, Dict]) -> List[str]:
        """Generate health recommendations based on table statistics"""
        recommendations = []
        
        high_dead_tuple_tables = []
        never_vacuumed_tables = []
        
        for table_name, stats in table_sizes.items():
            live_tuples = stats.get('live_tuples', 1)
            dead_tuples = stats.get('dead_tuples', 0)
            last_vacuum = stats.get('last_vacuum')
            last_autovacuum = stats.get('last_autovacuum')
            
            # Check dead tuple ratio
            dead_ratio = dead_tuples / max(live_tuples, 1)
            if dead_ratio > 0.2:  # More than 20% dead tuples
                high_dead_tuple_tables.append(table_name)
            
            # Check vacuum status
            if not last_vacuum and not last_autovacuum:
                never_vacuumed_tables.append(table_name)
        
        if high_dead_tuple_tables:
            recommendations.append(
                f"Consider manual VACUUM for tables with high dead tuple ratio: "
                f"{', '.join(high_dead_tuple_tables[:5])}"
            )
        
        if never_vacuumed_tables:
            recommendations.append(
                f"Tables never vacuumed may need attention: "
                f"{', '.join(never_vacuumed_tables[:5])}"
            )
        
        if not recommendations:
            recommendations.append("Database health looks good")
        
        return recommendations