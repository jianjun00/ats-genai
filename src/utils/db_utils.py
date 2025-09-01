#!/usr/bin/env python3
"""
Database connection utilities for ATS scripts.

Provides simple helper functions for scripts to access the centralized
connection manager without having to understand the full implementation.

NO FALLBACKS - If centralized connection manager fails, the script should fail.
This prevents hiding real issues and ensures consistent connection management.
"""

import os
from typing import Dict, List, Any, Optional

# Use proper relative imports instead of sys.path manipulation
from ..core.database.connection_manager import get_connection_manager
from ..core.config.settings import get_settings


def get_db_connection():
    """
    Get a raw database connection for simple scripts.
    
    Returns a psycopg2 connection context manager.
    
    Usage:
        from utils.db_utils import get_db_connection
        
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM dev_instruments")
                results = cursor.fetchall()
    """
    return get_connection_manager().get_raw_connection()


def execute_query(query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """
    Execute a SELECT query and return results as list of dictionaries.
    
    Args:
        query: SQL query string
        params: Query parameters (optional)
    
    Returns:
        List of dictionaries with query results
    
    Usage:
        from utils.db_utils import execute_query
        
        results = execute_query("SELECT * FROM dev_instruments WHERE symbol = %s", ('AAPL',))
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            
            return [dict(zip(columns, row)) for row in rows]


def execute_update(query: str, params: Optional[tuple] = None) -> int:
    """
    Execute an INSERT/UPDATE/DELETE query and return affected row count.
    
    Args:
        query: SQL query string
        params: Query parameters (optional)
    
    Returns:
        Number of affected rows
    
    Usage:
        from utils.db_utils import execute_update
        
        affected = execute_update("UPDATE dev_instruments SET active = %s WHERE symbol = %s", (True, 'AAPL'))
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            return cursor.rowcount


def get_table_name(base_name: str) -> str:
    """
    Get environment-prefixed table name.
    
    Args:
        base_name: Base table name (e.g., "instruments")
    
    Returns:
        Environment-prefixed table name (e.g., "dev_instruments")
    
    Usage:
        from utils.db_utils import get_table_name
        
        table = get_table_name("instruments")  # Returns "dev_instruments" in dev env
    """
    settings = get_settings()
    return settings.get_table_name(base_name)


def check_connection() -> bool:
    """
    Check if database connection is healthy.
    
    Returns:
        True if connection is healthy, False otherwise
    
    Usage:
        from utils.db_utils import check_connection
        
        if check_connection():
            print("Database is available")
        else:
            print("Database connection failed")
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
    except Exception:
        return False


def get_connection_config() -> Dict[str, Any]:
    """
    Get database connection configuration from centralized settings.
    
    Returns:
        Dictionary with connection parameters
    
    Usage:
        from utils.db_utils import get_connection_config
        
        config = get_connection_config()
        print(f"Connecting to {config['host']}:{config['port']}")
    """
    settings = get_settings()
    return {
        "host": settings.database_host,
        "port": settings.database_port,
        "database": settings.database_name,
        "user": settings.database_user,
        "password": settings.database_password
    }


def get_api_key(vendor: str) -> Optional[str]:
    """
    Get API key for specific vendor.
    
    Args:
        vendor: Vendor name (polygon, tiingo, alpha_vantage, etc.)
    
    Returns:
        API key if available, None otherwise
    
    Usage:
        from utils.db_utils import get_api_key
        
        polygon_key = get_api_key("polygon")
        if polygon_key:
            # Use the API key
            pass
    """
    return get_settings().get_api_key(vendor)


# Environment-specific helpers
def is_dev_environment() -> bool:
    """Check if running in development environment."""
    return get_settings().is_development


def is_production_environment() -> bool:
    """Check if running in production environment."""
    return get_settings().is_production


# Legacy compatibility functions for existing scripts
def get_database_connection_sync():
    """Legacy sync connection for existing scripts."""
    return get_db_connection()


async def get_database_connection():
    """Legacy async connection for existing scripts."""
    # For async compatibility, we return a sync connection
    # This is not ideal but maintains compatibility
    return get_db_connection()