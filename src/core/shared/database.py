"""
Compatibility layer for database imports.

This module provides backward compatibility for existing imports of shared.utils.database
while the codebase transitions to the new database_connections module.
"""

# Import the main Database class from its actual location (gin configurable version)
from core.config.database import Database

# Import utilities from database_connections module
from core.shared.utils.database_connections import get_database_pool, get_table_name

# Legacy function name compatibility
get_connection_pool = get_database_pool

# Re-export everything for backward compatibility
__all__ = ['Database', 'get_database_pool', 'get_table_name', 'get_connection_pool']