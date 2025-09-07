"""
Compatibility layer for database imports.

This module provides backward compatibility for existing imports of shared.utils.database
while the codebase transitions to the new shared.utils.database_connections module.
"""

# Import the main Database class from its actual location
try:
    from infrastructure.database.database import Database
except ImportError:
    try:
        from shared.data_handling.utils.database import Database
    except ImportError:
        # Create a minimal Database class for compatibility
        class Database:
            """Minimal Database class for compatibility"""
            pass

# Import new utilities from database_connections module
try:
    from shared.utils.database_connections import get_database_pool, get_table_name
except ImportError:
    # Provide minimal implementations if not available
    async def get_database_pool(*args, **kwargs):
        """Compatibility function - implementation depends on database_connections module"""
        raise NotImplementedError("get_database_pool requires shared.utils.database_connections module")
    
    def get_table_name(*args, **kwargs):
        """Compatibility function - implementation depends on database_connections module"""
        raise NotImplementedError("get_table_name requires shared.utils.database_connections module")

# Legacy function name compatibility
try:
    from infrastructure.database.database import get_connection_pool
except ImportError:
    # Alias get_database_pool to get_connection_pool for backward compatibility
    get_connection_pool = get_database_pool

# Re-export everything for backward compatibility
__all__ = ['Database', 'get_database_pool', 'get_table_name', 'get_connection_pool']