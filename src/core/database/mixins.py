"""
Database connection mixins for standardizing database access patterns.

This module provides reusable mixins for common database operations,
eliminating duplicate code across services and ensuring consistent
connection management.
"""

import logging
from typing import Dict, List, Any, Optional

from .connection_manager import get_connection_manager, get_raw_connection
from ..config.settings import get_settings
from ..exceptions.custom_exceptions import DatabaseError

logger = logging.getLogger(__name__)


class DatabaseMixin:
    """
    Mixin providing standardized database access methods.
    
    This mixin should be used by services that need database access
    to ensure consistent connection management and error handling.
    
    Usage:
        class MyService(DatabaseMixin):
            def __init__(self):
                super().__init__()
                self.initialize_database()
                
            def get_data(self):
                return self.execute_query("SELECT * FROM my_table")
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._db_manager = None
        self._settings = None
        
    @property
    def db_manager(self):
        """Get database connection manager (lazy initialization)."""
        if self._db_manager is None:
            self._db_manager = get_connection_manager()
        return self._db_manager
    
    @property
    def settings(self):
        """Get settings (lazy initialization)."""
        if self._settings is None:
            self._settings = get_settings()
        return self._settings
    
    def initialize_database(self) -> bool:
        """
        Initialize database connection and verify connectivity.
        
        Returns:
            True if connection is successful, False otherwise
            
        Raises:
            DatabaseError: If critical database initialization fails
        """
        try:
            if self.db_manager.check_connection():
                logger.info("✅ Database connection established")
                return True
            else:
                logger.warning("⚠️ Database connection check failed")
                return False
        except Exception as e:
            logger.error(f"❌ Database initialization failed: {e}")
            raise DatabaseError(f"Database initialization failed: {e}")
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results.
        
        Args:
            query: SQL query string
            params: Query parameters (optional)
            
        Returns:
            List of dictionaries representing query results
            
        Raises:
            DatabaseError: If query execution fails
        """
        try:
            with get_raw_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    columns = [desc[0] for desc in cursor.description]
                    results = cursor.fetchall()
                    
                    # Convert to list of dictionaries
                    return [dict(zip(columns, row)) for row in results]
                    
        except Exception as e:
            logger.error(f"Query execution failed: {e}", extra={"query": query})
            raise DatabaseError(f"Query execution failed: {e}")
    
    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """
        Execute an INSERT/UPDATE/DELETE query.
        
        Args:
            query: SQL query string
            params: Query parameters (optional)
            
        Returns:
            Number of affected rows
            
        Raises:
            DatabaseError: If query execution fails
        """
        try:
            with get_raw_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    return cursor.rowcount
                    
        except Exception as e:
            logger.error(f"Update query execution failed: {e}", extra={"query": query})
            raise DatabaseError(f"Update query execution failed: {e}")
    
    def get_table_name(self, base_name: str) -> str:
        """
        Get environment-prefixed table name.
        
        Args:
            base_name: Base table name (e.g., "instruments")
            
        Returns:
            Environment-prefixed table name (e.g., "dev_instruments")
        """
        return self.settings.get_table_name(base_name)
    
    def get_connection_stats(self) -> Dict[str, Any]:
        """Get database connection pool statistics."""
        return self.db_manager.get_connection_stats()


class AsyncDatabaseMixin:
    """
    Async version of DatabaseMixin for services requiring async database operations.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._db_manager = None
        self._settings = None
        
    @property
    def db_manager(self):
        """Get database connection manager (lazy initialization)."""
        if self._db_manager is None:
            self._db_manager = get_connection_manager()
        return self._db_manager
    
    @property
    def settings(self):
        """Get settings (lazy initialization)."""
        if self._settings is None:
            self._settings = get_settings()
        return self._settings
    
    async def initialize_database(self) -> bool:
        """
        Initialize async database connection and verify connectivity.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            if await self.db_manager.check_async_connection():
                logger.info("✅ Async database connection established")
                return True
            else:
                logger.warning("⚠️ Async database connection check failed")
                return False
        except Exception as e:
            logger.error(f"❌ Async database initialization failed: {e}")
            raise DatabaseError(f"Async database initialization failed: {e}")


class DatabaseInitializationMixin:
    """
    Lightweight mixin for simple database initialization patterns.
    
    Use this for classes that only need basic connection verification
    without full query execution capabilities.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._db_manager = None
    
    @property
    def db_manager(self):
        """Get database connection manager (lazy initialization)."""
        if self._db_manager is None:
            self._db_manager = get_connection_manager()
        return self._db_manager
    
    def check_database_connection(self) -> bool:
        """Check if database connection is healthy."""
        try:
            return self.db_manager.check_connection()
        except Exception as e:
            logger.warning(f"Database health check failed: {e}")
            return False
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get basic database connection statistics."""
        try:
            return self.db_manager.get_connection_stats()
        except Exception as e:
            logger.warning(f"Failed to get database stats: {e}")
            return {"error": str(e)}


# Legacy compatibility functions for gradual migration
def create_database_service(service_class):
    """
    Decorator to automatically add database capabilities to a service class.
    
    Usage:
        @create_database_service
        class MyService:
            def __init__(self):
                # Database capabilities automatically available
                pass
    """
    class DatabaseEnabledService(DatabaseMixin, service_class):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.initialize_database()
    
    return DatabaseEnabledService