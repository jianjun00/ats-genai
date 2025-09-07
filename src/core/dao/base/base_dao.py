"""
Base DAO class with common CRUD operations.

This module provides the foundation for all DAOs, eliminating code duplication
by centralizing common database operations, connection management, and error handling.
"""

import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
import pandas as pd

from core.platform.database.connection_manager import get_session, get_async_session, get_raw_connection
from core.platform.config.settings import get_settings
from core.platform.logging.logger_config import get_logger
from core.security.exceptions.custom_exceptions import (
    DataValidationError, handle_database_error
)
from core.security.validation.data_validators import ValidationResult


logger = get_logger(__name__)


class BaseDAO(ABC):
    """
    Base Data Access Object with common CRUD operations.
    
    Provides standardized database access patterns with connection management,
    error handling, and validation integrated from core infrastructure.
    """
    
    def __init__(self, table_name: str):
        """
        Initialize DAO with table name.
        
        Args:
            table_name: Base table name (will be prefixed with environment)
        """
        self.settings = get_settings()
        self.base_table_name = table_name
        self.table_name = self.settings.get_table_name(table_name)
        self.logger = get_logger(f"{self.__class__.__module__}.{self.__class__.__name__}")
    
    @abstractmethod
    def get_schema(self) -> Dict[str, Any]:
        """Get table schema definition."""
    
    def validate_data(self, data: Dict[str, Any]) -> ValidationResult:
        """
        Validate data before database operations.
        
        Args:
            data: Data to validate
            
        Returns:
            Validation result
        """
        # Default implementation - can be overridden by subclasses
        from core.security.validation.data_validators import ValidationResult
        return ValidationResult(is_valid=True)
    
    # Sync operations using context managers
    def create(self, data: Dict[str, Any]) -> Optional[int]:
        """
        Create a new record.
        
        Args:
            data: Record data
            
        Returns:
            Created record ID or None
        """
        # Validate data
        validation = self.validate_data(data)
        if not validation.is_valid:
            raise DataValidationError(f"Validation failed: {validation.errors}")
        
        try:
            with get_session() as session:
                return self._create_impl(session, data)
        except Exception as e:
            db_error = handle_database_error(e, "create")
            self.logger.error("Create operation failed", extra=db_error.context)
            raise db_error
    
    def read(self, record_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """
        Read a record by ID.
        
        Args:
            record_id: Record ID
            
        Returns:
            Record data or None if not found
        """
        try:
            with get_session() as session:
                return self._read_impl(session, record_id)
        except Exception as e:
            db_error = handle_database_error(e, "read")
            self.logger.error("Read operation failed", extra=db_error.context)
            raise db_error
    
    def update(self, record_id: Union[int, str], data: Dict[str, Any]) -> bool:
        """
        Update a record.
        
        Args:
            record_id: Record ID
            data: Updated data
            
        Returns:
            True if updated, False if not found
        """
        # Validate data
        validation = self.validate_data(data)
        if not validation.is_valid:
            raise DataValidationError(f"Validation failed: {validation.errors}")
        
        try:
            with get_session() as session:
                return self._update_impl(session, record_id, data)
        except Exception as e:
            db_error = handle_database_error(e, "update")
            self.logger.error("Update operation failed", extra=db_error.context)
            raise db_error
    
    def delete(self, record_id: Union[int, str]) -> bool:
        """
        Delete a record.
        
        Args:
            record_id: Record ID
            
        Returns:
            True if deleted, False if not found
        """
        try:
            with get_session() as session:
                return self._delete_impl(session, record_id)
        except Exception as e:
            db_error = handle_database_error(e, "delete")
            self.logger.error("Delete operation failed", extra=db_error.context)
            raise db_error
    
    def list_all(self, limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
        """
        List all records with optional pagination.
        
        Args:
            limit: Maximum number of records
            offset: Number of records to skip
            
        Returns:
            List of records
        """
        try:
            with get_session() as session:
                return self._list_all_impl(session, limit, offset)
        except Exception as e:
            db_error = handle_database_error(e, "list_all")
            self.logger.error("List operation failed", extra=db_error.context)
            raise db_error
    
    def count(self, where_clause: Optional[str] = None, params: Optional[Dict[str, Any]] = None) -> int:
        """
        Count records with optional filter.
        
        Args:
            where_clause: WHERE clause condition
            params: Query parameters
            
        Returns:
            Number of records
        """
        try:
            with get_session() as session:
                return self._count_impl(session, where_clause, params)
        except Exception as e:
            db_error = handle_database_error(e, "count")
            self.logger.error("Count operation failed", extra=db_error.context)
            raise db_error
    
    def bulk_insert(self, records: List[Dict[str, Any]], batch_size: int = 1000) -> int:
        """
        Bulk insert records for efficiency.
        
        Args:
            records: List of records to insert
            batch_size: Number of records per batch
            
        Returns:
            Number of records inserted
        """
        if not records:
            return 0
        
        # Validate all records
        for i, record in enumerate(records):
            validation = self.validate_data(record)
            if not validation.is_valid:
                raise DataValidationError(f"Record {i} validation failed: {validation.errors}")
        
        try:
            total_inserted = 0
            with get_session() as session:
                for i in range(0, len(records), batch_size):
                    batch = records[i:i + batch_size]
                    inserted = self._bulk_insert_impl(session, batch)
                    total_inserted += inserted
                    
                    self.logger.info(f"Inserted batch {i//batch_size + 1}", extra={
                        "batch_size": len(batch),
                        "total_inserted": total_inserted
                    })
            
            return total_inserted
        except Exception as e:
            db_error = handle_database_error(e, "bulk_insert")
            self.logger.error("Bulk insert operation failed", extra=db_error.context)
            raise db_error
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute custom query.
        
        Args:
            query: SQL query
            params: Query parameters
            
        Returns:
            Query results
        """
        try:
            with get_raw_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params or {})
                    columns = [desc[0] for desc in cursor.description] if cursor.description else []
                    results = cursor.fetchall()
                    return [dict(zip(columns, row)) for row in results]
        except Exception as e:
            db_error = handle_database_error(e, "execute_query")
            self.logger.error("Custom query failed", extra={**db_error.context, "query": query})
            raise db_error
    
    def to_dataframe(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Execute query and return as DataFrame.
        
        Args:
            query: SQL query
            params: Query parameters
            
        Returns:
            Query results as DataFrame
        """
        try:
            results = self.execute_query(query, params)
            return pd.DataFrame(results)
        except Exception as e:
            db_error = handle_database_error(e, "to_dataframe")
            self.logger.error("DataFrame query failed", extra=db_error.context)
            raise db_error
    
    # Async operations
    async def create_async(self, data: Dict[str, Any]) -> Optional[int]:
        """Async version of create."""
        validation = self.validate_data(data)
        if not validation.is_valid:
            raise DataValidationError(f"Validation failed: {validation.errors}")
        
        try:
            async with get_async_session() as session:
                return await self._create_async_impl(session, data)
        except Exception as e:
            db_error = handle_database_error(e, "create_async")
            self.logger.error("Async create operation failed", extra=db_error.context)
            raise db_error
    
    async def read_async(self, record_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """Async version of read."""
        try:
            async with get_async_session() as session:
                return await self._read_async_impl(session, record_id)
        except Exception as e:
            db_error = handle_database_error(e, "read_async")
            self.logger.error("Async read operation failed", extra=db_error.context)
            raise db_error
    
    # Abstract methods for subclasses to implement
    @abstractmethod
    def _create_impl(self, session, data: Dict[str, Any]) -> Optional[int]:
        """Implementation of create operation."""
    
    @abstractmethod
    def _read_impl(self, session, record_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """Implementation of read operation."""
    
    @abstractmethod
    def _update_impl(self, session, record_id: Union[int, str], data: Dict[str, Any]) -> bool:
        """Implementation of update operation."""
    
    @abstractmethod
    def _delete_impl(self, session, record_id: Union[int, str]) -> bool:
        """Implementation of delete operation."""
    
    @abstractmethod
    def _list_all_impl(self, session, limit: Optional[int], offset: int) -> List[Dict[str, Any]]:
        """Implementation of list all operation."""
    
    @abstractmethod
    def _count_impl(self, session, where_clause: Optional[str], params: Optional[Dict[str, Any]]) -> int:
        """Implementation of count operation."""
    
    @abstractmethod
    def _bulk_insert_impl(self, session, records: List[Dict[str, Any]]) -> int:
        """Implementation of bulk insert operation."""
    
    # Async abstract methods (default implementations that call sync versions)
    async def _create_async_impl(self, session, data: Dict[str, Any]) -> Optional[int]:
        """Async implementation of create operation."""
        # Default implementation - subclasses can override for true async
        return await asyncio.to_thread(self._create_impl, session, data)
    
    async def _read_async_impl(self, session, record_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """Async implementation of read operation."""
        return await asyncio.to_thread(self._read_impl, session, record_id)
    
    # Utility methods
    def get_table_info(self) -> Dict[str, Any]:
        """Get table information."""
        return {
            "base_table_name": self.base_table_name,
            "full_table_name": self.table_name,
            "environment": self.settings.environment.value,
            "schema": self.get_schema()
        }
    
    def health_check(self) -> bool:
        """Check if DAO can connect to database."""
        try:
            self.count()
            return True
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return False