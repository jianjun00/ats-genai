"""
Vendor DAO for managing vendor data and operations.

Provides data access layer for vendor entities following the established BaseDAO pattern.
"""

from typing import Dict, Any, List, Optional, Union

from dao.base.base_dao import BaseDAO
from core.validation.data_validators import ValidationResult


class VendorDAO(BaseDAO):
    """
    Data Access Object for vendor operations.
    
    Manages CRUD operations for vendors following the existing BaseDAO pattern.
    """
    
    def __init__(self):
        super().__init__("vendors")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get vendor table schema definition."""
        return {
            "table_name": self.table_name,
            "columns": {
                "vendor_id": {"type": "SERIAL", "primary_key": True},
                "vendor_name": {"type": "VARCHAR(100)", "unique": True, "not_null": True},
                "vendor_description": {"type": "TEXT"},
                "api_endpoint": {"type": "VARCHAR(255)"},
                "api_key": {"type": "VARCHAR(255)"},
                "is_active": {"type": "BOOLEAN", "default": True},
                "created_at": {"type": "TIMESTAMP", "default": "now()"},
                "updated_at": {"type": "TIMESTAMP", "default": "now()"}
            },
            "indexes": [
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_vendors_name ON {table} (vendor_name)",
                "CREATE INDEX IF NOT EXISTS idx_vendors_active ON {table} (is_active)"
            ]
        }
    
    def validate_data(self, data: Dict[str, Any]) -> ValidationResult:
        """Validate vendor data before database operations."""
        errors = []
        
        # Required fields
        if not data.get('vendor_name'):
            errors.append("vendor_name is required")
        elif len(data['vendor_name']) > 100:
            errors.append("vendor_name must be 100 characters or less")
        
        # Optional field validation
        if data.get('api_endpoint') and len(data['api_endpoint']) > 255:
            errors.append("api_endpoint must be 255 characters or less")
            
        if data.get('api_key') and len(data['api_key']) > 255:
            errors.append("api_key must be 255 characters or less")
        
        return ValidationResult(is_valid=len(errors) == 0, errors=errors)
    
    # Sync CRUD implementations
    def _create_impl(self, session, data: Dict[str, Any]) -> Optional[int]:
        """Implementation of create operation."""
        query = f"""
        INSERT INTO {self.table_name} 
        (vendor_name, vendor_description, api_endpoint, api_key, is_active, created_at) 
        VALUES (%(vendor_name)s, %(vendor_description)s, %(api_endpoint)s, 
                %(api_key)s, COALESCE(%(is_active)s, true), now()) 
        RETURNING vendor_id
        """
        
        result = session.execute(query, data)
        row = result.fetchone()
        return row[0] if row else None
    
    def _read_impl(self, session, record_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """Implementation of read operation."""
        if isinstance(record_id, str):
            # Search by vendor_name
            query = f"SELECT * FROM {self.table_name} WHERE vendor_name = %(record_id)s"
        else:
            # Search by vendor_id
            query = f"SELECT * FROM {self.table_name} WHERE vendor_id = %(record_id)s"
            
        result = session.execute(query, {"record_id": record_id})
        row = result.fetchone()
        
        if row:
            return dict(zip(result.keys(), row))
        return None
    
    def _update_impl(self, session, record_id: Union[int, str], data: Dict[str, Any]) -> bool:
        """Implementation of update operation."""
        # Build dynamic update query
        update_fields = []
        update_data = {"record_id": record_id}
        
        for field in ['vendor_description', 'api_endpoint', 'api_key', 'is_active']:
            if field in data:
                update_fields.append(f"{field} = %({field})s")
                update_data[field] = data[field]
        
        if not update_fields:
            return False
        
        update_fields.append("updated_at = now()")
        
        if isinstance(record_id, str):
            where_clause = "vendor_name = %(record_id)s"
        else:
            where_clause = "vendor_id = %(record_id)s"
        
        query = f"""
        UPDATE {self.table_name} 
        SET {', '.join(update_fields)} 
        WHERE {where_clause}
        """
        
        result = session.execute(query, update_data)
        return result.rowcount > 0
    
    def _delete_impl(self, session, record_id: Union[int, str]) -> bool:
        """Implementation of delete operation."""
        if isinstance(record_id, str):
            query = f"DELETE FROM {self.table_name} WHERE vendor_name = %(record_id)s"
        else:
            query = f"DELETE FROM {self.table_name} WHERE vendor_id = %(record_id)s"
            
        result = session.execute(query, {"record_id": record_id})
        return result.rowcount > 0
    
    def _list_all_impl(self, session, limit: Optional[int], offset: int) -> List[Dict[str, Any]]:
        """Implementation of list all operation."""
        query = f"SELECT * FROM {self.table_name} ORDER BY vendor_name"
        
        if limit:
            query += f" LIMIT {limit} OFFSET {offset}"
        
        result = session.execute(query)
        return [dict(zip(result.keys(), row)) for row in result.fetchall()]
    
    def _count_impl(self, session, where_clause: Optional[str], params: Optional[Dict[str, Any]]) -> int:
        """Implementation of count operation."""
        query = f"SELECT COUNT(*) FROM {self.table_name}"
        
        if where_clause:
            query += f" WHERE {where_clause}"
            
        result = session.execute(query, params or {})
        return result.fetchone()[0]
    
    def _bulk_insert_impl(self, session, records: List[Dict[str, Any]]) -> int:
        """Implementation of bulk insert operation."""
        if not records:
            return 0
        
        # Prepare bulk insert data
        insert_data = []
        for record in records:
            insert_data.append((
                record.get('vendor_name'),
                record.get('vendor_description'),
                record.get('api_endpoint'),
                record.get('api_key'),
                record.get('is_active', True)
            ))
        
        query = f"""
        INSERT INTO {self.table_name} 
        (vendor_name, vendor_description, api_endpoint, api_key, is_active, created_at) 
        VALUES (%s, %s, %s, %s, %s, now())
        """
        
        session.executemany(query, insert_data)
        return len(insert_data)
    
    # Vendor-specific methods
    def get_by_name(self, vendor_name: str) -> Optional[Dict[str, Any]]:
        """Get vendor by vendor name."""
        return self.read(vendor_name)
    
    def list_active_vendors(self) -> List[Dict[str, Any]]:
        """List all active vendors."""
        try:
            query = f"SELECT * FROM {self.table_name} WHERE is_active = true ORDER BY vendor_name"
            return self.execute_query(query)
        except Exception as e:
            self.logger.error(f"Error listing active vendors: {e}")
            raise
    
    def get_exchange_vendor_id(self) -> Optional[int]:
        """Get the vendor ID for the 'exchange' vendor."""
        try:
            exchange_vendor = self.get_by_name('exchange')
            return exchange_vendor['vendor_id'] if exchange_vendor else None
        except Exception as e:
            self.logger.error(f"Error getting exchange vendor ID: {e}")
            raise
    
    def search_vendors(self, search_term: str) -> List[Dict[str, Any]]:
        """Search vendors by name or description."""
        try:
            query = f"""
            SELECT * FROM {self.table_name} 
            WHERE vendor_name ILIKE %(term)s OR vendor_description ILIKE %(term)s
            ORDER BY vendor_name
            """
            return self.execute_query(query, {"term": f"%{search_term}%"})
        except Exception as e:
            self.logger.error(f"Error searching vendors: {e}")
            raise