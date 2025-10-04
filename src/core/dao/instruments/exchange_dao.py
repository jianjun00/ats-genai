"""
Exchange DAO for managing exchange data and operations.

Provides data access layer for exchange entities following the established BaseDAO pattern.
"""

from typing import Dict, Any, List, Optional, Union

from core.dao.base_dao import BaseDAO
from core.data_validators import ValidationResult


class ExchangeDAO(BaseDAO):
    """
    Data Access Object for exchange operations.

    Manages CRUD operations for exchanges following the existing BaseDAO pattern.
    """

    def __init__(self):
        super().__init__("exchanges")

    def get_schema(self) -> Dict[str, Any]:
        """Get exchange table schema definition."""
        return {
            "table_name": self.table_name,
            "columns": {
                "id": {"type": "SERIAL", "primary_key": True},
                "exchange_code": {"type": "VARCHAR(10)", "unique": True, "not_null": True},
                "exchange_name": {"type": "VARCHAR(100)", "not_null": True},
                "country": {"type": "VARCHAR(50)"},
                "timezone": {"type": "VARCHAR(50)"},
                "is_active": {"type": "BOOLEAN", "default": True},
                "created_at": {"type": "TIMESTAMP", "default": "now()"},
                "updated_at": {"type": "TIMESTAMP", "default": "now()"}
            },
            "indexes": [
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_exchanges_code ON {table} (exchange_code)",
                "CREATE INDEX IF NOT EXISTS idx_exchanges_active ON {table} (is_active)"
            ]
        }

    def validate_data(self, data: Dict[str, Any]) -> ValidationResult:
        """Validate exchange data before database operations."""
        errors = []

        # Required fields
        if not data.get('exchange_code'):
            errors.append("exchange_code is required")
        elif len(data['exchange_code']) > 10:
            errors.append("exchange_code must be 10 characters or less")

        if not data.get('exchange_name'):
            errors.append("exchange_name is required")
        elif len(data['exchange_name']) > 100:
            errors.append("exchange_name must be 100 characters or less")

        # Optional field validation
        if data.get('country') and len(data['country']) > 50:
            errors.append("country must be 50 characters or less")

        if data.get('timezone') and len(data['timezone']) > 50:
            errors.append("timezone must be 50 characters or less")

        return ValidationResult(is_valid=len(errors) == 0, errors=errors)

    # Sync CRUD implementations
    def _create_impl(self, session, data: Dict[str, Any]) -> Optional[int]:
        """Implementation of create operation."""
        query = f"""
        INSERT INTO {self.table_name}
        (exchange_code, exchange_name, country, timezone, is_active, created_at)
        VALUES (%(exchange_code)s, %(exchange_name)s, %(country)s, %(timezone)s,
                COALESCE(%(is_active)s, true), now())
        RETURNING id
        """

        result = session.execute(query, data)
        row = result.fetchone()
        return row[0] if row else None

    def _read_impl(self, session, record_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """Implementation of read operation."""
        if isinstance(record_id, str):
            # Search by exchange_code
            query = f"SELECT * FROM {self.table_name} WHERE exchange_code = %(record_id)s"
        else:
            # Search by id
            query = f"SELECT * FROM {self.table_name} WHERE id = %(record_id)s"

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

        for field in ['exchange_name', 'country', 'timezone', 'is_active']:
            if field in data:
                update_fields.append(f"{field} = %({field})s")
                update_data[field] = data[field]

        if not update_fields:
            return False

        update_fields.append("updated_at = now()")

        if isinstance(record_id, str):
            where_clause = "exchange_code = %(record_id)s"
        else:
            where_clause = "id = %(record_id)s"

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
            query = f"DELETE FROM {self.table_name} WHERE exchange_code = %(record_id)s"
        else:
            query = f"DELETE FROM {self.table_name} WHERE id = %(record_id)s"

        result = session.execute(query, {"record_id": record_id})
        return result.rowcount > 0

    def _list_all_impl(self, session, limit: Optional[int], offset: int) -> List[Dict[str, Any]]:
        """Implementation of list all operation."""
        query = f"SELECT * FROM {self.table_name} ORDER BY exchange_name"

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
                record.get('exchange_code'),
                record.get('exchange_name'),
                record.get('country'),
                record.get('timezone'),
                record.get('is_active', True)
            ))

        query = f"""
        INSERT INTO {self.table_name}
        (exchange_code, exchange_name, country, timezone, is_active, created_at)
        VALUES (%s, %s, %s, %s, %s, now())
        """

        session.executemany(query, insert_data)
        return len(insert_data)

    # Exchange-specific methods
    def get_by_code(self, exchange_code: str) -> Optional[Dict[str, Any]]:
        """Get exchange by exchange code."""
        return self.read(exchange_code)

    def list_active_exchanges(self) -> List[Dict[str, Any]]:
        """List all active exchanges."""
        try:
            query = f"SELECT * FROM {self.table_name} WHERE is_active = true ORDER BY exchange_name"
            return self.execute_query(query)
        except Exception as e:
            self.logger.error(f"Error listing active exchanges: {e}")
            raise

    def search_exchanges(self, search_term: str) -> List[Dict[str, Any]]:
        """Search exchanges by name or code."""
        try:
            query = f"""
            SELECT * FROM {self.table_name}
            WHERE exchange_code ILIKE %(term)s OR exchange_name ILIKE %(term)s
            ORDER BY exchange_name
            """
            return self.execute_query(query, {"term": f"%{search_term}%"})
        except Exception as e:
            self.logger.error(f"Error searching exchanges: {e}")
            raise