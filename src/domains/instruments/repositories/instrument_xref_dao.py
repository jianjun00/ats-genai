"""
Instrument Cross-Reference DAO for managing exchange history and vendor mappings.

Provides data access layer for instrument_xrefs table, tracking instrument exchange
history with temporal data for migration analysis.
"""

from typing import Dict, Any, List, Optional, Union
from datetime import date

from infrastructure.database.repositories.base.base_dao import BaseDAO
from core.security.validation.data_validators import ValidationResult


class InstrumentXrefDAO(BaseDAO):
    """
    Data Access Object for instrument cross-reference operations.

    Manages exchange history, vendor mappings, and temporal tracking for instruments.
    """

    def __init__(self):
        super().__init__("instrument_xrefs")

    def get_schema(self) -> Dict[str, Any]:
        """Get instrument_xrefs table schema definition."""
        return {
            "table_name": self.table_name,
            "columns": {
                "id": {"type": "SERIAL", "primary_key": True},
                "instrument_id": {"type": "INTEGER", "foreign_key": "dev_instruments(id)", "not_null": True},
                "vendor_id": {"type": "INTEGER", "foreign_key": "vendors(vendor_id)", "not_null": True},
                "external_symbol": {"type": "VARCHAR(50)", "not_null": True},
                "start_date": {"type": "DATE", "not_null": True},
                "end_date": {"type": "DATE", "nullable": True},
                "created_at": {"type": "TIMESTAMP", "default": "now()"},
                "updated_at": {"type": "TIMESTAMP", "default": "now()"}
            },
            "constraints": [
                "UNIQUE(instrument_id, vendor_id, external_symbol)",
                "CHECK (end_date IS NULL OR end_date > start_date)"
            ],
            "indexes": [
                "CREATE INDEX IF NOT EXISTS idx_instrument_xrefs_instrument ON {table} (instrument_id)",
                "CREATE INDEX IF NOT EXISTS idx_instrument_xrefs_vendor ON {table} (vendor_id)",
                "CREATE INDEX IF NOT EXISTS idx_instrument_xrefs_symbol ON {table} (external_symbol)",
                "CREATE INDEX IF NOT EXISTS idx_instrument_xrefs_dates ON {table} (start_date, end_date)",
                "CREATE INDEX IF NOT EXISTS idx_instrument_xrefs_active ON {table} (instrument_id, vendor_id) WHERE end_date IS NULL"
            ]
        }

    def validate_data(self, data: Dict[str, Any]) -> ValidationResult:
        """Validate instrument xref data before database operations."""
        errors = []

        # Required fields
        if not data.get('instrument_id'):
            errors.append("instrument_id is required")
        elif not isinstance(data['instrument_id'], int):
            errors.append("instrument_id must be an integer")

        if not data.get('vendor_id'):
            errors.append("vendor_id is required")
        elif not isinstance(data['vendor_id'], int):
            errors.append("vendor_id must be an integer")

        if not data.get('external_symbol'):
            errors.append("external_symbol is required")
        elif len(data['external_symbol']) > 50:
            errors.append("external_symbol must be 50 characters or less")

        if not data.get('start_date'):
            errors.append("start_date is required")

        # Date validation
        start_date = data.get('start_date')
        end_date = data.get('end_date')

        if end_date and start_date and end_date <= start_date:
            errors.append("end_date must be after start_date")

        return ValidationResult(is_valid=len(errors) == 0, errors=errors)

    # Sync CRUD implementations
    def _create_impl(self, session, data: Dict[str, Any]) -> Optional[int]:
        """Implementation of create operation."""
        query = f"""
        INSERT INTO {self.table_name}
        (instrument_id, vendor_id, external_symbol, start_date, end_date, created_at)
        VALUES (%(instrument_id)s, %(vendor_id)s, %(external_symbol)s,
                %(start_date)s, %(end_date)s, now())
        RETURNING id
        """

        result = session.execute(query, data)
        row = result.fetchone()
        return row[0] if row else None

    def _read_impl(self, session, record_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """Implementation of read operation."""
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

        for field in ['external_symbol', 'start_date', 'end_date']:
            if field in data:
                update_fields.append(f"{field} = %({field})s")
                update_data[field] = data[field]

        if not update_fields:
            return False

        update_fields.append("updated_at = now()")

        query = f"""
        UPDATE {self.table_name}
        SET {', '.join(update_fields)}
        WHERE id = %(record_id)s
        """

        result = session.execute(query, update_data)
        return result.rowcount > 0

    def _delete_impl(self, session, record_id: Union[int, str]) -> bool:
        """Implementation of delete operation."""
        query = f"DELETE FROM {self.table_name} WHERE id = %(record_id)s"
        result = session.execute(query, {"record_id": record_id})
        return result.rowcount > 0

    def _list_all_impl(self, session, limit: Optional[int], offset: int) -> List[Dict[str, Any]]:
        """Implementation of list all operation."""
        query = f"""
        SELECT ix.*, i.symbol, v.vendor_name
        FROM {self.table_name} ix
        JOIN dev_instruments i ON ix.instrument_id = i.id
        JOIN vendors v ON ix.vendor_id = v.vendor_id
        ORDER BY i.symbol, ix.start_date
        """

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
                record.get('instrument_id'),
                record.get('vendor_id'),
                record.get('external_symbol'),
                record.get('start_date'),
                record.get('end_date')
            ))

        query = f"""
        INSERT INTO {self.table_name}
        (instrument_id, vendor_id, external_symbol, start_date, end_date, created_at)
        VALUES (%s, %s, %s, %s, %s, now())
        ON CONFLICT (instrument_id, vendor_id, external_symbol) DO UPDATE SET
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            updated_at = now()
        """

        session.executemany(query, insert_data)
        return len(insert_data)

    # Exchange-specific business methods
    def get_current_exchange(self, instrument_id: int, exchange_vendor_id: int) -> Optional[Dict[str, Any]]:
        """Get current exchange for an instrument."""
        try:
            query = f"""
            SELECT ix.*, e.exchange_name
            FROM {self.table_name} ix
            JOIN exchanges e ON ix.external_symbol = e.exchange_code
            WHERE ix.instrument_id = %(instrument_id)s
              AND ix.vendor_id = %(exchange_vendor_id)s
              AND ix.end_date IS NULL
            """

            results = self.execute_query(query, {
                "instrument_id": instrument_id,
                "exchange_vendor_id": exchange_vendor_id
            })

            return results[0] if results else None

        except Exception as e:
            self.logger.error(f"Error getting current exchange: {e}")
            raise

    def get_exchange_history(self, instrument_id: int, exchange_vendor_id: int) -> List[Dict[str, Any]]:
        """Get complete exchange history for an instrument."""
        try:
            query = f"""
            SELECT
                ix.*,
                e.exchange_name,
                COALESCE(ix.end_date, CURRENT_DATE) - ix.start_date as duration_days
            FROM {self.table_name} ix
            JOIN exchanges e ON ix.external_symbol = e.exchange_code
            WHERE ix.instrument_id = %(instrument_id)s
              AND ix.vendor_id = %(exchange_vendor_id)s
            ORDER BY ix.start_date
            """

            return self.execute_query(query, {
                "instrument_id": instrument_id,
                "exchange_vendor_id": exchange_vendor_id
            })

        except Exception as e:
            self.logger.error(f"Error getting exchange history: {e}")
            raise

    def find_exchange_migrations(self, exchange_vendor_id: int, limit: int = 100) -> List[Dict[str, Any]]:
        """Find instruments that have migrated between exchanges."""
        try:
            query = f"""
            WITH transitions AS (
                SELECT
                    ix1.instrument_id,
                    ix1.external_symbol as from_exchange,
                    ix2.external_symbol as to_exchange,
                    ix1.end_date as transition_date,
                    i.symbol
                FROM {self.table_name} ix1
                JOIN {self.table_name} ix2 ON ix1.instrument_id = ix2.instrument_id
                JOIN dev_instruments i ON ix1.instrument_id = i.id
                WHERE ix1.vendor_id = %(exchange_vendor_id)s
                  AND ix2.vendor_id = %(exchange_vendor_id)s
                  AND ix1.end_date = ix2.start_date
                  AND ix1.external_symbol != ix2.external_symbol
            )
            SELECT
                t.*,
                e1.exchange_name as from_exchange_name,
                e2.exchange_name as to_exchange_name,
                is_major_exchange(t.from_exchange) as from_major,
                t.to_exchange = 'OTC' as to_otc
            FROM transitions t
            JOIN exchanges e1 ON t.from_exchange = e1.exchange_code
            JOIN exchanges e2 ON t.to_exchange = e2.exchange_code
            ORDER BY t.transition_date DESC
            LIMIT %(limit)s
            """

            return self.execute_query(query, {
                "exchange_vendor_id": exchange_vendor_id,
                "limit": limit
            })

        except Exception as e:
            self.logger.error(f"Error finding exchange migrations: {e}")
            raise

    def get_instruments_on_exchange(self, exchange_code: str, exchange_vendor_id: int,
                                   as_of_date: Optional[date] = None) -> List[Dict[str, Any]]:
        """Get all instruments trading on a specific exchange as of a date."""
        try:
            if as_of_date is None:
                as_of_date = date.today()

            query = f"""
            SELECT
                i.symbol,
                i.instrument_name,
                ix.start_date,
                ix.end_date
            FROM {self.table_name} ix
            JOIN dev_instruments i ON ix.instrument_id = i.id
            WHERE ix.vendor_id = %(exchange_vendor_id)s
              AND ix.external_symbol = %(exchange_code)s
              AND ix.start_date <= %(as_of_date)s
              AND (ix.end_date IS NULL OR ix.end_date > %(as_of_date)s)
            ORDER BY i.symbol
            """

            return self.execute_query(query, {
                "exchange_vendor_id": exchange_vendor_id,
                "exchange_code": exchange_code,
                "as_of_date": as_of_date
            })

        except Exception as e:
            self.logger.error(f"Error getting instruments on exchange: {e}")
            raise

    def create_exchange_entry(self, instrument_id: int, exchange_vendor_id: int,
                            exchange_code: str, start_date: date,
                            end_date: Optional[date] = None) -> Optional[int]:
        """Create a new exchange history entry for an instrument."""
        data = {
            "instrument_id": instrument_id,
            "vendor_id": exchange_vendor_id,
            "external_symbol": exchange_code,
            "start_date": start_date,
            "end_date": end_date
        }

        return self.create(data)

    def close_exchange_entry(self, instrument_id: int, exchange_vendor_id: int,
                           exchange_code: str, end_date: date) -> bool:
        """Close an active exchange entry by setting end_date."""
        try:
            # Use raw connection for UPDATE operations
            from core.platform.database.connection_manager import get_raw_connection

            with get_raw_connection() as conn:
                with conn.cursor() as cursor:
                    query = f"""
                    UPDATE {self.table_name}
                    SET end_date = %(end_date)s, updated_at = now()
                    WHERE instrument_id = %(instrument_id)s
                      AND vendor_id = %(exchange_vendor_id)s
                      AND external_symbol = %(exchange_code)s
                      AND end_date IS NULL
                    """

                    cursor.execute(query, {
                        "instrument_id": instrument_id,
                        "exchange_vendor_id": exchange_vendor_id,
                        "exchange_code": exchange_code,
                        "end_date": end_date
                    })

                    conn.commit()
                    return cursor.rowcount > 0

        except Exception as e:
            self.logger.error(f"Error closing exchange entry: {e}")
            raise