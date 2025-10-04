"""
Unified dividends DAO that consolidates vendor-specific dividend operations.

This module replaces separate dividend DAOs for different vendors with a
unified interface supporting multi-vendor dividend data.
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime, date
from sqlalchemy import text

from core.dao.base_dao import BaseDAO
from core.platform.logging.logger_config import get_logger


class DividendsDAO(BaseDAO):
    """
    Unified DAO for dividend data across all vendors.

    Provides standardized interface for dividend operations while supporting
    vendor-specific data through a unified table structure.
    """

    def __init__(self):
        super().__init__("dividends")
        self.logger = get_logger(__name__)

    def get_schema(self) -> Dict[str, Any]:
        """Get dividends table schema."""
        return {
            "id": "SERIAL PRIMARY KEY",
            "symbol": "VARCHAR(10) NOT NULL",
            "ex_dividend_date": "DATE NOT NULL",
            "cash_amount": "DECIMAL(12,4) NOT NULL",
            "declaration_date": "DATE",
            "payment_date": "DATE",
            "record_date": "DATE",
            "description": "TEXT",
            "frequency": "VARCHAR(20)",  # quarterly, annual, etc.
            "yield_percent": "DECIMAL(8,4)",
            "vendor": "VARCHAR(20)",
            "vendor_ref_id": "VARCHAR(50)",
            "created_at": "TIMESTAMP DEFAULT NOW()",
            "updated_at": "TIMESTAMP DEFAULT NOW()",
            "UNIQUE": "(symbol, ex_dividend_date, vendor)"
        }

    def _create_impl(self, session, data: Dict[str, Any]) -> Optional[int]:
        """Create dividend record."""
        query = text(f"""
            INSERT INTO {self.table_name}
            (symbol, ex_dividend_date, cash_amount, declaration_date, payment_date,
             record_date, description, frequency, yield_percent, vendor, vendor_ref_id)
            VALUES (:symbol, :ex_dividend_date, :cash_amount, :declaration_date, :payment_date,
                    :record_date, :description, :frequency, :yield_percent, :vendor, :vendor_ref_id)
            RETURNING id
        """)

        result = session.execute(query, data)
        return result.scalar()

    def _read_impl(self, session, record_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """Read dividend record by ID."""
        query = text(f"SELECT * FROM {self.table_name} WHERE id = :id")
        result = session.execute(query, {"id": record_id})
        row = result.fetchone()
        return dict(row._mapping) if row else None

    def _update_impl(self, session, record_id: Union[int, str], data: Dict[str, Any]) -> bool:
        """Update dividend record."""
        set_clauses = []
        params = {"id": record_id}

        for key, value in data.items():
            if key != "id":
                set_clauses.append(f"{key} = :{key}")
                params[key] = value

        if not set_clauses:
            return False

        set_clauses.append("updated_at = NOW()")
        query = text(f"""
            UPDATE {self.table_name}
            SET {', '.join(set_clauses)}
            WHERE id = :id
        """)

        result = session.execute(query, params)
        return result.rowcount > 0

    def _delete_impl(self, session, record_id: Union[int, str]) -> bool:
        """Delete dividend record."""
        query = text(f"DELETE FROM {self.table_name} WHERE id = :id")
        result = session.execute(query, {"id": record_id})
        return result.rowcount > 0

    def _list_all_impl(self, session, limit: Optional[int], offset: int) -> List[Dict[str, Any]]:
        """List all dividend records."""
        query_str = f"SELECT * FROM {self.table_name} ORDER BY ex_dividend_date DESC, symbol"

        if limit:
            query_str += f" LIMIT {limit} OFFSET {offset}"

        query = text(query_str)
        result = session.execute(query)
        return [dict(row._mapping) for row in result]

    def _count_impl(self, session, where_clause: Optional[str], params: Optional[Dict[str, Any]]) -> int:
        """Count dividend records."""
        query_str = f"SELECT COUNT(*) FROM {self.table_name}"

        if where_clause:
            query_str += f" WHERE {where_clause}"

        query = text(query_str)
        result = session.execute(query, params or {})
        return result.scalar()

    def _bulk_insert_impl(self, session, records: List[Dict[str, Any]]) -> int:
        """Bulk insert dividend records."""
        if not records:
            return 0

        query = text(f"""
            INSERT INTO {self.table_name}
            (symbol, ex_dividend_date, cash_amount, declaration_date, payment_date,
             record_date, description, frequency, yield_percent, vendor, vendor_ref_id)
            VALUES (:symbol, :ex_dividend_date, :cash_amount, :declaration_date, :payment_date,
                    :record_date, :description, :frequency, :yield_percent, :vendor, :vendor_ref_id)
            ON CONFLICT (symbol, ex_dividend_date, vendor) DO UPDATE SET
                cash_amount = EXCLUDED.cash_amount,
                declaration_date = EXCLUDED.declaration_date,
                payment_date = EXCLUDED.payment_date,
                record_date = EXCLUDED.record_date,
                description = EXCLUDED.description,
                frequency = EXCLUDED.frequency,
                yield_percent = EXCLUDED.yield_percent,
                updated_at = NOW()
        """)

        session.execute(query, records)
        return len(records)

    # Specialized dividend methods
    def get_dividends_by_symbol(
        self,
        symbol: str,
        vendor: Optional[str] = None,
        start_date: Optional[Union[date, datetime]] = None,
        end_date: Optional[Union[date, datetime]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get dividend records for a symbol.

        Args:
            symbol: Stock symbol
            vendor: Optional vendor filter
            start_date: Optional start date
            end_date: Optional end date

        Returns:
            List of dividend records
        """
        params = {"symbol": symbol.upper()}
        conditions = ["symbol = :symbol"]

        if vendor:
            conditions.append("vendor = :vendor")
            params["vendor"] = vendor

        if start_date:
            conditions.append("ex_dividend_date >= :start_date")
            params["start_date"] = start_date if isinstance(start_date, date) else start_date.date()

        if end_date:
            conditions.append("ex_dividend_date <= :end_date")
            params["end_date"] = end_date if isinstance(end_date, date) else end_date.date()

        query = f"""
            SELECT * FROM {self.table_name}
            WHERE {' AND '.join(conditions)}
            ORDER BY ex_dividend_date DESC
        """

        try:
            return self.execute_query(query, params)
        except Exception as e:
            self.logger.error(f"Failed to get dividends for {symbol}: {e}")
            return []

    def get_dividends_for_date(
        self,
        date: Union[date, datetime],
        symbols: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all dividends for a specific date.

        Args:
            date: Ex-dividend date
            symbols: Optional symbols filter

        Returns:
            List of dividend records for the date
        """
        params = {
            "date": date if isinstance(date, date) else date.date()
        }

        query_str = f"""
            SELECT * FROM {self.table_name}
            WHERE ex_dividend_date = :date
        """

        if symbols:
            placeholders = ",".join([f":symbol_{i}" for i in range(len(symbols))])
            query_str += f" AND symbol IN ({placeholders})"
            for i, symbol in enumerate(symbols):
                params[f"symbol_{i}"] = symbol.upper()

        query_str += " ORDER BY symbol"

        try:
            return self.execute_query(query_str, params)
        except Exception as e:
            self.logger.error(f"Failed to get dividends for date {date}: {e}")
            return []

    def get_upcoming_dividends(
        self,
        days_ahead: int = 30,
        symbols: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get upcoming dividends within specified days.

        Args:
            days_ahead: Number of days to look ahead
            symbols: Optional symbols filter

        Returns:
            List of upcoming dividend records
        """
        params = {
            "start_date": datetime.now().date(),
            "end_date": (datetime.now() + datetime.timedelta(days=days_ahead)).date()
        }

        query_str = f"""
            SELECT * FROM {self.table_name}
            WHERE ex_dividend_date BETWEEN :start_date AND :end_date
        """

        if symbols:
            placeholders = ",".join([f":symbol_{i}" for i in range(len(symbols))])
            query_str += f" AND symbol IN ({placeholders})"
            for i, symbol in enumerate(symbols):
                params[f"symbol_{i}"] = symbol.upper()

        query_str += " ORDER BY ex_dividend_date, symbol"

        try:
            return self.execute_query(query_str, params)
        except Exception as e:
            self.logger.error(f"Failed to get upcoming dividends: {e}")
            return []

    def get_dividend_history_summary(
        self,
        symbol: str,
        years: int = 5
    ) -> Dict[str, Any]:
        """
        Get dividend history summary for a symbol.

        Args:
            symbol: Stock symbol
            years: Number of years to look back

        Returns:
            Dividend summary statistics
        """
        params = {
            "symbol": symbol.upper(),
            "start_date": (datetime.now() - datetime.timedelta(days=years*365)).date()
        }

        query = f"""
            SELECT
                COUNT(*) as total_dividends,
                SUM(cash_amount) as total_amount,
                AVG(cash_amount) as avg_amount,
                MIN(cash_amount) as min_amount,
                MAX(cash_amount) as max_amount,
                MIN(ex_dividend_date) as earliest_date,
                MAX(ex_dividend_date) as latest_date,
                AVG(yield_percent) as avg_yield
            FROM {self.table_name}
            WHERE symbol = :symbol AND ex_dividend_date >= :start_date
        """

        try:
            results = self.execute_query(query, params)
            return results[0] if results else {}
        except Exception as e:
            self.logger.error(f"Failed to get dividend summary for {symbol}: {e}")
            return {}

    def get_high_yield_dividends(
        self,
        min_yield: float = 3.0,
        symbols: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get dividends with yield above threshold.

        Args:
            min_yield: Minimum yield percentage
            symbols: Optional symbols filter

        Returns:
            List of high-yield dividend records
        """
        params = {"min_yield": min_yield}

        query_str = f"""
            SELECT * FROM {self.table_name}
            WHERE yield_percent >= :min_yield
        """

        if symbols:
            placeholders = ",".join([f":symbol_{i}" for i in range(len(symbols))])
            query_str += f" AND symbol IN ({placeholders})"
            for i, symbol in enumerate(symbols):
                params[f"symbol_{i}"] = symbol.upper()

        query_str += " ORDER BY yield_percent DESC, ex_dividend_date DESC"

        try:
            return self.execute_query(query_str, params)
        except Exception as e:
            self.logger.error(f"Failed to get high yield dividends: {e}")
            return []