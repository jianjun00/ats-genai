"""
Unified daily prices DAO that replaces vendor-specific implementations.

This module consolidates all daily price operations across vendors,
eliminating the duplication between daily_prices_dao.py, daily_price_polygon_dao.py,
and daily_price_tiingo_dao.py.
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime, date
import pandas as pd
from sqlalchemy import text

from core.dao.base.base_dao import BaseDAO
from core.security.validation.data_validators import MarketDataValidator, ValidationResult
from core.platform.logging.logger_config import get_logger


class DailyPricesDAO(BaseDAO):
    """
    Unified DAO for daily price data across all vendors.

    Provides standardized interface for daily price operations while supporting
    vendor-specific data through a unified table structure.
    """

    def __init__(self):
        super().__init__("daily_prices")
        self.validator = MarketDataValidator()
        self.logger = get_logger(__name__)

    def get_schema(self) -> Dict[str, Any]:
        """Get daily prices table schema."""
        return {
            "id": "SERIAL PRIMARY KEY",
            "symbol": "VARCHAR(10) NOT NULL",
            "date": "DATE NOT NULL",
            "open": "DECIMAL(12,4)",
            "high": "DECIMAL(12,4)",
            "low": "DECIMAL(12,4)",
            "close": "DECIMAL(12,4)",
            "volume": "BIGINT",
            "adjusted_close": "DECIMAL(12,4)",
            "market_cap": "BIGINT",
            "vendor": "VARCHAR(20)",
            "instrument_id": "INTEGER",
            "created_at": "TIMESTAMP DEFAULT NOW()",
            "updated_at": "TIMESTAMP DEFAULT NOW()",
            "UNIQUE": "(symbol, date, vendor)"
        }

    def validate_data(self, data: Dict[str, Any]) -> ValidationResult:
        """Validate daily price data."""
        # Convert to DataFrame for validation
        df = pd.DataFrame([data])
        return self.validator.validate(df)

    def _create_impl(self, session, data: Dict[str, Any]) -> Optional[int]:
        """Create daily price record."""
        query = text(f"""
            INSERT INTO {self.table_name}
            (symbol, date, open, high, low, close, volume, adjusted_close, market_cap, vendor, instrument_id)
            VALUES (:symbol, :date, :open, :high, :low, :close, :volume, :adjusted_close, :market_cap, :vendor, :instrument_id)
            RETURNING id
        """)

        result = session.execute(query, data)
        return result.scalar()

    def _read_impl(self, session, record_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """Read daily price record by ID."""
        query = text(f"SELECT * FROM {self.table_name} WHERE id = :id")
        result = session.execute(query, {"id": record_id})
        row = result.fetchone()
        return dict(row._mapping) if row else None

    def _update_impl(self, session, record_id: Union[int, str], data: Dict[str, Any]) -> bool:
        """Update daily price record."""
        # Build SET clause dynamically
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
        """Delete daily price record."""
        query = text(f"DELETE FROM {self.table_name} WHERE id = :id")
        result = session.execute(query, {"id": record_id})
        return result.rowcount > 0

    def _list_all_impl(self, session, limit: Optional[int], offset: int) -> List[Dict[str, Any]]:
        """List all daily price records."""
        query_str = f"SELECT * FROM {self.table_name} ORDER BY date DESC, symbol"

        if limit:
            query_str += f" LIMIT {limit} OFFSET {offset}"

        query = text(query_str)
        result = session.execute(query)
        return [dict(row._mapping) for row in result]

    def _count_impl(self, session, where_clause: Optional[str], params: Optional[Dict[str, Any]]) -> int:
        """Count daily price records."""
        query_str = f"SELECT COUNT(*) FROM {self.table_name}"

        if where_clause:
            query_str += f" WHERE {where_clause}"

        query = text(query_str)
        result = session.execute(query, params or {})
        return result.scalar()

    def _bulk_insert_impl(self, session, records: List[Dict[str, Any]]) -> int:
        """Bulk insert daily price records."""
        if not records:
            return 0

        # Use ON CONFLICT to handle duplicates
        query = text(f"""
            INSERT INTO {self.table_name}
            (symbol, date, open, high, low, close, volume, adjusted_close, market_cap, vendor, instrument_id)
            VALUES (:symbol, :date, :open, :high, :low, :close, :volume, :adjusted_close, :market_cap, :vendor, :instrument_id)
            ON CONFLICT (symbol, date, vendor) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                adjusted_close = EXCLUDED.adjusted_close,
                market_cap = EXCLUDED.market_cap,
                updated_at = NOW()
        """)

        session.execute(query, records)
        return len(records)  # Return number of records processed

    # Specialized methods for daily prices
    def get_price_by_symbol_date(
        self,
        symbol: str,
        date: Union[date, datetime],
        vendor: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get price for specific symbol and date.

        Args:
            symbol: Stock symbol
            date: Price date
            vendor: Optional vendor filter

        Returns:
            Price record or None
        """
        params = {
            "symbol": symbol.upper(),
            "date": date if isinstance(date, date) else date.date()
        }

        query_str = f"""
            SELECT * FROM {self.table_name}
            WHERE symbol = :symbol AND date = :date
        """

        if vendor:
            query_str += " AND vendor = :vendor"
            params["vendor"] = vendor

        query_str += " ORDER BY created_at DESC LIMIT 1"

        try:
            results = self.execute_query(query_str, params)
            return results[0] if results else None
        except Exception as e:
            self.logger.error(f"Failed to get price for {symbol} on {date}: {e}")
            return None

    def get_price_history(
        self,
        symbol: str,
        start_date: Union[date, datetime],
        end_date: Union[date, datetime],
        vendor: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get price history for a symbol.

        Args:
            symbol: Stock symbol
            start_date: Start date
            end_date: End date
            vendor: Optional vendor filter

        Returns:
            List of price records
        """
        params = {
            "symbol": symbol.upper(),
            "start_date": start_date if isinstance(start_date, date) else start_date.date(),
            "end_date": end_date if isinstance(end_date, date) else end_date.date()
        }

        query_str = f"""
            SELECT * FROM {self.table_name}
            WHERE symbol = :symbol
            AND date >= :start_date
            AND date <= :end_date
        """

        if vendor:
            query_str += " AND vendor = :vendor"
            params["vendor"] = vendor

        query_str += " ORDER BY date"

        try:
            return self.execute_query(query_str, params)
        except Exception as e:
            self.logger.error(f"Failed to get price history for {symbol}: {e}")
            return []

    def get_latest_prices(
        self,
        symbols: Optional[List[str]] = None,
        vendor: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get latest prices for symbols.

        Args:
            symbols: Optional list of symbols to filter
            vendor: Optional vendor filter
            limit: Maximum number of records

        Returns:
            List of latest price records
        """
        params = {"limit": limit}

        # Use window function to get latest price per symbol
        query_str = f"""
            SELECT * FROM (
                SELECT *,
                       ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC, created_at DESC) as rn
                FROM {self.table_name}
                WHERE 1=1
        """

        if symbols:
            placeholders = ",".join([f":symbol_{i}" for i in range(len(symbols))])
            query_str += f" AND symbol IN ({placeholders})"
            for i, symbol in enumerate(symbols):
                params[f"symbol_{i}"] = symbol.upper()

        if vendor:
            query_str += " AND vendor = :vendor"
            params["vendor"] = vendor

        query_str += """
            ) ranked
            WHERE rn = 1
            ORDER BY date DESC
            LIMIT :limit
        """

        try:
            return self.execute_query(query_str, params)
        except Exception as e:
            self.logger.error(f"Failed to get latest prices: {e}")
            return []

    def get_prices_for_date(
        self,
        date: Union[date, datetime],
        symbols: Optional[List[str]] = None,
        vendor: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all prices for a specific date.

        Args:
            date: Price date
            symbols: Optional symbols filter
            vendor: Optional vendor filter

        Returns:
            List of price records for the date
        """
        from datetime import date as date_type
        params = {
            "date": date if isinstance(date, date_type) else date.date()
        }

        query_str = f"""
            SELECT * FROM {self.table_name}
            WHERE date = :date
        """

        if symbols:
            placeholders = ",".join([f":symbol_{i}" for i in range(len(symbols))])
            query_str += f" AND symbol IN ({placeholders})"
            for i, symbol in enumerate(symbols):
                params[f"symbol_{i}"] = symbol.upper()

        if vendor:
            query_str += " AND vendor = :vendor"
            params["vendor"] = vendor

        query_str += " ORDER BY symbol"

        try:
            return self.execute_query(query_str, params)
        except Exception as e:
            self.logger.error(f"Failed to get prices for date {date}: {e}")
            return []

    def get_symbols_with_data(
        self,
        start_date: Optional[Union[date, datetime]] = None,
        end_date: Optional[Union[date, datetime]] = None,
        vendor: Optional[str] = None
    ) -> List[str]:
        """
        Get list of symbols that have price data.

        Args:
            start_date: Optional start date filter
            end_date: Optional end date filter
            vendor: Optional vendor filter

        Returns:
            List of symbols
        """
        params = {}
        query_str = f"SELECT DISTINCT symbol FROM {self.table_name} WHERE 1=1"

        if start_date:
            query_str += " AND date >= :start_date"
            params["start_date"] = start_date if isinstance(start_date, date) else start_date.date()

        if end_date:
            query_str += " AND date <= :end_date"
            params["end_date"] = end_date if isinstance(end_date, date) else end_date.date()

        if vendor:
            query_str += " AND vendor = :vendor"
            params["vendor"] = vendor

        query_str += " ORDER BY symbol"

        try:
            results = self.execute_query(query_str, params)
            return [row["symbol"] for row in results]
        except Exception as e:
            self.logger.error(f"Failed to get symbols with data: {e}")
            return []

    def get_data_quality_stats(self, vendor: Optional[str] = None) -> Dict[str, Any]:
        """
        Get data quality statistics.

        Args:
            vendor: Optional vendor filter

        Returns:
            Data quality statistics
        """
        params = {}
        where_clause = ""

        if vendor:
            where_clause = " WHERE vendor = :vendor"
            params["vendor"] = vendor

        query_str = f"""
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT symbol) as unique_symbols,
                COUNT(DISTINCT date) as unique_dates,
                MIN(date) as earliest_date,
                MAX(date) as latest_date,
                AVG(volume) as avg_volume,
                COUNT(*) FILTER (WHERE volume = 0) as zero_volume_records,
                COUNT(*) FILTER (WHERE high < low) as invalid_ohlc_records
            FROM {self.table_name}
            {where_clause}
        """

        try:
            results = self.execute_query(query_str, params)
            return results[0] if results else {}
        except Exception as e:
            self.logger.error(f"Failed to get data quality stats: {e}")
            return {}

    def to_dataframe(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[Union[date, datetime]] = None,
        end_date: Optional[Union[date, datetime]] = None,
        vendor: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Get price data as DataFrame.

        Args:
            symbol: Optional symbol filter
            start_date: Optional start date
            end_date: Optional end date
            vendor: Optional vendor filter

        Returns:
            Price data as DataFrame
        """
        params = {}
        conditions = []

        if symbol:
            conditions.append("symbol = :symbol")
            params["symbol"] = symbol.upper()

        if start_date:
            conditions.append("date >= :start_date")
            params["start_date"] = start_date if isinstance(start_date, date) else start_date.date()

        if end_date:
            conditions.append("date <= :end_date")
            params["end_date"] = end_date if isinstance(end_date, date) else end_date.date()

        if vendor:
            conditions.append("vendor = :vendor")
            params["vendor"] = vendor

        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

        query_str = f"""
            SELECT symbol, date, open, high, low, close, volume, adjusted_close, vendor
            FROM {self.table_name}
            {where_clause}
            ORDER BY symbol, date
        """

        try:
            results = self.execute_query(query_str, params)
            df = pd.DataFrame(results)

            if not df.empty:
                df["date"] = pd.to_datetime(df["date"])
                df = df.set_index("date")

            return df
        except Exception as e:
            self.logger.error(f"Failed to create DataFrame: {e}")
            return pd.DataFrame()