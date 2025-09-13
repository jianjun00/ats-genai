"""
Tiingo.com data access operations.

This module consolidates all Tiingo-specific database operations that were
previously scattered across multiple files like daily_price_polygon_tiingo_dao.py,
dividend_tiingo_dao.py, and stock_splits_tiingo_dao.py.
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime, date
from sqlalchemy import text

from core.dao.base.vendor_dao import MarketDataVendorDAO, VendorType
from core.dao.market_data.daily_price_polygon_dao import DailyPricesDAO
from core.platform.logging.logger_config import get_logger


class TiingoDAO(MarketDataVendorDAO):
    """
    Consolidated DAO for all Tiingo.com data operations.

    Replaces the multiple Tiingo-specific DAOs with a single, unified interface
    while maintaining all the original functionality including async operations.
    """

    def __init__(self):
        super().__init__("tiingo_data", VendorType.TIINGO)
        self.daily_price_polygon_dao = DailyPricesDAO()
        self.logger = get_logger(__name__)

    def get_vendor_config(self) -> Dict[str, Any]:
        """Get Tiingo-specific configuration."""
        return {
            "api_base_url": "https://api.tiingo.com",
            "rate_limit_requests_per_minute": 1000,  # Higher rate limit than Polygon
            "supports_real_time": True,
            "supports_options": False,
            "supports_crypto": True,
            "supports_forex": False,
            "supports_fundamentals": True
        }

    def get_required_fields(self) -> List[str]:
        """Get required fields for Tiingo data."""
        return ["symbol", "date"]

    def get_schema(self) -> Dict[str, Any]:
        """Get Tiingo data table schema."""
        return {
            "id": "SERIAL PRIMARY KEY",
            "data_type": "VARCHAR(50) NOT NULL",  # 'daily_price_polygon', 'dividends', 'splits', etc.
            "symbol": "VARCHAR(10) NOT NULL",
            "date": "DATE NOT NULL",
            "data": "JSONB NOT NULL",  # Store all Tiingo-specific data
            "created_at": "TIMESTAMP DEFAULT NOW()",
            "updated_at": "TIMESTAMP DEFAULT NOW()",
            "UNIQUE": "(data_type, symbol, date)"
        }

    def transform_vendor_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Tiingo data to standardized format."""
        transformed = super().transform_price_data(raw_data)

        # Tiingo-specific transformations
        if "adjClose" in transformed:
            transformed["adjusted_close"] = transformed.pop("adjClose")

        if "ticker" in transformed:
            transformed["symbol"] = transformed.pop("ticker")

        return transformed

    # Daily Prices Operations (replacing daily_price_polygon_tiingo_dao.py)
    def insert_daily_price(
        self,
        symbol: str,
        date: Union[date, datetime],
        open_price: float,
        high: float,
        low: float,
        close: float,
        volume: int,
        adjusted_close: Optional[float] = None,
        instrument_id: Optional[int] = None,
        **kwargs
    ) -> Optional[int]:
        """
        Insert daily price data for a symbol.

        Args:
            symbol: Stock symbol
            date: Price date
            open_price: Opening price
            high: High price
            low: Low price
            close: Closing price
            volume: Trading volume
            adjusted_close: Adjusted closing price
            instrument_id: Optional instrument ID
            **kwargs: Additional Tiingo-specific fields

        Returns:
            Created record ID
        """
        price_data = {
            "symbol": symbol,
            "date": date if isinstance(date, date) else date.date(),
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "adjusted_close": adjusted_close or close,
            "vendor": "tiingo",
            "instrument_id": instrument_id
        }

        # Validate as price data
        if not self.validate_price_data(price_data):
            self.logger.error(f"Invalid price data for {symbol} on {date}")
            return None

        return self.daily_price_polygon_dao.create(price_data)

    def get_daily_price(
        self,
        symbol: str,
        date: Union[date, datetime],
        instrument_id: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get daily price for symbol and date.

        Args:
            symbol: Stock symbol
            date: Price date
            instrument_id: Optional instrument ID filter

        Returns:
            Price record or None
        """
        return self.daily_price_polygon_dao.get_price_by_symbol_date(symbol, date, vendor="tiingo")

    def get_price_by_instrument(
        self,
        instrument_id: int,
        date: Union[date, datetime]
    ) -> Optional[Dict[str, Any]]:
        """
        Get price by instrument ID and date (Tiingo-specific method).

        Args:
            instrument_id: Instrument ID
            date: Price date

        Returns:
            Price record or None
        """
        query = f"""
            SELECT * FROM {self.daily_price_polygon_dao.table_name}
            WHERE instrument_id = :instrument_id AND date = :date AND vendor = 'tiingo'
            LIMIT 1
        """

        params = {
            "instrument_id": instrument_id,
            "date": date if isinstance(date, date) else date.date()
        }

        results = self.daily_price_polygon_dao.execute_query(query, params)
        return results[0] if results else None

    def list_prices_by_instrument(self, instrument_id: int) -> List[Dict[str, Any]]:
        """
        List all prices for an instrument (Tiingo-specific method).

        Args:
            instrument_id: Instrument ID

        Returns:
            List of price records
        """
        query = f"""
            SELECT * FROM {self.daily_price_polygon_dao.table_name}
            WHERE instrument_id = :instrument_id AND vendor = 'tiingo'
            ORDER BY date
        """

        return self.daily_price_polygon_dao.execute_query(query, {"instrument_id": instrument_id})

    def batch_insert_daily_price_polygon(self, price_records: List[Dict[str, Any]]) -> int:
        """
        Batch insert daily price records.

        Args:
            price_records: List of price records

        Returns:
            Number of records inserted
        """
        # Add vendor information to all records
        enhanced_records = []
        for record in price_records:
            enhanced = record.copy()
            enhanced["vendor"] = "tiingo"

            # Handle Tiingo-specific field names
            if "adjClose" in enhanced:
                enhanced["adjusted_close"] = enhanced.pop("adjClose")

            # Validate each record
            if self.validate_price_data(enhanced):
                enhanced_records.append(enhanced)
            else:
                self.logger.warning(f"Skipping invalid price record: {record}")

        return self.daily_price_polygon_dao.bulk_insert(enhanced_records)

    # Dividend Operations (replacing dividend_tiingo_dao.py)
    def insert_dividend(
        self,
        symbol: str,
        ex_dividend_date: Union[date, datetime],
        cash_amount: float,
        declaration_date: Optional[Union[date, datetime]] = None,
        payment_date: Optional[Union[date, datetime]] = None,
        record_date: Optional[Union[date, datetime]] = None,
        description: Optional[str] = None,
        refid: Optional[str] = None,
        **kwargs
    ) -> Optional[int]:
        """
        Insert dividend data.

        Args:
            symbol: Stock symbol
            ex_dividend_date: Ex-dividend date
            cash_amount: Dividend amount
            declaration_date: Optional declaration date
            payment_date: Optional payment date
            record_date: Optional record date
            description: Optional description
            refid: Optional reference ID
            **kwargs: Additional Tiingo-specific fields

        Returns:
            Created record ID
        """
        dividend_data = {
            "data_type": "dividend",
            "symbol": symbol,
            "date": ex_dividend_date if isinstance(ex_dividend_date, date) else ex_dividend_date.date(),
            "data": {
                "cash_amount": cash_amount,
                "ex_dividend_date": (ex_dividend_date if isinstance(ex_dividend_date, date) else ex_dividend_date.date()).isoformat(),
                "declaration_date": declaration_date.isoformat() if declaration_date else None,
                "payment_date": payment_date.isoformat() if payment_date else None,
                "record_date": record_date.isoformat() if record_date else None,
                "description": description,
                "refid": refid,
                **kwargs
            }
        }

        return self.create_with_vendor_metadata(dividend_data)

    def get_dividends_by_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        """
        Get dividend records for a symbol.

        Args:
            symbol: Stock symbol

        Returns:
            List of dividend records
        """
        query = f"""
            SELECT * FROM {self.table_name}
            WHERE symbol = :symbol AND data_type = 'dividend'
            ORDER BY date
        """

        return self.execute_query(query, {"symbol": symbol.upper()})

    def get_all_dividends(self) -> List[Dict[str, Any]]:
        """
        Get all dividend records.

        Returns:
            List of all dividend records
        """
        query = f"""
            SELECT * FROM {self.table_name}
            WHERE data_type = 'dividend'
            ORDER BY date, symbol
        """

        return self.execute_query(query)

    # Stock Split Operations (replacing stock_splits_tiingo_dao.py)
    def insert_stock_split(
        self,
        symbol: str,
        execution_date: Union[date, datetime],
        split_from: float,
        split_to: float,
        cash_amount: Optional[float] = None,
        declaration_date: Optional[Union[date, datetime]] = None,
        payment_date: Optional[Union[date, datetime]] = None,
        record_date: Optional[Union[date, datetime]] = None,
        description: Optional[str] = None,
        refid: Optional[str] = None,
        **kwargs
    ) -> Optional[int]:
        """
        Insert stock split data.

        Args:
            symbol: Stock symbol
            execution_date: Split execution date
            split_from: Split from ratio
            split_to: Split to ratio
            cash_amount: Optional cash amount
            declaration_date: Optional declaration date
            payment_date: Optional payment date
            record_date: Optional record date
            description: Optional description
            refid: Optional reference ID
            **kwargs: Additional Tiingo-specific fields

        Returns:
            Created record ID
        """
        split_ratio = split_to / split_from if split_from != 0 else 0

        split_data = {
            "data_type": "stock_split",
            "symbol": symbol,
            "date": execution_date if isinstance(execution_date, date) else execution_date.date(),
            "data": {
                "split_from": split_from,
                "split_to": split_to,
                "split_ratio": split_ratio,
                "execution_date": (execution_date if isinstance(execution_date, date) else execution_date.date()).isoformat(),
                "cash_amount": cash_amount,
                "declaration_date": declaration_date.isoformat() if declaration_date else None,
                "payment_date": payment_date.isoformat() if payment_date else None,
                "record_date": record_date.isoformat() if record_date else None,
                "description": description,
                "refid": refid,
                **kwargs
            }
        }

        return self.create_with_vendor_metadata(split_data)

    def get_splits_by_symbol(self, symbol: str) -> List[Dict[str, Any]]:
        """
        Get stock split records for a symbol.

        Args:
            symbol: Stock symbol

        Returns:
            List of stock split records
        """
        query = f"""
            SELECT * FROM {self.table_name}
            WHERE symbol = :symbol AND data_type = 'stock_split'
            ORDER BY date
        """

        return self.execute_query(query, {"symbol": symbol.upper()})

    def get_all_splits(self) -> List[Dict[str, Any]]:
        """
        Get all stock split records.

        Returns:
            List of all stock split records
        """
        query = f"""
            SELECT * FROM {self.table_name}
            WHERE data_type = 'stock_split'
            ORDER BY date, symbol
        """

        return self.execute_query(query)

    # Generic data operations
    def insert_data(
        self,
        data_type: str,
        symbol: str,
        date: Union[date, datetime],
        data: Dict[str, Any]
    ) -> Optional[int]:
        """
        Insert generic Tiingo data.

        Args:
            data_type: Type of data (e.g., 'fundamentals', 'news', 'crypto')
            symbol: Stock symbol
            date: Data date
            data: Data payload

        Returns:
            Created record ID
        """
        record_data = {
            "data_type": data_type,
            "symbol": symbol,
            "date": date if isinstance(date, date) else date.date(),
            "data": data
        }

        return self.create_with_vendor_metadata(record_data)

    def get_data(
        self,
        data_type: str,
        symbol: Optional[str] = None,
        start_date: Optional[Union[date, datetime]] = None,
        end_date: Optional[Union[date, datetime]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get generic Tiingo data.

        Args:
            data_type: Type of data
            symbol: Optional symbol filter
            start_date: Optional start date
            end_date: Optional end date

        Returns:
            List of data records
        """
        params = {"data_type": data_type}
        conditions = ["data_type = :data_type"]

        if symbol:
            conditions.append("symbol = :symbol")
            params["symbol"] = symbol.upper()

        if start_date:
            conditions.append("date >= :start_date")
            params["start_date"] = start_date if isinstance(start_date, date) else start_date.date()

        if end_date:
            conditions.append("date <= :end_date")
            params["end_date"] = end_date if isinstance(end_date, date) else end_date.date()

        query = f"""
            SELECT * FROM {self.table_name}
            WHERE {' AND '.join(conditions)}
            ORDER BY date, symbol
        """

        return self.execute_query(query, params)

    # Implementation of abstract methods
    def _create_impl(self, session, data: Dict[str, Any]) -> Optional[int]:
        """Create Tiingo data record."""
        query = text(f"""
            INSERT INTO {self.table_name} (data_type, symbol, date, data)
            VALUES (:data_type, :symbol, :date, :data)
            RETURNING id
        """)

        result = session.execute(query, data)
        return result.scalar()

    def _read_impl(self, session, record_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """Read Tiingo data record."""
        query = text(f"SELECT * FROM {self.table_name} WHERE id = :id")
        result = session.execute(query, {"id": record_id})
        row = result.fetchone()
        return dict(row._mapping) if row else None

    def _update_impl(self, session, record_id: Union[int, str], data: Dict[str, Any]) -> bool:
        """Update Tiingo data record."""
        data["updated_at"] = datetime.utcnow()

        set_clauses = []
        params = {"id": record_id}

        for key, value in data.items():
            if key != "id":
                set_clauses.append(f"{key} = :{key}")
                params[key] = value

        if not set_clauses:
            return False

        query = text(f"""
            UPDATE {self.table_name}
            SET {', '.join(set_clauses)}
            WHERE id = :id
        """)

        result = session.execute(query, params)
        return result.rowcount > 0

    def _delete_impl(self, session, record_id: Union[int, str]) -> bool:
        """Delete Tiingo data record."""
        query = text(f"DELETE FROM {self.table_name} WHERE id = :id")
        result = session.execute(query, {"id": record_id})
        return result.rowcount > 0

    def _list_all_impl(self, session, limit: Optional[int], offset: int) -> List[Dict[str, Any]]:
        """List all Tiingo data records."""
        query_str = f"SELECT * FROM {self.table_name} ORDER BY date DESC, symbol"

        if limit:
            query_str += f" LIMIT {limit} OFFSET {offset}"

        query = text(query_str)
        result = session.execute(query)
        return [dict(row._mapping) for row in result]

    def _count_impl(self, session, where_clause: Optional[str], params: Optional[Dict[str, Any]]) -> int:
        """Count Tiingo data records."""
        query_str = f"SELECT COUNT(*) FROM {self.table_name}"

        if where_clause:
            query_str += f" WHERE {where_clause}"

        query = text(query_str)
        result = session.execute(query, params or {})
        return result.scalar()

    def _bulk_insert_impl(self, session, records: List[Dict[str, Any]]) -> int:
        """Bulk insert Tiingo data records."""
        if not records:
            return 0

        query = text(f"""
            INSERT INTO {self.table_name} (data_type, symbol, date, data, vendor, created_at, updated_at)
            VALUES (:data_type, :symbol, :date, :data, :vendor, :created_at, :updated_at)
            ON CONFLICT (data_type, symbol, date) DO UPDATE SET
                data = EXCLUDED.data,
                updated_at = EXCLUDED.updated_at
        """)

        session.execute(query, records)
        return len(records)