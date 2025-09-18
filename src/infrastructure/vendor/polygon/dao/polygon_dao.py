"""
Polygon.io data access operations.

This module consolidates all Polygon-specific database operations that were
previously scattered across multiple files like daily_price_polygon_dao.py,
dividend_polygon_dao.py, and stock_splits_polygon_dao.py.
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime, date
from sqlalchemy import text

from src.core.dao.base.vendor_dao import MarketDataVendorDAO, VendorType
from src.core.dao.market_data.daily_price_polygon_dao import DailyPriceDAO
from src.core.platform.logging.logger_config import get_logger


class PolygonDAO(MarketDataVendorDAO):
    """
    Consolidated DAO for all Polygon.io data operations.

    Replaces the multiple Polygon-specific DAOs with a single, unified interface
    while maintaining all the original functionality.
    """

    def __init__(self):
        super().__init__("polygon_data", VendorType.POLYGON)
        self.daily_price_polygon_dao = DailyPriceDAO()
        self.logger = get_logger(__name__)

    def get_vendor_config(self) -> Dict[str, Any]:
        """Get Polygon-specific configuration."""
        return {
            "api_base_url": "https://api.polygon.io",
            "rate_limit_requests_per_minute": 5,
            "supports_real_time": True,
            "supports_options": True,
            "supports_crypto": True,
            "supports_forex": True
        }

    def get_required_fields(self) -> List[str]:
        """Get required fields for Polygon data."""
        return ["symbol", "date"]

    def get_schema(self) -> Dict[str, Any]:
        """Get Polygon data table schema."""
        return {
            "id": "SERIAL PRIMARY KEY",
            "data_type": "VARCHAR(50) NOT NULL",  # 'daily_price_polygon', 'dividends', 'splits', etc.
            "symbol": "VARCHAR(10) NOT NULL",
            "date": "DATE NOT NULL",
            "data": "JSONB NOT NULL",  # Store all Polygon-specific data
            "created_at": "TIMESTAMP DEFAULT NOW()",
            "updated_at": "TIMESTAMP DEFAULT NOW()",
            "UNIQUE": "(data_type, symbol, date)"
        }

    def transform_vendor_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform Polygon data to standardized format."""
        transformed = super().transform_price_data(raw_data)

        # Polygon-specific transformations
        if "ticker" in transformed:
            transformed["symbol"] = transformed.pop("ticker")

        if "t" in transformed:  # Polygon uses 't' for timestamp in some APIs
            transformed["date"] = datetime.fromtimestamp(transformed["t"] / 1000).date()

        return transformed

    # Daily Prices Operations (replacing daily_price_polygon_dao.py)
    def insert_daily_price(
        self,
        symbol: str,
        date: Union[date, datetime],
        open_price: float,
        high: float,
        low: float,
        close: float,
        volume: int,
        market_cap: Optional[float] = None,
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
            market_cap: Optional market capitalization
            **kwargs: Additional Polygon-specific fields

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
            "adjusted_close": kwargs.get("adjusted_close", close),
            "market_cap": market_cap,
            "vendor": "polygon",
            "instrument_id": kwargs.get("instrument_id")
        }

        # Validate as price data
        if not self.validate_price_data(price_data):
            self.logger.error(f"Invalid price data for {symbol} on {date}")
            return None

        return self.daily_price_polygon_dao.create(price_data)

    def get_daily_price(
        self,
        symbol: str,
        date: Union[date, datetime]
    ) -> Optional[Dict[str, Any]]:
        """
        Get daily price for symbol and date.

        Args:
            symbol: Stock symbol
            date: Price date

        Returns:
            Price record or None
        """
        return self.daily_price_polygon_dao.get_price_by_symbol_date(symbol, date, vendor="polygon")

    def list_daily_price_polygon(
        self,
        symbol: str,
        start_date: Optional[Union[date, datetime]] = None,
        end_date: Optional[Union[date, datetime]] = None
    ) -> List[Dict[str, Any]]:
        """
        List daily prices for a symbol.

        Args:
            symbol: Stock symbol
            start_date: Optional start date
            end_date: Optional end date

        Returns:
            List of price records
        """
        if start_date and end_date:
            return self.daily_price_polygon_dao.get_price_history(symbol, start_date, end_date, vendor="polygon")
        else:
            # Get all prices for symbol
            query = f"""
                SELECT * FROM {self.daily_price_polygon_dao.table_name}
                WHERE symbol = :symbol AND vendor = 'polygon'
                ORDER BY date
            """
            return self.daily_price_polygon_dao.execute_query(query, {"symbol": symbol.upper()})

    def bulk_insert_daily_price_polygon(self, price_records: List[Dict[str, Any]]) -> int:
        """
        Bulk insert daily price records.

        Args:
            price_records: List of price records

        Returns:
            Number of records inserted
        """
        # Add vendor information to all records
        enhanced_records = []
        for record in price_records:
            enhanced = record.copy()
            enhanced["vendor"] = "polygon"

            # Validate each record
            if self.validate_price_data(enhanced):
                enhanced_records.append(enhanced)
            else:
                self.logger.warning(f"Skipping invalid price record: {record}")

        return self.daily_price_polygon_dao.bulk_insert(enhanced_records)

    # Dividend Operations (replacing dividend_polygon_dao.py)
    def insert_dividend(
        self,
        symbol: str,
        ex_date: Union[date, datetime],
        amount: float,
        payment_date: Optional[Union[date, datetime]] = None,
        **kwargs
    ) -> Optional[int]:
        """
        Insert dividend data.

        Args:
            symbol: Stock symbol
            ex_date: Ex-dividend date
            amount: Dividend amount
            payment_date: Optional payment date
            **kwargs: Additional Polygon-specific fields

        Returns:
            Created record ID
        """
        dividend_data = {
            "data_type": "dividend",
            "symbol": symbol,
            "date": ex_date if isinstance(ex_date, date) else ex_date.date(),
            "data": {
                "amount": amount,
                "payment_date": payment_date.isoformat() if payment_date else None,
                "ex_date": (ex_date if isinstance(ex_date, date) else ex_date.date()).isoformat(),
                **kwargs
            }
        }

        return self.create_with_vendor_metadata(dividend_data)

    def get_dividends(
        self,
        symbol: str,
        start_date: Optional[Union[date, datetime]] = None,
        end_date: Optional[Union[date, datetime]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get dividend records for a symbol.

        Args:
            symbol: Stock symbol
            start_date: Optional start date
            end_date: Optional end date

        Returns:
            List of dividend records
        """
        params = {
            "symbol": symbol.upper(),
            "data_type": "dividend"
        }

        query_str = f"""
            SELECT * FROM {self.table_name}
            WHERE symbol = :symbol AND data_type = :data_type
        """

        if start_date:
            query_str += " AND date >= :start_date"
            params["start_date"] = start_date if isinstance(start_date, date) else start_date.date()

        if end_date:
            query_str += " AND date <= :end_date"
            params["end_date"] = end_date if isinstance(end_date, date) else end_date.date()

        query_str += " ORDER BY date"

        return self.execute_query(query_str, params)

    # Stock Split Operations (replacing stock_splits_polygon_dao.py)
    def insert_stock_split(
        self,
        symbol: str,
        split_date: Union[date, datetime],
        split_ratio: float,
        **kwargs
    ) -> Optional[int]:
        """
        Insert stock split data.

        Args:
            symbol: Stock symbol
            split_date: Split effective date
            split_ratio: Split ratio (e.g., 2.0 for 2:1 split)
            **kwargs: Additional Polygon-specific fields

        Returns:
            Created record ID
        """
        split_data = {
            "data_type": "stock_split",
            "symbol": symbol,
            "date": split_date if isinstance(split_date, date) else split_date.date(),
            "data": {
                "split_ratio": split_ratio,
                "split_date": (split_date if isinstance(split_date, date) else split_date.date()).isoformat(),
                **kwargs
            }
        }

        return self.create_with_vendor_metadata(split_data)

    def get_stock_splits(
        self,
        symbol: str,
        start_date: Optional[Union[date, datetime]] = None,
        end_date: Optional[Union[date, datetime]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get stock split records for a symbol.

        Args:
            symbol: Stock symbol
            start_date: Optional start date
            end_date: Optional end date

        Returns:
            List of stock split records
        """
        params = {
            "symbol": symbol.upper(),
            "data_type": "stock_split"
        }

        query_str = f"""
            SELECT * FROM {self.table_name}
            WHERE symbol = :symbol AND data_type = :data_type
        """

        if start_date:
            query_str += " AND date >= :start_date"
            params["start_date"] = start_date if isinstance(start_date, date) else start_date.date()

        if end_date:
            query_str += " AND date <= :end_date"
            params["end_date"] = end_date if isinstance(end_date, date) else end_date.date()

        query_str += " ORDER BY date"

        return self.execute_query(query_str, params)

    # Instrument Operations (replacing instrument_polygon_dao.py functionality)
    def insert_instrument(
        self,
        symbol: str,
        name: str,
        market: str,
        **kwargs
    ) -> Optional[int]:
        """
        Insert instrument data.

        Args:
            symbol: Stock symbol
            name: Company name
            market: Market exchange
            **kwargs: Additional Polygon-specific fields

        Returns:
            Created record ID
        """
        instrument_data = {
            "data_type": "instrument",
            "symbol": symbol,
            "date": datetime.now().date(),
            "data": {
                "name": name,
                "market": market,
                "symbol": symbol,
                **kwargs
            }
        }

        return self.create_with_vendor_metadata(instrument_data)

    def get_instrument(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get instrument data for a symbol.

        Args:
            symbol: Stock symbol

        Returns:
            Instrument record or None
        """
        query = f"""
            SELECT * FROM {self.table_name}
            WHERE symbol = :symbol AND data_type = 'instrument'
            ORDER BY created_at DESC
            LIMIT 1
        """

        results = self.execute_query(query, {"symbol": symbol.upper()})
        return results[0] if results else None

    # Generic data operations
    def insert_data(
        self,
        data_type: str,
        symbol: str,
        date: Union[date, datetime],
        data: Dict[str, Any]
    ) -> Optional[int]:
        """
        Insert generic Polygon data.

        Args:
            data_type: Type of data (e.g., 'earnings', 'news', 'technicals')
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
        Get generic Polygon data.

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
        """Create Polygon data record."""
        query = text(f"""
            INSERT INTO {self.table_name} (data_type, symbol, date, data)
            VALUES (:data_type, :symbol, :date, :data)
            RETURNING id
        """)

        result = session.execute(query, data)
        return result.scalar()

    def _read_impl(self, session, record_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """Read Polygon data record."""
        query = text(f"SELECT * FROM {self.table_name} WHERE id = :id")
        result = session.execute(query, {"id": record_id})
        row = result.fetchone()
        return dict(row._mapping) if row else None

    def _update_impl(self, session, record_id: Union[int, str], data: Dict[str, Any]) -> bool:
        """Update Polygon data record."""
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
        """Delete Polygon data record."""
        query = text(f"DELETE FROM {self.table_name} WHERE id = :id")
        result = session.execute(query, {"id": record_id})
        return result.rowcount > 0

    def _list_all_impl(self, session, limit: Optional[int], offset: int) -> List[Dict[str, Any]]:
        """List all Polygon data records."""
        query_str = f"SELECT * FROM {self.table_name} ORDER BY date DESC, symbol"

        if limit:
            query_str += f" LIMIT {limit} OFFSET {offset}"

        query = text(query_str)
        result = session.execute(query)
        return [dict(row._mapping) for row in result]

    def _count_impl(self, session, where_clause: Optional[str], params: Optional[Dict[str, Any]]) -> int:
        """Count Polygon data records."""
        query_str = f"SELECT COUNT(*) FROM {self.table_name}"

        if where_clause:
            query_str += f" WHERE {where_clause}"

        query = text(query_str)
        result = session.execute(query, params or {})
        return result.scalar()

    def _bulk_insert_impl(self, session, records: List[Dict[str, Any]]) -> int:
        """Bulk insert Polygon data records."""
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