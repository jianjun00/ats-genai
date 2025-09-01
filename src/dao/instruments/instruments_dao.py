"""
Unified instruments DAO that consolidates vendor-specific instrument operations.

This module replaces separate instrument DAOs for different vendors with a 
unified interface supporting multi-vendor instrument data.
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from sqlalchemy import text

from dao.base.base_dao import BaseDAO
from core.logging.logger_config import get_logger


class InstrumentsDAO(BaseDAO):
    """
    Unified DAO for instrument data across all vendors.
    
    Provides standardized interface for instrument operations while supporting
    vendor-specific data through a unified table structure.
    """
    
    def __init__(self):
        super().__init__("instruments")
        self.logger = get_logger(__name__)
    
    def get_schema(self) -> Dict[str, Any]:
        """Get instruments table schema."""
        return {
            "id": "SERIAL PRIMARY KEY",
            "symbol": "VARCHAR(20) NOT NULL",
            "name": "VARCHAR(255) NOT NULL",
            "description": "TEXT",
            "exchange": "VARCHAR(50)",
            "country": "VARCHAR(10)",
            "currency": "VARCHAR(10)",
            "sector": "VARCHAR(100)",
            "industry": "VARCHAR(100)",
            "market_cap": "BIGINT",
            "shares_outstanding": "BIGINT",
            "instrument_type": "VARCHAR(50)",  # stock, etf, option, bond, etc.
            "asset_class": "VARCHAR(50)",      # equity, fixed_income, commodity, etc.
            "is_active": "BOOLEAN DEFAULT TRUE",
            "listing_date": "DATE",
            "delisting_date": "DATE",
            "vendor": "VARCHAR(20)",
            "vendor_instrument_id": "VARCHAR(100)",
            "created_at": "TIMESTAMP DEFAULT NOW()",
            "updated_at": "TIMESTAMP DEFAULT NOW()",
            "UNIQUE": "(symbol, exchange)",
            "INDEX": "symbol, vendor, is_active"
        }
    
    def _create_impl(self, session, data: Dict[str, Any]) -> Optional[int]:
        """Create instrument record."""
        query = text(f"""
            INSERT INTO {self.table_name} 
            (symbol, name, description, exchange, country, currency, sector, industry,
             market_cap, shares_outstanding, instrument_type, asset_class, is_active,
             listing_date, delisting_date, vendor, vendor_instrument_id)
            VALUES (:symbol, :name, :description, :exchange, :country, :currency, :sector, :industry,
                    :market_cap, :shares_outstanding, :instrument_type, :asset_class, :is_active,
                    :listing_date, :delisting_date, :vendor, :vendor_instrument_id)
            RETURNING id
        """)
        
        result = session.execute(query, data)
        return result.scalar()
    
    def _read_impl(self, session, record_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """Read instrument record by ID."""
        query = text(f"SELECT * FROM {self.table_name} WHERE id = :id")
        result = session.execute(query, {"id": record_id})
        row = result.fetchone()
        return dict(row._mapping) if row else None
    
    def _update_impl(self, session, record_id: Union[int, str], data: Dict[str, Any]) -> bool:
        """Update instrument record."""
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
        """Delete instrument record."""
        query = text(f"DELETE FROM {self.table_name} WHERE id = :id")
        result = session.execute(query, {"id": record_id})
        return result.rowcount > 0
    
    def _list_all_impl(self, session, limit: Optional[int], offset: int) -> List[Dict[str, Any]]:
        """List all instrument records."""
        query_str = f"SELECT * FROM {self.table_name} ORDER BY symbol"
        
        if limit:
            query_str += f" LIMIT {limit} OFFSET {offset}"
        
        query = text(query_str)
        result = session.execute(query)
        return [dict(row._mapping) for row in result]
    
    def _count_impl(self, session, where_clause: Optional[str], params: Optional[Dict[str, Any]]) -> int:
        """Count instrument records."""
        query_str = f"SELECT COUNT(*) FROM {self.table_name}"
        
        if where_clause:
            query_str += f" WHERE {where_clause}"
        
        query = text(query_str)
        result = session.execute(query, params or {})
        return result.scalar()
    
    def _bulk_insert_impl(self, session, records: List[Dict[str, Any]]) -> int:
        """Bulk insert instrument records."""
        if not records:
            return 0
        
        query = text(f"""
            INSERT INTO {self.table_name}
            (symbol, name, description, exchange, country, currency, sector, industry,
             market_cap, shares_outstanding, instrument_type, asset_class, is_active,
             listing_date, delisting_date, vendor, vendor_instrument_id)
            VALUES (:symbol, :name, :description, :exchange, :country, :currency, :sector, :industry,
                    :market_cap, :shares_outstanding, :instrument_type, :asset_class, :is_active,
                    :listing_date, :delisting_date, :vendor, :vendor_instrument_id)
            ON CONFLICT (symbol, exchange) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                country = EXCLUDED.country,
                currency = EXCLUDED.currency,
                sector = EXCLUDED.sector,
                industry = EXCLUDED.industry,
                market_cap = EXCLUDED.market_cap,
                shares_outstanding = EXCLUDED.shares_outstanding,
                instrument_type = EXCLUDED.instrument_type,
                asset_class = EXCLUDED.asset_class,
                is_active = EXCLUDED.is_active,
                listing_date = EXCLUDED.listing_date,
                delisting_date = EXCLUDED.delisting_date,
                updated_at = NOW()
        """)
        
        session.execute(query, records)
        return len(records)
    
    # Specialized instrument methods
    def get_by_symbol(
        self, 
        symbol: str,
        exchange: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get instrument by symbol.
        
        Args:
            symbol: Stock symbol
            exchange: Optional exchange filter
            
        Returns:
            Instrument record or None
        """
        params = {"symbol": symbol.upper()}
        
        query_str = f"""
            SELECT * FROM {self.table_name}
            WHERE symbol = :symbol AND is_active = true
        """
        
        if exchange:
            query_str += " AND exchange = :exchange"
            params["exchange"] = exchange
        
        query_str += " ORDER BY created_at DESC LIMIT 1"
        
        try:
            results = self.execute_query(query_str, params)
            return results[0] if results else None
        except Exception as e:
            self.logger.error(f"Failed to get instrument for {symbol}: {e}")
            return None
    
    def get_by_vendor_id(
        self,
        vendor: str,
        vendor_instrument_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get instrument by vendor-specific ID.
        
        Args:
            vendor: Vendor name
            vendor_instrument_id: Vendor-specific instrument ID
            
        Returns:
            Instrument record or None
        """
        query = f"""
            SELECT * FROM {self.table_name}
            WHERE vendor = :vendor AND vendor_instrument_id = :vendor_instrument_id
            AND is_active = true
            LIMIT 1
        """
        
        params = {
            "vendor": vendor,
            "vendor_instrument_id": vendor_instrument_id
        }
        
        try:
            results = self.execute_query(query, params)
            return results[0] if results else None
        except Exception as e:
            self.logger.error(f"Failed to get instrument by vendor ID {vendor_instrument_id}: {e}")
            return None
    
    def search_instruments(
        self,
        search_term: str,
        exchange: Optional[str] = None,
        sector: Optional[str] = None,
        instrument_type: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Search instruments by name or symbol.
        
        Args:
            search_term: Search term for name or symbol
            exchange: Optional exchange filter
            sector: Optional sector filter
            instrument_type: Optional instrument type filter
            limit: Maximum number of results
            
        Returns:
            List of matching instrument records
        """
        params = {
            "search_term": f"%{search_term.upper()}%",
            "limit": limit
        }
        
        conditions = [
            "is_active = true",
            "(UPPER(symbol) LIKE :search_term OR UPPER(name) LIKE :search_term)"
        ]
        
        if exchange:
            conditions.append("exchange = :exchange")
            params["exchange"] = exchange
        
        if sector:
            conditions.append("sector = :sector")
            params["sector"] = sector
        
        if instrument_type:
            conditions.append("instrument_type = :instrument_type")
            params["instrument_type"] = instrument_type
        
        query = f"""
            SELECT * FROM {self.table_name}
            WHERE {' AND '.join(conditions)}
            ORDER BY symbol
            LIMIT :limit
        """
        
        try:
            return self.execute_query(query, params)
        except Exception as e:
            self.logger.error(f"Failed to search instruments: {e}")
            return []
    
    def get_by_sector(
        self,
        sector: str,
        exchange: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get instruments by sector.
        
        Args:
            sector: Sector name
            exchange: Optional exchange filter
            limit: Maximum number of results
            
        Returns:
            List of instruments in the sector
        """
        params = {
            "sector": sector,
            "limit": limit
        }
        
        query_str = f"""
            SELECT * FROM {self.table_name}
            WHERE sector = :sector AND is_active = true
        """
        
        if exchange:
            query_str += " AND exchange = :exchange"
            params["exchange"] = exchange
        
        query_str += " ORDER BY market_cap DESC LIMIT :limit"
        
        try:
            return self.execute_query(query_str, params)
        except Exception as e:
            self.logger.error(f"Failed to get instruments for sector {sector}: {e}")
            return []
    
    def get_by_market_cap_range(
        self,
        min_market_cap: Optional[int] = None,
        max_market_cap: Optional[int] = None,
        exchange: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get instruments by market cap range.
        
        Args:
            min_market_cap: Minimum market cap
            max_market_cap: Maximum market cap
            exchange: Optional exchange filter
            limit: Maximum number of results
            
        Returns:
            List of instruments in the market cap range
        """
        params = {"limit": limit}
        conditions = ["is_active = true", "market_cap IS NOT NULL"]
        
        if min_market_cap is not None:
            conditions.append("market_cap >= :min_market_cap")
            params["min_market_cap"] = min_market_cap
        
        if max_market_cap is not None:
            conditions.append("market_cap <= :max_market_cap")
            params["max_market_cap"] = max_market_cap
        
        if exchange:
            conditions.append("exchange = :exchange")
            params["exchange"] = exchange
        
        query = f"""
            SELECT * FROM {self.table_name}
            WHERE {' AND '.join(conditions)}
            ORDER BY market_cap DESC
            LIMIT :limit
        """
        
        try:
            return self.execute_query(query, params)
        except Exception as e:
            self.logger.error(f"Failed to get instruments by market cap: {e}")
            return []
    
    def get_active_instruments(
        self,
        exchange: Optional[str] = None,
        instrument_type: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all active instruments.
        
        Args:
            exchange: Optional exchange filter
            instrument_type: Optional instrument type filter
            
        Returns:
            List of active instruments
        """
        params = {}
        conditions = ["is_active = true"]
        
        if exchange:
            conditions.append("exchange = :exchange")
            params["exchange"] = exchange
        
        if instrument_type:
            conditions.append("instrument_type = :instrument_type")
            params["instrument_type"] = instrument_type
        
        query = f"""
            SELECT * FROM {self.table_name}
            WHERE {' AND '.join(conditions)}
            ORDER BY symbol
        """
        
        try:
            return self.execute_query(query, params)
        except Exception as e:
            self.logger.error(f"Failed to get active instruments: {e}")
            return []
    
    def deactivate_instrument(self, symbol: str, exchange: Optional[str] = None) -> bool:
        """
        Deactivate an instrument (soft delete).
        
        Args:
            symbol: Stock symbol
            exchange: Optional exchange filter
            
        Returns:
            True if successfully deactivated
        """
        params = {
            "symbol": symbol.upper(),
            "delisting_date": datetime.now().date()
        }
        
        query_str = f"""
            UPDATE {self.table_name}
            SET is_active = false, delisting_date = :delisting_date, updated_at = NOW()
            WHERE symbol = :symbol
        """
        
        if exchange:
            query_str += " AND exchange = :exchange"
            params["exchange"] = exchange
        
        try:
            self.execute_query(query_str, params)
            return True
        except Exception as e:
            self.logger.error(f"Failed to deactivate instrument {symbol}: {e}")
            return False
    
    def get_instrument_stats(self) -> Dict[str, Any]:
        """
        Get instrument statistics.
        
        Returns:
            Statistics about instruments in the database
        """
        query = f"""
            SELECT 
                COUNT(*) as total_instruments,
                COUNT(*) FILTER (WHERE is_active = true) as active_instruments,
                COUNT(*) FILTER (WHERE is_active = false) as inactive_instruments,
                COUNT(DISTINCT exchange) as unique_exchanges,
                COUNT(DISTINCT sector) as unique_sectors,
                COUNT(DISTINCT instrument_type) as unique_types,
                AVG(market_cap) FILTER (WHERE market_cap IS NOT NULL) as avg_market_cap,
                MAX(market_cap) as max_market_cap,
                MIN(market_cap) FILTER (WHERE market_cap > 0) as min_market_cap
            FROM {self.table_name}
        """
        
        try:
            results = self.execute_query(query)
            return results[0] if results else {}
        except Exception as e:
            self.logger.error(f"Failed to get instrument stats: {e}")
            return {}