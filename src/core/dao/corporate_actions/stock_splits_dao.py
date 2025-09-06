"""
Unified stock splits DAO that consolidates vendor-specific split operations.

This module replaces separate stock split DAOs for different vendors with a 
unified interface supporting multi-vendor stock split data.
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime, date
from sqlalchemy import text

from core.dao.base.base_dao import BaseDAO
from core.logging.logger_config import get_logger


class StockSplitsDAO(BaseDAO):
    """
    Unified DAO for stock split data across all vendors.
    
    Provides standardized interface for stock split operations while supporting
    vendor-specific data through a unified table structure.
    """
    
    def __init__(self):
        super().__init__("stock_splits")
        self.logger = get_logger(__name__)
    
    def get_schema(self) -> Dict[str, Any]:
        """Get stock splits table schema."""
        return {
            "id": "SERIAL PRIMARY KEY",
            "symbol": "VARCHAR(10) NOT NULL",
            "execution_date": "DATE NOT NULL",
            "split_from": "DECIMAL(8,2) NOT NULL",  # e.g., 1 in 2:1 split
            "split_to": "DECIMAL(8,2) NOT NULL",    # e.g., 2 in 2:1 split  
            "split_ratio": "DECIMAL(12,6) NOT NULL", # calculated: split_to/split_from
            "declaration_date": "DATE",
            "payment_date": "DATE",
            "record_date": "DATE",
            "cash_amount": "DECIMAL(12,4)",  # cash in lieu of fractional shares
            "description": "TEXT",
            "vendor": "VARCHAR(20)",
            "vendor_ref_id": "VARCHAR(50)",
            "created_at": "TIMESTAMP DEFAULT NOW()",
            "updated_at": "TIMESTAMP DEFAULT NOW()",
            "UNIQUE": "(symbol, execution_date, vendor)"
        }
    
    def _create_impl(self, session, data: Dict[str, Any]) -> Optional[int]:
        """Create stock split record."""
        query = text(f"""
            INSERT INTO {self.table_name} 
            (symbol, execution_date, split_from, split_to, split_ratio, declaration_date, 
             payment_date, record_date, cash_amount, description, vendor, vendor_ref_id)
            VALUES (:symbol, :execution_date, :split_from, :split_to, :split_ratio, :declaration_date,
                    :payment_date, :record_date, :cash_amount, :description, :vendor, :vendor_ref_id)
            RETURNING id
        """)
        
        result = session.execute(query, data)
        return result.scalar()
    
    def _read_impl(self, session, record_id: Union[int, str]) -> Optional[Dict[str, Any]]:
        """Read stock split record by ID."""
        query = text(f"SELECT * FROM {self.table_name} WHERE id = :id")
        result = session.execute(query, {"id": record_id})
        row = result.fetchone()
        return dict(row._mapping) if row else None
    
    def _update_impl(self, session, record_id: Union[int, str], data: Dict[str, Any]) -> bool:
        """Update stock split record."""
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
        """Delete stock split record."""
        query = text(f"DELETE FROM {self.table_name} WHERE id = :id")
        result = session.execute(query, {"id": record_id})
        return result.rowcount > 0
    
    def _list_all_impl(self, session, limit: Optional[int], offset: int) -> List[Dict[str, Any]]:
        """List all stock split records."""
        query_str = f"SELECT * FROM {self.table_name} ORDER BY execution_date DESC, symbol"
        
        if limit:
            query_str += f" LIMIT {limit} OFFSET {offset}"
        
        query = text(query_str)
        result = session.execute(query)
        return [dict(row._mapping) for row in result]
    
    def _count_impl(self, session, where_clause: Optional[str], params: Optional[Dict[str, Any]]) -> int:
        """Count stock split records."""
        query_str = f"SELECT COUNT(*) FROM {self.table_name}"
        
        if where_clause:
            query_str += f" WHERE {where_clause}"
        
        query = text(query_str)
        result = session.execute(query, params or {})
        return result.scalar()
    
    def _bulk_insert_impl(self, session, records: List[Dict[str, Any]]) -> int:
        """Bulk insert stock split records."""
        if not records:
            return 0
        
        query = text(f"""
            INSERT INTO {self.table_name}
            (symbol, execution_date, split_from, split_to, split_ratio, declaration_date,
             payment_date, record_date, cash_amount, description, vendor, vendor_ref_id)
            VALUES (:symbol, :execution_date, :split_from, :split_to, :split_ratio, :declaration_date,
                    :payment_date, :record_date, :cash_amount, :description, :vendor, :vendor_ref_id)
            ON CONFLICT (symbol, execution_date, vendor) DO UPDATE SET
                split_from = EXCLUDED.split_from,
                split_to = EXCLUDED.split_to,
                split_ratio = EXCLUDED.split_ratio,
                declaration_date = EXCLUDED.declaration_date,
                payment_date = EXCLUDED.payment_date,
                record_date = EXCLUDED.record_date,
                cash_amount = EXCLUDED.cash_amount,
                description = EXCLUDED.description,
                updated_at = NOW()
        """)
        
        session.execute(query, records)
        return len(records)
    
    # Specialized stock split methods
    def get_splits_by_symbol(
        self, 
        symbol: str,
        vendor: Optional[str] = None,
        start_date: Optional[Union[date, datetime]] = None,
        end_date: Optional[Union[date, datetime]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get stock split records for a symbol.
        
        Args:
            symbol: Stock symbol
            vendor: Optional vendor filter
            start_date: Optional start date
            end_date: Optional end date
            
        Returns:
            List of stock split records
        """
        params = {"symbol": symbol.upper()}
        conditions = ["symbol = :symbol"]
        
        if vendor:
            conditions.append("vendor = :vendor")
            params["vendor"] = vendor
        
        if start_date:
            conditions.append("execution_date >= :start_date")
            params["start_date"] = start_date if isinstance(start_date, date) else start_date.date()
        
        if end_date:
            conditions.append("execution_date <= :end_date")
            params["end_date"] = end_date if isinstance(end_date, date) else end_date.date()
        
        query = f"""
            SELECT * FROM {self.table_name}
            WHERE {' AND '.join(conditions)}
            ORDER BY execution_date DESC
        """
        
        try:
            return self.execute_query(query, params)
        except Exception as e:
            self.logger.error(f"Failed to get splits for {symbol}: {e}")
            return []
    
    def get_splits_for_date(
        self,
        date: Union[date, datetime],
        symbols: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get all stock splits for a specific date.
        
        Args:
            date: Execution date
            symbols: Optional symbols filter
            
        Returns:
            List of stock split records for the date
        """
        params = {
            "date": date if isinstance(date, date) else date.date()
        }
        
        query_str = f"""
            SELECT * FROM {self.table_name}
            WHERE execution_date = :date
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
            self.logger.error(f"Failed to get splits for date {date}: {e}")
            return []
    
    def get_upcoming_splits(
        self,
        days_ahead: int = 30,
        symbols: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Get upcoming stock splits within specified days.
        
        Args:
            days_ahead: Number of days to look ahead
            symbols: Optional symbols filter
            
        Returns:
            List of upcoming stock split records
        """
        params = {
            "start_date": datetime.now().date(),
            "end_date": (datetime.now() + datetime.timedelta(days=days_ahead)).date()
        }
        
        query_str = f"""
            SELECT * FROM {self.table_name}
            WHERE execution_date BETWEEN :start_date AND :end_date
        """
        
        if symbols:
            placeholders = ",".join([f":symbol_{i}" for i in range(len(symbols))])
            query_str += f" AND symbol IN ({placeholders})"
            for i, symbol in enumerate(symbols):
                params[f"symbol_{i}"] = symbol.upper()
        
        query_str += " ORDER BY execution_date, symbol"
        
        try:
            return self.execute_query(query_str, params)
        except Exception as e:
            self.logger.error(f"Failed to get upcoming splits: {e}")
            return []
    
    def get_split_history_summary(
        self,
        symbol: str,
        years: int = 10
    ) -> Dict[str, Any]:
        """
        Get stock split history summary for a symbol.
        
        Args:
            symbol: Stock symbol
            years: Number of years to look back
            
        Returns:
            Split summary statistics
        """
        params = {
            "symbol": symbol.upper(),
            "start_date": (datetime.now() - datetime.timedelta(days=years*365)).date()
        }
        
        query = f"""
            SELECT 
                COUNT(*) as total_splits,
                AVG(split_ratio) as avg_split_ratio,
                MIN(split_ratio) as min_split_ratio,
                MAX(split_ratio) as max_split_ratio,
                MIN(execution_date) as earliest_split,
                MAX(execution_date) as latest_split,
                SUM(CASE WHEN split_ratio > 1 THEN 1 ELSE 0 END) as stock_splits,
                SUM(CASE WHEN split_ratio < 1 THEN 1 ELSE 0 END) as reverse_splits
            FROM {self.table_name}
            WHERE symbol = :symbol AND execution_date >= :start_date
        """
        
        try:
            results = self.execute_query(query, params)
            return results[0] if results else {}
        except Exception as e:
            self.logger.error(f"Failed to get split summary for {symbol}: {e}")
            return {}
    
    def get_large_splits(
        self,
        min_ratio: float = 2.0,
        symbols: Optional[List[str]] = None,
        years: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Get large stock splits above a ratio threshold.
        
        Args:
            min_ratio: Minimum split ratio (e.g., 2.0 for 2:1 splits)
            symbols: Optional symbols filter
            years: Number of years to look back
            
        Returns:
            List of large split records
        """
        params = {
            "min_ratio": min_ratio,
            "start_date": (datetime.now() - datetime.timedelta(days=years*365)).date()
        }
        
        query_str = f"""
            SELECT * FROM {self.table_name}
            WHERE split_ratio >= :min_ratio AND execution_date >= :start_date
        """
        
        if symbols:
            placeholders = ",".join([f":symbol_{i}" for i in range(len(symbols))])
            query_str += f" AND symbol IN ({placeholders})"
            for i, symbol in enumerate(symbols):
                params[f"symbol_{i}"] = symbol.upper()
        
        query_str += " ORDER BY split_ratio DESC, execution_date DESC"
        
        try:
            return self.execute_query(query_str, params)
        except Exception as e:
            self.logger.error(f"Failed to get large splits: {e}")
            return []
    
    def get_reverse_splits(
        self,
        symbols: Optional[List[str]] = None,
        years: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Get reverse stock splits (ratio < 1).
        
        Args:
            symbols: Optional symbols filter
            years: Number of years to look back
            
        Returns:
            List of reverse split records
        """
        params = {
            "start_date": (datetime.now() - datetime.timedelta(days=years*365)).date()
        }
        
        query_str = f"""
            SELECT * FROM {self.table_name}
            WHERE split_ratio < 1 AND execution_date >= :start_date
        """
        
        if symbols:
            placeholders = ",".join([f":symbol_{i}" for i in range(len(symbols))])
            query_str += f" AND symbol IN ({placeholders})"
            for i, symbol in enumerate(symbols):
                params[f"symbol_{i}"] = symbol.upper()
        
        query_str += " ORDER BY execution_date DESC, symbol"
        
        try:
            return self.execute_query(query_str, params)
        except Exception as e:
            self.logger.error(f"Failed to get reverse splits: {e}")
            return []
    
    def calculate_price_adjustment_factor(
        self,
        symbol: str,
        target_date: Union[date, datetime]
    ) -> float:
        """
        Calculate cumulative price adjustment factor for splits after a date.
        
        Args:
            symbol: Stock symbol
            target_date: Date to calculate adjustments from
            
        Returns:
            Cumulative adjustment factor
        """
        params = {
            "symbol": symbol.upper(),
            "target_date": target_date if isinstance(target_date, date) else target_date.date()
        }
        
        query = f"""
            SELECT split_ratio FROM {self.table_name}
            WHERE symbol = :symbol AND execution_date > :target_date
            ORDER BY execution_date
        """
        
        try:
            results = self.execute_query(query, params)
            
            # Calculate cumulative adjustment factor
            adjustment_factor = 1.0
            for result in results:
                adjustment_factor *= float(result["split_ratio"])
            
            return adjustment_factor
        except Exception as e:
            self.logger.error(f"Failed to calculate adjustment factor for {symbol}: {e}")
            return 1.0