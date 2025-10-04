"""
Unified Fundamentals DAO - Single implementation for all vendor fundamentals operations.

Eliminates duplicate logic across Tiingo, Polygon, and FMP fundamentals DAOs by
providing a configurable base class that handles vendor differences through
configuration-driven approach.
"""

from typing import Dict, List, Optional
from datetime import date, datetime
from dataclasses import dataclass
import asyncpg
import logging
from core.platform.config_env.environment import Environment


@dataclass
class UnifiedFundamental:
    """Unified fundamental data container for all vendors."""
    symbol: str
    date: date
    vendor: str
    fiscal_period: Optional[str] = None
    revenue: Optional[int] = None
    gross_profit: Optional[int] = None
    operating_income: Optional[int] = None
    net_income: Optional[int] = None
    ebitda: Optional[int] = None
    eps: Optional[float] = None
    total_assets: Optional[int] = None
    total_liabilities: Optional[int] = None
    shareholders_equity: Optional[int] = None
    current_assets: Optional[int] = None
    current_liabilities: Optional[int] = None
    total_debt: Optional[int] = None
    cash_and_equivalents: Optional[int] = None
    operating_cash_flow: Optional[int] = None
    investing_cash_flow: Optional[int] = None
    financing_cash_flow: Optional[int] = None
    free_cash_flow: Optional[int] = None
    market_cap: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class UnifiedFundamentalsDAO:
    """
    Unified DAO for all vendor fundamentals operations.
    
    Replaces:
    - TiingoFundamentalsDAO
    - PolygonFundamentalsDAO  
    - FmpFundamentalsDAO
    """
    
    VENDOR_CONFIGS = {
        'tiingo': {
            'table_key': 'fundamentals_tiingo',
            'vendor_name': 'tiingo'
        },
        'polygon': {
            'table_key': 'fundamentals_polygon', 
            'vendor_name': 'polygon'
        },
        'fmp': {
            'table_key': 'fundamentals_fmp',
            'vendor_name': 'fmp'
        }
    }

    def __init__(self, env: Environment, vendor: str):
        self.env = env
        self.vendor = vendor.lower()
        
        if self.vendor not in self.VENDOR_CONFIGS:
            raise ValueError(f"Unsupported vendor: {vendor}. Supported: {list(self.VENDOR_CONFIGS.keys())}")
            
        self.config = self.VENDOR_CONFIGS[self.vendor]
        self.table_name = self.env.get_table_name(self.config['table_key'])
        self.db_url = self.env.get_database_url()
        self.logger = logging.getLogger(__name__)

    async def insert_fundamental(self, fundamental: UnifiedFundamental) -> bool:
        """Insert or update fundamental data."""
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO fundamentals (
                        symbol, date, vendor, fiscal_period, revenue, gross_profit,
                        operating_income, net_income, ebitda, eps, total_assets,
                        total_liabilities, shareholders_equity, current_assets,
                        current_liabilities, total_debt, cash_and_equivalents,
                        operating_cash_flow, investing_cash_flow, financing_cash_flow,
                        free_cash_flow, market_cap
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                        $15, $16, $17, $18, $19, $20, $21, $22
                    )
                    ON CONFLICT (symbol, date, vendor, fiscal_period) DO UPDATE SET
                        revenue=EXCLUDED.revenue,
                        gross_profit=EXCLUDED.gross_profit,
                        operating_income=EXCLUDED.operating_income,
                        net_income=EXCLUDED.net_income,
                        ebitda=EXCLUDED.ebitda,
                        eps=EXCLUDED.eps,
                        total_assets=EXCLUDED.total_assets,
                        total_liabilities=EXCLUDED.total_liabilities,
                        shareholders_equity=EXCLUDED.shareholders_equity,
                        current_assets=EXCLUDED.current_assets,
                        current_liabilities=EXCLUDED.current_liabilities,
                        total_debt=EXCLUDED.total_debt,
                        cash_and_equivalents=EXCLUDED.cash_and_equivalents,
                        operating_cash_flow=EXCLUDED.operating_cash_flow,
                        investing_cash_flow=EXCLUDED.investing_cash_flow,
                        financing_cash_flow=EXCLUDED.financing_cash_flow,
                        free_cash_flow=EXCLUDED.free_cash_flow,
                        market_cap=EXCLUDED.market_cap,
                        updated_at=NOW()
                """, 
                fundamental.symbol, fundamental.date, self.config['vendor_name'],
                fundamental.fiscal_period, fundamental.revenue, fundamental.gross_profit,
                fundamental.operating_income, fundamental.net_income, fundamental.ebitda,
                fundamental.eps, fundamental.total_assets, fundamental.total_liabilities,
                fundamental.shareholders_equity, fundamental.current_assets,
                fundamental.current_liabilities, fundamental.total_debt,
                fundamental.cash_and_equivalents, fundamental.operating_cash_flow,
                fundamental.investing_cash_flow, fundamental.financing_cash_flow,
                fundamental.free_cash_flow, fundamental.market_cap)
                return True
        except Exception as e:
            self.logger.error(f"Error inserting fundamental for {fundamental.symbol}: {e}")
            return False
        finally:
            await pool.close()

    async def get_fundamentals(self, symbol: str, start_date: Optional[date] = None, 
                             end_date: Optional[date] = None) -> List[UnifiedFundamental]:
        """Get fundamentals for a symbol within date range."""
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                query = "SELECT * FROM fundamentals WHERE symbol = $1 AND vendor = $2"
                params = [symbol, self.config['vendor_name']]
                
                if start_date:
                    query += " AND date >= $3"
                    params.append(start_date)
                    
                if end_date:
                    query += f" AND date <= ${len(params) + 1}"
                    params.append(end_date)
                    
                query += " ORDER BY date DESC"
                
                rows = await conn.fetch(query, *params)
                return [self._row_to_fundamental(row) for row in rows]
        finally:
            await pool.close()

    async def get_latest_fundamental(self, symbol: str) -> Optional[UnifiedFundamental]:
        """Get most recent fundamental data for a symbol."""
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT * FROM fundamentals 
                    WHERE symbol = $1 AND vendor = $2
                    ORDER BY date DESC LIMIT 1
                """, symbol, self.config['vendor_name'])
                
                return self._row_to_fundamental(row) if row else None
        finally:
            await pool.close()

    async def bulk_insert_fundamentals(self, fundamentals: List[UnifiedFundamental]) -> int:
        """Bulk insert fundamentals and return count of successful insertions."""
        success_count = 0
        for fundamental in fundamentals:
            if await self.insert_fundamental(fundamental):
                success_count += 1
        return success_count

    async def get_symbols_with_fundamentals(self) -> List[str]:
        """Get all symbols that have fundamental data for this vendor."""
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT DISTINCT symbol FROM fundamentals 
                    WHERE vendor = $1 ORDER BY symbol
                """, self.config['vendor_name'])
                return [row['symbol'] for row in rows]
        finally:
            await pool.close()

    async def delete_fundamentals(self, symbol: str, date_range: Optional[tuple] = None) -> int:
        """Delete fundamentals for symbol, optionally within date range."""
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                if date_range:
                    result = await conn.execute("""
                        DELETE FROM fundamentals 
                        WHERE symbol = $1 AND vendor = $2 AND date BETWEEN $3 AND $4
                    """, symbol, self.config['vendor_name'], date_range[0], date_range[1])
                else:
                    result = await conn.execute("""
                        DELETE FROM fundamentals 
                        WHERE symbol = $1 AND vendor = $2
                    """, symbol, self.config['vendor_name'])
                return int(result.split()[-1])
        finally:
            await pool.close()

    def _row_to_fundamental(self, row) -> UnifiedFundamental:
        """Convert database row to UnifiedFundamental object."""
        if not row:
            return None
            
        return UnifiedFundamental(
            symbol=row['symbol'],
            date=row['date'],
            vendor=row['vendor'],
            fiscal_period=row.get('fiscal_period'),
            revenue=row.get('revenue'),
            gross_profit=row.get('gross_profit'),
            operating_income=row.get('operating_income'),
            net_income=row.get('net_income'),
            ebitda=row.get('ebitda'),
            eps=row.get('eps'),
            total_assets=row.get('total_assets'),
            total_liabilities=row.get('total_liabilities'),
            shareholders_equity=row.get('shareholders_equity'),
            current_assets=row.get('current_assets'),
            current_liabilities=row.get('current_liabilities'),
            total_debt=row.get('total_debt'),
            cash_and_equivalents=row.get('cash_and_equivalents'),
            operating_cash_flow=row.get('operating_cash_flow'),
            investing_cash_flow=row.get('investing_cash_flow'),
            financing_cash_flow=row.get('financing_cash_flow'),
            free_cash_flow=row.get('free_cash_flow'),
            market_cap=row.get('market_cap'),
            created_at=row.get('created_at'),
            updated_at=row.get('updated_at')
        )


# Factory functions for backward compatibility
def create_tiingo_fundamentals_dao(env: Environment):
    """Factory for Tiingo fundamentals DAO."""
    return UnifiedFundamentalsDAO(env, 'tiingo')

def create_polygon_fundamentals_dao(env: Environment):
    """Factory for Polygon fundamentals DAO."""
    return UnifiedFundamentalsDAO(env, 'polygon')

def create_fmp_fundamentals_dao(env: Environment):
    """Factory for FMP fundamentals DAO."""
    return UnifiedFundamentalsDAO(env, 'fmp')