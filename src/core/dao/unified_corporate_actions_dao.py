"""
Unified Corporate Actions DAO - Single implementation for dividends and stock splits.

Eliminates duplicate logic across vendor-specific dividend and stock splits DAOs.
"""

from core.platform.config_env.environment import Environment
import asyncpg
import logging
from typing import Dict, Optional, Any
from dataclasses import dataclass


@dataclass
class CorporateActionSchema:
    """Configuration for vendor-specific corporate action schemas."""
    dividend_table_key: str
    splits_table_key: str
    vendor_name: str


class UnifiedCorporateActionsDAO:
    """
    Unified DAO for dividend and stock split operations across all vendors.
    
    Replaces:
    - DividendTiingoDAO + StockSplitsTiingoDAO  
    - DividendPolygonDAO + StockSplitsPolygonDAO
    """
    
    SCHEMAS = {
        'tiingo': CorporateActionSchema(
            dividend_table_key='dividend_tiingo',
            splits_table_key='stock_splits_tiingo', 
            vendor_name='tiingo'
        ),
        'polygon': CorporateActionSchema(
            dividend_table_key='dividend_polygon',
            splits_table_key='stock_splits_polygon',
            vendor_name='polygon'
        )
    }

    def __init__(self, env: Environment, vendor: str):
        self.env = env
        self.vendor = vendor.lower()
        
        if self.vendor not in self.SCHEMAS:
            raise ValueError(f"Unsupported vendor: {vendor}. Supported: {list(self.SCHEMAS.keys())}")
            
        self.schema = self.SCHEMAS[self.vendor]
        self.dividend_table = self.env.get_table_name(self.schema.dividend_table_key)
        self.splits_table = self.env.get_table_name(self.schema.splits_table_key)
        self.db_url = self.env.get_database_url()
        self.logger = logging.getLogger(__name__)

    async def insert_dividend(self, dividend):
        """Insert dividend data with vendor-specific table."""
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.dividend_table} (
                        symbol, ex_dividend_date, cash_amount, declaration_date, 
                        payment_date, record_date, description, refid
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (symbol, ex_dividend_date) DO UPDATE SET
                        cash_amount=EXCLUDED.cash_amount,
                        declaration_date=EXCLUDED.declaration_date,
                        payment_date=EXCLUDED.payment_date,
                        record_date=EXCLUDED.record_date,
                        description=EXCLUDED.description,
                        refid=EXCLUDED.refid
                """, 
                dividend.symbol, dividend.ex_dividend_date, dividend.cash_amount,
                dividend.declaration_date, dividend.payment_date, dividend.record_date,
                dividend.description, dividend.refid)
        finally:
            await pool.close()

    async def insert_split(self, split):
        """Insert stock split data with vendor-specific table."""
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.splits_table} (
                        symbol, execution_date, ratio, description, refid
                    ) VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (symbol, execution_date) DO UPDATE SET
                        ratio=EXCLUDED.ratio,
                        description=EXCLUDED.description,
                        refid=EXCLUDED.refid
                """,
                split.symbol, split.execution_date, split.ratio, split.description, split.refid)
        finally:
            await pool.close()

    async def get_dividends(self, symbol: str, start_date=None, end_date=None):
        """Get dividends for symbol within date range."""
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                query = f"SELECT * FROM {self.dividend_table} WHERE symbol = $1"
                params = [symbol]
                
                if start_date:
                    query += " AND ex_dividend_date >= $2"
                    params.append(start_date)
                    
                if end_date:
                    query += f" AND ex_dividend_date <= ${len(params) + 1}"
                    params.append(end_date)
                    
                query += " ORDER BY ex_dividend_date DESC"
                return await conn.fetch(query, *params)
        finally:
            await pool.close()

    async def get_splits(self, symbol: str, start_date=None, end_date=None):
        """Get stock splits for symbol within date range."""
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                query = f"SELECT * FROM {self.splits_table} WHERE symbol = $1"
                params = [symbol]
                
                if start_date:
                    query += " AND execution_date >= $2"
                    params.append(start_date)
                    
                if end_date:
                    query += f" AND execution_date <= ${len(params) + 1}"
                    params.append(end_date)
                    
                query += " ORDER BY execution_date DESC"
                return await conn.fetch(query, *params)
        finally:
            await pool.close()


# Factory functions for backward compatibility
def create_tiingo_corporate_actions_dao(env: Environment):
    """Factory for Tiingo corporate actions DAO."""
    return UnifiedCorporateActionsDAO(env, 'tiingo')

def create_polygon_corporate_actions_dao(env: Environment):
    """Factory for Polygon corporate actions DAO."""
    return UnifiedCorporateActionsDAO(env, 'polygon')