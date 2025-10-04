"""
Unified Daily Prices DAO - Single implementation for all vendor daily price operations.

Eliminates duplicate logic across Tiingo, Polygon, FMP, and AlphaVantage DAOs by
providing a configurable base class that handles all vendor-specific differences
through column mapping configuration.
"""

from core.platform.config_env.environment import Environment
import asyncpg
import logging
from typing import Dict, Optional, Any
from dataclasses import dataclass


@dataclass
class DailyPriceSchema:
    """Configuration for vendor-specific daily price schemas."""
    table_key: str
    columns: Dict[str, str]  # Maps standard names to vendor column names
    additional_columns: Dict[str, str] = None  # Additional vendor-specific columns


class UnifiedDailyPricesDAO:
    """
    Unified DAO for all vendor daily price operations.
    
    Replaces:
    - DailyPricesTiingoDAO
    - DailyPricesPolygonDAO  
    - DailyPricesFmpDAO
    - DailyPricesAlphaVantageDAO
    """
    
    # Vendor schema configurations
    SCHEMAS = {
        'tiingo': DailyPriceSchema(
            table_key='daily_price_tiingo',
            columns={
                'date': 'date',
                'instrument_id': 'instrument_id', 
                'open': 'open',
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'volume': 'volume'
            },
            additional_columns={'adj_close': 'adjClose', 'status_id': 'status_id'}
        ),
        'polygon': DailyPriceSchema(
            table_key='daily_price_polygon',
            columns={
                'date': 'date',
                'instrument_id': 'instrument_id',
                'open': 'open', 
                'high': 'high',
                'low': 'low',
                'close': 'close',
                'volume': 'volume'
            },
            additional_columns={'market_cap': 'market_cap'}
        ),
        'fmp': DailyPriceSchema(
            table_key='daily_price_polygon_fmp',
            columns={
                'date': 'date',
                'instrument_id': 'instrument_id',
                'open': 'open_price',
                'high': 'high_price', 
                'low': 'low_price',
                'close': 'close',
                'volume': 'volume'
            },
            additional_columns={'adj_close': 'adj_close'}
        ),
        'alphavantage': DailyPriceSchema(
            table_key='daily_price_alphavantage',
            columns={
                'date': 'date',
                'instrument_id': 'instrument_id',
                'open': 'open',
                'high': 'high',
                'low': 'low', 
                'close': 'close',
                'volume': 'volume'
            }
        )
    }

    def __init__(self, env: Environment, vendor: str):
        self.env = env
        self.vendor = vendor.lower()
        
        if self.vendor not in self.SCHEMAS:
            raise ValueError(f"Unsupported vendor: {vendor}. Supported: {list(self.SCHEMAS.keys())}")
            
        self.schema = self.SCHEMAS[self.vendor]
        self.table_name = self.env.get_table_name(self.schema.table_key)
        self.db_url = self.env.get_database_url()
        self.logger = logging.getLogger(__name__)

    async def insert_price(self, date, instrument_id, open_price, high_price, low_price, 
                          close_price, volume, **additional_fields):
        """
        Insert daily price with vendor-specific column mapping.
        
        Args:
            Standard OHLCV fields plus vendor-specific additional_fields
        """
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                # Build column names and values based on vendor schema
                columns = []
                values = []
                placeholders = []
                param_count = 1
                
                # Standard columns
                standard_data = {
                    'date': date,
                    'instrument_id': instrument_id,
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price,
                    'volume': volume
                }
                
                for std_name, vendor_col in self.schema.columns.items():
                    columns.append(vendor_col)
                    values.append(standard_data[std_name])
                    placeholders.append(f"${param_count}")
                    param_count += 1
                
                # Additional vendor-specific columns
                if self.schema.additional_columns:
                    for std_name, vendor_col in self.schema.additional_columns.items():
                        if std_name in additional_fields:
                            columns.append(vendor_col)
                            values.append(additional_fields[std_name])
                            placeholders.append(f"${param_count}")
                            param_count += 1
                
                # Build upsert query
                columns_str = ', '.join(columns)
                placeholders_str = ', '.join(placeholders)
                
                # Build conflict resolution
                updates = []
                for col in columns:
                    if col not in ['date', 'instrument_id']:  # Don't update key columns
                        updates.append(f"{col}=EXCLUDED.{col}")
                updates_str = ', '.join(updates)
                
                query = f"""
                    INSERT INTO {self.table_name} ({columns_str})
                    VALUES ({placeholders_str})
                    ON CONFLICT (date, instrument_id) DO UPDATE SET
                        {updates_str}
                """
                
                await conn.execute(query, *values)
                
        finally:
            await pool.close()

    async def get_price(self, date, instrument_id):
        """Get daily price for specific date and instrument."""
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                return await conn.fetchrow(
                    f"SELECT * FROM {self.table_name} WHERE date = $1 AND instrument_id = $2",
                    date, instrument_id
                )
        finally:
            await pool.close()

    async def list_prices_for_date(self, as_of_date):
        """List all prices for a specific date."""
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(
                    f"SELECT * FROM {self.table_name} WHERE date = $1",
                    as_of_date
                )
        finally:
            await pool.close()

    async def list_prices_for_instruments_and_date(self, instrument_ids, as_of_date):
        """List prices for multiple instruments on specific date."""
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(
                    f"SELECT * FROM {self.table_name} WHERE date = $1 AND instrument_id = ANY($2)",
                    as_of_date, instrument_ids
                )
        finally:
            await pool.close()

    async def list_prices(self, instrument_id):
        """List all prices for an instrument."""
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=1) 
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(
                    f"SELECT * FROM {self.table_name} WHERE instrument_id = $1",
                    instrument_id
                )
        finally:
            await pool.close()


# Factory functions for backward compatibility
def create_tiingo_dao(env: Environment):
    """Factory for Tiingo daily prices DAO."""
    return UnifiedDailyPricesDAO(env, 'tiingo')

def create_polygon_dao(env: Environment):
    """Factory for Polygon daily prices DAO."""
    return UnifiedDailyPricesDAO(env, 'polygon')

def create_fmp_dao(env: Environment):
    """Factory for FMP daily prices DAO."""
    return UnifiedDailyPricesDAO(env, 'fmp')

def create_alphavantage_dao(env: Environment):
    """Factory for AlphaVantage daily prices DAO."""
    return UnifiedDailyPricesDAO(env, 'alphavantage')