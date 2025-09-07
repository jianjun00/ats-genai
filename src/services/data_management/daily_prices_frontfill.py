#!/usr/bin/env python3
"""
Daily Prices Frontfill Job.
Continuously updates daily price data from Polygon and Tiingo APIs.
"""

import asyncio
import aiohttp
import logging
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Tuple
import pandas as pd

from frontfill.base_frontfill_job import BaseFrontfillJob, FrontfillConfig, CheckpointType
from core.platform.config.environment import Environment
import asyncpg

logger = logging.getLogger(__name__)


class DailyPricesFrontfillJob(BaseFrontfillJob):
    """Frontfill job for daily prices data."""
    
    def __init__(self, config: FrontfillConfig, connection_pool: asyncpg.Pool, 
                 env: Environment, api_key: str):
        super().__init__(config, connection_pool, env)
        self.api_key = api_key
        
        # Vendor-specific configurations
        if config.vendor.lower() == "polygon":
            self.base_url = "https://api.polygon.io/v2/aggs/ticker"
            self.table_name = env.get_table_name("daily_prices_polygon")
        elif config.vendor.lower() == "tiingo":
            self.base_url = "https://api.tiingo.com/tiingo/daily"
            self.table_name = env.get_table_name("daily_prices_tiingo")
        else:
            raise ValueError(f"Unsupported vendor: {config.vendor}")
    
    async def get_default_starting_checkpoint(self) -> str:
        """Get default starting checkpoint - yesterday's date."""
        yesterday = date.today() - timedelta(days=1)
        return yesterday.strftime("%Y-%m-%d")
    
    async def fetch_data_batch(self, checkpoint: str, batch_size: int) -> Tuple[List[Dict[str, Any]], str]:
        """Fetch daily prices for a batch of instruments."""
        try:
            # Parse checkpoint as date
            checkpoint_date = datetime.strptime(checkpoint, "%Y-%m-%d").date()
            
            # Get active instruments for this batch
            instruments = await self._get_active_instruments(batch_size)
            
            if not instruments:
                return [], checkpoint
            
            batch_data = []
            
            # Fetch prices for each instrument
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                for instrument in instruments:
                    symbol = instrument["symbol"]
                    instrument_id = instrument["id"]
                    
                    try:
                        price_data = await self._fetch_instrument_prices(
                            session, symbol, checkpoint_date
                        )
                        
                        # Add instrument_id to each price record
                        for price in price_data:
                            price["instrument_id"] = instrument_id
                            price["symbol"] = symbol
                            batch_data.append(price)
                        
                    except Exception as e:
                        logger.warning(f"Failed to fetch prices for {symbol}: {e}")
                        self.stats["error_count"] += 1
                    
                    # Rate limiting
                    await asyncio.sleep(self.config.rate_limit_delay)
            
            # Next checkpoint is next business day
            next_checkpoint = self._get_next_business_day(checkpoint_date).strftime("%Y-%m-%d")
            
            return batch_data, next_checkpoint
            
        except Exception as e:
            logger.error(f"Error fetching data batch: {e}")
            raise
    
    async def _get_active_instruments(self, limit: int) -> List[Dict[str, Any]]:
        """Get a batch of active instruments."""
        instruments_table = self.env.get_table_name("instruments")
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT id, symbol, name
                FROM {instruments_table}
                WHERE is_active = true 
                AND symbol IS NOT NULL
                AND symbol ~ '^[A-Z]+$'
                ORDER BY symbol
                LIMIT $1
            """, limit)
            
            return [dict(row) for row in rows]
    
    async def _fetch_instrument_prices(self, session: aiohttp.ClientSession, 
                                     symbol: str, price_date: date) -> List[Dict[str, Any]]:
        """Fetch price data for a specific instrument and date."""
        if self.config.vendor.lower() == "polygon":
            return await self._fetch_polygon_prices(session, symbol, price_date)
        elif self.config.vendor.lower() == "tiingo":
            return await self._fetch_tiingo_prices(session, symbol, price_date)
        else:
            return []
    
    async def _fetch_polygon_prices(self, session: aiohttp.ClientSession,
                                  symbol: str, price_date: date) -> List[Dict[str, Any]]:
        """Fetch prices from Polygon API."""
        url = f"{self.base_url}/{symbol}/range/1/day/{price_date}/{price_date}"
        params = {"apikey": self.api_key}
        
        try:
            async with session.get(url, params=params) as response:
                if response.status == 429:
                    await self.handle_rate_limit("Polygon", 12)
                    return []
                elif response.status != 200:
                    logger.warning(f"Polygon API error {response.status} for {symbol}")
                    return []
                
                data = await response.json()
                results = data.get("results", [])
                
                prices = []
                for result in results:
                    # Convert Polygon timestamp to date
                    result_date = pd.to_datetime(result["t"], unit="ms").date()
                    
                    prices.append({
                        "date": result_date,
                        "open": result.get("o"),
                        "high": result.get("h"),
                        "low": result.get("l"),
                        "close": result.get("c"),
                        "volume": result.get("v")
                    })
                
                return prices
                
        except Exception as e:
            logger.error(f"Error fetching Polygon prices for {symbol}: {e}")
            return []
    
    async def _fetch_tiingo_prices(self, session: aiohttp.ClientSession,
                                 symbol: str, price_date: date) -> List[Dict[str, Any]]:
        """Fetch prices from Tiingo API."""
        url = f"{self.base_url}/{symbol}/prices"
        params = {
            "startDate": price_date.strftime("%Y-%m-%d"),
            "endDate": price_date.strftime("%Y-%m-%d"),
            "format": "json",
            "token": self.api_key
        }
        
        try:
            async with session.get(url, params=params) as response:
                if response.status == 429:
                    await self.handle_rate_limit("Tiingo", 5)
                    return []
                elif response.status != 200:
                    logger.warning(f"Tiingo API error {response.status} for {symbol}")
                    return []
                
                data = await response.json()
                
                prices = []
                for result in data:
                    # Parse Tiingo date format
                    result_date = pd.to_datetime(result["date"]).date()
                    
                    prices.append({
                        "date": result_date,
                        "open": result.get("open"),
                        "high": result.get("high"),
                        "low": result.get("low"),
                        "close": result.get("close"),
                        "adjclose": result.get("adjClose"),
                        "volume": result.get("volume")
                    })
                
                return prices
                
        except Exception as e:
            logger.error(f"Error fetching Tiingo prices for {symbol}: {e}")
            return []
    
    def _get_next_business_day(self, current_date: date) -> date:
        """Get the next business day."""
        next_date = current_date + timedelta(days=1)
        
        # Skip weekends
        while next_date.weekday() > 4:  # 5=Saturday, 6=Sunday
            next_date += timedelta(days=1)
        
        return next_date
    
    async def process_data_batch(self, batch_data: List[Dict[str, Any]]) -> Tuple[int, int]:
        """Process and store daily prices data."""
        if not batch_data:
            return 0, 0
        
        inserted_count = 0
        updated_count = 0
        
        try:
            async with self.pool.acquire() as conn:
                # Prepare records for insertion
                price_records = []
                for record in batch_data:
                    if self.config.vendor.lower() == "polygon":
                        price_records.append((
                            record["date"],
                            record["instrument_id"],
                            record.get("open"),
                            record.get("high"),
                            record.get("low"),
                            record.get("close"),
                            record.get("volume")
                        ))
                    elif self.config.vendor.lower() == "tiingo":
                        price_records.append((
                            record["date"],
                            record["instrument_id"],
                            record.get("open"),
                            record.get("high"),
                            record.get("low"),
                            record.get("close"),
                            record.get("adjclose"),
                            record.get("volume")
                        ))
                
                # Insert data with conflict resolution
                if self.config.vendor.lower() == "polygon":
                    await conn.executemany(f"""
                        INSERT INTO {self.table_name} 
                        (date, instrument_id, open, high, low, close, volume)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (instrument_id, date) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        updated_at = CURRENT_TIMESTAMP
                    """, price_records)
                elif self.config.vendor.lower() == "tiingo":
                    await conn.executemany(f"""
                        INSERT INTO {self.table_name} 
                        (date, instrument_id, open, high, low, close, adjclose, volume)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ON CONFLICT (instrument_id, date) DO UPDATE SET
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        adjclose = EXCLUDED.adjclose,
                        volume = EXCLUDED.volume,
                        updated_at = CURRENT_TIMESTAMP
                    """, price_records)
                
                inserted_count = len(price_records)
                logger.info(f"Inserted {inserted_count} price records for {self.config.vendor}")
                
        except Exception as e:
            logger.error(f"Error processing data batch: {e}")
            raise
        
        return inserted_count, updated_count


# Factory function to create configured frontfill jobs
async def create_daily_prices_frontfill_jobs(connection_pool: asyncpg.Pool, 
                                           env: Environment,
                                           polygon_api_key: str,
                                           tiingo_api_key: str) -> List[DailyPricesFrontfillJob]:
    """Create daily prices frontfill jobs for both vendors."""
    jobs = []
    
    # Polygon daily prices job
    polygon_config = FrontfillConfig(
        job_name="daily_prices_polygon_frontfill",
        job_type="daily_prices",
        vendor="polygon",
        checkpoint_type=CheckpointType.TIMESTAMP,
        batch_size=50,  # 50 instruments per batch
        rate_limit_delay=0.1,  # 100ms between API calls
        duplicate_check_hours=48  # Check last 48 hours for duplicates
    )
    
    polygon_job = DailyPricesFrontfillJob(polygon_config, connection_pool, env, polygon_api_key)
    jobs.append(polygon_job)
    
    # Tiingo daily prices job
    tiingo_config = FrontfillConfig(
        job_name="daily_prices_tiingo_frontfill",
        job_type="daily_prices",
        vendor="tiingo",
        checkpoint_type=CheckpointType.TIMESTAMP,
        batch_size=20,  # Smaller batch for Tiingo
        rate_limit_delay=0.5,  # 500ms between API calls
        duplicate_check_hours=48
    )
    
    tiingo_job = DailyPricesFrontfillJob(tiingo_config, connection_pool, env, tiingo_api_key)
    jobs.append(tiingo_job)
    
    return jobs