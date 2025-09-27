#!/usr/bin/env python3
"""
Backfill Daily Prices for Past 30 Trading Days - All Vendors

Backfills missing daily price data for EODHD, Tiingo, and Polygon vendors
for the past 30 trading days.

Usage:
    PYTHONPATH=src python -m domains.workflow.daily_prices.thirty_day_backfill
    PYTHONPATH=src python -m domains.workflow.daily_prices.thirty_day_backfill --vendor polygon
    PYTHONPATH=src python -m domains.workflow.daily_prices.thirty_day_backfill --debug
"""

import os
import asyncio
import asyncpg
import logging
import argparse
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple
import aiohttp

from core.shared.run_aware_logging import setup_run_aware_logging

class DailyPriceBackfiller:
    """Backfills daily price data for multiple vendors."""
    
    def __init__(self, db_host: str = "localhost", db_port: int = 4432,
                 db_user: str = "postgres", db_password: str = "intg_password",
                 db_name: str = "intg_db"):
        self.db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        self.logger = logging.getLogger(__name__)
        
        # API Keys from environment
        self.polygon_api_key = os.getenv("POLYGON_API_KEY", "wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD")
        self.tiingo_api_key = os.getenv("TIINGO_API_KEY", "5f40b4f36e171405746304ec0e5a6f3aa9ca77e5")
        self.eodhd_api_key = os.getenv("EODHD_API_KEY", "68aa0c7d2fe831.67386369")
        
        self.logger.info(f"Initialized backfiller for {db_host}:{db_port}/{db_name}")

    def get_trading_days(self, days: int = 30) -> List[date]:
        """Get list of trading days (excluding weekends) for past N days."""
        today = date.today()
        all_days = [today - timedelta(days=i) for i in range(days)]
        
        # Filter out weekends (Saturday=5, Sunday=6)
        trading_days = [d for d in all_days if d.weekday() < 5]
        return sorted(trading_days)

    async def get_instruments_missing_data(self, vendor: str, trading_date: date) -> List[Dict]:
        """Get instruments that are missing data for a specific date and vendor."""
        async with asyncpg.create_pool(self.db_url) as pool:
            async with pool.acquire() as conn:
                query = f"""
                SELECT DISTINCT i.id, i.symbol, i.exchange
                FROM intg_instrument i
                WHERE i.active = true
                AND NOT EXISTS (
                    SELECT 1 FROM intg_daily_price_{vendor} p
                    WHERE p.instrument_id = i.id AND p.date = $1
                )
                ORDER BY i.symbol
                """
                
                results = await conn.fetch(query, trading_date)
                return [dict(r) for r in results]

    async def fetch_polygon_daily_price(self, symbol: str, date: date, session: aiohttp.ClientSession) -> Optional[Dict]:
        """Fetch daily price from Polygon API."""
        url = f"https://api.polygon.io/v1/open-close/{symbol}/{date.strftime('%Y-%m-%d')}"
        params = {"adjusted": "true", "apiKey": self.polygon_api_key}
        
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('status') == 'OK':
                        return {
                            'open': data.get('open'),
                            'high': data.get('high'),
                            'low': data.get('low'),
                            'close': data.get('close'),
                            'volume': data.get('volume'),
                            'date': date
                        }
                elif response.status == 429:
                    self.logger.warning(f"Rate limit hit for {symbol} on Polygon")
                    await asyncio.sleep(12)  # 5 calls/min = 12s delay
                return None
        except Exception as e:
            self.logger.error(f"Error fetching Polygon data for {symbol}: {e}")
            return None

    async def fetch_tiingo_daily_price(self, symbol: str, date: date, session: aiohttp.ClientSession) -> Optional[Dict]:
        """Fetch daily price from Tiingo API."""
        url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
        params = {
            "startDate": date.strftime('%Y-%m-%d'),
            "endDate": date.strftime('%Y-%m-%d'),
            "token": self.tiingo_api_key
        }
        
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    if data:
                        day_data = data[0]
                        return {
                            'open': day_data.get('open'),
                            'high': day_data.get('high'),
                            'low': day_data.get('low'),
                            'close': day_data.get('close'),
                            'volume': day_data.get('volume'),
                            'date': date
                        }
                elif response.status == 429:
                    self.logger.warning(f"Rate limit hit for {symbol} on Tiingo")
                    await asyncio.sleep(1)  # 1000 calls/hr
                return None
        except Exception as e:
            self.logger.error(f"Error fetching Tiingo data for {symbol}: {e}")
            return None

    async def fetch_eodhd_daily_price(self, symbol: str, date: date, session: aiohttp.ClientSession) -> Optional[Dict]:
        """Fetch daily price from EODHD API."""
        url = f"https://eodhistoricaldata.com/api/eod/{symbol}.US"
        params = {
            "from": date.strftime('%Y-%m-%d'),
            "to": date.strftime('%Y-%m-%d'),
            "api_token": self.eodhd_api_key,
            "fmt": "json"
        }
        
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    if data:
                        day_data = data[0]
                        return {
                            'open': day_data.get('open'),
                            'high': day_data.get('high'),
                            'low': day_data.get('low'),
                            'close': day_data.get('close'),
                            'volume': day_data.get('volume'),
                            'date': date
                        }
                elif response.status == 429:
                    self.logger.warning(f"Rate limit hit for {symbol} on EODHD")
                    await asyncio.sleep(3)  # 20 calls/min = 3s delay
                return None
        except Exception as e:
            self.logger.error(f"Error fetching EODHD data for {symbol}: {e}")
            return None

    async def insert_daily_price(self, vendor: str, instrument_id: int, symbol: str, price_data: Dict) -> bool:
        """Insert daily price record into database."""
        async with asyncpg.create_pool(self.db_url) as pool:
            async with pool.acquire() as conn:
                try:
                    query = f"""
                    INSERT INTO intg_daily_price_{vendor} 
                    (instrument_id, symbol, date, open, high, low, close, volume, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())
                    ON CONFLICT (date, instrument_id) DO UPDATE 
                    SET updated_at = NOW()
                    """
                    
                    await conn.execute(
                        query,
                        instrument_id,
                        symbol,
                        price_data['date'],
                        price_data['open'],
                        price_data['high'],
                        price_data['low'],
                        price_data['close'],
                        price_data['volume']
                    )
                    return True
                except Exception as e:
                    self.logger.error(f"Error inserting {vendor} price for {symbol}: {e}")
                    return False

    async def backfill_vendor_date(self, vendor: str, trading_date: date, limit: Optional[int] = None) -> Tuple[int, int]:
        """Backfill data for a specific vendor and date."""
        self.logger.info(f"Backfilling {vendor} data for {trading_date}")
        
        # Get instruments missing data
        missing_instruments = await self.get_instruments_missing_data(vendor, trading_date)
        
        if limit:
            missing_instruments = missing_instruments[:limit]
        
        total = len(missing_instruments)
        self.logger.info(f"Found {total} instruments missing {vendor} data for {trading_date}")
        
        if total == 0:
            return 0, 0
        
        success_count = 0
        
        async with aiohttp.ClientSession() as session:
            for idx, instrument in enumerate(missing_instruments, 1):
                symbol = instrument['symbol']
                instrument_id = instrument['id']
                
                # Fetch data from appropriate vendor
                if vendor == 'polygon':
                    price_data = await self.fetch_polygon_daily_price(symbol, trading_date, session)
                elif vendor == 'tiingo':
                    price_data = await self.fetch_tiingo_daily_price(symbol, trading_date, session)
                elif vendor == 'eodhd':
                    price_data = await self.fetch_eodhd_daily_price(symbol, trading_date, session)
                else:
                    continue
                
                if price_data:
                    if await self.insert_daily_price(vendor, instrument_id, symbol, price_data):
                        success_count += 1
                        if idx % 10 == 0:
                            self.logger.info(f"Progress: {idx}/{total} ({success_count} successful)")
                
                # Rate limiting
                if vendor == 'polygon':
                    await asyncio.sleep(12)  # 5 calls/min
                elif vendor == 'tiingo':
                    await asyncio.sleep(1)   # 1000 calls/hr
                elif vendor == 'eodhd':
                    await asyncio.sleep(3)   # 20 calls/min
        
        self.logger.info(f"Completed {vendor} backfill for {trading_date}: {success_count}/{total} successful")
        return success_count, total

    async def backfill_all_vendors(self, days: int = 30, vendor_filter: Optional[str] = None, 
                                   limit_per_date: Optional[int] = None) -> Dict:
        """Backfill all vendors for past N trading days."""
        trading_days = self.get_trading_days(days)
        vendors = [vendor_filter] if vendor_filter else ['polygon', 'tiingo', 'eodhd']
        
        self.logger.info(f"Starting backfill for {len(trading_days)} trading days")
        self.logger.info(f"Vendors: {', '.join(vendors)}")
        
        results = {vendor: {'success': 0, 'total': 0} for vendor in vendors}
        
        for trading_date in reversed(trading_days):  # Start from oldest
            self.logger.info(f"\n{'='*60}")
            self.logger.info(f"Processing {trading_date}")
            self.logger.info(f"{'='*60}")
            
            for vendor in vendors:
                success, total = await self.backfill_vendor_date(vendor, trading_date, limit_per_date)
                results[vendor]['success'] += success
                results[vendor]['total'] += total
        
        return results

async def main():
    """Main backfill entry point."""
    parser = argparse.ArgumentParser(description="Backfill Daily Prices for Past 30 Trading Days")
    parser.add_argument("--days", type=int, default=30, help="Number of trading days to backfill (default: 30)")
    parser.add_argument("--vendor", choices=['polygon', 'tiingo', 'eodhd'], help="Specific vendor to backfill")
    parser.add_argument("--limit", type=int, help="Limit instruments per date (for testing)")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = "DEBUG" if args.debug else "INFO"
    setup_run_aware_logging(log_level=log_level)
    
    logger = logging.getLogger(__name__)
    logger.info("🚀 Starting Daily Price Backfill")
    logger.info(f"📅 Days: {args.days}")
    logger.info(f"🏢 Vendor: {args.vendor or 'ALL'}")
    if args.limit:
        logger.info(f"🔢 Limit: {args.limit} instruments per date")
    
    try:
        backfiller = DailyPriceBackfiller()
        results = await backfiller.backfill_all_vendors(
            days=args.days,
            vendor_filter=args.vendor,
            limit_per_date=args.limit
        )
        
        logger.info(f"\n{'='*60}")
        logger.info("📊 BACKFILL SUMMARY")
        logger.info(f"{'='*60}")
        
        for vendor, stats in results.items():
            success_rate = (stats['success'] / stats['total'] * 100) if stats['total'] > 0 else 0
            logger.info(f"{vendor.upper():8s}: {stats['success']:,} / {stats['total']:,} ({success_rate:.1f}%)")
        
        logger.info("✅ Daily price backfill completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Daily price backfill failed: {e}")
        if args.debug:
            import traceback
            logger.error(f"📋 Full traceback: {traceback.format_exc()}")

if __name__ == "__main__":
    asyncio.run(main())