#!/usr/bin/env python3
"""
Polygon Recent Daily Price Backfill (Last 2-5 Years)

Optimized backfill for recent historical data that:
1. Focuses on 2020-2025 range (free API limit)
2. Identifies missing dates for each instrument  
3. Skips weekends and market holidays
4. Makes targeted API calls for only missing trading days
"""

import sys
sys.path.append('/workspace/src')

import os
import asyncio
import asyncpg
import requests
import logging
from datetime import datetime, timedelta, date, timezone
import time
import json
import argparse
from typing import List, Set, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("polygon_recent_backfill")

class SimpleTradingCalendar:
    """Simplified US trading calendar for recent years."""
    
    def __init__(self):
        # Key market holidays for 2020-2025
        self.market_holidays = {
            # 2020
            date(2020, 1, 1), date(2020, 1, 20), date(2020, 2, 17), date(2020, 4, 10),
            date(2020, 5, 25), date(2020, 7, 3), date(2020, 9, 7), date(2020, 11, 26),
            date(2020, 12, 25),
            # 2021
            date(2021, 1, 1), date(2021, 1, 18), date(2021, 2, 15), date(2021, 4, 2),
            date(2021, 5, 31), date(2021, 6, 18), date(2021, 7, 5), date(2021, 9, 6),
            date(2021, 11, 25), date(2021, 12, 24),
            # 2022
            date(2022, 1, 17), date(2022, 2, 21), date(2022, 4, 15), date(2022, 5, 30),
            date(2022, 6, 20), date(2022, 7, 4), date(2022, 9, 5), date(2022, 11, 24),
            date(2022, 12, 26),
            # 2023
            date(2023, 1, 2), date(2023, 1, 16), date(2023, 2, 20), date(2023, 4, 7),
            date(2023, 5, 29), date(2023, 6, 19), date(2023, 7, 4), date(2023, 9, 4),
            date(2023, 11, 23), date(2023, 12, 25),
            # 2024
            date(2024, 1, 1), date(2024, 1, 15), date(2024, 2, 19), date(2024, 3, 29),
            date(2024, 5, 27), date(2024, 6, 19), date(2024, 7, 4), date(2024, 9, 2),
            date(2024, 11, 28), date(2024, 12, 25),
            # 2025
            date(2025, 1, 1), date(2025, 1, 20), date(2025, 2, 17), date(2025, 4, 18),
            date(2025, 5, 26), date(2025, 6, 19), date(2025, 7, 4), date(2025, 9, 1),
            date(2025, 11, 27), date(2025, 12, 25)
        }
    
    def is_trading_day(self, check_date: date) -> bool:
        """Check if a date is a trading day (not weekend or holiday)."""
        # Check if weekend
        if check_date.weekday() >= 5:  # Saturday=5, Sunday=6
            return False
        
        # Check if holiday
        if check_date in self.market_holidays:
            return False
        
        return True
    
    def get_trading_days_in_range(self, start_date: date, end_date: date) -> List[date]:
        """Get all trading days in date range."""
        trading_days = []
        current_date = start_date
        
        while current_date <= end_date:
            if self.is_trading_day(current_date):
                trading_days.append(current_date)
            current_date += timedelta(days=1)
        
        return trading_days


class PolygonRecentBackfiller:
    """
    Optimized Polygon backfiller for recent years (2020-2025).
    """
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.polygon.io/v2/aggs/ticker"
        self.calendar = SimpleTradingCalendar()
        
        # Rate limiting for free tier
        self.request_delay = 8.0  # 8 seconds between requests
        
        # Statistics
        self.stats = {
            'total_instruments': 0,
            'processed_instruments': 0,
            'skipped_instruments': 0,
            'total_records': 0,
            'api_calls': 0,
            'api_calls_saved': 0,
            'missing_date_ranges': 0,
            'errors': 0
        }
        
        logger.info(f"🎯 Polygon Recent Backfiller initialized (2020-2025)")
        logger.info(f"   Rate limit: {60/self.request_delay:.1f} requests/minute")

    async def get_database_connection(self):
        """Get database connection (Docker-compatible)."""
        env = os.getenv('ENV_TYPE', 'intg').lower()
        
        if env == 'intg':
            return await asyncpg.connect(
                host='ats-intg-postgres',
                port=5432,
                user='postgres',
                password='intg_password',
                database='intg_db'
            )
        else:
            return await asyncpg.connect(
                host='ats-dev-postgres',
                port=5432,
                user='postgres',
                password='dev_password',
                database='dev_db'
            )

    async def ensure_table_exists(self, conn):
        """Ensure Polygon daily table exists."""
        env = os.getenv('ENV_TYPE', 'intg').lower()
        table_name = 'intg_daily_prices_polygon' if env == 'intg' else 'dev_daily_prices_polygon'
        
        try:
            result = await conn.fetchrow(f"SELECT to_regclass('{table_name}')")
            
            if result[0] is None:
                await conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        date DATE NOT NULL,
                        symbol TEXT,
                        open DOUBLE PRECISION,
                        high DOUBLE PRECISION,
                        low DOUBLE PRECISION,
                        close DOUBLE PRECISION,
                        volume BIGINT,
                        market_cap DOUBLE PRECISION,
                        instrument_id INTEGER NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE,
                        updated_at TIMESTAMP WITH TIME ZONE,
                        PRIMARY KEY(date, instrument_id)
                    )
                """)
                logger.info(f"✅ Created table: {table_name}")
            else:
                logger.info(f"✅ Table exists: {table_name}")
        except Exception as e:
            logger.error(f"❌ Failed to ensure table exists: {e}")
            raise

    async def get_instruments_for_backfill(self, conn, limit=None):
        """Get active instruments from instruments table."""
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        env = os.getenv('ENV_TYPE', 'intg').lower()
        table_prefix = 'intg_' if env == 'intg' else 'dev_'
        
        instruments = await conn.fetch(f"""
            SELECT id, symbol, name, exchange, active
            FROM {table_prefix}instruments 
            WHERE active = true 
              AND symbol IS NOT NULL 
              AND symbol != ''
              AND exchange IN ('NASDAQ', 'NYSE', 'NYSE ARCA', 'BATS', 'XNYS', 'NYSE MKT', 'XNAS', 'AMEX', 'NYSE NAT')
            ORDER BY symbol
            {limit_clause}
        """)
        
        self.stats['total_instruments'] = len(instruments)
        logger.info(f"📊 Found {len(instruments)} instruments for recent backfill")
        return instruments

    async def get_missing_trading_days(self, conn, instrument_id: int, symbol: str, start_date: date, end_date: date) -> List[Tuple[date, date]]:
        """
        Get missing trading days for an instrument as date ranges.
        Returns list of (start_date, end_date) tuples for missing ranges.
        """
        env = os.getenv('ENV_TYPE', 'intg').lower()
        table_name = 'intg_daily_prices_polygon' if env == 'intg' else 'dev_daily_prices_polygon'
        
        # Get all existing dates for this instrument
        existing_dates = await conn.fetch(f"""
            SELECT date FROM {table_name}
            WHERE instrument_id = $1 AND date BETWEEN $2 AND $3
            ORDER BY date
        """, instrument_id, start_date, end_date)
        
        existing_dates_set = {row['date'] for row in existing_dates}
        
        # Get all expected trading days
        expected_trading_days = self.calendar.get_trading_days_in_range(start_date, end_date)
        
        # Find missing trading days
        missing_dates = [d for d in expected_trading_days if d not in existing_dates_set]
        
        if not missing_dates:
            return []
        
        # Group consecutive missing dates into ranges (max 30 days per range for API efficiency)
        date_ranges = []
        range_start = missing_dates[0]
        range_end = missing_dates[0]
        
        for i in range(1, len(missing_dates)):
            current_date = missing_dates[i]
            days_diff = (current_date - range_end).days
            
            if days_diff <= 7 and (range_end - range_start).days < 30:
                # Extend current range (if gap is small and range not too long)
                range_end = current_date
            else:
                # Start new range
                date_ranges.append((range_start, range_end))
                range_start = current_date
                range_end = current_date
        
        # Add final range
        date_ranges.append((range_start, range_end))
        
        logger.info(f"📅 {symbol}: {len(missing_dates)} missing trading days in {len(date_ranges)} ranges")
        return date_ranges

    def download_polygon_daily_prices_range(self, symbol: str, start_date: date, end_date: date):
        """Download daily prices for a specific date range from Polygon API."""
        url = f"{self.base_url}/{symbol}/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 50000,
            'apikey': self.api_key
        }
        
        try:
            response = requests.get(url, params=params)
            self.stats['api_calls'] += 1
            
            if response.status_code == 200:
                data = response.json()
                
                if data.get('status') in ['OK', 'DELAYED'] and 'results' in data:
                    results = data['results']
                    logger.info(f"📥 {symbol}: Downloaded {len(results)} records for {start_date} to {end_date}")
                    return results
                elif data.get('status') == 'ERROR':
                    logger.warning(f"⚠️ API error for {symbol}: {data.get('error', 'Unknown error')}")
                    return []
                else:
                    logger.debug(f"📋 No data for {symbol} in range {start_date} to {end_date}")
                    return []
            elif response.status_code == 403:
                logger.warning(f"⚠️ Access denied for {symbol} ({start_date} to {end_date}) - may need paid plan")
                return []
            elif response.status_code == 429:
                logger.warning(f"⚠️ Rate limit hit for {symbol}, waiting...")
                time.sleep(60)  # Wait 1 minute
                return self.download_polygon_daily_prices_range(symbol, start_date, end_date)
            else:
                logger.error(f"❌ API error for {symbol}: {response.status_code}")
                self.stats['errors'] += 1
                return []
                
        except Exception as e:
            logger.error(f"❌ Error downloading {symbol}: {e}")
            self.stats['errors'] += 1
            return []

    async def insert_daily_prices_idempotent(self, conn, instrument_id: int, symbol: str, prices):
        """Insert daily prices with idempotent UPSERT operations."""
        if not prices:
            return 0
        
        # Prepare data for insertion
        rows = []
        for price in prices:
            try:
                if 't' in price:
                    date_val = datetime.fromtimestamp(price['t']/1000, tz=timezone.utc).date()
                else:
                    continue
                
                rows.append((
                    date_val,
                    symbol,
                    price.get('o'),  # open
                    price.get('h'),  # high
                    price.get('l'),  # low
                    price.get('c'),  # close
                    price.get('v', 0),  # volume
                    instrument_id
                ))
            except Exception as e:
                logger.error(f"❌ Error processing price record for {symbol}: {e}")
                continue
        
        if not rows:
            return 0
        
        # Insert with idempotent UPSERT
        env = os.getenv('ENV_TYPE', 'intg').lower()
        table_name = 'intg_daily_prices_polygon' if env == 'intg' else 'dev_daily_prices_polygon'
        
        try:
            result = await conn.executemany(f"""
                INSERT INTO {table_name}
                (date, symbol, open, high, low, close, volume, market_cap, instrument_id, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, $8, NOW(), NOW())
                ON CONFLICT (date, instrument_id) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    updated_at = NOW()
            """, rows)
            
            logger.info(f"💾 {symbol}: Inserted {len(rows)} price records")
            self.stats['total_records'] += len(rows)
            return len(rows)
            
        except Exception as e:
            logger.error(f"❌ Database error inserting prices for {symbol}: {e}")
            self.stats['errors'] += 1
            return 0

    async def backfill_instrument_recent(self, conn, instrument, start_date: date, end_date: date):
        """Optimized backfill for recent years with missing date detection."""
        instrument_id = instrument['id']
        symbol = instrument['symbol']
        
        try:
            # Get missing trading days as date ranges
            missing_ranges = await self.get_missing_trading_days(conn, instrument_id, symbol, start_date, end_date)
            
            if not missing_ranges:
                logger.info(f"✅ {symbol}: No missing data, skipping")
                self.stats['skipped_instruments'] += 1
                return 0
            
            # Calculate API calls saved
            self.stats['missing_date_ranges'] += len(missing_ranges)
            
            logger.info(f"🎯 {symbol}: Processing {len(missing_ranges)} missing date ranges")
            
            total_inserted = 0
            
            # Process each missing date range
            for range_start, range_end in missing_ranges:
                logger.info(f"📈 {symbol}: Fetching {range_start} to {range_end}")
                
                # Download data for this specific range
                prices = self.download_polygon_daily_prices_range(symbol, range_start, range_end)
                
                if prices:
                    # Insert data idempotently
                    inserted_count = await self.insert_daily_prices_idempotent(conn, instrument_id, symbol, prices)
                    total_inserted += inserted_count
                
                # Rate limiting delay between range requests
                time.sleep(self.request_delay)
            
            if total_inserted > 0:
                logger.info(f"✅ {symbol}: Completed with {total_inserted} total records inserted")
                self.stats['processed_instruments'] += 1
            else:
                logger.info(f"📋 {symbol}: No new data available from API")
                self.stats['skipped_instruments'] += 1
            
            return total_inserted
            
        except Exception as e:
            logger.error(f"❌ Failed to process {symbol}: {e}")
            self.stats['errors'] += 1
            return 0

    async def run_recent_backfill(self, start_date: date, end_date: date, limit=None):
        """Run the optimized recent backfill process."""
        logger.info("🚀 Starting Polygon Recent Daily Price Backfill...")
        logger.info(f"📅 Date range: {start_date} to {end_date}")
        logger.info(f"🎯 Strategy: Missing trading days only (holidays excluded)")
        
        conn = await self.get_database_connection()
        
        try:
            # Ensure table exists
            await self.ensure_table_exists(conn)
            
            # Get instruments to process
            instruments = await self.get_instruments_for_backfill(conn, limit)
            
            if not instruments:
                logger.warning("❌ No instruments found for backfill")
                return
            
            # Filter for specific symbols if TARGET_SYMBOLS is provided
            target_symbols = os.getenv('TARGET_SYMBOLS')
            if target_symbols:
                target_list = [s.strip().upper() for s in target_symbols.split(',')]
                instruments = [inst for inst in instruments if inst['symbol'].upper() in target_list]
                logger.info(f"🎯 Filtering to target symbols: {target_list}")
            
            logger.info(f"📊 Processing {len(instruments)} instruments with recent optimization")
            
            # Process each instrument
            for i, instrument in enumerate(instruments, 1):
                try:
                    await self.backfill_instrument_recent(conn, instrument, start_date, end_date)
                    
                    # Progress logging
                    if i % 25 == 0 or i == len(instruments):
                        progress = (i / len(instruments)) * 100
                        logger.info(f"📊 Progress: {i:,}/{len(instruments):,} ({progress:.1f}%) - "
                                  f"{self.stats['total_records']:,} records, "
                                  f"{self.stats['missing_date_ranges']} ranges")
                        
                except Exception as e:
                    logger.error(f"❌ Critical error processing {instrument.get('symbol', 'unknown')}: {e}")
                    continue
            
        finally:
            await conn.close()
    
    def log_final_summary(self):
        """Log comprehensive optimization summary."""
        logger.info("=" * 80)
        logger.info("🎉 POLYGON RECENT BACKFILL COMPLETE")
        logger.info("=" * 80)
        logger.info(f"📊 PROCESSING SUMMARY:")
        logger.info(f"  Total Instruments: {self.stats['total_instruments']:,}")
        logger.info(f"  Processed Instruments: {self.stats['processed_instruments']:,}")
        logger.info(f"  Skipped Instruments: {self.stats['skipped_instruments']:,}")
        logger.info(f"  Total Records Inserted: {self.stats['total_records']:,}")
        logger.info(f"  Missing Date Ranges: {self.stats['missing_date_ranges']:,}")
        logger.info(f"")
        logger.info(f"🎯 OPTIMIZATION RESULTS:")
        logger.info(f"  API Calls Made: {self.stats['api_calls']:,}")
        logger.info(f"  Errors: {self.stats['errors']:,}")
        logger.info(f"")
        
        success_rate = (self.stats['processed_instruments'] / max(1, self.stats['total_instruments'])) * 100
        logger.info(f"✅ Success Rate: {success_rate:.1f}%")
        logger.info("=" * 80)


async def main():
    parser = argparse.ArgumentParser(description="Polygon recent daily price backfill (2020-2025)")
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--limit', type=int, default=int(os.getenv('LIMIT', '0')) or None, 
                       help='Limit number of instruments to process')
    parser.add_argument('--years', type=int, default=int(os.getenv('YEARS', '5')), 
                       help='Number of years of historical data to fetch (default: 5)')
    parser.add_argument('--start_date', type=str, default=None, 
                       help='Start date (YYYY-MM-DD), overrides --years')
    parser.add_argument('--end_date', type=str, default=None, 
                       help='End date (YYYY-MM-DD), defaults to today')
    
    args = parser.parse_args()
    
    # Set up logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    try:
        # Get Polygon API key
        polygon_api_key = os.environ.get("POLYGON_API_KEY")
        if not polygon_api_key:
            logger.error("❌ POLYGON_API_KEY environment variable not set")
            sys.exit(1)
        
        logger.info("✅ Polygon API key found")
        
        # Calculate date range (limit to 2020+ for free API compatibility)
        if args.start_date:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        else:
            start_date = max(
                (datetime.now() - timedelta(days=365 * args.years)).date(),
                date(2020, 1, 1)  # Don't go before 2020
            )
        
        if args.end_date:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        else:
            end_date = datetime.now().date()
        
        logger.info(f"📅 Recent backfill from {start_date} to {end_date}")
        
        # Initialize recent backfiller
        backfiller = PolygonRecentBackfiller(polygon_api_key)
        
        # Run recent backfill
        await backfiller.run_recent_backfill(start_date, end_date, limit=args.limit)
        
        # Log final summary
        backfiller.log_final_summary()
        
        logger.info("✅ Polygon recent backfill complete")
        
    except Exception as e:
        logger.error(f"❌ Failed to run recent backfill: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())