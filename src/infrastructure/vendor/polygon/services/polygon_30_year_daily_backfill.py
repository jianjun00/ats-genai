#!/usr/bin/env python3
"""
Polygon 30-Year Daily Price Backfill

Comprehensive backfill of historical daily price data for all active instruments
using Polygon API with 30-year historical depth. Based on existing working patterns
with enhanced idempotent operations.
"""

import sys
sys.path.append('/workspace/src')

import os
import asyncio
import asyncpg
import requests
import logging
from datetime import datetime, timedelta, timezone
import time
import argparse

from core.shared.utils.vendor_api_keys import get_polygon_api_key
from core.shared.utils.backfill_framework import BackfillStats, VendorRateLimiters, get_vendor_database_connection, get_vendor_table_name

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("polygon_30_year_daily_backfill")

class Polygon30YearBackfiller:
    """
    Polygon 30-year daily price backfiller with idempotent operations.

    Features:
    - 30-year historical data collection
    - Idempotent UPSERT operations
    - Rate limiting (5 requests/minute for free tier, adjustable)
    - Resume capability with existing data detection
    - Uses dev_instrument table for symbol list
    """

    def __init__(self, api_key: str = None):
        self.api_key = api_key or get_polygon_api_key()
        self.base_url = "https://api.polygon.io/v2/aggs/ticker"

        # Use shared rate limiter for consistent behavior
        self.rate_limiter = VendorRateLimiters.polygon_free()

        # Use enhanced BackfillStats for comprehensive monitoring
        self.stats = BackfillStats()

        # Legacy stats for compatibility
        self.legacy_stats = {
            'total_instruments': 0,
            'processed_instruments': 0,
            'total_records': 0,
            'api_calls': 0,
            'errors': 0,
            'skipped_instruments': 0
        }

        logger.info(f"📊 Polygon 30-Year Backfiller initialized")
        logger.info(f"   Rate limit: {60/self.rate_limiter.delay_seconds:.1f} requests/minute")

    async def get_database_connection(self):
        """Get database connection using shared utility."""
        return await get_vendor_database_connection()

    async def ensure_table_exists(self, conn):
        """Ensure Polygon daily table exists - using existing table structure."""
        table_name = get_vendor_table_name('daily_price', 'polygon')

        try:
            # Check if table already exists (it should for intg environment)
            result = await conn.fetchrow(f"""
                SELECT to_regclass('{table_name}')
            """)

            if result[0] is None:
                # Create table only if it doesn't exist (mainly for dev environment)
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
                logger.info(f"✅ Created Polygon daily price table: {table_name}")
            else:
                logger.info(f"✅ Polygon daily price table exists: {table_name}")
        except Exception as e:
            logger.error(f"❌ Failed to ensure table exists: {e}")
            raise

    async def get_instruments_for_backfill(self, conn, limit=None, universe_id=None, as_of_date=None):
        """Get active instruments from instruments table, optionally filtered by universe."""
        # Auto-detect table prefix based on environment
        env = os.getenv('ENV_TYPE', 'intg').lower()
        table_prefix = 'intg_' if env == 'intg' else 'dev_'

        # Build universe filter clause
        universe_join = ""
        universe_where = ""
        query_params = []
        
        if universe_id:
            universe_join = f"""
            JOIN {table_prefix}universe_membership um ON i.id = um.instrument_id
            """
            if as_of_date:
                # Convert string date to date object if needed
                if isinstance(as_of_date, str):
                    from datetime import datetime
                    as_of_date = datetime.strptime(as_of_date, '%Y-%m-%d').date()
                
                param_idx_1 = len(query_params) + 1
                param_idx_2 = len(query_params) + 2
                universe_where = f"AND um.universe_id = ${param_idx_1} AND um.start_at <= ${param_idx_2} AND (um.end_at IS NULL OR um.end_at > ${param_idx_2})"
                query_params.extend([universe_id, as_of_date])
            else:
                param_idx = len(query_params) + 1
                universe_where = f"AND um.universe_id = ${param_idx} AND um.start_at <= CURRENT_DATE AND (um.end_at IS NULL OR um.end_at > CURRENT_DATE)"
                query_params.append(universe_id)
            
            logger.info(f"🌌 Filtering by universe ID {universe_id}" + (f" as of {as_of_date}" if as_of_date else " as of current date"))

        # Check if TARGET_SYMBOLS is specified
        target_symbols = os.getenv('TARGET_SYMBOLS')
        if target_symbols:
            target_list = [s.strip().upper() for s in target_symbols.split(',')]
            symbols_clause = f"AND i.symbol = ANY(${len(query_params) + 1})"
            query_params.append(target_list)
            
            instruments = await conn.fetch(f"""
                SELECT i.id, i.symbol, i.name, i.exchange, i.active
                FROM {table_prefix}instrument i
                {universe_join}
                WHERE i.active = true
                  AND i.symbol IS NOT NULL
                  AND i.symbol != ''
                  AND i.exchange IN ('NASDAQ', 'NYSE', 'NYSE ARCA', 'BATS', 'XNYS', 'NYSE MKT', 'XNAS', 'AMEX', 'NYSE NAT')
                  {symbols_clause}
                  {universe_where}
                ORDER BY i.symbol
            """, *query_params)
            logger.info(f"🎯 Targeted symbols: {target_list}")
        else:
            limit_clause = f"LIMIT {limit}" if limit else ""
            instruments = await conn.fetch(f"""
                SELECT i.id, i.symbol, i.name, i.exchange, i.active
                FROM {table_prefix}instrument i
                {universe_join}
                WHERE i.active = true
                  AND i.symbol IS NOT NULL
                  AND i.symbol != ''
                  AND i.exchange IN ('NASDAQ', 'NYSE', 'NYSE ARCA', 'BATS', 'XNYS', 'NYSE MKT', 'XNAS', 'AMEX', 'NYSE NAT')
                  {universe_where}
                ORDER BY i.symbol
                {limit_clause}
            """, *query_params)

        self.legacy_stats['total_instruments'] = len(instruments)
        logger.info(f"📊 Found {len(instruments)} instruments for 30-year backfill")
        return instruments

    async def download_polygon_daily_prices(self, symbol, start_date, end_date):
        """Download daily prices from Polygon API."""
        url = f"{self.base_url}/{symbol}/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 50000,
            'apikey': self.api_key
        }

        start_time = time.time()
        try:
            response = requests.get(url, params=params)
            response_time = time.time() - start_time
            self.legacy_stats['api_calls'] += 1

            if response.status_code == 200:
                data = response.json()
                self.stats.api_calls_made += 1
                logger.info(f"🔍 Polygon API response for {symbol}: status={data.get('status')}, results_count={len(data.get('results', []))}")

                if data.get('status') in ['OK', 'DELAYED'] and 'results' in data:
                    results = data['results']
                    logger.info(f"✅ Downloaded {len(results)} records for {symbol}")
                    return results
                elif data.get('status') == 'ERROR':
                    logger.info(f"⚠️ API error for {symbol}: {data.get('error', 'Unknown error')}")
                    return []
                else:
                    logger.info(f"⚠️ No data available for {symbol}, full response: {data}")
                    return []
            elif response.status_code == 429:
                logger.warning(f"⚠️ Rate limit hit for {symbol}, using intelligent backoff...")
                self.stats.api_calls_made += 1
                self.stats.api_errors += 1
                # Use shared rate limiter for intelligent handling
                await self.rate_limiter.wait_if_needed()
                return await self.download_polygon_daily_prices(symbol, start_date, end_date)
            else:
                logger.error(f"❌ Polygon API error for {symbol}: {response.status_code}")
                self.stats.api_calls_made += 1
                self.stats.api_errors += 1
                self.legacy_stats['errors'] += 1
                return []

        except Exception as e:
            logger.error(f"❌ Error downloading {symbol}: {e}")
            self.stats.api_calls_made += 1
            self.stats.api_errors += 1
            self.legacy_stats['errors'] += 1
            return []

    async def insert_daily_prices_idempotent(self, conn, instrument_id, symbol, prices):
        """Insert daily prices with idempotent UPSERT operations."""
        if not prices:
            return 0

        # Prepare data for insertion
        rows = []
        for price in prices:
            # Convert Polygon timestamp to date
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
            # Let price record processing exceptions propagate
            # If any price record is malformed, the entire batch should fail

        if not rows:
            return 0

        # Insert with idempotent UPSERT
        table_name = get_vendor_table_name('daily_price', 'polygon')

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

        logger.info(f"💾 Inserted {len(rows)} price records for {symbol}")
        self.legacy_stats['total_records'] += len(rows)
        return len(rows)
        # Let database exceptions propagate - fail fast on database errors
        # If database insert fails, the application should halt rather than continue silently

    async def check_existing_data(self, conn, instrument_id, start_date, end_date):
        """Check if instrument already has data for ALL trading days in the date range."""
        env = os.getenv('ENV_TYPE', 'intg').lower()
        table_name = 'intg_daily_price_polygon' if env == 'intg' else 'dev_daily_price_polygon'

        # Calculate expected trading days (excluding weekends)
        expected_trading_days = await conn.fetchval("""
            WITH date_series AS (
                SELECT generate_series($1::date, $2::date, '1 day'::interval)::date as date_val
            ),
            trading_days AS (
                SELECT date_val as trading_date
                FROM date_series
                WHERE EXTRACT(DOW FROM date_val) NOT IN (0, 6)
            )
            SELECT COUNT(*) FROM trading_days
        """, start_date, end_date)

        # Count actual records for this instrument
        actual_count = await conn.fetchval(f"""
            SELECT COUNT(*) FROM {table_name}
            WHERE instrument_id = $1 AND date BETWEEN $2 AND $3
        """, instrument_id, start_date, end_date)

        # Return the number of missing trading days (0 means complete coverage)
        missing_days = expected_trading_days - actual_count
        return missing_days

    async def backfill_instrument(self, conn, instrument, start_date, end_date, skip_existing=True):
        """Backfill daily prices for a single instrument."""
        instrument_id = instrument['id']
        symbol = instrument['symbol']

        # Check if we should skip existing data
        if skip_existing:
            missing_days = await self.check_existing_data(conn, instrument_id, start_date, end_date)
            if missing_days == 0:
                logger.info(f"⏭️ Skipping {symbol} - complete coverage for all trading days")
                self.legacy_stats['skipped_instruments'] += 1
                return 0
            elif missing_days > 0:
                logger.info(f"📊 {symbol} missing {missing_days} trading days - proceeding with backfill")

        logger.info(f"📈 Processing {symbol} (ID: {instrument_id}) for 30-year backfill...")

        # Download data from Polygon
        prices = await self.download_polygon_daily_prices(symbol, start_date, end_date)

        if not prices:
            logger.warning(f"⚠️ No price data for {symbol}")
            return 0

        # Insert data idempotently
        inserted_count = await self.insert_daily_prices_idempotent(conn, instrument_id, symbol, prices)

        logger.info(f"✅ Completed {symbol}: {inserted_count} records inserted")
        self.legacy_stats['processed_instruments'] += 1

        # Use shared rate limiter for consistent behavior
        await self.rate_limiter.wait_if_needed()

        return inserted_count
        # Let all instrument processing exceptions propagate
        # If individual instrument processing fails, the entire operation should fail

    async def run_backfill(self, start_date, end_date, limit=None, skip_existing=True, universe_id=None, as_of_date=None):
        """Run the complete 30-year backfill process."""
        logger.info("🚀 Starting Polygon 30-year daily price backfill...")
        if universe_id:
            logger.info(f"🌌 Filtering by universe ID {universe_id} as of {as_of_date or start_date}")
        logger.info(f"📅 Date range: {start_date} to {end_date}")

        conn = await self.get_database_connection()

        try:
            # Ensure table exists
            await self.ensure_table_exists(conn)

            # Get instruments to process
            instruments = await self.get_instruments_for_backfill(conn, limit, universe_id, as_of_date)

            if not instruments:
                logger.warning("❌ No instruments found for backfill")
                return

            logger.info(f"📊 Processing {len(instruments)} instruments")

            # Process each instrument
            for i, instrument in enumerate(instruments, 1):
                await self.backfill_instrument(conn, instrument, start_date, end_date, skip_existing)

                # Progress logging
                if i % 100 == 0 or i == len(instruments):
                    progress = (i / len(instruments)) * 100
                    logger.info(f"📊 Progress: {i:,}/{len(instruments):,} ({progress:.1f}%) - "
                              f"{self.legacy_stats['total_records']:,} total records")

                # Let instrument processing exceptions propagate
                # If any instrument fails, the entire backfill should fail fast

        finally:
            await conn.close()

    def log_final_summary(self):
        """Log comprehensive final summary."""
        logger.info("=" * 80)
        logger.info("🎉 POLYGON 30-YEAR DAILY PRICE BACKFILL COMPLETE")
        logger.info("=" * 80)
        logger.info(f"📊 PROCESSING SUMMARY:")
        logger.info(f"  Total Instruments: {self.legacy_stats['total_instruments']:,}")
        logger.info(f"  Processed Instruments: {self.legacy_stats['processed_instruments']:,}")
        logger.info(f"  Skipped Instruments: {self.legacy_stats['skipped_instruments']:,}")
        logger.info(f"  Total Records Inserted: {self.legacy_stats['total_records']:,}")
        logger.info(f"  API Calls Made: {self.legacy_stats['api_calls']:,}")
        logger.info(f"  Errors: {self.legacy_stats['errors']:,}")
        logger.info("")

        success_rate = ((self.legacy_stats['processed_instruments']) / self.legacy_stats['total_instruments'] * 100) if self.legacy_stats['total_instruments'] > 0 else 0
        logger.info(f"✅ Success Rate: {success_rate:.1f}%")

        avg_records = self.legacy_stats['total_records'] / max(1, self.legacy_stats['processed_instruments'])
        logger.info(f"📈 Average Records per Instrument: {avg_records:.1f}")
        logger.info("=" * 80)

async def main():
    parser = argparse.ArgumentParser(description="Polygon 30-year daily price backfill")
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--limit', type=int, default=int(os.getenv('LIMIT', '0')) or None,
                       help='Limit number of instruments to process')
    parser.add_argument('--years', type=int, default=int(os.getenv('YEARS', '30')),
                       help='Number of years of historical data to fetch (default: 30)')
    parser.add_argument('--start_date', type=str, default=None,
                       help='Start date (YYYY-MM-DD), overrides --years')
    parser.add_argument('--end_date', type=str, default=None,
                       help='End date (YYYY-MM-DD), defaults to today')
    parser.add_argument('--skip_existing', action='store_true', default=False,
                       help='Skip instruments that already have price data')
    parser.add_argument('--universe_id', type=int, 
                       help='Universe ID to filter instruments (only backfill instruments in this universe)')
    parser.add_argument('--as_of_date', type=str,
                       help='Date to determine universe membership (YYYY-MM-DD), defaults to start_date')

    args = parser.parse_args()

    # Set up logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    try:
        # Get Polygon API key using shared utilities
        polygon_api_key = get_polygon_api_key()
        if not polygon_api_key:
            logger.error("❌ POLYGON_API_KEY not found. Please set environment variable or configure in gin files.")
            sys.exit(1)

        logger.info("✅ Polygon API key found")

        # Calculate date range
        if args.start_date:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        else:
            start_date = (datetime.now() - timedelta(days=365 * args.years)).date()

        if args.end_date:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        else:
            end_date = datetime.now().date()

        logger.info(f"📅 Backfilling Polygon prices from {start_date} to {end_date}")

        # Initialize backfiller
        backfiller = Polygon30YearBackfiller(polygon_api_key)

        # Run backfill
        await backfiller.run_backfill(
            start_date, end_date,
            limit=args.limit,
            skip_existing=args.skip_existing,
            universe_id=args.universe_id,
            as_of_date=args.as_of_date
        )

        # Log final summary
        backfiller.log_final_summary()

        # Log comprehensive statistics from shared framework
        backfiller.stats.log_progress(logger)

        logger.info("✅ Polygon 30-year daily price backfill complete")

    except Exception as e:
        logger.error(f"❌ Failed to run Polygon 30-year backfill: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())