#!/usr/bin/env python3
"""
Tiingo 30-Year Daily Price Backfill

Comprehensive backfill of historical daily price data for all active instruments
using Tiingo API with 30-year historical depth. Based on existing working patterns
with enhanced idempotent operations.
"""

import sys
sys.path.append('/workspace/src')

import os
import asyncio
import asyncpg
import requests
import logging
from datetime import datetime, timedelta
import time
import argparse

# Prometheus metrics support
try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logging.warning("prometheus_client not installed. Metrics will not be pushed to Prometheus.")

from core.shared.utils.vendor_api_keys import get_tiingo_api_key
from core.shared.utils.backfill_framework import BackfillStats, VendorRateLimiters, get_vendor_database_connection, get_vendor_table_name

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("tiingo_30_year_daily_backfill")

class Tiingo30YearBackfiller:
    """
    Tiingo 30-year daily price backfiller with idempotent operations.

    Features:
    - 30-year historical data collection
    - Idempotent UPSERT operations
    - Rate limiting (1000 requests/hour for paid Tiingo)
    - Resume capability with existing data detection
    - Uses dev_instrument table for symbol list
    """

    def __init__(self, api_key: str = None):
        self.api_key = api_key or get_tiingo_api_key()
        self.base_url = "https://api.tiingo.com/tiingo/daily"

        # Use shared rate limiter for consistent behavior
        self.rate_limiter = VendorRateLimiters.tiingo()

        # Use enhanced BackfillStats for comprehensive monitoring
        self.stats = BackfillStats()

        # Rate limiting configuration
        self.request_delay = 3.6  # 3.6 seconds = 1000 requests/hour for Tiingo

        # Legacy stats for compatibility
        self.legacy_stats = {
            'total_instruments': 0,
            'processed_instruments': 0,
            'total_records': 0,
            'api_calls': 0,
            'errors': 0,
            'skipped_instruments': 0
        }

        # Initialize Prometheus metrics if available
        if PROMETHEUS_AVAILABLE:
            self.backfill_symbols_processed = Counter(
                'ats_daily_prices_backfill_symbols_processed_total',
                'Total number of symbols processed during daily prices backfill',
                ['vendor', 'environment']
            )
            self.backfill_prices_collected = Counter(
                'ats_daily_prices_backfill_prices_collected_total',
                'Total number of price records collected during backfill',
                ['vendor', 'environment']
            )
            self.backfill_api_calls = Counter(
                'ats_daily_prices_backfill_api_calls_total',
                'Total number of API calls made during backfill',
                ['vendor', 'environment', 'status']
            )
            self.backfill_duration_seconds = Histogram(
                'ats_daily_prices_backfill_duration_seconds',
                'Duration of daily prices backfill operations in seconds',
                ['vendor', 'environment']
            )
            self.backfill_success_rate = Gauge(
                'ats_daily_prices_backfill_success_rate',
                'Success rate of daily prices backfill operations (0.0 to 1.0)',
                ['vendor', 'environment']
            )
        else:
            self.backfill_symbols_processed = None
            self.backfill_prices_collected = None
            self.backfill_api_calls = None
            self.backfill_duration_seconds = None
            self.backfill_success_rate = None

        logger.info(f"📊 Tiingo 30-Year Backfiller initialized")
        logger.info(f"   Rate limit: {3600/self.request_delay:.1f} requests/hour")

    async def get_database_connection(self):
        """Get database connection using shared utility."""
        return await get_vendor_database_connection()

    async def get_instruments_for_backfill(self, conn, limit=None):
        """Get active instruments from instruments table."""
        # Auto-detect table prefix based on environment
        env = os.getenv('ENV_TYPE', 'intg').lower()
        table_prefix = 'intg_' if env == 'intg' else 'dev_'

        # Check if TARGET_SYMBOLS is specified
        target_symbols = os.getenv('TARGET_SYMBOLS')
        if target_symbols:
            target_list = [s.strip().upper() for s in target_symbols.split(',')]
            symbols_clause = "AND symbol = ANY($1)"
            instruments = await conn.fetch(f"""
                SELECT id, symbol, name, exchange, active
                FROM {table_prefix}instrument
                WHERE active = true
                  AND symbol IS NOT NULL
                  AND symbol != ''
                  AND exchange IN ('NASDAQ', 'NYSE', 'NYSE ARCA', 'BATS', 'XNYS', 'NYSE MKT', 'XNAS', 'AMEX', 'NYSE NAT')
                  {symbols_clause}
                ORDER BY symbol
            """, target_list)
            logger.info(f"🎯 Targeted symbols: {target_list}")
        else:
            limit_clause = f"LIMIT {limit}" if limit else ""
            instruments = await conn.fetch(f"""
                SELECT id, symbol, name, exchange, active
                FROM {table_prefix}instrument
                WHERE active = true
                  AND symbol IS NOT NULL
                  AND symbol != ''
                  AND exchange IN ('NASDAQ', 'NYSE', 'NYSE ARCA', 'BATS', 'XNYS', 'NYSE MKT', 'XNAS', 'AMEX', 'NYSE NAT')
                ORDER BY symbol
                {limit_clause}
            """)

        self.legacy_stats['total_instruments'] = len(instruments)
        logger.info(f"📊 Found {len(instruments)} instruments for 30-year backfill")
        return instruments

    def download_tiingo_daily_prices(self, symbol, start_date, end_date):
        """Download daily prices from Tiingo API."""
        url = f"{self.base_url}/{symbol}/prices"
        params = {
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'format': 'json',
            'token': self.api_key
        }

        try:
            response = requests.get(url, params=params)
            self.legacy_stats['api_calls'] += 1

            if response.status_code == 200:
                data = response.json()
                logger.debug(f"✅ Downloaded {len(data)} records for {symbol}")
                return data
            elif response.status_code == 404:
                logger.debug(f"⚠️ No data available for {symbol}")
                return []
            elif response.status_code == 429:
                logger.warning(f"⚠️ Rate limit hit for {symbol}, waiting...")
                time.sleep(60)  # Wait 1 minute
                return self.download_tiingo_daily_prices(symbol, start_date, end_date)
            else:
                logger.error(f"❌ Tiingo API error for {symbol}: {response.status_code}")
                self.legacy_stats['errors'] += 1
                return []

        except Exception as e:
            logger.error(f"❌ Error downloading {symbol}: {e}")
            self.legacy_stats['errors'] += 1
            return []

    async def insert_daily_prices_idempotent(self, conn, instrument_id, symbol, prices):
        """Insert daily prices with idempotent UPSERT operations."""
        if not prices:
            return 0

        # Prepare data for insertion
        rows = []
        for price in prices:
            try:
                # Parse Tiingo date format
                if isinstance(price['date'], str):
                    date_val = datetime.strptime(price['date'][:10], '%Y-%m-%d').date()
                else:
                    date_val = price['date']

                rows.append((
                    date_val,
                    symbol,
                    price.get('open'),
                    price.get('high'),
                    price.get('low'),
                    price.get('close'),
                    price.get('volume', 0),
                    instrument_id
                ))
            except Exception as e:
                logger.error(f"❌ Error processing price record for {symbol}: {e}")
                continue

        if not rows:
            return 0

        # Insert with idempotent UPSERT (using existing Tiingo table schema)
        table_name = get_vendor_table_name('daily_price', 'tiingo')

        try:
            result = await conn.executemany(f"""
                INSERT INTO {table_name}
                (date, symbol, open, high, low, close, volume, instrument_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (date, instrument_id) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
            """, rows)

            logger.info(f"💾 Inserted {len(rows)} price records for {symbol}")
            self.legacy_stats['total_records'] += len(rows)
            return len(rows)

        except Exception as e:
            logger.error(f"❌ Database error inserting prices for {symbol}: {e}")
            self.legacy_stats['errors'] += 1
            return 0

    async def check_existing_data(self, conn, instrument_id, start_date, end_date):
        """Check if instrument already has data in the date range."""
        table_name = get_vendor_table_name('daily_price', 'tiingo')

        count = await conn.fetchval(f"""
            SELECT COUNT(*) FROM {table_name}
            WHERE instrument_id = $1 AND date BETWEEN $2 AND $3
        """, instrument_id, start_date, end_date)

        return count

    async def backfill_instrument(self, conn, instrument, start_date, end_date, skip_existing=True):
        """Backfill daily prices for a single instrument."""
        instrument_id = instrument['id']
        symbol = instrument['symbol']

        try:
            # Check if we should skip existing data
            if skip_existing:
                existing_count = await self.check_existing_data(conn, instrument_id, start_date, end_date)
                if existing_count > 0:
                    logger.info(f"⏭️ Skipping {symbol} - already has {existing_count} records")
                    self.legacy_stats['skipped_instruments'] += 1
                    return 0

            logger.info(f"📈 Processing {symbol} (ID: {instrument_id}) for 30-year backfill...")

            # Download data from Tiingo
            prices = self.download_tiingo_daily_prices(symbol, start_date, end_date)

            if not prices:
                logger.warning(f"⚠️ No price data for {symbol}")
                return 0

            # Insert data idempotently
            inserted_count = await self.insert_daily_prices_idempotent(conn, instrument_id, symbol, prices)

            logger.info(f"✅ Completed {symbol}: {inserted_count} records inserted")
            self.legacy_stats['processed_instruments'] += 1

            # Rate limiting delay
            time.sleep(self.request_delay)

            return inserted_count

        except Exception as e:
            logger.error(f"❌ Failed to process {symbol}: {e}")
            self.legacy_stats['errors'] += 1
            return 0

    async def run_backfill(self, start_date, end_date, limit=None, skip_existing=True):
        """Run the complete 30-year backfill process."""
        logger.info("🚀 Starting Tiingo 30-year daily price backfill...")
        logger.info(f"📅 Date range: {start_date} to {end_date}")

        conn = await self.get_database_connection()

        try:
            # Get instruments to process
            instruments = await self.get_instruments_for_backfill(conn, limit)

            if not instruments:
                logger.warning("❌ No instruments found for backfill")
                return

            logger.info(f"📊 Processing {len(instruments)} instruments")

            # Process each instrument
            for i, instrument in enumerate(instruments, 1):
                try:
                    await self.backfill_instrument(conn, instrument, start_date, end_date, skip_existing)

                    # Progress logging
                    if i % 100 == 0 or i == len(instruments):
                        progress = (i / len(instruments)) * 100
                        logger.info(f"📊 Progress: {i:,}/{len(instruments):,} ({progress:.1f}%) - "
                                  f"{self.legacy_stats['total_records']:,} total records")

                except Exception as e:
                    logger.error(f"❌ Critical error processing instrument {instrument.get('symbol', 'unknown')}: {e}")
                    continue

        finally:
            await conn.close()

    def log_final_summary(self):
        """Log comprehensive final summary."""
        logger.info("=" * 80)
        logger.info("🎉 TIINGO 30-YEAR DAILY PRICE BACKFILL COMPLETE")
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
    parser = argparse.ArgumentParser(description="Tiingo 30-year daily price backfill")
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

    args = parser.parse_args()

    # Set up logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    try:
        # Get Tiingo API key using shared utilities
        tiingo_api_key = get_tiingo_api_key()
        if not tiingo_api_key:
            logger.error("❌ TIINGO_API_KEY not found. Please set environment variable or configure in gin files.")
            sys.exit(1)

        logger.info("✅ Tiingo API key found")

        # Calculate date range
        if args.start_date:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        else:
            start_date = (datetime.now() - timedelta(days=365 * args.years)).date()

        if args.end_date:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        else:
            end_date = datetime.now().date()

        logger.info(f"📅 Backfilling Tiingo prices from {start_date} to {end_date}")

        # Initialize backfiller
        backfiller = Tiingo30YearBackfiller(tiingo_api_key)

        # Run backfill
        await backfiller.run_backfill(
            start_date, end_date,
            limit=args.limit,
            skip_existing=args.skip_existing
        )

        # Log final summary
        backfiller.log_final_summary()

        # Log comprehensive statistics from shared framework
        backfiller.stats.log_progress(logger)

        logger.info("✅ Tiingo 30-year daily price backfill complete")

    except Exception as e:
        logger.error(f"❌ Failed to run Tiingo 30-year backfill: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())