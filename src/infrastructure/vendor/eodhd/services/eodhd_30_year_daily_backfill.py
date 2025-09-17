#!/usr/bin/env python3
"""
EODHD 30-Year Daily Price Backfill

Comprehensive backfill of historical daily price data for all active instruments
using EODHD API with 30-year historical depth. Based on working patterns
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
    from prometheus_client import Counter, Gauge, Histogram, push_to_gateway
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logging.warning("prometheus_client not installed. Metrics will not be pushed to Prometheus.")

from shared.utils.vendor_api_keys import get_eodhd_api_key
from shared.utils.backfill_framework import BackfillStats, VendorRateLimiters, get_vendor_database_connection, get_vendor_table_name

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("eodhd_30_year_daily_backfill")

class EODHD30YearBackfiller:
    """
    EODHD 30-year daily price backfiller with idempotent operations.

    Features:
    - 30-year historical data collection
    - Idempotent UPSERT operations
    - Rate limiting (20 requests/minute for free tier)
    - Resume capability with existing data detection
    - Uses dev_instrument table for symbol list
    """

    def __init__(self, api_key: str = None):
        self.api_key = api_key or get_eodhd_api_key()
        self.base_url = "https://eodhd.com/api/eod"

        # Use shared rate limiter for consistent behavior
        self.rate_limiter = VendorRateLimiters.eodhd()

        # Use enhanced BackfillStats for comprehensive monitoring
        self.stats = BackfillStats()

        # Rate limiting configuration
        self.request_delay = 3.0  # 3 seconds = 20 requests/minute for EODHD free tier

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

        logger.info(f"📊 EODHD 30-Year Backfiller initialized")
        logger.info(f"   Rate limit: {60/self.request_delay:.1f} requests/minute")

    async def get_database_connection(self):
        """Get database connection using shared utility."""
        return await get_vendor_database_connection()

    async def ensure_table_exists(self, conn):
        """Ensure EODHD table exists."""
        table_name = get_vendor_table_name('daily_price', 'eodhd')

        try:
            await conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    date DATE NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    open NUMERIC(12,4),
                    high NUMERIC(12,4),
                    low NUMERIC(12,4),
                    close NUMERIC(12,4),
                    adjusted_close NUMERIC(12,4),
                    volume BIGINT,
                    instrument_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(date, instrument_id)
                )
            """)
            logger.info("✅ EODHD table ready")
        except Exception as e:
            logger.error(f"❌ Failed to ensure table exists: {e}")
            raise

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

    def download_eodhd_daily_prices(self, symbol, start_date, end_date):
        """Download daily prices from EODHD API."""
        url = f"{self.base_url}/{symbol}.US"
        params = {
            'from': start_date.strftime('%Y-%m-%d'),
            'to': end_date.strftime('%Y-%m-%d'),
            'period': 'd',
            'fmt': 'json',
            'api_token': self.api_key
        }

        try:
            response = requests.get(url, params=params)
            self.legacy_stats['api_calls'] += 1

            # Track API calls in Prometheus if available
            if PROMETHEUS_AVAILABLE:
                env = os.getenv('ENV_TYPE', 'intg').lower()
                status = 'success' if response.status_code == 200 else 'error'
                self.backfill_api_calls.labels(
                    vendor='eodhd',
                    environment=env,
                    status=status
                ).inc()

            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list):
                    logger.debug(f"✅ Downloaded {len(data)} records for {symbol}")
                    return data
                else:
                    logger.debug(f"⚠️ Unexpected response format for {symbol}")
                    return []
            elif response.status_code == 404:
                logger.debug(f"⚠️ No data available for {symbol}")
                return []
            elif response.status_code == 429:
                logger.warning(f"⚠️ Rate limit hit for {symbol}, waiting...")
                time.sleep(60)  # Wait 1 minute
                return self.download_eodhd_daily_prices(symbol, start_date, end_date)
            else:
                logger.error(f"❌ EODHD API error for {symbol}: {response.status_code}")
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
                # Parse EODHD date format
                date_val = datetime.strptime(price['date'], '%Y-%m-%d').date()

                rows.append((
                    date_val,
                    symbol,
                    price.get('open'),
                    price.get('high'),
                    price.get('low'),
                    price.get('close'),
                    price.get('adjusted_close'),
                    price.get('volume', 0),
                    instrument_id
                ))
            except Exception as e:
                logger.error(f"❌ Error processing price record for {symbol}: {e}")
                continue

        if not rows:
            return 0

        # Insert with idempotent UPSERT
        try:
            # Insert with idempotent UPSERT
            table_name = get_vendor_table_name('daily_price', 'eodhd')

            result = await conn.executemany(f"""
                INSERT INTO {table_name}
                (date, symbol, open, high, low, close, adjusted_close, volume, instrument_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (date, instrument_id) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    adjusted_close = EXCLUDED.adjusted_close,
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
        table_name = get_vendor_table_name('daily_price', 'eodhd')

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

            # Download data from EODHD
            prices = self.download_eodhd_daily_prices(symbol, start_date, end_date)

            if not prices:
                logger.warning(f"⚠️ No price data for {symbol}")
                return 0

            # Insert data idempotently
            inserted_count = await self.insert_daily_prices_idempotent(conn, instrument_id, symbol, prices)

            logger.info(f"✅ Completed {symbol}: {inserted_count} records inserted")
            self.legacy_stats['processed_instruments'] += 1

            # Update Prometheus metrics if available
            if PROMETHEUS_AVAILABLE:
                env = os.getenv('ENV_TYPE', 'intg').lower()
                self.backfill_symbols_processed.labels(
                    vendor='eodhd',
                    environment=env
                ).inc()

                self.backfill_prices_collected.labels(
                    vendor='eodhd',
                    environment=env
                ).inc(inserted_count)

            # Rate limiting delay
            time.sleep(self.request_delay)

            return inserted_count

        except Exception as e:
            logger.error(f"❌ Failed to process {symbol}: {e}")
            self.legacy_stats['errors'] += 1
            return 0

    async def run_backfill(self, start_date, end_date, limit=None, skip_existing=True):
        """Run the complete 30-year backfill process."""
        logger.info("🚀 Starting EODHD 30-year daily price backfill...")
        logger.info(f"📅 Date range: {start_date} to {end_date}")

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
        logger.info("🎉 EODHD 30-YEAR DAILY PRICE BACKFILL COMPLETE")
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
    parser = argparse.ArgumentParser(description="EODHD 30-year daily price backfill")
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--limit', type=int, default=int(os.getenv('LIMIT', '0')) or None,
                       help='Limit number of instruments to process')
    parser.add_argument('--years', type=int, default=int(os.getenv('YEARS', '30')),
                       help='Number of years of historical data to fetch (default: 30)')
    parser.add_argument('--start_date', type=str, default=None,
                       help='Start date (YYYY-MM-DD), overrides --years')
    parser.add_argument('--end_date', type=str, default=None,
                       help='End date (YYYY-MM-DD), defaults to today')
    parser.add_argument('--skip_existing', action='store_true', default=True,
                       help='Skip instruments that already have price data')

    args = parser.parse_args()

    # Set up logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    try:
        # Get EODHD API key using shared utilities
        eodhd_api_key = get_eodhd_api_key()
        if not eodhd_api_key:
            logger.error("❌ EODHD_API_KEY not found. Please set environment variable or configure in gin files.")
            sys.exit(1)

        logger.info("✅ EODHD API key found")

        # Calculate date range
        if args.start_date:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        else:
            start_date = (datetime.now() - timedelta(days=365 * args.years)).date()

        if args.end_date:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        else:
            end_date = datetime.now().date()

        logger.info(f"📅 Backfilling EODHD prices from {start_date} to {end_date}")

        # Initialize backfiller
        backfiller = EODHD30YearBackfiller(eodhd_api_key)

        # Run backfill with timing
        backfill_start_time = time.time()

        await backfiller.run_backfill(
            start_date, end_date,
            limit=args.limit,
            skip_existing=args.skip_existing
        )

        # Record backfill duration
        if PROMETHEUS_AVAILABLE and backfiller.backfill_duration_seconds:
            env = os.getenv('ENV_TYPE', 'intg').lower()
            duration = time.time() - backfill_start_time
            backfiller.backfill_duration_seconds.labels(
                vendor='eodhd',
                environment=env
            ).observe(duration)

        # Log final summary
        backfiller.log_final_summary()

        # Log comprehensive statistics from shared framework
        backfiller.stats.log_progress(logger)

        # Update final Prometheus metrics if available
        if PROMETHEUS_AVAILABLE and backfiller.backfill_success_rate:
            env = os.getenv('ENV_TYPE', 'intg').lower()
            success_rate = backfiller.legacy_stats['processed_instruments'] / max(1, backfiller.legacy_stats['total_instruments'])
            backfiller.backfill_success_rate.labels(
                vendor='eodhd',
                environment=env
            ).set(success_rate)

            # Push metrics to Prometheus gateway if configured
            try:
                gateway = os.getenv('PROMETHEUS_GATEWAY', 'localhost:9091')
                job_name = 'daily-prices-backfill-eodhd'
                push_to_gateway(gateway, job=job_name, registry=None,
                              grouping_key={'vendor': 'eodhd', 'environment': env})
                logger.info(f"📊 Pushed metrics to Prometheus gateway: {gateway}")
            except Exception as e:
                logger.debug(f"Could not push to Prometheus gateway: {e}")

        logger.info("✅ EODHD 30-year daily price backfill complete")

    except Exception as e:
        logger.error(f"❌ Failed to run EODHD 30-year backfill: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())