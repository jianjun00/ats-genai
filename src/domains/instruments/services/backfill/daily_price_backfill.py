#!/usr/bin/env python3
"""
Daily price backfill script for ats-dev environment.
Fetches 5 years of daily price data for instruments using Polygon API.
"""

import os
import asyncio
import argparse
import gin
import sys
import logging
import datetime as dt
import time
import requests
from core.platform.config.environment import Environment, EnvironmentType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("daily_price_backfill")

BASE_URL = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}"

def download_prices_polygon(ticker, start, end, api_key):
    """Download daily prices from Polygon API."""
    url = BASE_URL.format(ticker=ticker, start=start, end=end, api_key=api_key)

    try:
        resp = requests.get(url)
        if resp.status_code != 200:
            logger.error(f"Failed to fetch {ticker}: {resp.status_code} {resp.text}")
            return []

        data = resp.json()
        if 'results' not in data:
            logger.warning(f"No results for {ticker}: {data}")
            return []

        return data['results']
    except Exception as e:
        logger.error(f"Error fetching {ticker}: {e}")
        return []

async def insert_daily_price_polygon(pool, env, ticker, instrument_id, prices):
    """Insert daily prices into the database."""
    if not prices:
        return 0

    table_name = env.get_table_name('daily_price_polygon')

    async with pool.acquire() as conn:
        rows = []
        for price in prices:
            try:
                date_val = dt.datetime.utcfromtimestamp(price['t']/1000).date()
                rows.append((
                    date_val,
                    ticker,
                    price['o'],  # open
                    price['h'],  # high
                    price['l'],  # low
                    price['c'],  # close
                    price['v'],  # volume
                    None,        # market_cap (calculated separately)
                    instrument_id
                ))
            except Exception as e:
                logger.error(f"Error processing price data for {ticker}: {e}")
                continue

        if not rows:
            return 0

        # Insert with ON CONFLICT handling
        try:
            await conn.executemany(f"""
                INSERT INTO {table_name}
                (date, symbol, open, high, low, close, volume, market_cap, instrument_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (date, instrument_id) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    market_cap = EXCLUDED.market_cap
            """, rows)

            logger.info(f"Inserted {len(rows)} price records for {ticker}")
            return len(rows)

        except Exception as e:
            logger.error(f"Error inserting prices for {ticker}: {e}")
            return 0

async def get_instruments_to_backfill(pool, env, limit=None):
    """Get list of instruments that need price backfill."""
    async with pool.acquire() as conn:
        # Get instruments with their vendor symbols from Polygon
        limit_clause = f"LIMIT {limit}" if limit else ""

        instruments = await conn.fetch(f"""
            SELECT i.id, i.symbol, x.vendor_symbol
            FROM {env.get_table_name('instruments')} i
            JOIN {env.get_table_name('instrument_xrefs')} x ON i.id = x.instrument_id
            JOIN dev_vendors v ON x.vendor_id = v.id
            WHERE v.name = 'polygon' AND i.is_active = true
            ORDER BY i.symbol
            {limit_clause}
        """)

        return instruments

async def check_existing_prices(pool, env, instrument_id, start_date, end_date):
    """Check how many price records already exist for an instrument."""
    table_name = env.get_table_name('daily_price_polygon')

    async with pool.acquire() as conn:
        count = await conn.fetchval(f"""
            SELECT COUNT(*) FROM {table_name}
            WHERE instrument_id = $1 AND date BETWEEN $2 AND $3
        """, instrument_id, start_date, end_date)

        return count

async def backfill_daily_price_polygon(pool, env, polygon_api_key, start_date, end_date, limit=None, skip_existing=True):
    """
    Backfill daily prices for instruments.

    Args:
        pool: Database connection pool
        env: Environment instance
        polygon_api_key: Polygon API key
        start_date: Start date for backfill
        end_date: End date for backfill
        limit: Optional limit on number of instruments to process
        skip_existing: Whether to skip instruments that already have price data
    """

    # Get instruments to process
    instruments = await get_instruments_to_backfill(pool, env, limit)
    logger.info(f"Found {len(instruments)} instruments to process")

    if not instruments:
        logger.warning("No instruments found for backfill")
        return

    total_processed = 0
    total_prices_inserted = 0

    for instrument in instruments:
        instrument_id = instrument['id']
        symbol = instrument['symbol']
        vendor_symbol = instrument['vendor_symbol']

        try:
            # Check if we should skip this instrument
            if skip_existing:
                existing_count = await check_existing_prices(pool, env, instrument_id, start_date, end_date)
                if existing_count > 0:
                    logger.info(f"Skipping {symbol} - already has {existing_count} price records")
                    continue

            logger.info(f"Processing {symbol} (instrument_id: {instrument_id})...")

            # Download prices from Polygon
            prices = download_prices_polygon(
                vendor_symbol,
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d'),
                polygon_api_key
            )

            if not prices:
                logger.warning(f"No price data returned for {symbol}")
                continue

            # Insert prices into database
            inserted_count = await insert_daily_price_polygon(pool, env, symbol, instrument_id, prices)
            total_prices_inserted += inserted_count
            total_processed += 1

            logger.info(f"Completed {symbol}: {inserted_count} prices inserted")

            # Rate limiting - sleep between API calls
            time.sleep(0.2)  # 5 calls per second max

        except Exception as e:
            logger.error(f"Failed to process {symbol}: {e}")
            continue

    logger.info(f"Backfill complete. Processed {total_processed} instruments, inserted {total_prices_inserted} total prices")

async def main():
    parser = argparse.ArgumentParser(description="Backfill daily prices from Polygon API")
    parser.add_argument('--environment', type=str, default='dev', choices=['test', 'intg', 'prod', 'dev'],
                       help='Environment to use (default: dev)')
    parser.add_argument('--gin_config', type=str, default=None, help='Path to Gin config file (optional)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of instruments to process')
    parser.add_argument('--years', type=int, default=5, help='Number of years of historical data to fetch (default: 5)')
    parser.add_argument('--start_date', type=str, default=None, help='Start date (YYYY-MM-DD), overrides --years')
    parser.add_argument('--end_date', type=str, default=None, help='End date (YYYY-MM-DD), defaults to today')
    parser.add_argument('--skip_existing', action='store_true', default=True, help='Skip instruments that already have price data')
    parser.add_argument('--db_host', type=str, default=None, help='Database host override')
    parser.add_argument('--db_port', type=str, default=None, help='Database port override')
    parser.add_argument('--db_user', type=str, default=None, help='Database user override')
    parser.add_argument('--db_password', type=str, default=None, help='Database password override')
    parser.add_argument('--db_name', type=str, default=None, help='Database name override')

    args = parser.parse_args()

    # Set up logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    # Set database environment variables if provided
    if args.db_host:
        os.environ["DB_HOST"] = args.db_host
    if args.db_port:
        os.environ["DB_PORT"] = args.db_port
    if args.db_user:
        os.environ["DB_USER"] = args.db_user
    if args.db_password:
        os.environ["DB_PASSWORD"] = args.db_password
    if args.db_name:
        os.environ["DB_NAME"] = args.db_name

    # Determine Gin config file
    if args.gin_config:
        gin_config_path = args.gin_config
    else:
        gin_config_map = {
            'test': 'config/app_test.gin',
            'intg': 'config/app_intg.gin',
            'prod': 'config/app_prod.gin',
            'dev': 'config/app_dev.gin',
        }
        gin_config_path = gin_config_map.get(args.environment)

    logger.info(f"Using Gin config: {gin_config_path}")

    if not os.path.exists(gin_config_path):
        logger.error(f"Gin config file not found: {gin_config_path}")
        sys.exit(1)

    try:
        # Import Database before parsing Gin config
        from core.shared.utils.database import Database

        gin.parse_config_file(gin_config_path)
        logger.info(f"Successfully parsed Gin config: {gin_config_path}")
    except Exception as e:
        logger.error(f"Failed to parse Gin config: {e}")
        sys.exit(1)

    try:
        # Set environment
        env_type = EnvironmentType(args.environment)
        env = Environment(gin_config_path=gin_config_path, env_type=env_type)
        logger.info(f"Using environment: {env_type}")

        # Get Polygon API key
        polygon_api_key = os.environ.get("POLYGON_API_KEY") or env.get_api_key('polygon')
        if not polygon_api_key:
            logger.error("No Polygon API key found. Set POLYGON_API_KEY environment variable or configure in Gin file.")
            sys.exit(1)

        logger.info("Polygon API key found")

        # Calculate date range
        if args.start_date:
            start_date = dt.datetime.strptime(args.start_date, '%Y-%m-%d').date()
        else:
            start_date = (dt.datetime.now() - dt.timedelta(days=365 * args.years)).date()

        if args.end_date:
            end_date = dt.datetime.strptime(args.end_date, '%Y-%m-%d').date()
        else:
            end_date = dt.datetime.now().date()

        logger.info(f"Backfilling prices from {start_date} to {end_date}")

        # Create database connection pool
        pool = await Database.create_connection_pool(max_retries=3, initial_delay=1.0, timeout=10.0)
        logger.info("Connected to database")

        # Run backfill
        await backfill_daily_price_polygon(
            pool, env, polygon_api_key, start_date, end_date,
            limit=args.limit, skip_existing=args.skip_existing
        )

        await pool.close()
        logger.info("Daily price backfill complete")

    except Exception as e:
        logger.error(f"Failed to run daily price backfill: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())