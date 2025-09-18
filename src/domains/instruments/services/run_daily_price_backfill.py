#!/usr/bin/env python3
"""
Daily price backfill script for ats-dev environment.
Uses existing daily_price_polygon.py infrastructure with fixes for dev environment.
"""

import os
import asyncio
import argparse
import gin
import sys
import logging
import datetime as dt
from core.shared.utils.environment import Environment, EnvironmentType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("run_daily_price_backfill")

async def run_backfill(env, start_date, end_date, limit=None, tickers=None):
    """
    Run daily price backfill using existing infrastructure.

    Args:
        env: Environment instance
        start_date: Start date for backfill (datetime.date)
        end_date: End date for backfill (datetime.date)
        limit: Optional limit on number of instruments to process
        tickers: Optional list of specific tickers to process
    """
    from vendor.polygon.dao.daily_price_polygon_dao import DailyPricesPolygonDAO
    from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO
    from domains.market_data.services.eod.daily_price_polygon import download_prices_polygon, insert_prices

    # Initialize DAOs
    prices_dao = DailyPricesPolygonDAO(env)
    InstrumentXrefsDAO(env)

    # Get Polygon API key
    polygon_api_key = os.environ.get("POLYGON_API_KEY") or env.get_api_key('polygon')
    if not polygon_api_key:
        logger.error("No Polygon API key found. Set POLYGON_API_KEY environment variable or configure in Gin file.")
        return False

    logger.info("Polygon API key found")

    # Get list of instruments to process
    if tickers:
        # Process specific tickers
        ticker_list = [t.strip().upper() for t in tickers.split(',') if t.strip()]
        logger.info(f"Processing specific tickers: {ticker_list}")
    else:
        # Get instruments from database
        from core.shared.utils.database import Database
        pool = await Database.create_connection_pool(max_retries=3, initial_delay=1.0, timeout=10.0)

        try:
            async with pool.acquire() as conn:
                # Get instruments with their vendor symbols from Polygon vendor
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

                ticker_list = [inst['vendor_symbol'] for inst in instruments]
                logger.info(f"Found {len(ticker_list)} instruments to process")

        finally:
            await pool.close()

    if not ticker_list:
        logger.warning("No tickers found to process")
        return False

    # Process each ticker
    total_success = 0
    total_fail = 0

    for ticker in ticker_list:
        try:
            # Resolve instrument_id - look up by Polygon vendor instead of 'ticker' vendor
            instrument_id = await resolve_instrument_id_by_polygon_vendor(env, ticker)

            if instrument_id is None:
                logger.warning(f"Could not resolve instrument_id for ticker {ticker}. Skipping.")
                total_fail += 1
                continue

            logger.info(f"Processing {ticker} (instrument_id={instrument_id})...")

            # Download prices from Polygon
            prices = download_prices_polygon(
                ticker,
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d'),
                polygon_api_key
            )

            if not prices:
                logger.warning(f"No prices fetched for {ticker}")
                total_fail += 1
                continue

            # Insert prices using existing infrastructure
            await insert_prices(prices, instrument_id, None, prices_dao, env=env)
            logger.info(f"Inserted {len(prices)} prices for {ticker}")
            total_success += 1

        except Exception as e:
            logger.error(f"Failed to process {ticker}: {e}")
            total_fail += 1

    logger.info(f"Backfill complete. Success: {total_success}, Failures: {total_fail}")
    return total_success > 0

async def resolve_instrument_id_by_polygon_vendor(env, symbol):
    """
    Resolve instrument_id for a symbol using the Polygon vendor.
    This fixes the issue where xrefs_dao looks for 'ticker' vendor but we have 'polygon'.
    """
    from core.shared.utils.database import Database

    pool = await Database.create_connection_pool(max_retries=3, initial_delay=1.0, timeout=10.0)

    try:
        async with pool.acquire() as conn:
            # Look up instrument_id by joining with polygon vendor
            row = await conn.fetchrow(f"""
                SELECT x.instrument_id
                FROM {env.get_table_name('instrument_xrefs')} x
                JOIN dev_vendors v ON x.vendor_id = v.id
                WHERE v.name = 'polygon' AND x.vendor_symbol = $1
            """, symbol)

            return row['instrument_id'] if row else None

    finally:
        await pool.close()

async def main():
    parser = argparse.ArgumentParser(description="Run daily price backfill for ats-dev")
    parser.add_argument('--environment', type=str, default='dev', choices=['test', 'intg', 'prod', 'dev'],
                       help='Environment to use (default: dev)')
    parser.add_argument('--gin_config', type=str, default=None, help='Path to Gin config file (optional)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of instruments to process')
    parser.add_argument('--tickers', type=str, default=None, help='Comma-separated list of specific tickers to process')
    parser.add_argument('--years', type=int, default=5, help='Number of years of historical data to fetch (default: 5)')
    parser.add_argument('--start_date', type=str, default=None, help='Start date (YYYY-MM-DD), overrides --years')
    parser.add_argument('--end_date', type=str, default=None, help='End date (YYYY-MM-DD), defaults to today')
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
        pass

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

        # Run backfill
        success = await run_backfill(env, start_date, end_date, limit=args.limit, tickers=args.tickers)

        if success:
            logger.info("Daily price backfill completed successfully")
        else:
            logger.error("Daily price backfill failed")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Failed to run daily price backfill: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())