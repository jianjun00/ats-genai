#!/usr/bin/env python3
"""
Compute market cap from outstanding shares and daily prices.
This script populates the daily_market_cap table using shares_outstanding * close_price.
"""

import os
import asyncio
import argparse
import logging
from datetime import date, timedelta
from typing import Optional, List

from src.core.shared.utils.environment import Environment, EnvironmentType

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("compute_market_cap_from_shares")


async def compute_and_populate_market_cap(
    env: Environment,
    limit: Optional[int] = None,
    symbols: Optional[List[str]] = None,
    days_back: int = 30,
    batch_size: int = 100
) -> bool:
    """
    Compute market cap from shares outstanding and daily prices.

    Args:
        env: Environment instance
        limit: Optional limit on number of instruments to process
        symbols: Optional list of specific symbols to process
        days_back: Number of days back to compute market cap for
        batch_size: Batch size for database operations
    """
    from src.core.shared.utils.database import Database

    # Calculate date range
    end_date = date.today()
    start_date = end_date - timedelta(days=days_back)

    logger.info(f"Computing market cap from {start_date} to {end_date}")

    # Get database connection
    pool = await Database.create_connection_pool(max_retries=3, initial_delay=1.0, timeout=30.0)

    try:
        async with pool.acquire() as conn:
            # Get instruments that have daily prices data
            if symbols:
                # Process specific symbols
                symbol_list = "', '".join(symbols)
                instruments_query = f"""
                    SELECT DISTINCT p.instrument_id, x.vendor_symbol as symbol, i.symbol as base_symbol
                    FROM {env.get_table_name('daily_price_polygon')} p
                    JOIN {env.get_table_name('instruments')} i ON p.instrument_id = i.id
                    JOIN {env.get_table_name('instrument_xrefs')} x ON i.id = x.instrument_id
                    JOIN {env.get_table_name('vendors')} v ON x.vendor_id = v.id
                    WHERE v.name = 'ticker'
                      AND x.vendor_symbol IN ('{symbol_list}')
                      AND p.date >= $1
                    ORDER BY i.symbol
                """
                params = [start_date]
            else:
                # Get all instruments with price data
                limit_clause = f"LIMIT {limit}" if limit else ""
                instruments_query = f"""
                    SELECT DISTINCT p.instrument_id, x.vendor_symbol as symbol, i.symbol as base_symbol
                    FROM {env.get_table_name('daily_price_polygon')} p
                    JOIN {env.get_table_name('instruments')} i ON p.instrument_id = i.id
                    JOIN {env.get_table_name('instrument_xrefs')} x ON i.id = x.instrument_id
                    JOIN {env.get_table_name('vendors')} v ON x.vendor_id = v.id
                    WHERE v.name = 'ticker'
                      AND p.date >= $1
                    ORDER BY i.symbol
                    {limit_clause}
                """
                params = [start_date]

            instruments = await conn.fetch(instruments_query, *params)
            logger.info(f"Found {len(instruments)} instruments with price data to process")

            if not instruments:
                logger.warning("No instruments found with price data")
                return False

            # Process in batches
            total_success = 0
            total_skipped = 0

            for batch_start in range(0, len(instruments), batch_size):
                batch_end = min(batch_start + batch_size, len(instruments))
                batch = instruments[batch_start:batch_end]

                logger.info(f"Processing batch {batch_start//batch_size + 1}: items {batch_start+1}-{batch_end} of {len(instruments)}")

                batch_records = []

                for inst in batch:
                    instrument_id = inst['instrument_id']
                    symbol = inst['symbol']

                    try:
                        # Get shares outstanding from Polygon API (like the existing code does)
                        shares_outstanding = await get_shares_outstanding_polygon(symbol)

                        if shares_outstanding is None:
                            logger.debug(f"No shares outstanding data for {symbol}, skipping")
                            total_skipped += 1
                            continue

                        # Get daily prices for this instrument in our date range
                        prices_query = f"""
                            SELECT date, close
                            FROM {env.get_table_name('daily_price_polygon')}
                            WHERE instrument_id = $1
                              AND date >= $2
                              AND date <= $3
                              AND close IS NOT NULL
                            ORDER BY date
                        """

                        price_rows = await conn.fetch(prices_query, instrument_id, start_date, end_date)

                        # Compute market cap for each day
                        for price_row in price_rows:
                            market_cap = float(price_row['close']) * shares_outstanding

                            batch_records.append({
                                'date': price_row['date'],
                                'instrument_id': instrument_id,
                                'market_cap': market_cap,
                                'shares_outstanding': shares_outstanding
                            })

                        logger.debug(f"Prepared {len(price_rows)} market cap records for {symbol}")

                    except Exception as e:
                        logger.error(f"Failed to process {symbol}: {e}")
                        total_skipped += 1

                # Batch insert all records
                if batch_records:
                    try:
                        insert_query = f"""
                            INSERT INTO {env.get_table_name('daily_market_cap')}
                            (date, instrument_id, market_cap, shares_outstanding)
                            VALUES ($1, $2, $3, $4)
                            ON CONFLICT (instrument_id, date) DO UPDATE SET
                                market_cap = EXCLUDED.market_cap,
                                shares_outstanding = EXCLUDED.shares_outstanding
                        """

                        for record in batch_records:
                            await conn.execute(
                                insert_query,
                                record['date'],
                                record['instrument_id'],
                                record['market_cap'],
                                record['shares_outstanding']
                            )

                        total_success += len(batch_records)
                        logger.info(f"Batch inserted {len(batch_records)} market cap records")

                    except Exception as e:
                        logger.error(f"Failed to batch insert market cap records: {e}")
                        total_skipped += len(batch_records)

                # Small delay between batches
                await asyncio.sleep(0.1)

    finally:
        await pool.close()

    logger.info(f"Market cap computation complete. Success: {total_success}, Skipped: {total_skipped}")
    return total_success > 0


async def get_shares_outstanding_polygon(symbol: str) -> Optional[int]:
    """
    Get shares outstanding from Polygon API (replicating existing logic).
    """
    import aiohttp

    polygon_api_key = os.environ.get("POLYGON_API_KEY")
    if not polygon_api_key:
        logger.warning("POLYGON_API_KEY not set, cannot fetch shares outstanding")
        return None

    url = f"https://api.polygon.io/v3/reference/tickers/{symbol}"
    params = {"apikey": polygon_api_key}

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    results = data.get('results', {})
                    shares_outstanding = results.get('share_class_shares_outstanding')

                    if shares_outstanding:
                        logger.debug(f"Found shares outstanding for {symbol}: {shares_outstanding:,}")
                        return shares_outstanding
                    else:
                        logger.debug(f"No shares outstanding in API response for {symbol}")
                        return None
                else:
                    logger.debug(f"API request failed for {symbol}: HTTP {response.status}")
                    return None

    except Exception as e:
        logger.error(f"Error fetching shares outstanding for {symbol}: {e}")
        return None


async def main():
    parser = argparse.ArgumentParser(description="Compute market cap from shares outstanding and daily prices")
    parser.add_argument('--environment', type=str, default='dev', choices=['test', 'intg', 'prod', 'dev'],
                       help='Environment to use (default: dev)')
    parser.add_argument('--gin_config', type=str, default=None, help='Path to Gin config file (optional)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of instruments to process')
    parser.add_argument('--symbols', type=str, default=None, help='Comma-separated list of specific symbols to process')
    parser.add_argument('--days_back', type=int, default=30, help='Number of days back to compute (default: 30)')
    parser.add_argument('--batch_size', type=int, default=100, help='Batch size for processing (default: 100)')
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
        return 1

    try:
        import gin
        gin.parse_config_file(gin_config_path)
        logger.info(f"Successfully parsed Gin config: {gin_config_path}")
    except Exception as e:
        logger.error(f"Failed to parse Gin config: {e}")
        return 1

    try:
        # Set environment
        env_type = EnvironmentType(args.environment)
        env = Environment(gin_config_path=gin_config_path, env_type=env_type)
        logger.info(f"Using environment: {env_type}")

        # Parse symbols if provided
        symbol_list = None
        if args.symbols:
            symbol_list = [s.strip().upper() for s in args.symbols.split(',') if s.strip()]

        # Run market cap computation
        success = await compute_and_populate_market_cap(
            env,
            limit=args.limit,
            symbols=symbol_list,
            days_back=args.days_back,
            batch_size=args.batch_size
        )

        if success:
            logger.info("Market cap computation completed successfully")
            return 0
        else:
            logger.error("Market cap computation failed")
            return 1

    except Exception as e:
        logger.error(f"Failed to run market cap computation: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)