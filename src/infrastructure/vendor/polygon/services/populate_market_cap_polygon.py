#!/usr/bin/env python3
"""
Market cap population script for Polygon data source.
Fetches market cap data from Polygon API and populates daily_market_cap table.
"""

import os
import asyncio
import argparse
import logging
import requests
import time
from datetime import date
from typing import Optional, List, Dict, Any

from shared.utils.environment import Environment, EnvironmentType
from shared.utils.vendor_api_keys import get_polygon_api_key
from shared.utils.backfill_framework import BackfillStats, VendorRateLimiters
from infrastructure.database.repositories.daily_market_cap_dao import DailyMarketCapDAO

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("populate_market_cap_polygon")

class PolygonMarketCapFetcher:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or get_polygon_api_key()
        self.base_url = "https://api.polygon.io"
        self.stats = BackfillStats()
        self.rate_limiter = VendorRateLimiters.polygon_free()

    def fetch_ticker_details(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch ticker details including market cap from Polygon API.
        """
        url = f"{self.base_url}/v3/reference/tickers/{symbol}"
        params = {"apikey": self.api_key}

        start_time = time.time()
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            response_time = time.time() - start_time

            if data.get("status") == "OK" and "results" in data:
                self.stats.record_api_call(success=True, response_time=response_time)
                return data["results"]
            else:
                logger.warning(f"No data returned for {symbol}: {data}")
                self.stats.record_api_call(success=False, response_time=response_time)
                return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data for {symbol}: {e}")
            self.stats.record_api_call(success=False, response_time=time.time() - start_time)
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching {symbol}: {e}")
            self.stats.record_api_call(success=False, response_time=time.time() - start_time)
            return None

async def populate_market_cap_from_polygon(
    env: Environment,
    api_key: str,
    limit: Optional[int] = None,
    symbols: Optional[List[str]] = None,
    batch_size: int = 50,
    db_delay: float = 0.1
) -> bool:
    """
    Populate market cap data from Polygon API with batching and rate limiting.

    Args:
        env: Environment instance
        api_key: Polygon API key
        limit: Optional limit on number of instruments to process
        symbols: Optional list of specific symbols to process
        batch_size: Number of instruments to process per batch (default: 50)
        db_delay: Delay between database operations in seconds (default: 0.1)
    """
    fetcher = PolygonMarketCapFetcher(api_key)

    # Get instruments to process
    if symbols:
        instrument_symbols = symbols
        logger.info(f"Processing specific symbols: {instrument_symbols}")
    else:
        # Get instruments with Polygon vendor symbols
        from shared.utils.database import Database
        pool = await Database.create_connection_pool(max_retries=3, initial_delay=1.0, timeout=30.0)

        try:
            async with pool.acquire() as conn:
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

                instrument_symbols = [(inst['vendor_symbol'], inst['id']) for inst in instruments]
                logger.info(f"Found {len(instrument_symbols)} instruments to process")

        finally:
            await pool.close()

    if not instrument_symbols:
        logger.warning("No instruments found to process")
        return False

    # Process in batches to avoid connection exhaustion
    total_success = 0
    total_fail = 0
    current_date = date.today()

    # Split into batches
    for batch_start in range(0, len(instrument_symbols), batch_size):
        batch_end = min(batch_start + batch_size, len(instrument_symbols))
        batch = instrument_symbols[batch_start:batch_end]

        logger.info(f"Processing batch {batch_start//batch_size + 1}: items {batch_start+1}-{batch_end} of {len(instrument_symbols)}")

        # Create connection pool for this batch
        from shared.utils.database import Database
        pool = await Database.create_connection_pool(max_retries=3, initial_delay=1.0, timeout=30.0)
        DailyMarketCapDAO(env)
        batch_records = []

        try:
            for item in batch:
                if isinstance(item, tuple):
                    symbol, instrument_id = item
                else:
                    symbol = item
                    # Resolve instrument_id for symbol-only processing
                    instrument_id = await resolve_instrument_id_by_polygon_vendor(env, symbol)
                    if instrument_id is None:
                        logger.warning(f"Could not resolve instrument_id for symbol {symbol}")
                        total_fail += 1
                        continue

                try:
                    logger.info(f"Processing {symbol} (instrument_id={instrument_id})...")

                    # Fetch ticker details from Polygon
                    ticker_data = fetcher.fetch_ticker_details(symbol)

                    if not ticker_data:
                        logger.warning(f"No ticker data returned for {symbol}")
                        total_fail += 1
                        continue

                    # Extract market cap
                    market_cap = ticker_data.get('market_cap')
                    if market_cap is None:
                        logger.warning(f"No market cap data for {symbol}")
                        total_fail += 1
                        continue

                    # Store for batch insertion
                    batch_records.append({
                        'date': current_date,
                        'instrument_id': instrument_id,
                        'market_cap': market_cap,
                        'symbol': symbol
                    })

                    # Use shared rate limiter for API calls
                    await fetcher.rate_limiter.wait_if_needed()

                except Exception as e:
                    logger.error(f"Failed to process {symbol}: {e}")
                    total_fail += 1

            # Batch insert all records for this batch
            if batch_records:
                try:
                    async with pool.acquire() as conn:
                        for record in batch_records:
                            await conn.execute(f"""
                                INSERT INTO {env.get_table_name('daily_market_cap')}
                                (date, instrument_id, market_cap)
                                VALUES ($1, $2, $3)
                                ON CONFLICT (instrument_id, date) DO UPDATE SET
                                market_cap = EXCLUDED.market_cap
                            """, record['date'], record['instrument_id'], record['market_cap'])

                    total_success += len(batch_records)
                    logger.info(f"Batch inserted {len(batch_records)} market cap records")

                    # Rate limiting after batch database operation
                    await asyncio.sleep(db_delay)

                except Exception as e:
                    logger.error(f"Failed to batch insert records: {e}")
                    total_fail += len(batch_records)

        finally:
            await pool.close()

        # Delay between batches to further reduce load
        if batch_end < len(instrument_symbols):
            logger.info(f"Batch {batch_start//batch_size + 1} complete. Waiting 2 seconds before next batch...")
            await asyncio.sleep(2.0)

    logger.info(f"Market cap population complete. Success: {total_success}, Failures: {total_fail}")
    return total_success > 0

async def resolve_instrument_id_by_polygon_vendor(env: Environment, symbol: str) -> Optional[int]:
    """
    Resolve instrument_id for a symbol using the Polygon vendor.
    """
    from shared.utils.database import Database

    pool = await Database.create_connection_pool(max_retries=3, initial_delay=1.0, timeout=10.0)

    try:
        async with pool.acquire() as conn:
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
    parser = argparse.ArgumentParser(description="Populate market cap data from Polygon")
    parser.add_argument('--environment', type=str, default='dev', choices=['test', 'intg', 'prod', 'dev'],
                       help='Environment to use (default: dev)')
    parser.add_argument('--gin_config', type=str, default=None, help='Path to Gin config file (optional)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of instruments to process')
    parser.add_argument('--symbols', type=str, default=None, help='Comma-separated list of specific symbols to process')
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

    # Get Polygon API key using shared utilities
    polygon_api_key = get_polygon_api_key()
    if not polygon_api_key:
        logger.error("POLYGON_API_KEY not found. Please set environment variable or configure in gin files.")
        return 1

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

        # Run market cap population
        fetcher = PolygonMarketCapFetcher(polygon_api_key)

        success = await populate_market_cap_from_polygon(
            env,
            polygon_api_key,
            limit=args.limit,
            symbols=symbol_list,
            batch_size=50,  # Process 50 instruments per batch
            db_delay=0.2    # 200ms delay between database operations
        )

        # Log comprehensive statistics
        fetcher.stats.log_progress(logger)

        if success:
            logger.info("Market cap population completed successfully")
            return 0
        else:
            logger.error("Market cap population failed")
            return 1

    except Exception as e:
        logger.error(f"Failed to run market cap population: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)