#!/usr/bin/env python3
"""
Market cap population script for Tiingo data source.
Fetches market cap data from Tiingo fundamentals API and populates daily_market_cap table.
"""

import os
import asyncio
import argparse
import logging
import aiohttp
import time
from datetime import date
from typing import Optional, List, Dict, Any

from shared.utils.environment import Environment, EnvironmentType
from shared.utils.vendor_api_keys import get_tiingo_api_key
from shared.utils.database_connections import get_database_pool, get_table_name
from shared.utils.backfill_framework import BackfillStats, VendorRateLimiters
from infrastructure.database.repositories.daily_market_cap_dao import DailyMarketCapDAO
from domains.instruments.services.config.service_container import get_instrument_service

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("populate_market_cap_tiingo")

class TiingoMarketCapFetcher:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or get_tiingo_api_key()
        self.base_url = "https://api.tiingo.com"
        self.stats = BackfillStats()
        self.rate_limiter = VendorRateLimiters.tiingo()

    async def fetch_fundamentals(self, session: aiohttp.ClientSession, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch fundamentals data including market cap from Tiingo API.
        Note: Tiingo fundamentals API may require premium subscription.
        """
        # Try fundamentals endpoint first
        fundamentals_url = f"{self.base_url}/tiingo/fundamentals/{symbol}/daily"
        params = {"token": self.api_key}

        try:
            async with session.get(fundamentals_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and len(data) > 0:
                        return data[-1]  # Get most recent fundamentals
                elif response.status == 404:
                    logger.warning(f"Fundamentals not available for {symbol}")
                elif response.status == 403:
                    logger.warning(f"Fundamentals access denied for {symbol} - may require premium subscription")
                else:
                    logger.error(f"Failed to fetch fundamentals for {symbol}: HTTP {response.status}")

        except Exception as e:
            logger.error(f"Error fetching fundamentals for {symbol}: {e}")

        # Fallback: try to get market cap from meta endpoint
        try:
            meta_url = f"{self.base_url}/tiingo/daily/{symbol}"
            params = {"token": self.api_key}

            async with session.get(meta_url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and 'marketCap' in data:
                        return {'marketCap': data['marketCap']}

        except Exception as e:
            logger.error(f"Error fetching meta for {symbol}: {e}")

        return None

    async def calculate_market_cap_from_price(self, session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
        """
        Calculate market cap from current price and shares outstanding.
        This is a fallback when direct market cap is not available.
        """
        try:
            # Get current price and meta information
            meta_url = f"{self.base_url}/tiingo/daily/{symbol}"
            params = {"token": self.api_key}

            async with session.get(meta_url, params=params) as response:
                if response.status != 200:
                    return None

                meta_data = await response.json()

                # Check if shares outstanding is available
                shares_outstanding = meta_data.get('sharesOutstanding')
                if not shares_outstanding:
                    return None

                # Get latest price
                price_url = f"{self.base_url}/tiingo/daily/{symbol}/prices"
                params['startDate'] = (date.today()).strftime('%Y-%m-%d')
                params['endDate'] = (date.today()).strftime('%Y-%m-%d')

                async with session.get(price_url, params=params) as price_response:
                    if price_response.status != 200:
                        return None

                    price_data = await price_response.json()
                    if not price_data or len(price_data) == 0:
                        return None

                    latest_close = price_data[-1].get('close')
                    if latest_close is None:
                        return None

                    # Calculate market cap
                    market_cap = latest_close * shares_outstanding
                    return market_cap

        except Exception as e:
            logger.error(f"Error calculating market cap for {symbol}: {e}")
            return None

async def populate_market_cap_from_tiingo(
    env: Environment,
    api_key: str,
    limit: Optional[int] = None,
    symbols: Optional[List[str]] = None
) -> bool:
    """
    Populate market cap data from Tiingo API.

    Args:
        env: Environment instance
        api_key: Tiingo API key
        limit: Optional limit on number of instruments to process
        symbols: Optional list of specific symbols to process
    """
    market_cap_dao = DailyMarketCapDAO(env)
    # InstrumentService will be used for symbol lookups where needed
    fetcher = TiingoMarketCapFetcher(api_key)

    # Get instruments to process
    if symbols:
        instrument_symbols = symbols
        logger.info(f"Processing specific symbols: {instrument_symbols}")
    else:
        # Get instruments with Tiingo vendor symbols
        from shared.utils.database import Database
        pool = await Database.create_connection_pool(max_retries=3, initial_delay=1.0, timeout=10.0)

        try:
            async with pool.acquire() as conn:
                limit_clause = f"LIMIT {limit}" if limit else ""

                # Try to find Tiingo vendor first, fallback to ticker
                instruments = await conn.fetch(f"""
                    SELECT i.id, i.symbol, x.vendor_symbol
                    FROM {env.get_table_name('instruments')} i
                    JOIN {env.get_table_name('instrument_xrefs')} x ON i.id = x.instrument_id
                    JOIN dev_vendors v ON x.vendor_id = v.id
                    WHERE v.name IN ('tiingo', 'ticker') AND i.is_active = true
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

    # Process each symbol
    total_success = 0
    total_fail = 0
    current_date = date.today()

    async with aiohttp.ClientSession() as session:
        for item in instrument_symbols:
            if isinstance(item, tuple):
                symbol, instrument_id = item
            else:
                symbol = item
                # Resolve instrument_id for symbol-only processing
                instrument_id = await resolve_instrument_id_by_tiingo_vendor(env, symbol)
                if instrument_id is None:
                    logger.warning(f"Could not resolve instrument_id for symbol {symbol}")
                    total_fail += 1
                    continue

            try:
                logger.info(f"Processing {symbol} (instrument_id={instrument_id})...")

                # Fetch fundamentals data from Tiingo
                fundamentals_data = await fetcher.fetch_fundamentals(session, symbol)

                market_cap = None
                if fundamentals_data:
                    # Extract market cap from fundamentals
                    market_cap = fundamentals_data.get('marketCap')

                    # Sometimes market cap is nested under different keys
                    if market_cap is None and 'fundamentalDaily' in fundamentals_data:
                        fund_daily = fundamentals_data['fundamentalDaily']
                        market_cap = fund_daily.get('marketCap')

                # Fallback: calculate from price and shares
                if market_cap is None:
                    logger.info(f"No direct market cap for {symbol}, trying calculation from price...")
                    market_cap = await fetcher.calculate_market_cap_from_price(session, symbol)

                if market_cap is None:
                    logger.warning(f"No market cap data available for {symbol}")
                    total_fail += 1
                    continue

                # Insert market cap data
                await market_cap_dao.insert_market_cap(current_date, instrument_id, market_cap)
                logger.info(f"Inserted market cap ${market_cap:,.0f} for {symbol}")
                total_success += 1

                # Use shared rate limiter
                await self.rate_limiter.wait_if_needed()

            except Exception as e:
                logger.error(f"Failed to process {symbol}: {e}")
                total_fail += 1

    logger.info(f"Market cap population complete. Success: {total_success}, Failures: {total_fail}")
    return total_success > 0

async def resolve_instrument_id_by_tiingo_vendor(env: Environment, symbol: str) -> Optional[int]:
    """
    Resolve instrument_id for a symbol using the Tiingo vendor.
    """
    from shared.utils.database import Database

    pool = await Database.create_connection_pool(max_retries=3, initial_delay=1.0, timeout=10.0)

    try:
        async with pool.acquire() as conn:
            # Try Tiingo vendor first
            row = await conn.fetchrow(f"""
                SELECT x.instrument_id
                FROM {env.get_table_name('instrument_xrefs')} x
                JOIN dev_vendors v ON x.vendor_id = v.id
                WHERE v.name = 'tiingo' AND x.vendor_symbol = $1
            """, symbol)

            if row:
                return row['instrument_id']

            # Fallback to ticker vendor
            row = await conn.fetchrow(f"""
                SELECT x.instrument_id
                FROM {env.get_table_name('instrument_xrefs')} x
                JOIN dev_vendors v ON x.vendor_id = v.id
                WHERE v.name = 'ticker' AND x.vendor_symbol = $1
            """, symbol)

            return row['instrument_id'] if row else None

    finally:
        await pool.close()

async def main():
    parser = argparse.ArgumentParser(description="Populate market cap data from Tiingo")
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

    # Get Tiingo API key using shared utilities
    tiingo_api_key = get_tiingo_api_key()
    if not tiingo_api_key:
        logger.error("TIINGO_API_KEY not found. Please set environment variable or configure in gin files.")
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
        fetcher = TiingoMarketCapFetcher(tiingo_api_key)

        success = await populate_market_cap_from_tiingo(
            env,
            tiingo_api_key,
            limit=args.limit,
            symbols=symbol_list
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