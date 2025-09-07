import os
import requests
from dotenv import load_dotenv
from datetime import datetime
from shared.utils.environment import Environment, EnvironmentType
from shared.utils.vendor_api_keys import get_eodhd_api_key
from shared.utils.database_connections import get_database_pool, get_table_name
from shared.utils.backfill_framework import BackfillStats, VendorRateLimiters
import time
import json
import logging
import argparse
import gin
import sys

load_dotenv()

# EODHD API endpoint for exchanges and symbols
BASE_URL = "https://eodhd.com/api/exchanges-list"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("populate_instrument_eodhd")

def parse_date(val):
    if not val:
        return None
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d").date()
    except Exception:
        return None

async def get_exchange_symbols(exchange='US', api_key=None):
    """Get symbols from EODHD exchange-symbol-list API"""
    if not api_key:
        api_key = EODHD_API_KEY

    url = f"https://eodhd.com/api/exchange-symbol-list/{exchange}?api_token={api_key}&fmt=json"

    try:
        logger.info(f"Fetching symbols from exchange: {exchange}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()

        symbols = []
        for item in data:
            code = item.get('Code')
            if code and len(code) <= 10:  # Filter out overly long symbols
                symbols.append({
                    'symbol': code,
                    'name': item.get('Name'),
                    'exchange': item.get('Exchange'),
                    'type': item.get('Type'),
                    'currency': item.get('Currency'),
                    'country': item.get('Country')
                })

        logger.info(f"Retrieved {len(symbols)} symbols from {exchange} exchange")
        return symbols

    except Exception as e:
        logger.error(f"Failed to fetch symbols from exchange {exchange}: {e}")
        return []

async def fetch_fundamental_data(symbol, api_key=None):
    """Fetch fundamental data including IPO date for a symbol"""
    if not api_key:
        api_key = EODHD_API_KEY

    # Ensure symbol has exchange suffix
    if '.' not in symbol:
        symbol = f"{symbol}.US"

    url = f"https://eodhd.com/api/fundamentals/{symbol}?api_token={api_key}&fmt=json"

    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            logger.warning(f"Failed to fetch fundamentals for {symbol}: {response.status_code}")
            return None

        data = response.json()
        general = data.get('General', {})

        return {
            'symbol': symbol.split('.')[0],  # Remove exchange suffix
            'name': general.get('Name'),
            'exchange': general.get('Exchange'),
            'type': general.get('Type'),
            'currency': general.get('CurrencyCode'),
            'ipo_date': general.get('IPODate'),
            'country': general.get('Country'),
            'sector': general.get('Sector'),
            'industry': general.get('Industry'),
            'full_response': data
        }

    except Exception as e:
        logger.warning(f"Error fetching fundamentals for {symbol}: {e}")
        return None

async def fetch_and_store_instruments(start_ticker='', ticker=None, bulk_mode=False, exchange='US'):
    from shared.utils.database import Database

    # Use centralized database connection logic
    try:
        logger.info(f"Creating database connection pool using centralized logic")
        pool = await Database.create_connection_pool(env=env, max_retries=3, initial_delay=1.0, timeout=10.0)
        logger.info("Successfully connected to database")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

    total = 0

    # Create table structure first
    table_name = env.get_table_name('instrument_eodhd')
    async with pool.acquire() as conn:
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                symbol TEXT UNIQUE NOT NULL,
                name TEXT,
                exchange TEXT,
                asset_type TEXT,
                currency TEXT,
                ipo_date DATE,
                country TEXT,
                sector TEXT,
                industry TEXT,
                raw JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    logger.info(f"Created/verified {table_name} table structure")

    if ticker:
        # Handle specific tickers
        tickers = [t.strip() for t in ticker.split(',')]
        logger.info(f"Processing {len(tickers)} specific tickers: {tickers}")

        for symbol in tickers:
            logger.info(f"Fetching fundamentals for: {symbol}")

            # Fetch fundamental data with IPO date
            fundamental_data = await fetch_fundamental_data(symbol, EODHD_API_KEY)

            if fundamental_data:
                # Additional US filtering check
                country = fundamental_data.get('country', '')
                exchange = fundamental_data.get('exchange', '')

                # Filter for US only - check both country and known US exchanges
                US_EXCHANGES = ['US', 'NASDAQ', 'NYSE', 'AMEX', 'NYSE MKT', 'BATS', 'IEX']
                is_us_stock = (country == 'USA' or country == 'US' or
                              exchange in US_EXCHANGES or
                              any(us_ex in str(exchange).upper() for us_ex in ['NYSE', 'NASDAQ']))

                if not is_us_stock:
                    logger.info(f"Skipping {symbol} (non-US stock: country={country}, exchange={exchange})")
                else:
                    logger.info(f"Retrieved US data for {symbol}: {fundamental_data['name']}, IPO: {fundamental_data['ipo_date']}, country: {country}, exchange: {exchange}")
                    await upsert_instrument(pool, fundamental_data)
                    total += 1
            else:
                logger.warning(f"No fundamental data found for {symbol}")

            # Rate limiting
            time.sleep(0.5)

        logger.info(f"Individual ticker processing completed: {total}/{len(tickers)} successful")

    elif bulk_mode:
        # Handle bulk processing with fundamentals API
        logger.info(f"Starting bulk processing for {exchange} exchange")

        # First, get all symbols from exchange
        symbols = await get_exchange_symbols(exchange, EODHD_API_KEY)

        if not symbols:
            logger.error(f"No symbols retrieved from {exchange} exchange")
            await pool.close()
            return

        logger.info(f"Retrieved {len(symbols)} symbols from {exchange} exchange")

        # Filter by start_ticker if provided
        if start_ticker:
            symbols = [s for s in symbols if s['symbol'] >= start_ticker]
            logger.info(f"Filtered to {len(symbols)} symbols starting from {start_ticker}")

        # Process each symbol with fundamentals API
        successful = 0
        failed = 0
        api_calls = 0
        batch_size = 100  # Process in batches for logging

        for i, symbol_info in enumerate(symbols):
            symbol = symbol_info['symbol']

            try:
                # Fetch fundamental data including IPO date
                fundamental_data = await fetch_fundamental_data(symbol, EODHD_API_KEY)
                api_calls += 1

                if fundamental_data:
                    # Additional US filtering check for bulk mode
                    country = fundamental_data.get('country', '')
                    exchange = fundamental_data.get('exchange', '')

                    # Filter for US only - check both country and known US exchanges
                    US_EXCHANGES = ['US', 'NASDAQ', 'NYSE', 'AMEX', 'NYSE MKT', 'BATS', 'IEX']
                    is_us_stock = (country == 'USA' or country == 'US' or
                                  exchange in US_EXCHANGES or
                                  any(us_ex in str(exchange).upper() for us_ex in ['NYSE', 'NASDAQ']))

                    if not is_us_stock:
                        logger.debug(f"Skipping {symbol} (non-US stock: country={country}, exchange={exchange})")
                        failed += 1
                    else:
                        # Merge exchange list data with fundamental data
                        merged_data = {**symbol_info, **fundamental_data}
                        await upsert_instrument(pool, merged_data)
                        successful += 1

                        if fundamental_data['ipo_date']:
                            logger.debug(f"{symbol}: IPO date {fundamental_data['ipo_date']}")
                else:
                    failed += 1
                    logger.debug(f"No fundamental data for {symbol}")

                # Progress logging
                if (i + 1) % batch_size == 0:
                    progress = (i + 1) / len(symbols) * 100
                    logger.info(f"Progress: {i + 1}/{len(symbols)} ({progress:.1f}%) - "
                              f"Success: {successful}, Failed: {failed}, API calls: {api_calls}")

                # Rate limiting - EODHD allows up to 20 requests/minute on free tier
                # We'll use conservative rate limiting: 1 request per 3 seconds = 20/minute
                time.sleep(3.0)

            except Exception as e:
                failed += 1
                logger.error(f"Error processing {symbol}: {e}")

                # Continue processing other symbols
                continue

        total = successful
        logger.info(f"Bulk processing completed: {successful} successful, {failed} failed, {api_calls} API calls")

    else:
        # Legacy mode - just create table structure
        logger.info("Table structure created - no data processing requested")

    logger.info(f"Total instruments processed successfully: {total}")
    await pool.close()

async def upsert_instrument(pool, item):
    """Upsert instrument data with enhanced field mapping"""
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {env.get_table_name('instrument_eodhd')}
            (symbol, name, exchange, asset_type, currency, ipo_date, country, sector, industry, raw, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now())
            ON CONFLICT (symbol) DO UPDATE SET
                name=EXCLUDED.name,
                exchange=EXCLUDED.exchange,
                asset_type=EXCLUDED.asset_type,
                currency=EXCLUDED.currency,
                ipo_date=EXCLUDED.ipo_date,
                country=EXCLUDED.country,
                sector=EXCLUDED.sector,
                industry=EXCLUDED.industry,
                raw=EXCLUDED.raw,
                updated_at=now()
            """,
            item.get('symbol') or item.get('Code'),
            item.get('name') or item.get('Name'),
            item.get('exchange') or item.get('Exchange'),
            item.get('type') or item.get('Type'),
            item.get('currency') or item.get('CurrencyCode'),
            parse_date(item.get('ipo_date') or item.get('IPODate')),
            item.get('country'),
            item.get('sector'),
            item.get('industry'),
            json.dumps(item.get('full_response', item))
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Populate instrument_eodhd from EODHD API.")
    parser.add_argument('--start_ticker', type=str, default='', help='Only update/add instrument_eodhd if symbol > start_ticker (lexical order)')
    parser.add_argument('--environment', type=str, default='dev', choices=['test', 'intg', 'prod', 'dev'], help='Environment to use (test/intg/prod/dev)')
    parser.add_argument('--ticker', type=str, default=None, help='Populate only this ticker (optional, skips bulk)')
    parser.add_argument('--bulk', action='store_true', help='Run bulk processing using fundamentals API for all symbols')
    parser.add_argument('--exchange', type=str, default='US', help='Exchange to process for bulk mode (default: US)')
    parser.add_argument('--gin_config', type=str, default=None, help='Path to Gin config file (optional)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--db_host', type=str, default=None, help='Database host override')
    parser.add_argument('--db_port', type=str, default=None, help='Database port override')
    parser.add_argument('--db_user', type=str, default=None, help='Database user override')
    parser.add_argument('--db_password', type=str, default=None, help='Database password override')
    parser.add_argument('--db_name', type=str, default=None, help='Database name override')
    args = parser.parse_args()

    # Set up logging level based on debug flag
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    # Determine Gin config file if not explicitly provided
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

    # Set database environment variables if provided
    if args.db_host:
        os.environ["DB_HOST"] = args.db_host
        logger.info(f"Setting DB_HOST to {args.db_host}")
    if args.db_port:
        os.environ["DB_PORT"] = args.db_port
        logger.info(f"Setting DB_PORT to {args.db_port}")
    if args.db_user:
        os.environ["DB_USER"] = args.db_user
        logger.info(f"Setting DB_USER to {args.db_user}")
    if args.db_password:
        os.environ["DB_PASSWORD"] = args.db_password
        logger.info(f"Setting DB_PASSWORD to ******")
    if args.db_name:
        os.environ["DB_NAME"] = args.db_name
        logger.info(f"Setting DB_NAME to {args.db_name}")

    logger.info(f"Using Gin config path: {gin_config_path}")

    if not os.path.exists(gin_config_path):
        logger.error(f"Gin config file not found: {gin_config_path}")
        sys.exit(1)

    # Import Database before parsing Gin config so Gin can bind its parameters

    try:
        gin.parse_config_file(gin_config_path)
        logger.info(f"Successfully parsed Gin config file: {gin_config_path}")
    except Exception as e:
        logger.error(f"Failed to parse Gin config file: {e}")
        sys.exit(1)

    try:
        # Set environment type explicitly
        env_type = EnvironmentType(args.environment)
        logger.info(f"Using environment type: {env_type}")
        env = Environment(gin_config_path=gin_config_path, env_type=env_type)

        # Database configuration is now handled by the Database class
        # No need to manually set database host or name here
    except Exception as e:
        logger.error(f"Failed to create Environment: {e}")
        sys.exit(1)

    # Bind Gin-configurable API key
    try:
        # Get API key from environment variable first, then fall back to Gin config
        EODHD_API_KEY = get_eodhd_api_key()