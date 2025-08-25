import os
import requests
import asyncpg
from dotenv import load_dotenv
from datetime import datetime
from config.environment import Environment, EnvironmentType
from config.eodhd import set_eodhd_api_key, EODHD_API_KEY
import time
from requests.exceptions import ConnectionError
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

async def fetch_and_store_instruments(start_ticker='', ticker=None):
    from config.database import Database
    
    # Use centralized database connection logic
    try:
        logger.info(f"Creating database connection pool using centralized logic")
        pool = await Database.create_connection_pool(env=env, max_retries=3, initial_delay=1.0, timeout=10.0)
        logger.info("Successfully connected to database")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise
    
    total = 0
    if ticker:
        # Handle comma-separated ticker symbols
        tickers = [t.strip() for t in ticker.split(',')]
        logger.info(f"Processing {len(tickers)} tickers: {tickers}")
        
        for symbol in tickers:
            logger.info(f"Fetching ticker: {symbol}")
            # EODHD uses a different format - usually SYMBOL.EXCHANGE
            if '.' not in symbol:
                symbol = f"{symbol}.US"  # Default to US exchange
            
            detail_url = f"https://eodhd.com/api/fundamentals/{symbol}?api_token={EODHD_API_KEY}&fmt=json"
            # Log URL with masked API key for security
            logger.debug(f"Fetching https://eodhd.com/api/fundamentals/{symbol}?api_token={'*****' if EODHD_API_KEY else 'None'}")
            for attempt in range(3):
                try:
                    logger.debug(f"Fetching ticker details with API key: {'*****' if EODHD_API_KEY else 'None'}")
                    detail_resp = requests.get(detail_url)
                    logger.debug(f"Response status code: {detail_resp.status_code}")
                    logger.debug(f"Response text: {detail_resp.text[:500]}")  # First 500 chars
                    if detail_resp.status_code != 200:
                        logger.error(f"Failed to fetch detail for {symbol}: {detail_resp.status_code} {detail_resp.text}")
                        break
                    detail = detail_resp.json()
                    logger.debug(f"Detail parsed: {type(detail)}")
                    
                    # Extract basic info from EODHD response
                    general_info = detail.get('General', {})
                    logger.info(f"Ticker: {symbol}, Name: {general_info.get('Name')}, Exchange: {general_info.get('Exchange')}")
                    
                    instrument_data = {
                        'Code': symbol.split('.')[0],  # Remove exchange suffix
                        'Name': general_info.get('Name'),
                        'Exchange': general_info.get('Exchange'),
                        'Type': general_info.get('Type'),
                        'CurrencyCode': general_info.get('CurrencyCode'),
                        'IPODate': general_info.get('IPODate'),
                        'full_response': detail
                    }
                    
                    logger.debug(f"Calling upsert_instrument(pool, instrument_data)")
                    await upsert_instrument(pool, instrument_data)
                    total += 1
                    break
                except ConnectionError as e:
                    logger.error(f"Connection error for {symbol}: {e}, retrying...")
                    time.sleep(2 ** attempt)  # Exponential backoff
                except Exception as e:
                    logger.error(f"Exception in fetch_and_store_instruments for {symbol}: {e}")
            # Add a small delay between API calls to avoid rate limiting
            time.sleep(0.5)
        logger.info(f"Total tickers processed: {total}")
        await pool.close()
        return
    
    # For bulk processing, create table structure
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
                raw JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    logger.info(f"Created {table_name} table structure")
    await pool.close()

async def upsert_instrument(pool, item):
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {env.get_table_name('instrument_eodhd')} (symbol, name, exchange, asset_type, currency, ipo_date, raw, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,now())
            ON CONFLICT (symbol) DO UPDATE SET
                name=EXCLUDED.name,
                exchange=EXCLUDED.exchange,
                asset_type=EXCLUDED.asset_type,
                currency=EXCLUDED.currency,
                ipo_date=EXCLUDED.ipo_date,
                raw=EXCLUDED.raw,
                updated_at=now()
            """,
            item.get('Code'),
            item.get('Name'),
            item.get('Exchange'),
            item.get('Type'),
            item.get('CurrencyCode'),
            parse_date(item.get('IPODate')),
            json.dumps(item.get('full_response', item))
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Populate instrument_eodhd from EODHD API.")
    parser.add_argument('--start_ticker', type=str, default='', help='Only update/add instrument_eodhd if symbol > start_ticker (lexical order)')
    parser.add_argument('--environment', type=str, default='dev', choices=['test', 'intg', 'prod', 'dev'], help='Environment to use (test/intg/prod/dev)')
    parser.add_argument('--ticker', type=str, default=None, help='Populate only this ticker (optional, skips bulk)')
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
    from config.database import Database
    
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
        EODHD_API_KEY = os.environ.get("EODHD_API_KEY") or env.get_api_key('eodhd')
        logger.info(f"EODHD_API_KEY loaded: {EODHD_API_KEY is not None}")
        
        if not EODHD_API_KEY:
            logger.warning("No EODHD API key found in environment or Gin config")
    except Exception as e:
        logger.error(f"Failed to set EODHD API key: {e}")
        sys.exit(1)
    
    # Debugging output
    if args.debug:
        logger.debug("ENVIRONMENT VARIABLES:")
        for k, v in os.environ.items():
            if 'DATABASE' in k or 'GIN' in k or 'ENV' in k:
                logger.debug(f"    {k}={v}")
        logger.debug(f"env.env_type: {env.env_type}")
        logger.debug(f"env.get_database_url(): {'*****' if env.get_database_url() else None}")
        
        db_config = env.get_database_config()
        if db_config:
            # Mask password for security
            if 'password' in db_config:
                db_config['password'] = '*****' if db_config['password'] else None
            logger.debug(f"Database config: {db_config}")
        
        logger.debug(f"Gin config path: {gin_config_path}")
        logger.debug(f"env.get_api_key('eodhd'): {'*' * 5 + env.get_api_key('eodhd')[-4:] if env.get_api_key('eodhd') else 'None'}")
        
        # Print all Gin-configured parameters for debugging
        logger.debug("Gin-configured parameters:")
        for k in sorted(gin.operative_config_str().splitlines()):
            logger.debug(k)
    
    import asyncio
    try:
        asyncio.run(fetch_and_store_instruments(start_ticker=args.start_ticker, ticker=args.ticker))
    except Exception as e:
        logger.error(f"Failed to run fetch_and_store_instruments: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)