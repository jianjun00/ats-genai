import os
import requests
import asyncpg
from dotenv import load_dotenv
from datetime import datetime
from config.environment import Environment, EnvironmentType
from config.polygon import set_polygon_api_key, POLYGON_API_KEY
import time
from requests.exceptions import ConnectionError
import ray
import json
import logging

load_dotenv()

# Polygon reference API endpoint for all US stocks (paginated)
BASE_URL = "https://api.polygon.io/v3/reference/tickers"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("populate_instrument_polygon")

def parse_date(val):
    if not val:
        return None
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d").date()
    except Exception:
        return None

# This function has been integrated into the Ray worker function

@ray.remote
def fetch_and_upsert_ray(symbols, env_type, table_name, polygon_api_key):
    import logging
    ray_logger = logging.getLogger("ray_worker")
    ray_logger.setLevel(logging.INFO)
    
    details = []
    results = []
    for symbol in symbols:
        detail_url = f"https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={polygon_api_key}"
        for attempt in range(3):
            try:
                resp = requests.get(detail_url)
                if resp.status_code != 200:
                    ray_logger.error(f"{symbol}: {resp.status_code} {resp.text}")
                    continue
                detail = resp.json().get('results', {})
                if not detail.get('list_date'):
                    ray_logger.info(f"Skipping {symbol} (no list_date)")
                    results.append((symbol, 'skipped'))
                    break
                details.append(detail)
                results.append((symbol, 'ok'))
                break
            except ConnectionError as e:
                ray_logger.error(f"Connection error for {symbol}: {e}, retrying...")
                time.sleep(2 ** attempt)
            except Exception as e:
                ray_logger.error(f"{symbol}: {e}")
                results.append((symbol, 'fail'))
                break
        else:
            results.append((symbol, 'fail'))
    
    # Use the centralized database connection logic
    import asyncio
    async def process_details():
        from config.database import Database
        from config.environment import Environment, EnvironmentType
        if details:
            try:
                # Set environment type in os.environ for Database class to use
                import os
                os.environ["ENVIRONMENT"] = env_type
                
                # Create a connection pool using the centralized logic
                pool = await Database.create_connection_pool(max_retries=3, initial_delay=1.0, timeout=10.0)
                
                # Process the details
                async with pool.acquire() as conn:
                    rows = [
                        (
                            d.get('ticker'),
                            d.get('name'),
                            d.get('primary_exchange'),
                            d.get('type'),
                            d.get('currency_name'),
                            d.get('share_class_figi'),
                            d.get('isin'),
                            d.get('cusip'),
                            d.get('composite_figi'),
                            d.get('active'),
                            parse_date(d.get('list_date')),
                            parse_date(d.get('delisted_utc')),
                            json.dumps(d)
                        )
                        for d in details
                    ]
                    sql = f"""
                        INSERT INTO {table_name} (symbol, name, exchange, type, currency, figi, isin, cusip, composite_figi, active, list_date, delist_date, raw, updated_at)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,now())
                        ON CONFLICT (symbol) DO UPDATE SET
                            name=EXCLUDED.name,
                            exchange=EXCLUDED.exchange,
                            type=EXCLUDED.type,
                            currency=EXCLUDED.currency,
                            figi=EXCLUDED.figi,
                            isin=EXCLUDED.isin,
                            cusip=EXCLUDED.cusip,
                            composite_figi=EXCLUDED.composite_figi,
                            active=EXCLUDED.active,
                            list_date=EXCLUDED.list_date,
                            delist_date=EXCLUDED.delist_date,
                            raw=EXCLUDED.raw,
                            updated_at=now()
                    """
                    await conn.executemany(sql, rows)
                await pool.close()
                ray_logger.info(f"Successfully processed {len(details)} details")
            except Exception as e:
                ray_logger.error(f"Error processing details: {e}")
                import traceback
                ray_logger.error(traceback.format_exc())
    
    # Run the async function
    asyncio.run(process_details())
    
    for symbol, status in results:
        ray_logger.info(f"RESULT {symbol}: {status}")
    return results

async def fetch_and_store_instruments(start_ticker='', ticker=None):
    import ray
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
        symbol = ticker
        logger.info(f"Fetching single ticker: {ticker}")
        detail_url = f"https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={POLYGON_API_KEY}"
        # Log URL with masked API key for security
        logger.debug(f"Fetching https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={'*****' if POLYGON_API_KEY else 'None'}")
        for attempt in range(3):
            try:
                logger.debug(f"Fetching ticker details with API key: {'*****' if POLYGON_API_KEY else 'None'}")
                detail_resp = requests.get(detail_url)
                logger.debug(f"Response status code: {detail_resp.status_code}")
                logger.debug(f"Response text: {detail_resp.text}")
                if detail_resp.status_code != 200:
                    logger.error(f"Failed to fetch detail for {symbol}: {detail_resp.status_code} {detail_resp.text}")
                    break
                detail = detail_resp.json().get('results', {})
                logger.debug(f"Detail parsed: {detail}")
                logger.info(f"Ticker: {symbol}, list_date: {detail.get('list_date')}, delisted_utc: {detail.get('delisted_utc')}")
                logger.debug(f"Calling upsert_instrument(pool, detail)")
                await upsert_instrument(pool, detail)
                total += 1
                break
            except ConnectionError as e:
                logger.error(f"Connection error for {symbol}: {e}, retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
            except Exception as e:
                logger.error(f"Exception in fetch_and_store_instruments for {symbol}: {e}")
        logger.info(f"Total tickers processed: {total}")
        await pool.close()
        return
    url = BASE_URL + f"?market=stocks&active=true&limit=1000&apiKey={POLYGON_API_KEY}"
    all_symbols = []
    while url:
        logger.debug(f"Fetching tickers from {url}")
        resp = requests.get(url)
        if resp.status_code != 200:
            logger.error(f"Failed to fetch {url} : {resp.status_code} {resp.text}")
            break
        data = resp.json()
        tickers = data.get('results', [])
        logger.info(f"Fetched {len(tickers)} tickers from bulk endpoint.")
        for item in tickers:
            symbol = item.get('ticker')
            if symbol <= start_ticker:
                continue  # Skip until we pass start_ticker
            all_symbols.append(symbol)
        url = data.get('next_url')
        if url and 'apiKey=' not in url:
            url += f"&apiKey={POLYGON_API_KEY}"
    await pool.close()
    if not all_symbols:
        logger.info("No symbols to process.")
        return
    # Ray parallel processing
    ray.init(ignore_reinit_error=True)
    table_name = env.get_table_name('instrument_polygon')
    logger.info(f"Submitting {len(all_symbols)} Ray tasks with API key: {POLYGON_API_KEY is not None}")
    batch_size = 3  # Limit concurrency for DB and rate limits
    # Group all_symbols into batches
    def batcher(seq, size):
        for i in range(0, len(seq), size):
            yield seq[i:i+size]
    tasks = []
    for batch in batcher(all_symbols, batch_size):
        tasks.append(fetch_and_upsert_ray.remote(batch, env.env_type, table_name, POLYGON_API_KEY))
        if len(tasks) >= batch_size:
            results = ray.get(tasks)
            logger.info(f"Processed batch: {results}")
            tasks = []
            time.sleep(1.0)  # Sleep between batches for rate limits
    if tasks:
        results = ray.get(tasks)
        logger.info(f"Processed final batch: {results}")
    logger.info(f"Total tickers processed: {len(all_symbols)}")



import json

async def upsert_instrument(pool, item):
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO {env.get_table_name('instrument_polygon')} (symbol, name, exchange, type, currency, figi, isin, cusip, composite_figi, active, list_date, delist_date, raw, updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,now())
            ON CONFLICT (symbol) DO UPDATE SET
                name=EXCLUDED.name,
                exchange=EXCLUDED.exchange,
                type=EXCLUDED.type,
                currency=EXCLUDED.currency,
                figi=EXCLUDED.figi,
                isin=EXCLUDED.isin,
                cusip=EXCLUDED.cusip,
                composite_figi=EXCLUDED.composite_figi,
                active=EXCLUDED.active,
                list_date=EXCLUDED.list_date,
                delist_date=EXCLUDED.delist_date,
                raw=EXCLUDED.raw,
                updated_at=now()
            """,
            item.get('ticker'),
            item.get('name'),
            item.get('primary_exchange'),
            item.get('type'),
            item.get('currency_name'),
            item.get('share_class_figi'),
            item.get('isin'),
            item.get('cusip'),
            item.get('composite_figi'),
            item.get('active'),
            parse_date(item.get('list_date')),
            parse_date(item.get('delisted_utc')),  # may be None
            json.dumps(item)
        )

def parse_date(val):
    if not val:
        return None
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d").date()
    except Exception:
        return None

import argparse
import gin
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Populate instrument_polygon from Polygon bulk and detail endpoints.")
    parser.add_argument('--start_ticker', type=str, default='', help='Only update/add instrument_polygon if symbol > start_ticker (lexical order)')
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
            'dev': 'config/app_docker.gin',
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
        POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY") or env.get_api_key('polygon')
        logger.info(f"POLYGON_API_KEY loaded: {POLYGON_API_KEY is not None}")
        
        if not POLYGON_API_KEY:
            logger.warning("No Polygon API key found in environment or Gin config")
    except Exception as e:
        logger.error(f"Failed to set Polygon API key: {e}")
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
        logger.debug(f"env.get_api_key('polygon'): {'*' * 5 + env.get_api_key('polygon')[-4:] if env.get_api_key('polygon') else 'None'}")
        
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
