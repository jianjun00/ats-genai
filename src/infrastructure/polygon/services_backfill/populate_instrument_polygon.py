import os
import requests
from dotenv import load_dotenv
from datetime import datetime
from core.platform.config_env.environment import Environment, EnvironmentType
from core.shared.vendor_api_keys import get_polygon_api_key
from core.shared.database_connections import get_database_pool, get_table_name
from core.shared.utils_core.backfill_framework import BackfillStats, VendorRateLimiters
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

# Non-Ray version for direct execution
async def fetch_and_upsert_direct(symbols, env_type, table_name, polygon_api_key):
    import logging
    direct_logger = logging.getLogger("direct_worker")
    direct_logger.setLevel(logging.INFO)

    details = []
    results = []
    for symbol in symbols:
        detail_url = f"https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={polygon_api_key}"
        for attempt in range(3):
            try:
                resp = requests.get(detail_url)
                if resp.status_code != 200:
                    direct_logger.error(f"{symbol}: {resp.status_code} {resp.text}")
                    continue
                detail = resp.json().get('results', {})
                if not detail.get('list_date'):
                    direct_logger.info(f"Skipping {symbol} (no list_date)")
                    results.append((symbol, 'skipped'))
                    break

                # Filter for US exchanges only
                US_EXCHANGES = {'XNYS', 'XNAS', 'XASE', 'BATS'}
                primary_exchange = detail.get('primary_exchange', '')
                if primary_exchange not in US_EXCHANGES:
                    direct_logger.info(f"Skipping {symbol} (non-US exchange: {primary_exchange})")
                    results.append((symbol, 'skipped'))
                    break

                details.append(detail)
                results.append((symbol, 'ok'))
                break
            except ConnectionError as e:
                direct_logger.error(f"Connection error for {symbol}: {e}, retrying...")
                time.sleep(2 ** attempt)
            except Exception as e:
                direct_logger.error(f"{symbol}: {e}")
                results.append((symbol, 'fail'))
                break
        else:
            results.append((symbol, 'fail'))

    # Use the centralized database connection logic
    from core.shared.database import Database
    if details:
        try:
            # Set environment type in os.environ for Database class to use
            import os
            os.environ["ENVIRONMENT"] = env_type.value if hasattr(env_type, 'value') else str(env_type)

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
            direct_logger.info(f"Successfully processed {len(details)} details")
        except Exception as e:
            direct_logger.error(f"Error processing details: {e}")
            import traceback
            direct_logger.error(traceback.format_exc())

    for symbol, status in results:
        direct_logger.info(f"RESULT {symbol}: {status}")
    return results

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

                # Filter for US exchanges only
                US_EXCHANGES = {'XNYS', 'XNAS', 'XASE', 'BATS'}
                primary_exchange = detail.get('primary_exchange', '')
                if primary_exchange not in US_EXCHANGES:
                    ray_logger.info(f"Skipping {symbol} (non-US exchange: {primary_exchange})")
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
        from core.shared.database import Database
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
    from core.shared.database import Database

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

                    # Check for US exchanges only
                    US_EXCHANGES = {'XNYS', 'XNAS', 'XASE', 'BATS'}
                    primary_exchange = detail.get('primary_exchange', '')
                    if primary_exchange not in US_EXCHANGES:
                        logger.info(f"Skipping {symbol} (non-US exchange: {primary_exchange})")
                        break

                    logger.info(f"Ticker: {symbol}, list_date: {detail.get('list_date')}, exchange: {primary_exchange}")
                    logger.debug(f"Calling upsert_instrument(pool, detail)")
                    await upsert_instrument(pool, detail)
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
    # Define US exchanges for filtering
    US_EXCHANGES = {'XNYS', 'XNAS', 'XASE', 'BATS'}  # NYSE, NASDAQ, NYSE American, BATS

    url = BASE_URL + f"?market=stocks&active=true&limit=1000&apiKey={POLYGON_API_KEY}"
    all_symbols = []
    filtered_count = 0
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
            primary_exchange = item.get('primary_exchange', '')

            if symbol <= start_ticker:
                continue  # Skip until we pass start_ticker

            # Filter for US exchanges only
            if primary_exchange in US_EXCHANGES:
                all_symbols.append(symbol)
            else:
                filtered_count += 1
                logger.debug(f"Filtered out {symbol} (exchange: {primary_exchange})")

        # More efficient counting for logging
        batch_us_count = sum(1 for item in tickers if item.get('ticker', '') > start_ticker and item.get('primary_exchange', '') in US_EXCHANGES)
        batch_non_us_count = sum(1 for item in tickers if item.get('ticker', '') > start_ticker and item.get('primary_exchange', '') not in US_EXCHANGES)
        logger.info(f"Batch: Added {batch_us_count} US symbols, filtered {batch_non_us_count} non-US symbols")
        url = data.get('next_url')
        if url and 'apiKey=' not in url:
            url += f"&apiKey={POLYGON_API_KEY}"
    await pool.close()

    # Log filtering summary
    total_fetched = len(all_symbols) + filtered_count
    logger.info(f"US-only filtering summary: {len(all_symbols)} US symbols retained, {filtered_count} non-US symbols filtered out (total fetched: {total_fetched})")

    if not all_symbols:
        logger.info("No US symbols to process.")
        return
    # Use simple sequential processing instead of Ray to avoid working_dir issues
    table_name = env.get_table_name('instrument_polygon')
    logger.info(f"Processing {len(all_symbols)} instruments sequentially with API key: {POLYGON_API_KEY is not None}")

    # Process in smaller batches to manage rate limits
    batch_size = 10
    def batcher(seq, size):
        for i in range(0, len(seq), size):
            yield seq[i:i+size]

    processed_count = 0
    # Let all batch processing exceptions propagate - fail fast on batch errors
    for batch in batcher(all_symbols, batch_size):
        # Call the async version with await
        result = await fetch_and_upsert_direct(batch, env.env_type, table_name, POLYGON_API_KEY)
        logger.info(f"Processed batch of {len(batch)} instruments: {result}")
        processed_count += len(batch)
        time.sleep(2.0)  # Sleep between batches for rate limits

    logger.info(f"Total instruments processed: {processed_count}/{len(all_symbols)}")



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
    # Get API key from environment variable first, then fall back to Gin config
    POLYGON_API_KEY = get_polygon_api_key()