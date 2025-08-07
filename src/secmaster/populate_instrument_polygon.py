import os
import requests
import asyncpg
from dotenv import load_dotenv
from datetime import datetime
from config.environment import Environment, EnvironmentType

import gin

POLYGON_API_KEY = None
@gin.configurable
def set_polygon_api_key(polygon_api_key=None):
    global POLYGON_API_KEY
    POLYGON_API_KEY = polygon_api_key

load_dotenv()

# (moved to main)
# Polygon reference API endpoint for all US stocks (paginated)
BASE_URL = "https://api.polygon.io/v3/reference/tickers"

import time
from requests.exceptions import ConnectionError

import ray

import requests, asyncpg, json
from datetime import datetime
from requests.exceptions import ConnectionError
import time

def parse_date(val):
    if not val:
        return None
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d").date()
    except Exception:
        return None

async def batch_upsert_details(details, db_url, table_name):
    if not details:
        return
    pool = await asyncpg.create_pool(db_url, min_size=1, max_size=1)
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

@ray.remote
def fetch_and_upsert_ray(symbols, db_url, table_name, polygon_api_key):
    details = []
    results = []
    for symbol in symbols:
        detail_url = f"https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={polygon_api_key}"
        for attempt in range(3):
            try:
                resp = requests.get(detail_url)
                if resp.status_code != 200:
                    print(f"[RAY][ERROR] {symbol}: {resp.status_code} {resp.text}")
                    continue
                detail = resp.json().get('results', {})
                if not detail.get('list_date'):
                    print(f"[RAY][INFO] Skipping {symbol} (no list_date)")
                    results.append((symbol, 'skipped'))
                    break
                details.append(detail)
                results.append((symbol, 'ok'))
                break
            except ConnectionError as e:
                print(f"[RAY][ERROR] Connection error for {symbol}: {e}, retrying...")
                time.sleep(2 ** attempt)
            except Exception as e:
                print(f"[RAY][ERROR] {symbol}: {e}")
                results.append((symbol, 'fail'))
                break
        else:
            results.append((symbol, 'fail'))
    import asyncio
    asyncio.run(batch_upsert_details(details, db_url, table_name))
    for symbol, status in results:
        print(f"[RAY][RESULT] {symbol}: {status}")
    return results

async def fetch_and_store_instruments(start_ticker='', ticker=None):
    import ray
    pool = await asyncpg.create_pool(env.get_database_url())
    total = 0
    if ticker:
        symbol = ticker
        detail_url = f"https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={POLYGON_API_KEY}"
        for attempt in range(3):
            try:
                print(f"[DEBUG] Fetching {detail_url}")
                detail_resp = requests.get(detail_url)
                print(f"[DEBUG] detail_resp.status_code={detail_resp.status_code}")
                print(f"[DEBUG] detail_resp.text={detail_resp.text}")
                if detail_resp.status_code != 200:
                    print(f"[ERROR] Failed to fetch detail for {symbol}: {detail_resp.status_code} {detail_resp.text}")
                    break
                detail = detail_resp.json().get('results', {})
                print(f"[DEBUG] detail parsed: {detail}")
                print(f"Ticker: {symbol}, list_date: {detail.get('list_date')}, delisted_utc: {detail.get('delisted_utc')}")
                print(f"[DEBUG] Calling upsert_instrument(pool, detail) with detail={detail}")
                await upsert_instrument(pool, detail)
                total += 1
                break
            except ConnectionError as e:
                print(f"[ERROR] Connection error for {symbol}: {e}, retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
            except Exception as e:
                print(f"[DEBUG] Exception in fetch_and_store_instruments for {symbol}: {e}")
        print(f"Total tickers processed: {total}")
        await pool.close()
        return
    url = BASE_URL + f"?market=stocks&active=true&limit=1000&apiKey={POLYGON_API_KEY}"
    all_symbols = []
    while url:
        resp = requests.get(url)
        if resp.status_code != 200:
            print(f"[ERROR] Failed to fetch {url} : {resp.status_code} {resp.text}")
            break
        data = resp.json()
        tickers = data.get('results', [])
        print(f"Fetched {len(tickers)} tickers from bulk endpoint.")
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
        print("No symbols to process.")
        return
    # Ray parallel processing
    ray.init(ignore_reinit_error=True)
    db_url = env.get_database_url()
    table_name = env.get_table_name('instrument_polygon')
    print(f"Submitting {len(all_symbols)} Ray tasks...")
    batch_size = 3  # Limit concurrency for DB and rate limits
    # Group all_symbols into batches
    def batcher(seq, size):
        for i in range(0, len(seq), size):
            yield seq[i:i+size]
    tasks = []
    for batch in batcher(all_symbols, batch_size):
        tasks.append(fetch_and_upsert_ray.remote(batch, db_url, table_name, POLYGON_API_KEY))
        if len(tasks) >= batch_size:
            results = ray.get(tasks)
            print(f"Processed batch: {results}")
            tasks = []
            time.sleep(1.0)  # Sleep between batches for rate limits
    if tasks:
        results = ray.get(tasks)
        print(f"Processed final batch: {results}")
    print(f"Total tickers processed: {len(all_symbols)}")



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
import os

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Populate instrument_polygon from Polygon bulk and detail endpoints.")
    parser.add_argument('--start_ticker', type=str, default='', help='Only update/add instrument_polygon if symbol > start_ticker (lexical order)')
    parser.add_argument('--environment', type=str, default='intg', choices=['test', 'intg', 'prod'], help='Environment to use (test/intg/prod)')
    parser.add_argument('--ticker', type=str, default=None, help='Populate only this ticker (optional, skips bulk)')
    parser.add_argument('--gin_config', type=str, default=None, help='Path to Gin config file (optional)')
    args = parser.parse_args()

    # Determine Gin config file if not explicitly provided
    if args.gin_config:
        gin_config_path = args.gin_config
    else:
        gin_config_map = {
            'test': 'config/app_test.gin',
            'intg': 'config/app_intg.gin',
            'prod': 'config/app_prod.gin',
        }
        gin_config_path = gin_config_map.get(args.environment)
    if not os.path.exists(gin_config_path):
        print(f"[ERROR] Gin config file not found: {gin_config_path}", file=sys.stderr)
        sys.exit(1)
    # Import Database before parsing Gin config so Gin can bind its parameters
    from config.database import Database
    gin.parse_config_file(gin_config_path)

    env = Environment(gin_config_path=gin_config_path, env_type=EnvironmentType(args.environment))

    # Bind Gin-configurable API key
    set_polygon_api_key()  # This will set POLYGON_API_KEY from Gin config
    print(f"[DEBUG] POLYGON_API_KEY after Gin load: {POLYGON_API_KEY}")

    # Debugging output
    import os
    print("[DEBUG] ENVIRONMENT VARIABLES:")
    for k, v in os.environ.items():
        if 'DATABASE' in k or 'GIN' in k or 'ENV' in k:
            print(f"    {k}={v}")
    print(f"[DEBUG] env.env_type: {env.env_type}")
    print(f"[DEBUG] env.get_database_url(): {env.get_database_url()}")
    print(f"[DEBUG] env.get_database_config(): {getattr(env, 'get_database_config', lambda: None)()}")
    print(f"[DEBUG] Gin config path: {gin_config_path}")
    print(f"[DEBUG] env.get_api_key('polygon'): {env.get_api_key('polygon')}")

    # Print all Gin-configured parameters for debugging
    import gin
    print("[DEBUG] Gin-configured parameters:")
    for k in sorted(gin.operative_config_str().splitlines()):
        print(k)

    import asyncio
    asyncio.run(fetch_and_store_instruments(start_ticker=args.start_ticker, ticker=args.ticker))
