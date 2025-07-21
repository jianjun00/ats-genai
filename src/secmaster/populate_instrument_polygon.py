import os
import requests
import asyncpg
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

DB_URL = os.environ["TSDB_URL"]
POLYGON_API_KEY = os.environ["POLYGON_API_KEY"]

# Polygon reference API endpoint for all US stocks (paginated)
BASE_URL = "https://api.polygon.io/v3/reference/tickers"

import time
from requests.exceptions import ConnectionError

async def fetch_and_store_instruments(start_ticker=''):
    pool = await asyncpg.create_pool(DB_URL)
    url = BASE_URL + f"?market=stocks&active=true&limit=1000&apiKey={POLYGON_API_KEY}"
    total = 0
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
            detail_url = f"https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={POLYGON_API_KEY}"
            for attempt in range(3):
                try:
                    detail_resp = requests.get(detail_url)
                    if detail_resp.status_code != 200:
                        print(f"[ERROR] Failed to fetch detail for {symbol}: {detail_resp.status_code} {detail_resp.text}")
                        break
                    detail = detail_resp.json().get('results', {})
                    print(f"Ticker: {symbol}, list_date: {detail.get('list_date')}, delisted_utc: {detail.get('delisted_utc')}")
                    await upsert_instrument(pool, detail)
                    total += 1
                    break
                except ConnectionError as e:
                    print(f"[ERROR] Connection error for {symbol}: {e}, retrying...")
                    time.sleep(2 ** attempt)  # Exponential backoff
            time.sleep(0.25)  # Add a small delay between requests
        url = data.get('next_url')
        if url and 'apiKey=' not in url:
            url += f"&apiKey={POLYGON_API_KEY}"
    print(f"Total tickers processed: {total}")
    await pool.close()



import json

async def upsert_instrument(pool, item):
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO instrument_polygon (symbol, name, exchange, type, currency, figi, isin, cusip, composite_figi, active, list_date, delist_date, raw, updated_at)
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

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Populate instrument_polygon from Polygon bulk and detail endpoints.")
    parser.add_argument('--start_ticker', type=str, default='', help='Only update/add instrument_polygon if symbol > start_ticker (lexical order)')
    args = parser.parse_args()

    import asyncio
    asyncio.run(fetch_and_store_instruments(start_ticker=args.start_ticker))
