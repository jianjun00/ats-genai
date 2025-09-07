import os
import sys
import requests
import asyncpg
import json
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

DB_URL = os.environ["TSDB_URL"]
POLYGON_API_KEY = os.environ["POLYGON_API_KEY"]
BASE_URL = "https://api.polygon.io/v3/reference/tickers/"

# Utility to parse date

def parse_date(val):
    if not val:
        return None
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d").date()
    except Exception:
        return None

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
            parse_date(item.get('delisted_utc')),
            json.dumps(item)
        )

import psycopg2

def get_tickers_from_universe_id(universe_id):
    # Fetch tickers from universe_membership table for the given universe_id
    conn = psycopg2.connect(DB_URL)
    with conn.cursor() as cur:
        cur.execute("SELECT symbol FROM universe_membership WHERE universe_id = %s", (universe_id,))
        tickers = [row[0] for row in cur.fetchall()]
    conn.close()
    return tickers

async def fetch_and_store_symbols(symbols):
    pool = await asyncpg.create_pool(DB_URL)
    for symbol in symbols:
        url = f"{BASE_URL}{symbol}?apiKey={POLYGON_API_KEY}"
        resp = requests.get(url)
        if resp.status_code != 200:
            print(f"[ERROR] Failed to fetch {symbol}: {resp.status_code} {resp.text}")
            continue
        data = resp.json()
        item = data.get('results')
        if not item:
            print(f"[WARN] No data for {symbol}")
            continue
        print(f"Ticker: {symbol}, list_date: {item.get('list_date')}, delisted_utc: {item.get('delisted_utc')}")
        await upsert_instrument(pool, item)
    await pool.close()

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Populate instrument_polygon for specific tickers from Polygon")
    parser.add_argument('--tickers', nargs='*', help='List of tickers to fetch (space separated)')
    parser.add_argument('--universe_id', type=int, help='Universe ID to fetch tickers from universe_membership table')
    args = parser.parse_args()
    if args.tickers:
        tickers = args.tickers
    elif args.universe_id is not None:
        tickers = get_tickers_from_universe_id(args.universe_id)
    else:
        print("Please provide --tickers or --universe_id")
        sys.exit(1)
    import asyncio
    asyncio.run(fetch_and_store_symbols(tickers))

if __name__ == "__main__":
    main()
