import os
import asyncio
import asyncpg
import aiohttp
from datetime import datetime, timedelta
import pandas as pd
import argparse

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")
TIINGO_API_KEY = os.getenv("TIINGO_API_KEY")
TIINGO_BASE_URL = "https://api.tiingo.com/tiingo/daily/{symbol}/prices"

CREATE_DAILY_PRICES_TIINGO_SQL = """
CREATE TABLE IF NOT EXISTS daily_prices_tiingo (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    adjClose DOUBLE PRECISION,
    volume BIGINT,
    PRIMARY KEY (date, symbol)
);
"""

INSERT_SQL = """
INSERT INTO daily_prices_tiingo (date, symbol, open, high, low, close, adjClose, volume)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (date, symbol) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    adjClose = EXCLUDED.adjClose,
    volume = EXCLUDED.volume;
"""

async def get_spy_members(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT symbol FROM spy_membership")
        return [row['symbol'] for row in rows]

def tiingo_url(symbol, start_date, end_date):
    return (
        f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
        f"?startDate={start_date}&endDate={end_date}&format=json&token={TIINGO_API_KEY}"
    )

async def fetch_and_insert_symbol(pool, session, symbol, start_date, end_date):
    url = tiingo_url(symbol, start_date, end_date)
    print(f"[DEBUG] Fetching {symbol} from URL: {url}")
    async with session.get(url) as resp:
        print(f"[DEBUG] HTTP status for {symbol}: {resp.status}")
        if resp.status != 200:
            print(f"Failed to fetch {symbol}: {resp.status}")
            text = await resp.text()
            print(f"[DEBUG] Response for {symbol}: {text[:200]}")
            return
        data = await resp.json()
        print(f"[DEBUG] Response data for {symbol}: {str(data)[:300]}")
        rows = []
        for row in data:
            rows.append((
                row['date'][:10], symbol, row.get('open'), row.get('high'),
                row.get('low'), row.get('close'), row.get('adjClose'), row.get('volume')
            ))
        if not rows:
            print(f"No data for {symbol}")
            return
        try:
            async with pool.acquire() as conn:
                await conn.executemany(INSERT_SQL, rows)
            print(f"Inserted {len(rows)} rows for {symbol}")
        except Exception as e:
            print(f"[DEBUG] Insert failed for {symbol}: {e}")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=str, required=True, help='Start date YYYY-MM-DD')
    parser.add_argument('--end', type=str, required=True, help='End date YYYY-MM-DD')
    parser.add_argument('--symbols', type=str, nargs='*', default=None, help='Optional list of symbols')
    args = parser.parse_args()
    start_date = args.start
    end_date = args.end
    pool = await asyncpg.create_pool(TSDB_URL, min_size=1, max_size=4)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_DAILY_PRICES_TIINGO_SQL)
    if args.symbols:
        symbols = args.symbols
    else:
        symbols = await get_spy_members(pool)
    print(f"[DEBUG] Symbols to fetch: {symbols}")
    if not symbols:
        print("[DEBUG] No symbols found in spy_membership table.")
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_and_insert_symbol(pool, session, symbol, start_date, end_date) for symbol in symbols]
        await asyncio.gather(*tasks)
    await pool.close()
    print('Done loading daily prices from Tiingo for all SPY members.')

if __name__ == "__main__":
    asyncio.run(main())
