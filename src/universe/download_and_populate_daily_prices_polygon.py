import os
import asyncio
import asyncpg
import requests
from datetime import datetime, timedelta
import time

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")  # Set this in your environment
BASE_URL = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}"
START_DATE = (datetime.now() - timedelta(days=365*10)).strftime("%Y-%m-%d")
END_DATE = datetime.now().strftime("%Y-%m-%d")

async def get_all_spy_tickers():
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT added FROM spy_membership_change WHERE added IS NOT NULL")
        removed = await conn.fetch("SELECT DISTINCT removed FROM spy_membership_change WHERE removed IS NOT NULL")
    await pool.close()
    tickers = set([row['added'] for row in rows] + [row['removed'] for row in removed])
    tickers.discard(None)
    return sorted(tickers)

def download_prices_polygon(ticker, start, end, api_key):
    url = BASE_URL.format(ticker=ticker, start=start, end=end, api_key=api_key)
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Failed to fetch {ticker}: {resp.status_code} {resp.text}")
        return []
    data = resp.json()
    if 'results' not in data:
        print(f"No results for {ticker}: {data}")
        return []
    return data['results']

CREATE_DAILY_PRICES_POLYGON_SQL = """
CREATE TABLE IF NOT EXISTS daily_prices_polygon (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    market_cap DOUBLE PRECISION,
    PRIMARY KEY (date, symbol)
);
"""

async def insert_prices(prices, ticker, shares_outstanding):
    if not prices:
        return
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_DAILY_PRICES_POLYGON_SQL)
        await conn.executemany(
            "INSERT INTO daily_prices_polygon (date, symbol, open, high, low, close, volume, market_cap) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING",
            [(
                datetime.utcfromtimestamp(row['t']/1000).date(),
                ticker,
                row['o'], row['h'], row['l'], row['c'], row['v'],
                (row['c'] * shares_outstanding if shares_outstanding else None)
            ) for row in prices]
        )
    await pool.close()

import argparse

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ticker', type=str, default=None, help='Process only this ticker (optional)')
    args = parser.parse_args()
    if not POLYGON_API_KEY:
        raise Exception("Please set your POLYGON_API_KEY environment variable.")
    if args.ticker:
        tickers = [args.ticker]
    else:
        tickers = await get_all_spy_tickers()
    for ticker in tickers:
        print(f"Downloading {ticker}...")
        try:
            # Fetch shares outstanding
            url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={POLYGON_API_KEY}"
            resp = requests.get(url)
            if resp.status_code == 200:
                ref_data = resp.json()
                shares_outstanding = ref_data.get('results', {}).get('share_class_shares_outstanding', None)
            else:
                print(f"Failed to fetch shares outstanding for {ticker}: {resp.status_code} {resp.text}")
                shares_outstanding = None
            prices = download_prices_polygon(ticker, START_DATE, END_DATE, POLYGON_API_KEY)
            await insert_prices(prices, ticker, shares_outstanding)
            print(f"Inserted {len(prices)} rows for {ticker}")
            time.sleep(0.8)  # Polygon free tier: 5 requests/sec
        except Exception as e:
            print(f"Error with {ticker}: {e}")

if __name__ == "__main__":
    asyncio.run(main())
