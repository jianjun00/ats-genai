import os
import asyncio
import asyncpg
import httpx
from datetime import datetime, timedelta

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")  # Set this in your environment
START_DATE = (datetime.now() - timedelta(days=365*10)).strftime("%Y-%m-%d")
END_DATE = datetime.now().strftime("%Y-%m-%d")

RATE_LIMIT = 5  # Polygon free tier: 5 req/sec

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")  # Set this in your environment
START_DATE = (datetime.now() - timedelta(days=365*10)).strftime("%Y-%m-%d")
END_DATE = datetime.now().strftime("%Y-%m-%d")

from typing import List, Tuple, Set

async def get_existing_dates(pool, ticker: str) -> Set[str]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT date FROM daily_prices WHERE symbol = $1", ticker)
    return set(str(row['date']) for row in rows)

def compute_missing_ranges(existing_dates: Set[str], start: str, end: str) -> List[Tuple[str, str]]:
    from datetime import timedelta
    from datetime import datetime as dt
    start_dt = dt.strptime(start, "%Y-%m-%d")
    end_dt = dt.strptime(end, "%Y-%m-%d")
    all_dates = [start_dt + timedelta(days=i) for i in range((end_dt - start_dt).days + 1)]
    missing = [d for d in all_dates if d.strftime("%Y-%m-%d") not in existing_dates]
    if not missing:
        return []
    # Group missing dates into continuous ranges
    ranges = []
    range_start = missing[0]
    range_end = missing[0]
    for d in missing[1:]:
        if (d - range_end).days == 1:
            range_end = d
        else:
            ranges.append((range_start.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d")))
            range_start = d
            range_end = d
    ranges.append((range_start.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d")))
    return ranges

async def get_all_spy_tickers():
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT added FROM spy_membership_change WHERE added IS NOT NULL")
        removed = await conn.fetch("SELECT DISTINCT removed FROM spy_membership_change WHERE removed IS NOT NULL")
    await pool.close()
    tickers = set([row['added'] for row in rows] + [row['removed'] for row in removed])
    tickers.discard(None)
    return sorted(tickers)

async def download_prices_polygon(ticker, start, end, api_key, client, semaphore):
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}"
    async with semaphore:
        resp = await client.get(url)
    if resp.status_code != 200:
        print(f"Failed to fetch {ticker}: {resp.status_code} {resp.text}")
        return []
    data = resp.json()
    if 'results' not in data:
        print(f"No results for {ticker}: {data}")
        return []
    return data['results']

async def get_shares_outstanding_polygon(ticker, api_key, client, semaphore):
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={api_key}"
    async with semaphore:
        resp = await client.get(url)
    if resp.status_code == 200:
        ref_data = resp.json()
        return ref_data.get('results', {}).get('share_class_shares_outstanding', None)
    else:
        print(f"Failed to fetch shares outstanding for {ticker}: {resp.status_code} {resp.text}")
        return None

def calc_adv(prices, window=20):
    # Calculate rolling average daily volume (ADV) for each day
    advs = []
    vols = [row['v'] for row in prices]
    for i in range(len(prices)):
        if i+1 < window:
            advs.append(None)
        else:
            adv = sum(vols[i-window+1:i+1]) / window
            advs.append(adv)
    return advs

async def insert_adv_and_market_cap(prices, ticker, shares_outstanding, advs, pool):
    if not prices:
        return
    async with pool.acquire() as conn:
        # Update ADV in daily_prices
        await conn.executemany(
            "UPDATE daily_prices SET adv=$1 WHERE date=$2 AND symbol=$3",
            [
                (
                    advs[i],
                    datetime.utcfromtimestamp(row['t']/1000).date(),
                    ticker
                ) for i, row in enumerate(prices)
            ]
        )
        # Upsert market_cap into fundamentals
        await conn.executemany(
            """
            INSERT INTO fundamentals (ticker, date, market_cap)
            VALUES ($1, $2, $3)
            ON CONFLICT (ticker, date) DO UPDATE SET market_cap = EXCLUDED.market_cap
            """,
            [
                (
                    ticker,
                    datetime.utcfromtimestamp(row['t']/1000).date(),
                    (row['c'] * shares_outstanding if shares_outstanding else None)
                ) for row in prices
            ]
        )

async def process_ticker(ticker, client, semaphore, pool):
    print(f"Processing {ticker}...")
    try:
        shares_outstanding = await get_shares_outstanding_polygon(ticker, POLYGON_API_KEY, client, semaphore)
        existing_dates = await get_existing_dates(pool, ticker)
        missing_ranges = compute_missing_ranges(existing_dates, START_DATE, END_DATE)
        if not missing_ranges:
            print(f"All data present for {ticker}. Skipping download.")
            return
        all_prices = []
        for rng_start, rng_end in missing_ranges:
            prices = await download_prices_polygon(ticker, rng_start, rng_end, POLYGON_API_KEY, client, semaphore)
            all_prices.extend(prices)
        if not all_prices:
            print(f"No new data to update for {ticker}.")
            return
        advs = calc_adv(all_prices, window=20)
        await insert_adv_and_market_cap(all_prices, ticker, shares_outstanding, advs, pool)
        print(f"Updated adv and market cap for {ticker}")
    except Exception as e:
        print(f"Error with {ticker}: {e}")

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
    semaphore = asyncio.Semaphore(RATE_LIMIT)
    pool = await asyncpg.create_pool(TSDB_URL)
    async with httpx.AsyncClient(timeout=30) as client:
        tasks = [process_ticker(ticker, client, semaphore, pool) for ticker in tickers]
        # Run in batches to respect rate limit per second
        batch_size = RATE_LIMIT
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i+batch_size]
            await asyncio.gather(*batch)
            if i + batch_size < len(tasks):
                await asyncio.sleep(1)  # Wait 1 second between batches
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
