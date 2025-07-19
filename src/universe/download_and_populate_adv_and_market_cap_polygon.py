import os
import asyncio
import asyncpg
import requests
from datetime import datetime, timedelta
import time

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")  # Set this in your environment
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
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}"
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Failed to fetch {ticker}: {resp.status_code} {resp.text}")
        return []
    data = resp.json()
    if 'results' not in data:
        print(f"No results for {ticker}: {data}")
        return []
    return data['results']

def get_shares_outstanding_polygon(ticker, api_key):
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={api_key}"
    resp = requests.get(url)
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

async def insert_adv_and_market_cap(prices, ticker, shares_outstanding, advs):
    if not prices:
        return
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        # Assumes daily_prices table already has columns adv and market_cap
        await conn.executemany(
            "UPDATE daily_prices SET adv=$1, market_cap=$2 WHERE date=$3 AND symbol=$4",
            [
                (
                    advs[i],
                    (row['c'] * shares_outstanding if shares_outstanding else None),
                    datetime.utcfromtimestamp(row['t']/1000).date(),
                    ticker
                ) for i, row in enumerate(prices)
            ]
        )
    await pool.close()

async def main():
    if not POLYGON_API_KEY:
        raise Exception("Please set your POLYGON_API_KEY environment variable.")
    tickers = await get_all_spy_tickers()
    for ticker in tickers:
        print(f"Processing {ticker}...")
        try:
            shares_outstanding = get_shares_outstanding_polygon(ticker, POLYGON_API_KEY)
            prices = download_prices_polygon(ticker, START_DATE, END_DATE, POLYGON_API_KEY)
            advs = calc_adv(prices, window=20)
            await insert_adv_and_market_cap(prices, ticker, shares_outstanding, advs)
            print(f"Updated adv and market cap for {ticker}")
            time.sleep(0.8)  # Polygon free tier: 5 requests/sec
        except Exception as e:
            print(f"Error with {ticker}: {e}")

if __name__ == "__main__":
    asyncio.run(main())
