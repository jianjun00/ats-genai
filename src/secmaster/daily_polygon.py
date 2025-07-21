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

from calendars.exchange_calendar import ExchangeCalendar

async def get_existing_dates_polygon(pool, symbol, start_date, end_date):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT date FROM daily_prices_polygon WHERE symbol = $1 AND date >= $2 AND date <= $3",
            symbol, start_date, end_date
        )
    return set(row['date'] for row in rows)

def group_contiguous_dates(dates):
    # Given a sorted list of dates, group into contiguous ranges
    if not dates:
        return []
    dates = sorted(dates)
    ranges = []
    range_start = dates[0]
    prev = dates[0]
    for d in dates[1:]:
        if (d - prev).days > 1:
            ranges.append((range_start, prev))
            range_start = d
        prev = d
    ranges.append((range_start, prev))
    return ranges

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ticker', type=str, default=None, help='Process only this ticker (optional)')
    parser.add_argument('--start_date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='End date (YYYY-MM-DD)')
    args = parser.parse_args()
    if not POLYGON_API_KEY:
        raise Exception("Please set your POLYGON_API_KEY environment variable.")
    if args.ticker:
        tickers = [args.ticker]
    else:
        tickers = await get_all_spy_tickers()
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    nyse_cal = ExchangeCalendar('NYSE')
    trading_days = set(nyse_cal.all_trading_days(start_date, end_date))
    pool = await asyncpg.create_pool(TSDB_URL)
    for ticker in tickers:
        print(f"Processing {ticker}...")
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
            existing_dates = await get_existing_dates_polygon(pool, ticker, start_date, end_date)
            missing_days = [d for d in trading_days if d not in existing_dates]
            if not missing_days:
                print(f"[DEBUG] All data exists for {ticker} in {start_date} to {end_date}, skipping fetch.")
                continue
            # Group into contiguous ranges for efficient API calls
            ranges = group_contiguous_dates(missing_days)
            total_inserted = 0
            for range_start, range_end in ranges:
                prices = download_prices_polygon(ticker, range_start.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d"), POLYGON_API_KEY)
                # Filter prices to only missing trading days (Polygon may return extra)
                filtered_prices = [row for row in prices if datetime.utcfromtimestamp(row['t']/1000).date() in missing_days]
                await insert_prices(filtered_prices, ticker, shares_outstanding)
                total_inserted += len(filtered_prices)
                print(f"Inserted {len(filtered_prices)} rows for {ticker} from {range_start} to {range_end}")
                time.sleep(0.8)  # Polygon free tier: 5 requests/sec
        except Exception as e:
            print(f"Error with {ticker}: {e}")
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
