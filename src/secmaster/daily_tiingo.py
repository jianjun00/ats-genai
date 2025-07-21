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
        universe_row = await conn.fetchrow("SELECT id FROM universe WHERE name = 'S&P 500'")
        if not universe_row:
            print("[ERROR] Could not find S&P 500 universe in universe table.")
            return []
        universe_id = universe_row['id']
        rows = await conn.fetch("SELECT DISTINCT symbol FROM universe_membership WHERE universe_id = $1", universe_id)
        return [row['symbol'] for row in rows]

def tiingo_url(symbol, start_date, end_date):
    return (
        f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
        f"?startDate={start_date}&endDate={end_date}&format=json&token={TIINGO_API_KEY}"
    )

async def get_existing_dates(pool, symbol, start_date, end_date):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT date FROM daily_prices_tiingo WHERE symbol = $1 AND date >= $2 AND date <= $3",
            symbol, start_date, end_date
        )
    return set(row['date'] for row in rows)

def get_missing_date_ranges(existing_dates, start_date, end_date):
    # Returns a list of (range_start, range_end) for missing contiguous dates
    all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
    missing = [d.date() for d in all_dates if d.date() not in existing_dates]
    if not missing:
        return []
    # Group into contiguous ranges
    ranges = []
    range_start = missing[0]
    prev = missing[0]
    for d in missing[1:]:
        if (d - prev).days > 1:
            ranges.append((range_start, prev))
            range_start = d
        prev = d
    ranges.append((range_start, prev))
    return ranges

async def fetch_and_insert_symbol(pool, session, symbol, start_date, end_date):
    # Always use datetime.date for DB and date math
    if isinstance(start_date, str):
        start_date_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    else:
        start_date_dt = start_date
    if isinstance(end_date, str):
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    else:
        end_date_dt = end_date
    existing_dates = await get_existing_dates(pool, symbol, start_date_dt, end_date_dt)
    missing_ranges = get_missing_date_ranges(existing_dates, start_date_dt, end_date_dt)
    if not missing_ranges:
        print(f"[DEBUG] All data exists for {symbol} in {start_date} to {end_date}, skipping fetch.")
        return
    for range_start, range_end in missing_ranges:
        url = tiingo_url(symbol, range_start, range_end)
        print(f"[DEBUG] Fetching {symbol} from URL: {url}")
        async with session.get(url) as resp:
            print(f"[DEBUG] HTTP status for {symbol}: {resp.status}")
