import os
import asyncio
import asyncpg
from datetime import datetime

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")

async def get_all_symbols():
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT symbol FROM daily_prices")
    await pool.close()
    return [row['symbol'] for row in rows]

async def get_events_for_symbol(symbol):
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        # Get splits
        splits = await conn.fetch("SELECT split_date, numerator, denominator FROM stock_splits WHERE symbol=$1 ORDER BY split_date", symbol)
        # Get dividends
        dividends = await conn.fetch("SELECT ex_date, amount FROM dividends WHERE symbol=$1 ORDER BY ex_date", symbol)
        # Get daily prices
        prices = await conn.fetch("SELECT date, close FROM daily_prices WHERE symbol=$1 ORDER BY date", symbol)
    await pool.close()
    return splits, dividends, prices

async def update_adjusted_prices(symbol, adjusted):
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        await conn.executemany(
            "UPDATE daily_prices SET adjusted_price=$1 WHERE symbol=$2 AND date=$3",
            [(adj, symbol, dt) for dt, adj in adjusted.items()]
        )
    await pool.close()

def compute_adjusted_prices(prices, splits, dividends):
    # prices: list of (date, close)
    # splits: list of (split_date, numerator, denominator)
    # dividends: list of (ex_date, amount)
    # Returns: dict {date: adjusted_close}
    # Algorithm: back-adjust all prices so that each date reflects all splits/dividends after that date.
    close_map = {row['date']: row['close'] for row in prices}
    dates = sorted(close_map.keys())  # walk forward in time
    # Pre-sort splits and dividends by date ascending
    splits = sorted(splits, key=lambda x: x['split_date'])
    dividends = sorted(dividends, key=lambda x: x['ex_date'])
    # Precompute cumulative adjustment factors for each date
    split_factors = {}
    div_factors = {}
    factor = 1.0
    split_idx = 0
    div_idx = 0
    # Traverse forward, so we can accumulate all future splits/dividends
    split_events = [(s['split_date'], s['numerator'], s['denominator']) for s in splits]
    div_events = [(d['ex_date'], d['amount']) for d in dividends]
    # Prepare lists of event dates
    split_dates = [s[0] for s in split_events]
    div_dates = [d[0] for d in div_events]
    # For each date, compute the cumulative factor from all splits/dividends after that date
    adj = {}
    # Precompute split-adjusted close for all dates for use in dividend adjustment
    split_factors_by_date = {}
    for dt in dates:
        split_factor = 1.0
        for s in splits:
            if s['split_date'] >= dt:
                split_factor *= s['denominator'] / s['numerator']
        split_factors_by_date[dt] = split_factor
    split_adjusted_close_by_date = {dt: close_map[dt] * split_factors_by_date[dt] for dt in dates}

    # For each date, apply all future dividends, using split-adjusted price at the dividend ex-date
    adj = {}
    for dt in dates:
        # 1. Apply all splits after or on this date (standard back-adjustment)
        split_factor = 1.0
        for s in splits:
            if s['split_date'] >= dt:
                split_factor *= s['denominator'] / s['numerator']
        split_adjusted_close = close_map[dt] * split_factor

        # 2. For all dividends with ex_date >= dt, subtract the dividend amount (additive adjustment)
        div_factor = 1.0
        for d in dividends:
            if d['ex_date'] >= dt:
                px = close_map[d['ex_date']] * split_factors_by_date[d['ex_date']]
                if px > 0:
                    div_factor *= (px - d['amount']) / px
        adj_val = round(split_adjusted_close * div_factor, 6)
        adj[dt] = adj_val
    return adj

async def main():
    symbols = await get_all_symbols()
    for symbol in symbols:
        print(f"Processing {symbol}")
        splits, dividends, prices = await get_events_for_symbol(symbol)
        if not prices:
            continue
        adj = compute_adjusted_prices(prices, splits, dividends)
        await update_adjusted_prices(symbol, adj)
        print(f"Updated adjusted prices for {symbol}")

if __name__ == "__main__":
    asyncio.run(main())
