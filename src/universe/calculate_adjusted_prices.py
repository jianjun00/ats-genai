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
    # Algorithm: walk forward, apply splits and dividends as they happen
    adj_factors = {}
    split_idx = 0
    div_idx = 0
    factor = 1.0
    adj = {}
    splits = list(splits)
    dividends = list(dividends)
    # Create a date->close dict for fast lookup
    close_map = {row['date']: row['close'] for row in prices}
    dates = sorted(close_map.keys(), reverse=True)  # adjust backward in time
    for dt in dates:
        # Apply splits
        while split_idx < len(splits) and splits[split_idx]['split_date'] > dt:
            split = splits[split_idx]
            factor *= split['denominator'] / split['numerator']
            split_idx += 1
        # Apply dividends
        while div_idx < len(dividends) and dividends[div_idx]['ex_date'] > dt:
            div = dividends[div_idx]
            if close_map[dt] > 0:
                factor *= (close_map[dt] - div['amount']) / close_map[dt]
            div_idx += 1
        adj[dt] = close_map[dt] * factor
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
