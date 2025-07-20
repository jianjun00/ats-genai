import os
import asyncio
import asyncpg
import pytest
import pandas as pd
from datetime import date

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")
CREATE_DAILY_PRICES_SQL = """
CREATE TABLE IF NOT EXISTS daily_prices (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    market_cap DOUBLE PRECISION,
    adv DOUBLE PRECISION,
    PRIMARY KEY (date, symbol)
);
"""
CREATE_SPLITS_SQL = """
CREATE TABLE IF NOT EXISTS stock_splits (
    split_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    numerator DOUBLE PRECISION,
    denominator DOUBLE PRECISION
);
"""
CREATE_DIVIDENDS_SQL = """
CREATE TABLE IF NOT EXISTS dividends (
    ex_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    amount DOUBLE PRECISION
);
"""
CREATE_DAILY_ADJUSTED_PRICES_SQL = """
CREATE TABLE IF NOT EXISTS daily_adjusted_prices (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    market_cap DOUBLE PRECISION,
    original_open DOUBLE PRECISION,
    original_high DOUBLE PRECISION,
    original_low DOUBLE PRECISION,
    original_close DOUBLE PRECISION,
    split_numerator DOUBLE PRECISION,
    split_denominator DOUBLE PRECISION,
    dividend_amount DOUBLE PRECISION,
    adjustment_factor DOUBLE PRECISION,
    PRIMARY KEY (date, symbol)
);
"""

TEST_SYMBOL = "TESTADJ"

@pytest.mark.asyncio
async def test_adjusted_prices_basic(tmp_path):
    pool = await asyncpg.create_pool(TSDB_URL)
    # Ensure tables exist
    async with pool.acquire() as conn:
        await conn.execute(CREATE_DAILY_PRICES_SQL)
        await conn.execute(CREATE_SPLITS_SQL)
        await conn.execute(CREATE_DIVIDENDS_SQL)
        await conn.execute(CREATE_DAILY_ADJUSTED_PRICES_SQL)
    # Clean up any old data
    await cleanup(pool)
    # Insert test data: 3 days, no splits/dividends
    await insert_prices(pool, [
        (date(2022, 1, 1), TEST_SYMBOL, 100, 110, 90, 105, 1000, 105000, 100),
        (date(2022, 1, 2), TEST_SYMBOL, 105, 115, 95, 110, 1200, 110000, 110),
        (date(2022, 1, 3), TEST_SYMBOL, 110, 120, 100, 115, 1300, 115000, 115),
    ])
    # Run adjustment
    from src.universe.populate_daily_adjusted_prices import calculate_adjusted_prices
    prices = await fetch_prices(pool, TEST_SYMBOL)
    splits = await fetch_splits(pool, TEST_SYMBOL)
    dividends = await fetch_dividends(pool, TEST_SYMBOL)
    adj_df = calculate_adjusted_prices(prices, splits, dividends)
    # No adjustment expected
    assert all(adj_df['open'] == adj_df['adj_open'])
    assert all(adj_df['close'] == adj_df['adj_close'])
    assert all(adj_df['adjustment_factor'] == 1.0)
    # Clean up
    await cleanup(pool)
    await pool.close()

# --- One split only ---
@pytest.mark.asyncio
async def test_adjusted_prices_one_split():
    symbol = "TESTSPLIT"
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_DAILY_PRICES_SQL)
        await conn.execute(CREATE_SPLITS_SQL)
        await conn.execute(CREATE_DIVIDENDS_SQL)
        await conn.execute(CREATE_DAILY_ADJUSTED_PRICES_SQL)
    await cleanup(pool, symbol)
    await insert_prices(pool, [
        (date(2022, 1, 1), symbol, 100, 110, 90, 105, 1000, 105000, 100),
        (date(2022, 1, 2), symbol, 105, 115, 95, 110, 1200, 110000, 110),
        (date(2022, 1, 3), symbol, 110, 120, 100, 115, 1300, 115000, 115),
    ])
    await insert_split(pool, date(2022, 1, 2), 2, 1, symbol)  # 2-for-1 split
    from src.universe.populate_daily_adjusted_prices import calculate_adjusted_prices
    prices = await fetch_prices(pool, symbol)
    splits = await fetch_splits(pool, symbol)
    dividends = await fetch_dividends(pool, symbol)
    adj_df = calculate_adjusted_prices(prices, splits, dividends)
    assert adj_df.loc[0, 'adjustment_factor'] == pytest.approx(1.0)
    assert adj_df.loc[1, 'adjustment_factor'] == pytest.approx(0.5)
    assert adj_df.loc[2, 'adjustment_factor'] == 0.5
    await cleanup(pool, symbol)
    await pool.close()

# --- One dividend only ---
@pytest.mark.asyncio
async def test_adjusted_prices_one_dividend():
    symbol = "TESTDIV1"
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_DAILY_PRICES_SQL)
        await conn.execute(CREATE_SPLITS_SQL)
        await conn.execute(CREATE_DIVIDENDS_SQL)
        await conn.execute(CREATE_DAILY_ADJUSTED_PRICES_SQL)
    await cleanup(pool, symbol)
    await insert_prices(pool, [
        (date(2022, 1, 1), symbol, 100, 110, 90, 105, 1000, 105000, 100),
        (date(2022, 1, 2), symbol, 105, 115, 95, 110, 1200, 110000, 110),
        (date(2022, 1, 3), symbol, 110, 120, 100, 115, 1300, 115000, 115),
    ])
    await insert_dividend(pool, date(2022, 1, 2), 10, symbol)  # $10 dividend
    from src.universe.populate_daily_adjusted_prices import calculate_adjusted_prices
    prices = await fetch_prices(pool, symbol)
    splits = await fetch_splits(pool, symbol)
    dividends = await fetch_dividends(pool, symbol)
    adj_df = calculate_adjusted_prices(prices, splits, dividends)
    assert adj_df.loc[0, 'adjustment_factor'] == pytest.approx(1.0)
    assert adj_df.loc[1, 'adjustment_factor'] == pytest.approx((110-10)/110)
    assert adj_df.loc[2, 'adjustment_factor'] == pytest.approx((115-0)/115 * (110-10)/110)
    await cleanup(pool, symbol)
    await pool.close()

# --- One split and one dividend ---
@pytest.mark.asyncio
async def test_adjusted_prices_split_and_dividend():
    symbol = "TESTSPLITDIV"
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_DAILY_PRICES_SQL)
        await conn.execute(CREATE_SPLITS_SQL)
        await conn.execute(CREATE_DIVIDENDS_SQL)
        await conn.execute(CREATE_DAILY_ADJUSTED_PRICES_SQL)
    await cleanup(pool, symbol)
    await insert_prices(pool, [
        (date(2022, 1, 1), symbol, 100, 110, 90, 105, 1000, 105000, 100),
        (date(2022, 1, 2), symbol, 105, 115, 95, 110, 1200, 110000, 110),
        (date(2022, 1, 3), symbol, 110, 120, 100, 115, 1300, 115000, 115),
    ])
    await insert_split(pool, date(2022, 1, 2), 2, 1, symbol)  # 2-for-1 split
    await insert_dividend(pool, date(2022, 1, 3), 5, symbol)  # $5 dividend
    from src.universe.populate_daily_adjusted_prices import calculate_adjusted_prices
    prices = await fetch_prices(pool, symbol)
    splits = await fetch_splits(pool, symbol)
    dividends = await fetch_dividends(pool, symbol)
    adj_df = calculate_adjusted_prices(prices, splits, dividends)
    assert adj_df.loc[0, 'adjustment_factor'] == pytest.approx(1.0)
    assert adj_df.loc[1, 'adjustment_factor'] == pytest.approx(0.5)
    # Day 3: split (0.5) * dividend ((115-5)/115)
    assert adj_df.loc[2, 'adjustment_factor'] == pytest.approx(0.5 * (115-5)/115)
    await cleanup(pool, symbol)
    await pool.close()

# --- Two dividends ---
@pytest.mark.asyncio
async def test_adjusted_prices_two_dividends():
    symbol = "TESTDIV2"
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_DAILY_PRICES_SQL)
        await conn.execute(CREATE_SPLITS_SQL)
        await conn.execute(CREATE_DIVIDENDS_SQL)
        await conn.execute(CREATE_DAILY_ADJUSTED_PRICES_SQL)
    await cleanup(pool, symbol)
    await insert_prices(pool, [
        (date(2022, 1, 1), symbol, 100, 110, 90, 105, 1000, 105000, 100),
        (date(2022, 1, 2), symbol, 105, 115, 95, 110, 1200, 110000, 110),
        (date(2022, 1, 3), symbol, 110, 120, 100, 115, 1300, 115000, 115),
    ])
    await insert_dividend(pool, date(2022, 1, 2), 10, symbol)
    await insert_dividend(pool, date(2022, 1, 3), 5, symbol)
    from src.universe.populate_daily_adjusted_prices import calculate_adjusted_prices
    prices = await fetch_prices(pool, symbol)
    splits = await fetch_splits(pool, symbol)
    dividends = await fetch_dividends(pool, symbol)
    adj_df = calculate_adjusted_prices(prices, splits, dividends)
    assert adj_df.loc[0, 'adjustment_factor'] == pytest.approx(1.0)
    assert adj_df.loc[1, 'adjustment_factor'] == pytest.approx((110-10)/110)
    # Day 3: (115-5)/115 * (110-10)/110
    assert adj_df.loc[2, 'adjustment_factor'] == pytest.approx((115-5)/115 * (110-10)/110)
    await cleanup(pool, symbol)
    await pool.close()

# --- Update helpers for symbol ---
async def cleanup(pool, symbol=TEST_SYMBOL):
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM daily_prices WHERE symbol = '{symbol}'")
        await conn.execute(f"DELETE FROM stock_splits WHERE symbol = '{symbol}'")
        await conn.execute(f"DELETE FROM dividends WHERE symbol = '{symbol}'")
        await conn.execute(f"DELETE FROM daily_adjusted_prices WHERE symbol = '{symbol}'")

def make_row(row):
    return {
        'date': row[0],
        'symbol': row[1],
        'open': row[2],
        'high': row[3],
        'low': row[4],
        'close': row[5],
        'volume': row[6],
        'market_cap': row[7],
        'adv': row[8],
    }

async def insert_prices(pool, rows):
    async with pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO daily_prices (date, symbol, open, high, low, close, volume, market_cap, adv) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            rows
        )

async def insert_split(pool, split_date, numerator, denominator, symbol):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO stock_splits (split_date, symbol, numerator, denominator) VALUES ($1, $2, $3, $4)",
            split_date, symbol, numerator, denominator
        )

async def insert_dividend(pool, ex_date, amount, symbol):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO dividends (ex_date, symbol, amount) VALUES ($1, $2, $3)",
            ex_date, symbol, amount
        )


def make_row(row):
    return {
        'date': row[0],
        'symbol': row[1],
        'open': row[2],
        'high': row[3],
        'low': row[4],
        'close': row[5],
        'volume': row[6],
        'market_cap': row[7],
        'adv': row[8],
    }

async def insert_prices(pool, rows):
    async with pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO daily_prices (date, symbol, open, high, low, close, volume, market_cap, adv) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            rows
        )





async def fetch_prices(pool, symbol):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM daily_prices WHERE symbol = $1 ORDER BY date ASC", symbol
        )
        return pd.DataFrame([dict(row) for row in rows])

async def fetch_splits(pool, symbol):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT split_date, numerator, denominator FROM stock_splits WHERE symbol = $1 ORDER BY split_date ASC", symbol
        )
        return pd.DataFrame([dict(row) for row in rows])

async def fetch_dividends(pool, symbol):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT ex_date, amount FROM dividends WHERE symbol = $1 ORDER BY ex_date ASC", symbol
        )
        return pd.DataFrame([dict(row) for row in rows])
