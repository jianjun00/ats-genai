import os
import sys
import asyncio
import asyncpg
import pytest
import pandas as pd
from datetime import date

# Add src to path for environment configuration
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from config.environment import get_environment, set_environment, EnvironmentType

# Set integration environment for these tests
set_environment(EnvironmentType.INTEGRATION)
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
    env = get_environment()
    pool = await asyncpg.create_pool(env.get_database_url())
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
    from universe.populate_daily_adjusted_prices import calculate_adjusted_prices
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
    env = get_environment()
    pool = await asyncpg.create_pool(env.get_database_url())
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
    from universe.populate_daily_adjusted_prices import calculate_adjusted_prices
    prices = await fetch_prices(pool, symbol)
    splits = await fetch_splits(pool, symbol)
    dividends = await fetch_dividends(pool, symbol)
    adj_df = calculate_adjusted_prices(prices, splits, dividends)
    assert adj_df['adjustment_factor'].iloc[0] == pytest.approx(1.0)
    assert adj_df['adjustment_factor'].iloc[1] == pytest.approx(0.5)
    assert adj_df.loc[2, 'adjustment_factor'] == 0.5
    await cleanup(pool, symbol)
    await pool.close()

# --- One dividend only ---
@pytest.mark.asyncio
async def test_adjusted_prices_one_dividend():
    symbol = "TESTDIVIDEND"
    env = get_environment()
    pool = await asyncpg.create_pool(env.get_database_url())
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
    from universe.populate_daily_adjusted_prices import calculate_adjusted_prices
    prices = await fetch_prices(pool, symbol)
    splits = await fetch_splits(pool, symbol)
    dividends = await fetch_dividends(pool, symbol)
    adj_df = calculate_adjusted_prices(prices, splits, dividends)
    assert adj_df['adjustment_factor'].iloc[0] == pytest.approx(1.0)
    assert adj_df['adjustment_factor'].iloc[1] == pytest.approx((110-10)/110)
    assert adj_df['adjustment_factor'].iloc[2] == pytest.approx((115-0)/115 * (110-10)/110)
    await cleanup(pool, symbol)
    await pool.close()

# --- One split and one dividend ---
@pytest.mark.asyncio
async def test_adjusted_prices_split_and_dividend():
    symbol = "TESTSPLITDIV"
    env = get_environment()
    pool = await asyncpg.create_pool(env.get_database_url())
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
    from universe.populate_daily_adjusted_prices import calculate_adjusted_prices
    prices = await fetch_prices(pool, symbol)
    splits = await fetch_splits(pool, symbol)
    dividends = await fetch_dividends(pool, symbol)
    adj_df = calculate_adjusted_prices(prices, splits, dividends)
    assert adj_df['adjustment_factor'].iloc[0] == pytest.approx(1.0)
    assert adj_df['adjustment_factor'].iloc[1] == pytest.approx(0.5)
    # Day 3: split (0.5) * dividend ((115-5)/115)
    assert adj_df['adjustment_factor'].iloc[2] == pytest.approx(0.5 * (115-5)/115)
    await cleanup(pool, symbol)
    await pool.close()

# --- Two dividends ---
@pytest.mark.asyncio
async def test_adjusted_prices_two_dividends():
    symbol = "TESTTWODIV"
    env = get_environment()
    pool = await asyncpg.create_pool(env.get_database_url())
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
    from universe.populate_daily_adjusted_prices import calculate_adjusted_prices
    prices = await fetch_prices(pool, symbol)
    splits = await fetch_splits(pool, symbol)
    dividends = await fetch_dividends(pool, symbol)
    adj_df = calculate_adjusted_prices(prices, splits, dividends)
    assert adj_df['adjustment_factor'].iloc[0] == pytest.approx(1.0)
    assert adj_df['adjustment_factor'].iloc[1] == pytest.approx((110-10)/110)
    # Day 3: (115-5)/115 * (110-10)/110
    assert adj_df['adjustment_factor'].iloc[2] == pytest.approx((115-5)/115 * (110-10)/110)
    await cleanup(pool, symbol)
    await pool.close()

# --- Multiple splits on the same date ---
@pytest.mark.asyncio
async def test_adjusted_prices_multiple_splits_same_day():
    symbol = "TESTMULTISPLIT"
    env = get_environment()
    pool = await asyncpg.create_pool(env.get_database_url())
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
    await insert_split(pool, date(2022, 1, 2), 5, 4, symbol)  # 5-for-4 split (on same day)
    from universe.populate_daily_adjusted_prices import calculate_adjusted_prices
    prices = await fetch_prices(pool, symbol)
    splits = await fetch_splits(pool, symbol)
    dividends = await fetch_dividends(pool, symbol)
    adj_df = calculate_adjusted_prices(prices, splits, dividends)
    # Adjustment for day 1: 1.0
    assert adj_df['adjustment_factor'].iloc[0] == pytest.approx(1.0)
    # Adjustment for day 2: (1/2)*(4/5) = 0.4
    assert adj_df['adjustment_factor'].iloc[1] == pytest.approx(0.4)
    # Adjustment for day 3: 0.4
    assert adj_df['adjustment_factor'].iloc[2] == pytest.approx(0.4)
    await cleanup(pool, symbol)
    await pool.close()

# --- Multiple dividends on the same date ---
@pytest.mark.asyncio
async def test_adjusted_prices_multiple_dividends_same_day():
    symbol = "TESTMULTIDIV"
    env = get_environment()
    pool = await asyncpg.create_pool(env.get_database_url())
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
    await insert_dividend(pool, date(2022, 1, 2), 5, symbol)
    from universe.populate_daily_adjusted_prices import calculate_adjusted_prices
    prices = await fetch_prices(pool, symbol)
    splits = await fetch_splits(pool, symbol)
    dividends = await fetch_dividends(pool, symbol)
    adj_df = calculate_adjusted_prices(prices, splits, dividends)
    # Day 1: 1.0
    assert adj_df['adjustment_factor'].iloc[0] == pytest.approx(1.0)
    # Day 2: (110-15)/110
    assert adj_df['adjustment_factor'].iloc[1] == pytest.approx((110-15)/110)
    # Day 3: (115-0)/115 * (110-15)/110
    assert adj_df['adjustment_factor'].iloc[2] == pytest.approx((115-0)/115 * (110-15)/110)
    await cleanup(pool, symbol)
    await pool.close()

# --- Multiple splits and dividends on the same date ---
@pytest.mark.asyncio
async def test_adjusted_prices_multiple_splits_and_dividends_same_day():
    symbol = "TESTMULTIBOTH"
    env = get_environment()
    pool = await asyncpg.create_pool(env.get_database_url())
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
    await insert_split(pool, date(2022, 1, 2), 2, 1, symbol)
    await insert_split(pool, date(2022, 1, 2), 5, 4, symbol)
    await insert_dividend(pool, date(2022, 1, 2), 10, symbol)
    await insert_dividend(pool, date(2022, 1, 2), 5, symbol)
    from universe.populate_daily_adjusted_prices import calculate_adjusted_prices
    prices = await fetch_prices(pool, symbol)
    splits = await fetch_splits(pool, symbol)
    dividends = await fetch_dividends(pool, symbol)
    adj_df = calculate_adjusted_prices(prices, splits, dividends)
    # Day 1: 1.0
    assert adj_df['adjustment_factor'].iloc[0] == pytest.approx(1.0)
    # Day 2: (1/2)*(4/5)*((110-15)/110) = 0.4*((110-15)/110)
    assert adj_df['adjustment_factor'].iloc[1] == pytest.approx(0.4*((110-15)/110))
    # Day 3: 0.4*((115-0)/115)*((110-15)/110)
    assert adj_df['adjustment_factor'].iloc[2] == pytest.approx(0.4*((115-0)/115)*((110-15)/110))
    await cleanup(pool, symbol)
    await pool.close()

# --- Update helpers for symbol ---
async def cleanup(pool, symbol=TEST_SYMBOL):
    env = get_environment()
    async with pool.acquire() as conn:
        daily_prices_table = env.get_table_name("daily_prices")
        splits_table = env.get_table_name("stock_splits")
        dividends_table = env.get_table_name("dividends")
        daily_adjusted_prices_table = env.get_table_name("daily_adjusted_prices")
        await conn.execute(f"DELETE FROM {daily_prices_table} WHERE symbol = '{symbol}'")
        await conn.execute(f"DELETE FROM {splits_table} WHERE symbol = '{symbol}'")
        await conn.execute(f"DELETE FROM {dividends_table} WHERE symbol = '{symbol}'")
        await conn.execute(f"DELETE FROM {daily_adjusted_prices_table} WHERE symbol = '{symbol}'")

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
    env = get_environment()
    async with pool.acquire() as conn:
        splits_table = env.get_table_name("stock_splits")
        await conn.execute(
            f"INSERT INTO {splits_table} (split_date, symbol, numerator, denominator) VALUES ($1, $2, $3, $4)",
            split_date, symbol, numerator, denominator
        )

async def insert_dividend(pool, ex_date, amount, symbol):
    env = get_environment()
    async with pool.acquire() as conn:
        dividends_table = env.get_table_name("dividends")
        await conn.execute(
            f"INSERT INTO {dividends_table} (ex_date, symbol, amount) VALUES ($1, $2, $3)",
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
    env = get_environment()
    async with pool.acquire() as conn:
        daily_prices_table = env.get_table_name("daily_prices")
        rows = await conn.fetch(
            f"SELECT * FROM {daily_prices_table} WHERE symbol = $1 ORDER BY date ASC", symbol
        )
        return pd.DataFrame([dict(row) for row in rows])

async def fetch_splits(pool, symbol):
    env = get_environment()
    async with pool.acquire() as conn:
        splits_table = env.get_table_name("stock_splits")
        rows = await conn.fetch(
            f"SELECT split_date, numerator, denominator FROM {splits_table} WHERE symbol = $1 ORDER BY split_date ASC", symbol
        )
        return pd.DataFrame([dict(row) for row in rows])

async def fetch_dividends(pool, symbol):
    env = get_environment()
    async with pool.acquire() as conn:
        dividends_table = env.get_table_name("dividends")
        rows = await conn.fetch(
            f"SELECT ex_date, amount FROM {dividends_table} WHERE symbol = $1 ORDER BY ex_date ASC", symbol
        )
        return pd.DataFrame([dict(row) for row in rows])
