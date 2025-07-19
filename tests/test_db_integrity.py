import pytest
import asyncio
import asyncpg
import os
from datetime import date

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")

@pytest.mark.asyncio
async def test_no_duplicate_daily_prices():
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol, date, COUNT(*) as cnt
            FROM daily_prices
            GROUP BY symbol, date
            HAVING COUNT(*) > 1
        """)
    await pool.close()
    assert len(rows) == 0, f"Duplicate daily_prices rows found: {rows}"

@pytest.mark.asyncio
async def test_no_duplicate_splits():
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol, split_date, COUNT(*) as cnt
            FROM stock_splits
            GROUP BY symbol, split_date
            HAVING COUNT(*) > 1
        """)
    await pool.close()
    assert len(rows) == 0, f"Duplicate stock_splits rows found: {rows}"

@pytest.mark.asyncio
async def test_no_duplicate_dividends():
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol, ex_date, COUNT(*) as cnt
            FROM dividends
            GROUP BY symbol, ex_date
            HAVING COUNT(*) > 1
        """)
    await pool.close()
    assert len(rows) == 0, f"Duplicate dividends rows found: {rows}"

@pytest.mark.asyncio
async def test_adjusted_price_not_null():
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT COUNT(*) AS cnt FROM daily_prices WHERE adjusted_price IS NULL")
    await pool.close()
    assert row['cnt'] == 0, f"Found {row['cnt']} daily_prices rows with NULL adjusted_price."
