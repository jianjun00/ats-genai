import pytest
import asyncio
import asyncpg
import os
from datetime import date

from config.environment import get_environment, set_environment, EnvironmentType
set_environment(EnvironmentType.INTEGRATION)
env = get_environment()
TSDB_URL = env.get_database_url()

@pytest.mark.asyncio
async def test_no_duplicate_daily_prices():
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        daily_prices_table = env.get_table_name("daily_prices")
        rows = await conn.fetch(f"""
            SELECT symbol, date, COUNT(*) as cnt
            FROM {daily_prices_table}
            GROUP BY symbol, date
            HAVING COUNT(*) > 1
        """)
    await pool.close()
    assert len(rows) == 0, f"Duplicate daily_prices rows found: {rows}"

@pytest.mark.asyncio
async def test_no_duplicate_splits():
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        stock_splits_table = env.get_table_name("stock_splits")
        rows = await conn.fetch(f"""
            SELECT symbol, ex_date, COUNT(*) as cnt
            FROM {stock_splits_table}
            GROUP BY symbol, ex_date
            HAVING COUNT(*) > 1
        """)
    await pool.close()
    assert len(rows) == 0, f"Duplicate stock_splits rows found: {rows}"

@pytest.mark.asyncio
async def test_no_duplicate_dividends():
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        dividends_table = env.get_table_name("dividends")
        rows = await conn.fetch(f"""
            SELECT symbol, ex_date, COUNT(*) as cnt
            FROM {dividends_table}
            GROUP BY symbol, ex_date
            HAVING COUNT(*) > 1
        """)
    await pool.close()
    assert len(rows) == 0, f"Duplicate dividends rows found: {rows}"

