import pytest
import asyncio
import asyncpg
import os
from datetime import date

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")

@pytest.mark.asyncio
async def test_splits_and_dividends_present():
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        splits = await conn.fetchval("SELECT COUNT(*) FROM stock_splits")
        dividends = await conn.fetchval("SELECT COUNT(*) FROM dividends")
    await pool.close()
    assert splits > 0, "No splits found in database!"
    assert dividends > 0, "No dividends found in database!"

@pytest.mark.asyncio
async def test_adjusted_prices_not_null():
    pool = await asyncpg.create_pool(TSDB_URL)
    test_date = date(2023, 1, 3)
    async with pool.acquire() as conn:
        null_count = await conn.fetchval("SELECT COUNT(*) FROM daily_prices WHERE date = $1 AND adjusted_price IS NULL", test_date)
    await pool.close()
    assert null_count == 0, f"Found {null_count} rows with NULL adjusted_price on {test_date}"
