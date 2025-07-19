import pytest
import asyncio
import asyncpg
import os
from datetime import date

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")

@pytest.mark.asyncio
async def test_spy_membership_count_near_500():
    pool = await asyncpg.create_pool(TSDB_URL)
    test_date = date(2023, 1, 3)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT added FROM spy_membership_change WHERE event_date <= $1
            AND (removed IS NULL OR event_date > $1)
        """, test_date)
    await pool.close()
    tickers = set([row['added'] for row in rows if row['added']])
    assert 490 <= len(tickers) <= 510, f"Expected ~500 SPY members, got {len(tickers)}"

@pytest.mark.asyncio
async def test_daily_prices_coverage():
    pool = await asyncpg.create_pool(TSDB_URL)
    test_date = date(2023, 1, 3)
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM daily_prices WHERE date = $1", test_date)
    await pool.close()
    assert count > 450, f"Expected at least 450 daily prices for {test_date}, got {count}"
