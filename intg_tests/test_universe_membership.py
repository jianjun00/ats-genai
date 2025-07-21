import pytest
import asyncio
import asyncpg
import os
from datetime import date

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")

@pytest.mark.asyncio
async def test_spy_membership_consistency():
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        # Ensure all added tickers are in at least one period
        added = await conn.fetch("SELECT DISTINCT added FROM spy_membership_change WHERE added IS NOT NULL")
        for row in added:
            count = await conn.fetchval("SELECT COUNT(*) FROM daily_prices WHERE symbol=$1", row['added'])
            assert count > 0, f"Ticker {row['added']} never appears in daily_prices"
    await pool.close()
