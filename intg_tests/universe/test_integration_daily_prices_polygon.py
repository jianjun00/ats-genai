import os
import asyncio
import asyncpg
import pytest
from datetime import datetime

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")

pytestmark = pytest.mark.asyncio

# Sample data row for Polygon
SAMPLE_ROW = {
    't': int(datetime(2023, 1, 3).timestamp() * 1000),
    'o': 100.0,
    'h': 110.0,
    'l': 99.0,
    'c': 105.0,
    'v': 1000000
}
SAMPLE_SYMBOL = 'TESTSYM'
SAMPLE_SHARES_OUTSTANDING = 1000000

# Import the insert_prices function from the Polygon script
from src.universe.daily_polygon import insert_prices, CREATE_DAILY_PRICES_POLYGON_SQL

async def cleanup_test_data(pool, symbol):
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM daily_prices_polygon WHERE symbol = $1", symbol)

@pytest.mark.asyncio
async def test_insert_prices_polygon():
    pool = await asyncpg.create_pool(TSDB_URL)
    # Ensure table exists
    async with pool.acquire() as conn:
        await conn.execute(CREATE_DAILY_PRICES_POLYGON_SQL)
    # Clean up in case of previous test runs
    await cleanup_test_data(pool, SAMPLE_SYMBOL)
    # Insert sample data
    await insert_prices([SAMPLE_ROW], SAMPLE_SYMBOL, SAMPLE_SHARES_OUTSTANDING)
    # Verify data
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM daily_prices_polygon WHERE symbol = $1", SAMPLE_SYMBOL)
        assert row is not None, "No row inserted"
        assert row['symbol'] == SAMPLE_SYMBOL
        assert row['open'] == SAMPLE_ROW['o']
        assert row['close'] == SAMPLE_ROW['c']
        assert row['market_cap'] == SAMPLE_ROW['c'] * SAMPLE_SHARES_OUTSTANDING
    # Clean up
    await cleanup_test_data(pool, SAMPLE_SYMBOL)
    await pool.close()
