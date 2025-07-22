import pytest
import asyncio
import asyncpg
import os
from datetime import date

from config.environment import get_environment, set_environment, EnvironmentType
set_environment(EnvironmentType.INTEGRATION)
env = get_environment()
TSDB_URL = env.get_database_url()

from db.test_db_manager import TestDatabaseManager

@pytest.mark.asyncio
async def test_spy_membership_count_near_500(request):
    testname = request.node.name
    db_manager = TestDatabaseManager("integration")
    await db_manager.setup_isolated_test_tables(["spy_membership_change"], testname)
    pool = await asyncpg.create_pool(db_manager.db_url)
    test_date = date(2023, 1, 3)
    table_name = f"intg_spy_membership_change_{testname}"
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"""
            SELECT added FROM {table_name} WHERE change_date <= $1
            AND (removed IS NULL OR change_date > $1)
        """, test_date)
    await pool.close()
    tickers = set([row['added'] for row in rows if row['added']])
    assert 490 <= len(tickers) <= 510, f"Expected ~500 SPY members, got {len(tickers)}"

@pytest.mark.asyncio
async def test_daily_prices_coverage(request):
    testname = request.node.name
    db_manager = TestDatabaseManager("integration")
    await db_manager.setup_isolated_test_tables(["daily_prices"], testname)
    pool = await asyncpg.create_pool(db_manager.db_url)
    test_date = date(2023, 1, 3)
    table_name = f"intg_daily_prices_{testname}"
    async with pool.acquire() as conn:
        count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name} WHERE date = $1", test_date)
    await pool.close()
    assert count > 250, f"Expected at least 250 daily prices for {test_date}, got {count}"

@pytest.mark.asyncio
async def test_splits_and_dividends_present(request):
    testname = request.node.name
    db_manager = TestDatabaseManager("integration")
    await db_manager.setup_isolated_test_tables(["splits", "dividends"], testname)
    pool = await asyncpg.create_pool(db_manager.db_url)
    table_splits = f"intg_splits_{testname}"
    table_dividends = f"intg_dividends_{testname}"
    async with pool.acquire() as conn:
        splits = await conn.fetchval(f"SELECT COUNT(*) FROM {table_splits}")
        dividends = await conn.fetchval(f"SELECT COUNT(*) FROM {table_dividends}")
    await pool.close()
    assert splits > 0, "No splits found in database!"
    assert dividends > 0, "No dividends found in database!"

@pytest.mark.asyncio
async def test_adjusted_prices_not_null(request):
    testname = request.node.name
    db_manager = TestDatabaseManager("integration")
    await db_manager.setup_isolated_test_tables(["daily_prices"], testname)
    pool = await asyncpg.create_pool(db_manager.db_url)
    test_date = date(2023, 1, 3)
    table_name = f"intg_daily_prices_{testname}"
    async with pool.acquire() as conn:
        null_count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name} WHERE date = $1 AND adjusted_price IS NULL", test_date)
    await pool.close()
    assert null_count == 0, f"Found {null_count} rows with NULL adjusted_price on {test_date}"
