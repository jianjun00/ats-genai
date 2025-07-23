import pytest
import asyncio
from datetime import date
from trading.trading_universe import TradingUniverse, SecurityMaster
import os
import asyncpg

from config.environment import get_environment, set_environment, EnvironmentType
set_environment(EnvironmentType.INTEGRATION)
env = get_environment()
TSDB_URL = env.get_database_url()

@pytest.mark.asyncio
async def test_trading_universe_update(backup_and_restore_tables, monkeypatch):
    # Setup: Insert mock daily_prices data
    pool = await asyncpg.create_pool(TSDB_URL)
    today = date(2023, 7, 20)
    async with pool.acquire() as conn:
        # Clean up existing data
        daily_prices_table = env.get_table_name("daily_prices")
        daily_market_cap_table = env.get_table_name("daily_market_cap")
        await conn.execute(f"DELETE FROM {daily_prices_table} WHERE date = $1", today)
        await conn.execute(f"DELETE FROM {daily_market_cap_table} WHERE date = $1", today)
        
        # Insert data into daily_prices
        await conn.execute(f"""
            INSERT INTO {daily_prices_table} (date, symbol, open, high, low, close, volume)
            VALUES
            ($1, 'AAA', 10, 10, 10, 10, 2000000), -- eligible
            ($1, 'BBB', 4, 4, 4, 4, 2000000),     -- price too low
            ($1, 'CCC', 10, 10, 10, 10, 500000),  -- volume too low
            ($1, 'DDD', 10, 10, 10, 10, 2000000)  -- market cap too low
        """, today)
        # Insert data into daily_market_cap
        await conn.execute(f"""
            INSERT INTO {daily_market_cap_table} (date, symbol, market_cap)
            VALUES
            ($1, 'AAA', 1000000000), -- eligible
            ($1, 'BBB', 1000000000), -- price too low
            ($1, 'CCC', 1000000000), -- volume too low
            ($1, 'DDD', 10000000)    -- market cap too low
        """, today)

        # Debug: print inserted daily_prices rows
        print("--- DEBUG: daily_prices rows for test date ---")
        rows = await conn.fetch(f"SELECT * FROM {daily_prices_table} WHERE date = $1 ORDER BY symbol", today)
        for row in rows:
            print(dict(row))
        print("--- DEBUG: daily_market_cap rows for test date ---")
        rows = await conn.fetch(f"SELECT * FROM {daily_market_cap_table} WHERE date = $1 ORDER BY symbol", today)
        for row in rows:
            print(dict(row))

    await pool.close()

    universe = TradingUniverse(TSDB_URL)
    await universe.update_for_end_of_day(today)
    eligible = universe.get_current_universe()
    assert 'AAA' in eligible
    assert 'BBB' not in eligible
    assert 'CCC' not in eligible
    assert 'DDD' not in eligible

@pytest.mark.asyncio
async def test_security_master():
    today = date(2023, 7, 20)
    master = SecurityMaster(TSDB_URL)
    info = await master.get_security_info('AAA', today)
    assert info['close'] == 10
    assert info['volume'] == 2000000
    # Test missing symbol
    missing = await master.get_security_info('ZZZ', today)
    assert missing is None

@pytest.mark.asyncio
async def test_security_master_multiple():
    today = date(2023, 7, 20)
    master = SecurityMaster(TSDB_URL)
    infos = await master.get_multiple_securities_info(['AAA', 'BBB', 'CCC', 'DDD'], today)
    assert 'AAA' in infos
    assert 'BBB' in infos
    assert infos['AAA']['close'] == 10
    assert infos['BBB']['close'] == 4
