import pytest
import asyncio
from datetime import date
from src.trading.trading_universe import TradingUniverse, SecurityMaster
import os
import asyncpg

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")

@pytest.mark.asyncio
async def test_trading_universe_update(monkeypatch):
    # Setup: Insert mock daily_prices data
    pool = await asyncpg.create_pool(TSDB_URL)
    today = date(2023, 7, 20)
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM daily_prices WHERE date = $1", today)
        await conn.execute("""
            INSERT INTO daily_prices (date, symbol, close, adv, market_cap)
            VALUES
            ($1, 'AAA', 10, 2000000, 1000000000), -- eligible
            ($1, 'BBB', 4, 2000000, 1000000000),  -- price too low
            ($1, 'CCC', 10, 500000, 1000000000),  -- adv too low
            ($1, 'DDD', 10, 2000000, 10000000)    -- market cap too low
        """, today)
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
    assert info['adv'] == 2000000
    assert info['market_cap'] == 1000000000
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
