"""
Integration-style tests for all DAOs in src/db/dao using the test_trading_db database.
Each test will use the TEST environment and real DB access.
"""

import pytest
from datetime import date
import asyncpg
from config.environment import set_environment, EnvironmentType, Environment

# Import all DAOs
from src.db.dao.daily_market_cap_dao import DailyMarketCapDAO
from src.db.dao.daily_prices_dao import DailyPricesDAO
from src.db.dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
from src.db.dao.daily_prices_tiingo_dao import DailyPricesTiingoDAO
from src.db.dao.db_version_dao import DBVersionDAO
from src.db.dao.dividends_dao import DividendsDAO
from src.db.dao.events_dao import EventsDAO
from src.db.dao.fundamentals_dao import FundamentalsDAO
from src.db.dao.instrument_aliases_dao import InstrumentAliasesDAO
from src.db.dao.instrument_metadata_dao import InstrumentMetadataDAO
from src.db.dao.instrument_polygon_dao import InstrumentPolygonDAO
from src.db.dao.instruments_dao import InstrumentsDAO
from src.db.dao.secmaster_dao import SecMasterDAO
from src.db.dao.status_code_dao import StatusCodeDAO
from src.db.dao.stock_splits_dao import StockSplitsDAO
from src.db.dao.universe_dao import UniverseDAO
from src.db.dao.universe_membership_dao import UniverseMembershipDAO
from src.db.dao.vendors_dao import VendorsDAO

@pytest.fixture(scope="module", autouse=True)
def setup_test_env():
    set_environment(EnvironmentType.TEST)

import asyncio
from datetime import date
from config.environment import Environment

@pytest.mark.asyncio
async def test_instruments_dao_crud():
    env = Environment()
    dao = InstrumentsDAO(env)
    symbol = "TESTSYM"
    # Clean up if exists
    instruments = await dao.list_instruments()
    for inst in instruments:
        if inst['symbol'] == symbol:
            # Remove by direct SQL (no delete method in DAO)
            pool = await asyncpg.create_pool(env.get_database_url())
            try:
                async with pool.acquire() as conn:
                    await conn.execute(f"DELETE FROM {dao.table_name} WHERE symbol = $1", symbol)
            finally:
                await pool.close()
    # Create
    inst_id = await dao.create_instrument(symbol=symbol, name="Test Instrument", type_="stock")
    assert inst_id is not None
    # Read
    result = await dao.get_instrument(inst_id)
    assert result is not None
    assert result['symbol'] == symbol
    assert result['instrument_id'] == inst_id
    # List
    all_insts = await dao.list_instruments()
    assert any(inst['symbol'] == symbol for inst in all_insts)

@pytest.mark.asyncio
async def test_daily_market_cap_dao_crud():
    env = Environment()
    dao = DailyMarketCapDAO(env)
    test_date = date(2022, 1, 1)
    symbol = "TESTMCAP"
    market_cap = 12345678
    # Clean up if exists
    pool = await asyncpg.create_pool(env.get_database_url())
    try:
        async with pool.acquire() as conn:
            await conn.execute(f"DELETE FROM {dao.table_name} WHERE date = $1 AND symbol = $2", test_date, symbol)
    finally:
        await pool.close()
    # Insert
    await dao.insert_market_cap(test_date, symbol, market_cap)
    # Get
    result = await dao.get_market_cap(test_date, symbol)
    assert result is not None
    assert result['market_cap'] == market_cap
    # List for date
    results = await dao.list_market_caps_for_date(test_date)
    assert any(r['symbol'] == symbol for r in results)
    # List for symbol
    rows2 = await dao.list_market_caps(symbol)
    assert any(r['date'] == test_date for r in rows2)

@pytest.mark.asyncio
async def test_daily_prices_dao_crud():
    env = Environment()
    dao = DailyPricesDAO(env)
    test_date = date(2022, 2, 2)
    symbol = "TESTPRC"
    # Insert (simulate via direct SQL if needed)
    # No insert method, so test get/list
    # Should not raise error
    _ = await dao.list_prices_for_date(test_date)
    _ = await dao.list_prices_for_symbols_and_date([symbol], test_date)
    _ = await dao.get_price(test_date, symbol)
    _ = await dao.list_prices(symbol)

@pytest.mark.asyncio
async def test_universe_dao_crud():
    env = Environment()
    dao = UniverseDAO(env)
    name = "TESTUNI"
    desc = "Test universe"
    # Clean up if exists
    pool = await asyncpg.create_pool(env.get_database_url())
    try:
        async with pool.acquire() as conn:
            await conn.execute(f"DELETE FROM {dao.table_name} WHERE name = $1", name)
    finally:
        await pool.close()
    # Create
    uid = await dao.create_universe(name, desc)
    assert uid is not None
    # Get by id
    row = await dao.get_universe(uid)
    assert row is not None
    assert row['name'] == name
    # Get by name
    row2 = await dao.get_universe_by_name(name)
    assert row2 is not None
    assert row2['id'] == uid
    # List
    all_unis = await dao.list_universes()
    assert any(u['id'] == uid for u in all_unis)
    # Update with unique name to avoid duplicate
    import time
    new_name = name + f"_{int(time.time())}"
    await dao.update_universe(uid, new_name, "Updated desc")
    uni2 = await dao.get_universe(uid)
    assert uni2['description'] == "Updated desc"
    assert uni2['name'] == new_name

@pytest.mark.asyncio
async def test_universe_membership_dao_crud():
    env = Environment()
    dao = UniverseMembershipDAO(env)
    universe_id = 1
    symbol = "TESTMEMB"  # Use a unique symbol for the test
    start_at = date(2025, 7, 24)
    # Clean up if exists
    pool = await asyncpg.create_pool(env.get_database_url())
    try:
        async with pool.acquire() as conn:
            await conn.execute(f"DELETE FROM {dao.table_name} WHERE universe_id = $1 AND symbol = $2 AND start_at = $3", universe_id, symbol, start_at)
    finally:
        await pool.close()
    # Add membership
    await dao.add_membership(universe_id, symbol, start_at)
    # Get by universe
    memberships = await dao.get_memberships_by_universe(universe_id)
    assert any(m['symbol'] == symbol and m['start_at'] == start_at for m in memberships)
    # Get active memberships
    active = await dao.get_active_memberships(universe_id, start_at)
    assert any(m['symbol'] == symbol for m in active)
    # Remove: update end_at to simulate removal
    await dao.update_membership_end(universe_id, symbol, start_at)
    updated = await dao.get_memberships_by_universe(universe_id)
    assert any(m['symbol'] == symbol and m['end_at'] == start_at for m in updated)

@pytest.mark.asyncio
async def test_daily_prices_polygon_dao_crud():
    env = Environment()
    dao = DailyPricesPolygonDAO(env)
    test_date = date(2022, 3, 3)
    symbol = "TESTPOLY"
    # Insert
    await dao.insert_price(test_date, symbol, 1, 2, 0, 1.5, 1000, 99999)
    # Get
    row = await dao.get_price(test_date, symbol)
    assert row is not None
    assert row['symbol'] == symbol
    # List
    rows = await dao.list_prices(symbol)
    assert any(r['date'] == test_date for r in rows)

@pytest.mark.asyncio
async def test_daily_prices_tiingo_dao_crud():
    env = Environment()
    dao = DailyPricesTiingoDAO(env)
    test_date = date(2022, 4, 4)
    symbol = "TESTTIINGO"
    # Insert
    await dao.insert_price(test_date, symbol, 1, 2, 0, 1.5, 1.6, 1000, None)
    # Get
    row = await dao.get_price(test_date, symbol)
    assert row is not None
    assert row['symbol'] == symbol
    # List
    rows = await dao.list_prices(symbol)
    assert any(r['date'] == test_date for r in rows)

@pytest.mark.asyncio
async def test_db_version_dao_crud():
    env = Environment()
    dao = DBVersionDAO(env)
    version = 9999
    desc = "Test migration"
    mig = "test_mig.sql"
    # Insert
    await dao.insert_version(version, desc, mig)
    # Get
    rows = await dao.get_version()
    assert any(r['version'] == version for r in rows)
