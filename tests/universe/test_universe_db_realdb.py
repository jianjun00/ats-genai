import pytest
import asyncpg
from datetime import date
from src.universe.universe_db import UniverseDB
from config.environment import Environment, EnvironmentType
from db.test_db_manager import unit_test_db
from src.universe.universe_manager import UniverseManager

@pytest.mark.asyncio
async def test_add_and_get_universe_members_real_db(unit_test_db):
    """
    Real DB integration: create a universe, add membership, and verify retrieval using isolated test DB.
    """
    # Patch env to use the test DB URL
    env = Environment(EnvironmentType.TEST)
    env.get_database_url = lambda: unit_test_db
    db = UniverseDB(env=env)

    # Connect and setup tables if not present (should be handled by migrations)
    pool = await asyncpg.create_pool(unit_test_db)
    async with pool.acquire() as conn:
        # Clean up
        await conn.execute(f"DELETE FROM {env.get_table_name('universe_membership')}")
        await conn.execute(f"DELETE FROM {env.get_table_name('universe')}")

    # --- Setup vendor, instrument, and instrument_xref for AAPL ---
    from src.dao.vendors_dao import VendorsDAO
    from src.dao.instruments_dao import InstrumentsDAO
    from src.dao.instrument_xrefs_dao import InstrumentXrefsDAO
    vendor_name = "TEST_VENDOR"
    vendors_dao = VendorsDAO(env)
    instruments_dao = InstrumentsDAO(env)
    xrefs_dao = InstrumentXrefsDAO(env)
    vendor_id = None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(f"SELECT vendor_id FROM {env.get_table_name('vendors')} WHERE name=$1", vendor_name)
        if row:
            vendor_id = row['vendor_id']
        else:
            vendor_id = await vendors_dao.create_vendor(vendor_name, description="Test Vendor")
    # Insert or get instrument
    symbol = "AAPL"
    instrument_id = None
    async with pool.acquire() as conn:
        row = await conn.fetchrow(f"SELECT id FROM {env.get_table_name('instruments')} WHERE symbol=$1", symbol)
        if row:
            instrument_id = row['id']
        else:
            instrument_id = await instruments_dao.create_instrument(symbol, name=f"{symbol} Inc.", exchange="NASDAQ", type_="EQUITY", currency="USD")
    # Insert xref
    from datetime import date
    xref_start = date(2025, 7, 1)
    existing = await xrefs_dao.find_xref(vendor_id, symbol)
    if not existing:
        await xrefs_dao.create_xref(instrument_id, vendor_id, symbol, type="EQUITY", start_at=xref_start)

    # Add universe
    universe_name = "TEST_REALDB"
    universe_id = await db.add_universe(universe_name, "desc")
    assert isinstance(universe_id, int)

    # Add membership (now vendor/instrument/xref exist)
    # Add membership (pass vendor_id to ensure match)
    await db.universe_membership_dao.add_membership_full(universe_id=universe_id, symbol="AAPL", start_at=date(2025, 7, 25), end_at=None, vendor_id=vendor_id)

    # Retrieve members (UniverseDB.get_universe_members does not take vendor_id, but membership resolution will now work)
    members = await db.get_universe_members(universe_id, date(2025, 7, 25))
    assert "AAPL" in members

    # Clean up
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {env.get_table_name('universe_membership')}")
        await conn.execute(f"DELETE FROM {env.get_table_name('universe')}")
    await pool.close()


@pytest.mark.asyncio
async def test_universe_manager_multiday_multiinstrument_real_db(unit_test_db):
    """
    Integration test: UniverseManager with multi-day, multi-instrument membership changes.
    - Day 1: Add AAPL and TSLA
    - Day 2: Remove AAPL, keep TSLA
    - Day 3: Add AAPL back, remove TSLA
    Verifies correct membership for each day.
    """
    env = Environment(EnvironmentType.TEST)
    env.get_database_url = lambda: unit_test_db
    db = UniverseDB(env=env)
    manager = UniverseManager(env=env)
    pool = await asyncpg.create_pool(unit_test_db)
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {env.get_table_name('universe_membership')}")
        await conn.execute(f"DELETE FROM {env.get_table_name('universe')}")
    # --- Setup vendor, instruments, and instrument_xrefs for test isolation ---
    from src.dao.vendors_dao import VendorsDAO
    from src.dao.instruments_dao import InstrumentsDAO
    from src.dao.instrument_xrefs_dao import InstrumentXrefsDAO

    vendor_name = "TEST_VENDOR"
    vendors_dao = VendorsDAO(env)
    instruments_dao = InstrumentsDAO(env)
    xrefs_dao = InstrumentXrefsDAO(env)

    # Insert or get vendor
    vendor_id = None
    pool = await asyncpg.create_pool(unit_test_db)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(f"SELECT vendor_id FROM {env.get_table_name('vendors')} WHERE name=$1", vendor_name)
        if row:
            vendor_id = row['vendor_id']
        else:
            vendor_id = await vendors_dao.create_vendor(vendor_name, description="Test Vendor")
    # Insert or get instruments
    symbols = ["AAPL", "TSLA"]
    instrument_ids = {}
    for symbol in symbols:
        row = None
        async with pool.acquire() as conn:
            row = await conn.fetchrow(f"SELECT id FROM {env.get_table_name('instruments')} WHERE symbol=$1", symbol)
        if row:
            instrument_ids[symbol] = row['id']
        else:
            instrument_ids[symbol] = await instruments_dao.create_instrument(symbol, name=f"{symbol} Inc.", exchange="NASDAQ", type_="EQUITY", currency="USD")
    # Insert xrefs for each symbol
    from datetime import date
    xref_start = date(2025, 7, 1)
    for symbol in symbols:
        # Check if xref already exists
        existing = await xrefs_dao.find_xref(vendor_id, symbol)
        if not existing:
            await xrefs_dao.create_xref(instrument_ids[symbol], vendor_id, symbol, type="EQUITY", start_at=xref_start)
    # Debug: print all xrefs for vendor_id and symbols
    for symbol in symbols:
        xref = await xrefs_dao.find_xref(vendor_id, symbol)
        print(f"[DEBUG] xref for vendor_id={vendor_id}, symbol={symbol}: {xref}")
    import asyncio
    await asyncio.sleep(0)  # Yield control to ensure all inserts are committed

    universe_name = "TEST_MULTI_DAY"
    universe_id = await db.add_universe(universe_name, "desc")
    # Day 1: Add AAPL & TSLA
    await db.universe_membership_dao.add_membership_full(universe_id=universe_id, symbol="AAPL", start_at=date(2025, 7, 1), end_at=None, vendor_id=vendor_id)
    await db.universe_membership_dao.add_membership_full(universe_id=universe_id, symbol="TSLA", start_at=date(2025, 7, 1), end_at=None, vendor_id=vendor_id)
    # Day 2: Remove AAPL (set end_at)
    async with pool.acquire() as conn:
        await conn.execute(f"UPDATE {env.get_table_name('universe_membership')} SET end_at=$1 WHERE universe_id=$2 AND symbol='AAPL'", date(2025, 7, 2), universe_id)
    # Day 3: Remove TSLA, add AAPL back
    await db.universe_membership_dao.add_membership_full(universe_id=universe_id, symbol="AAPL", start_at=date(2025, 7, 3), end_at=None, vendor_id=vendor_id)
    async with pool.acquire() as conn:
        await conn.execute(f"UPDATE {env.get_table_name('universe_membership')} SET end_at=$1 WHERE universe_id=$2 AND symbol='TSLA' AND end_at IS NULL", date(2025, 7, 3), universe_id)
    # Verify membership for each day
    members_day1 = await db.get_universe_members(universe_id, date(2025, 7, 1))
    members_day2 = await db.get_universe_members(universe_id, date(2025, 7, 2))
    members_day3 = await db.get_universe_members(universe_id, date(2025, 7, 3))
    assert set(members_day1) == {"AAPL", "TSLA"}
    assert set(members_day2) == {"TSLA"}
    assert set(members_day3) == {"AAPL"}
    # Clean up
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {env.get_table_name('universe_membership')}")
        await conn.execute(f"DELETE FROM {env.get_table_name('universe')}")
    await pool.close()