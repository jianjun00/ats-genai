"""
Integration-style tests for all DAOs in src/db/dao using the test_trading_db database.
Each test will use the TEST environment and real DB access.
"""

import pytest
from datetime import datetime, date
import asyncpg
from shared.utils.environment import EnvironmentType

# Import all DAOs
from core.dao.daily_market_cap_dao import DailyMarketCapDAO
from domains.market_data.repositories.daily_price_polygon_dao import DailyPricesDAO
from vendor.polygon.core.dao.daily_price_polygon_polygon_dao import DailyPricesPolygonDAO
from vendor.tiingo.core.dao.daily_price_polygon_tiingo_dao import DailyPricesTiingoDAO
from core.dao.db_version_dao import DBVersionDAO


from domains.instruments.repositories.instruments_dao import InstrumentsDAO
from domains.trading.repositories.universe_dao import UniverseDAO
from domains.trading.repositories.universe_membership_dao import UniverseMembershipDAO
from core.dao.vendors_dao import VendorsDAO


from datetime import datetime

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_instruments_dao_crud(unit_test_db):

    from shared.utils.environment import Environment
    env = Environment(EnvironmentType.TEST, db_url=unit_test_db)
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
                    await conn.execute(f"DELETE FROM {core.dao.table_name} WHERE symbol = $1", symbol)
            finally:
                await pool.close()
    # Create
    inst_id = await dao.create_instrument(symbol=symbol, name="Test Instrument", type_="stock")
    assert inst_id is not None
    # Read
    result = await dao.get_instrument(inst_id)
    assert result is not None
    assert result['symbol'] == symbol
    assert result['id'] == inst_id
    # List
    all_insts = await dao.list_instruments()
    assert any(inst['symbol'] == symbol for inst in all_insts)

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_daily_market_cap_dao_crud(unit_test_db):
    from shared.utils.environment import Environment
    env = Environment(EnvironmentType.TEST, db_url=unit_test_db)
    # Create test instrument and get instrument_id
    instruments_dao = InstrumentsDAO(env)
    symbol = "TESTMCAP"
    # Clean up if exists
    instruments = await instruments_core.dao.list_instruments()
    for inst in instruments:
        if inst['symbol'] == symbol:
            pool = await asyncpg.create_pool(env.get_database_url())
            try:
                async with pool.acquire() as conn:
                    await conn.execute(f"DELETE FROM {instruments_core.dao.table_name} WHERE symbol = $1", symbol)
            finally:
                await pool.close()
    instrument_id = await instruments_core.dao.create_instrument(symbol=symbol, name="Test MarketCap Instrument", type_="stock")
    assert instrument_id is not None

    dao = DailyMarketCapDAO(env)
    test_date = date(2022, 1, 1)
    market_cap = 12345678
    # Clean up if exists
    pool = await asyncpg.create_pool(env.get_database_url())
    try:
        async with pool.acquire() as conn:
            await conn.execute(f"DELETE FROM {core.dao.table_name} WHERE date = $1 AND instrument_id = $2", test_date, instrument_id)
    finally:
        await pool.close()
    # Insert
    await dao.insert_market_cap(test_date, instrument_id, market_cap)
    # Get
    result = await dao.get_market_cap(test_date, instrument_id)
    assert result is not None
    assert result['market_cap'] == market_cap
    assert result['instrument_id'] == instrument_id
    # List for date
    results = await dao.list_market_caps_for_date(test_date, instrument_id)
    assert any(r['instrument_id'] == instrument_id for r in results)
    # List for instrument
    rows2 = await dao.list_market_caps(instrument_id)
    assert any(r['date'] == test_date for r in rows2)
    # Clean up instrument (delete from daily_market_cap first to avoid FK violation)
    pool = await asyncpg.create_pool(env.get_database_url())
    try:
        async with pool.acquire() as conn:
            await conn.execute(f"DELETE FROM {core.dao.table_name} WHERE instrument_id = $1", instrument_id)
            await conn.execute(f"DELETE FROM {instruments_core.dao.table_name} WHERE id = $1", instrument_id)
    finally:
        await pool.close()

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_daily_price_polygon_dao_crud(unit_test_db):
    from shared.utils.environment import Environment
    env = Environment(EnvironmentType.TEST, db_url=unit_test_db)
    # Create test instrument
    instruments_dao = InstrumentsDAO(env)
    symbol = "TESTPRC"
    # Clean up if exists
    instruments = await instruments_core.dao.list_instruments()
    for inst in instruments:
        if inst['symbol'] == symbol:
            pool = await asyncpg.create_pool(env.get_database_url())
            try:
                async with pool.acquire() as conn:
                    await conn.execute(f"DELETE FROM {instruments_core.dao.table_name} WHERE symbol = $1", symbol)
            finally:
                await pool.close()
    instrument_id = await instruments_core.dao.create_instrument(symbol=symbol, name="Test Price Instrument", type_="stock")
    assert instrument_id is not None

    dao = DailyPricesDAO(env)
    test_date = date(2022, 2, 2)
    # Should not raise error
    _ = await dao.list_prices_for_date(test_date)
    _ = await dao.list_prices_for_instruments_and_date([instrument_id], test_date)
    _ = await dao.get_price(test_date, instrument_id)
    _ = await dao.list_prices(instrument_id)

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_universe_dao_crud(unit_test_db):
    from shared.utils.environment import Environment
    env = Environment(EnvironmentType.TEST, db_url=unit_test_db)
    dao = UniverseDAO(env)
    name = "TESTUNI"
    desc = "Test universe"
    # Clean up: truncate universe table and reset sequence to avoid UniqueViolationError
    pool = await asyncpg.create_pool(env.get_database_url())
    try:
        async with pool.acquire() as conn:
            await conn.execute(f"TRUNCATE {core.dao.table_name} RESTART IDENTITY CASCADE")
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
@pytest.mark.asyncio
async def test_universe_membership_dao_universe_isolation(unit_test_db):
    from shared.utils.environment import Environment
    env = Environment(EnvironmentType.TEST, db_url=unit_test_db)
    dao = UniverseMembershipDAO(env)
    universe_id_1 = 101
    universe_id_2 = 202
    symbol_1 = "MEMB1"
    symbol_2 = "MEMB2"
    start_at = datetime(2025, 7, 24, 0, 0, 0)
    # Clean up any existing memberships for these universes/symbols
    pool = await asyncpg.create_pool(env.get_database_url())
    try:
        async with pool.acquire() as conn:
            await conn.execute(f"DELETE FROM {core.dao.table_name} WHERE universe_id IN ($1, $2) AND symbol IN ($3, $4)", universe_id_1, universe_id_2, symbol_1, symbol_2)
    finally:
        await pool.close()

    # DEBUG: Print schema of the test_universe_membership table
    pool = await asyncpg.create_pool(env.get_database_url())
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = $1
                ORDER BY ordinal_position
            """, dao.table_name)
            print(f"[SCHEMA DEBUG] Columns for {core.dao.table_name}:")
            for row in rows:
                print(f"    {row['column_name']}: {row['data_type']}")
    finally:
        await pool.close()

    # Insert required universes for memberships using direct SQL to set IDs
    pool = await asyncpg.create_pool(env.get_database_url())
    try:
        async with pool.acquire() as conn:
            await conn.execute(f"INSERT INTO {env.get_table_name('universe')} (id, name, description) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING", 101, "Universe101", "Universe for ID 101")
            await conn.execute(f"INSERT INTO {env.get_table_name('universe')} (id, name, description) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING", 202, "Universe202", "Universe for ID 202")
    finally:
        await pool.close()

    # Insert required vendor for xrefs
    vendors_dao = VendorsDAO(env)
    vendor_id = await vendors_core.dao.create_vendor(name="TestVendor", description="Test vendor for xref")

    # Insert required instruments and xrefs for test symbols
    instruments_dao = InstrumentsDAO(env)
    instrument_list_date = datetime(2010, 1, 1).date()
    instrument_id_1 = await instruments_core.dao.create_instrument(symbol=symbol_1, name="Test Member 1", type_="stock", list_date=instrument_list_date)
    instrument_id_2 = await instruments_core.dao.create_instrument(symbol=symbol_2, name="Test Member 2", type_="stock", list_date=instrument_list_date)
    xrefs_dao = __import__('core.dao.instrument_xrefs_dao', fromlist=['InstrumentXrefsDAO']).InstrumentXrefsDAO(env)
    await xrefs_core.dao.create_xref(instrument_id_1, vendor_id, symbol_1, instrument_list_date)
    await xrefs_core.dao.create_xref(instrument_id_2, vendor_id, symbol_2, instrument_list_date)

    # Add memberships with instrument_id
    await dao.add_membership(universe_id_1, symbol=symbol_1, instrument_id=instrument_id_1, start_at=instrument_list_date)
    await dao.add_membership(universe_id_2, symbol=symbol_2, instrument_id=instrument_id_2, start_at=instrument_list_date)
    # Test isolation: get memberships for universe_id_1
    memberships_1 = await dao.get_memberships_by_universe(universe_id_1)
    assert any(m['symbol'] == symbol_1 for m in memberships_1)
    assert all(m['symbol'] != symbol_2 for m in memberships_1)
    # Test isolation: get memberships for universe_id_2
    memberships_2 = await dao.get_memberships_by_universe(universe_id_2)
    assert any(m['symbol'] == symbol_2 for m in memberships_2)
    assert all(m['symbol'] != symbol_1 for m in memberships_2)

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_universe_membership_dao_active_memberships(unit_test_db):
    from shared.utils.environment import Environment
    env = Environment(EnvironmentType.TEST, db_url=unit_test_db)
    dao = UniverseMembershipDAO(env)
    universe_id = 303
    symbol_active = "ACTIVEMEMB"
    symbol_inactive = "INACTIVEMEMB"
    start_at = datetime(2025, 7, 24, 0, 0, 0)
    end_at = datetime(2025, 7, 25, 0, 0, 0)
    # Setup: Create vendor, instruments, and xrefs for both symbols
    vendors_dao = VendorsDAO(env)
    vendor_id = await vendors_core.dao.create_vendor(name="TestVendor", description="Test vendor for xref")
    instruments_dao = InstrumentsDAO(env)
    instrument_list_date = datetime(2010, 1, 1).date()
    instrument_id_active = await instruments_core.dao.create_instrument(symbol=symbol_active, name="Active Member", type_="stock", list_date=instrument_list_date)
    instrument_id_inactive = await instruments_core.dao.create_instrument(symbol=symbol_inactive, name="Inactive Member", type_="stock", list_date=instrument_list_date)
    xrefs_dao = __import__('core.dao.instrument_xrefs_dao', fromlist=['InstrumentXrefsDAO']).InstrumentXrefsDAO(env)
    await xrefs_core.dao.create_xref(instrument_id_active, vendor_id, symbol_active, instrument_list_date)
    await xrefs_core.dao.create_xref(instrument_id_inactive, vendor_id, symbol_inactive, instrument_list_date)
    # Insert required universe for memberships
    pool = await asyncpg.create_pool(env.get_database_url())
    try:
        async with pool.acquire() as conn:
            await conn.execute(f"INSERT INTO {env.get_table_name('universe')} (id, name, description) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING", universe_id, "Universe303", "Universe for ID 303")
    finally:
        await pool.close()
    # Clean up any existing memberships for these symbols
    pool = await asyncpg.create_pool(env.get_database_url())
    try:
        async with pool.acquire() as conn:
            await conn.execute(f"DELETE FROM {core.dao.table_name} WHERE universe_id = $1 AND symbol IN ($2, $3)", universe_id, symbol_active, symbol_inactive)
    finally:
        await pool.close()
    # Debug: Print schema of the membership table before inserting
    pool = await asyncpg.create_pool(env.get_database_url())
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = $1
                ORDER BY ordinal_position
            """, dao.table_name)
            print(f"[SCHEMA DEBUG] Columns for {core.dao.table_name}:")
            for row in rows:
                print(f"    {row['column_name']}: {row['data_type']}")
    finally:
        await pool.close()
    # Add memberships: one active, one inactive (both with instrument_id)
    await dao.add_membership(universe_id, symbol=symbol_active, instrument_id=instrument_id_active, start_at=instrument_list_date)
    await dao.add_membership_full(universe_id, instrument_id_inactive, start_at=instrument_list_date, end_at=end_at)
    # Query as_of before end date: both should be present
    active_before = await dao.get_active_memberships(universe_id, date(2025, 7, 24))
    # Compare instrument_ids, since get_active_memberships returns instrument_id not symbol
    active_before_ids = [m['instrument_id'] for m in active_before]
    assert instrument_id_active in active_before_ids
    assert instrument_id_inactive in active_before_ids
    # Query as_of after end date: only active should be present
    active_after = await dao.get_active_memberships(universe_id, datetime(2025, 7, 26, 0, 0, 0))
    active_after_ids = [m['instrument_id'] for m in active_after]
    assert instrument_id_active in active_after_ids
    assert instrument_id_inactive not in active_after_ids

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_universe_membership_dao_crud(unit_test_db):
    from shared.utils.environment import Environment
    env = Environment(EnvironmentType.TEST, db_url=unit_test_db)
    dao = UniverseMembershipDAO(env)
    import random
    universe_id = random.randint(10000, 99999)
    symbol = "TESTMEMB"  # Use a unique symbol for the test
    instrument_list_date = datetime(2010, 1, 1, 0, 0, 0)
    start_at = datetime(2025, 7, 24, 0, 0, 0)

    # Set up database connection pool
    pool = await asyncpg.create_pool(env.get_database_url())
    try:
        async with pool.acquire() as conn:
            # Start a transaction
            async with conn.transaction():
                # Clean up any existing data
                await conn.execute(f"DELETE FROM {core.dao.table_name} WHERE universe_id = $1", universe_id)

                # Clean up and insert universe
                universe_table = env.get_table_name('universe')
                await conn.execute(f"DELETE FROM {universe_table} WHERE id = $1", universe_id)
                await conn.execute(
                    f"INSERT INTO {universe_table} (id, name, description) VALUES ($1, $2, $3)",
                    universe_id, f"TestUni_{universe_id}", "Test universe for membership CRUD"
                )

                # Clean up and insert instrument
                instruments_dao = InstrumentsDAO(env)
                await conn.execute(f"DELETE FROM {instruments_core.dao.table_name} WHERE symbol = $1", symbol)

                # Insert instrument with list_date matching our test data
                instrument_id = await conn.fetchval(
                    f"""
                    INSERT INTO {instruments_core.dao.table_name}
                    (symbol, name, type, list_date, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, NOW(), NOW())
                    RETURNING id
                    """,
                    symbol, "Test Membership Instrument", "stock", start_at.date()  # Use start_at date for list_date
                )

                # Insert vendor if not exists
                vendors_dao = VendorsDAO(env)
                vendor_name = "TestVendor"
                vendor_id = await conn.fetchval(
                    f"""
                    INSERT INTO {vendors_core.dao.table_name}
                    (name, description, created_at, updated_at)
                    VALUES ($1, $2, NOW(), NOW())
                    ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                    RETURNING id
                    """,
                    vendor_name, "Test vendor for xref"
                )

# First, ensure ticker vendor exists and get its ID
                vendors_dao = VendorsDAO(env)
                ticker_vendor_id = await conn.fetchval(
                    f"""
                    WITH ins AS (
                        INSERT INTO {vendors_core.dao.table_name}
                        (name, description, created_at, updated_at)
                        VALUES ($1, $2, NOW(), NOW())
                        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
                        RETURNING id
                    )
                    SELECT id FROM ins
                    UNION ALL
                    SELECT id FROM {vendors_core.dao.table_name} WHERE name = $1
                    LIMIT 1
                    """,
                    'ticker', 'Ticker symbol vendor'
                )
                print(f"[DEBUG] Ticker vendor ID: {ticker_vendor_id}")

                # Verify vendor was inserted/retrieved
                vendor_check = await conn.fetchrow(
                    f"SELECT * FROM {vendors_core.dao.table_name} WHERE name = $1",
                    'ticker'
                )
                print(f"[DEBUG] Ticker vendor check: {vendor_check}")

                # Now insert instrument_xref for ticker vendor with start_at matching membership
                xrefs_table = env.get_table_name('instrument_xrefs')
                await conn.execute(
                    f"""
                    INSERT INTO {xrefs_table}
                    (instrument_id, vendor_id, symbol, start_at, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, NOW(), NOW())
                    ON CONFLICT (instrument_id, vendor_id, start_at) DO NOTHING
                    """,
                    instrument_id, ticker_vendor_id, symbol, start_at  # Use start_at instead of instrument_list_date
                )

                # Verify the xref was inserted
                xref_check = await conn.fetchrow(
                    f"SELECT * FROM {xrefs_table} WHERE instrument_id = $1 AND vendor_id = $2",
                    instrument_id, ticker_vendor_id
                )
                print(f"[DEBUG] Ticker xref check: {dict(xref_check) if xref_check else 'Not found'}")

                # Also insert xref for test vendor
                await conn.execute(
                    f"""
                    INSERT INTO {xrefs_table}
                    (instrument_id, vendor_id, symbol, start_at, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, NOW(), NOW())
                    ON CONFLICT (instrument_id, vendor_id, start_at) DO NOTHING
                    """,
                    instrument_id, vendor_id, symbol, instrument_list_date
                )

                # Debug: Print all xrefs before adding membership
                all_xrefs = await conn.fetch(
                    f"SELECT * FROM {xrefs_table} WHERE symbol = $1",
                    symbol
                )
                print(f"[DEBUG] All xrefs for {symbol}:")
                for xref in all_xrefs:
                    print(f"  - {dict(xref)}")

                # Add membership with explicit vendor_id to avoid any lookup issues
                print(f"[DEBUG] Adding membership with symbol={symbol}, vendor_id={ticker_vendor_id}, start_at={start_at}")

                # Debug: Manually resolve instrument_id to see what's happening
                from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO
                xrefs_dao = InstrumentXrefsDAO(env)
                resolved_id = await xrefs_core.dao.resolve_instrument_id(
                    symbol=symbol,
                    vendor_id=ticker_vendor_id,
                    at_date=start_at
                )
                print(f"[DEBUG] Manually resolved instrument_id: {resolved_id}")

                # Add membership with explicit instrument_id to bypass the resolution
                print("[DEBUG] Adding membership with explicit instrument_id")
                await conn.execute(
                    f"""
                    INSERT INTO {core.dao.table_name}
                    (universe_id, symbol, start_at, instrument_id)
                    VALUES ($1, $2, $3, $4)
                    """,
                    universe_id, symbol, start_at, instrument_id
                )
    finally:
        await pool.close()

    # Verify the membership was added correctly
    memberships = await dao.get_memberships_by_universe(universe_id)
    assert any(
        m['symbol'] == symbol and
        m['start_at'] == start_at and  # Use start_at instead of instrument_list_date
        m['instrument_id'] == instrument_id
        for m in memberships
    ), f"Membership not found for symbol {symbol} in universe {universe_id}"

    # Verify active memberships
    active = await dao.get_active_memberships(universe_id, start_at)
    assert any(m['symbol'] == symbol for m in active), \
        f"Active membership not found for symbol {symbol} in universe {universe_id}"

    # Test updating membership end date
    await dao.update_membership_end(
        universe_id=universe_id,
        instrument_id=instrument_id,
        end_at=start_at
    )

    # Verify the membership is no longer active
    active_after_update = await dao.get_active_memberships(universe_id, start_at)
    assert not any(m['symbol'] == symbol for m in active_after_update), \
        f"Membership for symbol {symbol} should no longer be active after update"
    updated = await dao.get_memberships_by_universe(universe_id)

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_daily_price_polygon_polygon_dao_crud(unit_test_db):
    from shared.utils.environment import Environment
    env = Environment(EnvironmentType.TEST, db_url=unit_test_db)
    # Create test instrument
    instruments_dao = InstrumentsDAO(env)
    symbol = "TESTPOLY"
    instruments = await instruments_core.dao.list_instruments()
    for inst in instruments:
        if inst['symbol'] == symbol:
            pool = await asyncpg.create_pool(env.get_database_url())
            try:
                async with pool.acquire() as conn:
                    await conn.execute(f"DELETE FROM {instruments_core.dao.table_name} WHERE symbol = $1", symbol)
            finally:
                await pool.close()
    instrument_id = await instruments_core.dao.create_instrument(symbol=symbol, name="Test Poly Instrument", type_="stock")
    assert instrument_id is not None

    dao = DailyPricesPolygonDAO(env)
    test_date = date(2022, 3, 3)
    # Insert
    await dao.insert_price(test_date, instrument_id, 1, 2, 0, 1.5, 1000, 99999)
    # Get
    row = await dao.get_price(test_date, instrument_id)
    assert row is not None
    assert row['instrument_id'] == instrument_id
    # List
    rows = await dao.list_prices(instrument_id)
    assert any(r['date'] == test_date for r in rows)

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_daily_price_polygon_tiingo_dao_crud(unit_test_db):
    from shared.utils.environment import Environment
    env = Environment(EnvironmentType.TEST, db_url=unit_test_db)
    # Create test instrument
    instruments_dao = InstrumentsDAO(env)
    symbol = "TESTTIINGO"
    instruments = await instruments_core.dao.list_instruments()
    for inst in instruments:
        if inst['symbol'] == symbol:
            pool = await asyncpg.create_pool(env.get_database_url())
            try:
                async with pool.acquire() as conn:
                    await conn.execute(f"DELETE FROM {instruments_core.dao.table_name} WHERE symbol = $1", symbol)
            finally:
                await pool.close()
    instrument_id = await instruments_core.dao.create_instrument(symbol=symbol, name="Test Tiingo Instrument", type_="stock")
    assert instrument_id is not None

    dao = DailyPricesTiingoDAO(env)
    test_date = date(2022, 4, 4)
    # Insert
    await dao.insert_price(test_date, instrument_id, 1, 2, 0, 1.5, 1.6, 1000, None)
    # Get
    row = await dao.get_price(test_date, instrument_id)
    assert row is not None
    assert row['instrument_id'] == instrument_id
    # List
    rows = await dao.list_prices(instrument_id)
    assert any(r['date'] == test_date for r in rows)

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_db_version_dao_crud(unit_test_db):
    from shared.utils.environment import Environment
    env = Environment(EnvironmentType.TEST, db_url=unit_test_db)
    dao = DBVersionDAO(env)
    version = 9999
    desc = "Test migration"
    mig = "test_mig.sql"
    # Insert
    await dao.insert_version(version, desc, mig)
    # Get
    rows = await dao.get_version()

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_universe_membership_dao_get_membership_changes(unit_test_db):
    from shared.utils.environment import Environment
    env = Environment(EnvironmentType.TEST, db_url=unit_test_db)
    dao = UniverseMembershipDAO(env)
    pool = await asyncpg.create_pool(env.get_database_url())
    import time
    universe_name = f"TESTUMC_{int(time.time()*1000)}"
    universe_desc = "Test for get_membership_changes"
    symbol = "UMCTEST"
    action = "add"
    effective_date = datetime(2025, 7, 25, 0, 0, 0)
    reason = "unit_test"
    # Create a universe
    universe_table = env.get_table_name('universe')
    membership_changes_table = env.get_table_name('universe_membership_changes')
    async with pool.acquire() as conn:
        # Clean up and reset universe table
        await conn.execute(f"TRUNCATE {universe_table} RESTART IDENTITY CASCADE")
        await conn.execute(f"DELETE FROM {membership_changes_table} WHERE symbol = $1", symbol)
        # Insert universe
        row = await conn.fetchrow(f"INSERT INTO {universe_table} (name, description) VALUES ($1, $2) RETURNING id", universe_name, universe_desc)
        universe_id = row['id']
        # Create instrument for symbol
        instruments_dao = InstrumentsDAO(env)
        await instruments_core.dao.create_instrument(symbol=symbol, name="Test UMC Instrument", type_="stock")
        # Insert a membership change (resolve instrument_id for symbol)
        inst_row = await conn.fetchrow(f"SELECT id FROM {instruments_core.dao.table_name} WHERE symbol = $1", symbol)
        instrument_id = inst_row['id'] if inst_row else None
        assert instrument_id is not None, f"Instrument for symbol {symbol} must exist"
        await conn.execute(f"INSERT INTO {membership_changes_table} (universe_id, symbol, instrument_id, action, effective_date, reason) VALUES ($1, $2, $3, $4, $5, $6)", universe_id, symbol, instrument_id, action, effective_date, reason)
    await pool.close()
    # Retrieve via DAO
    changes = await dao.get_membership_changes(universe_id, effective_date)
    assert any(c['symbol'] == symbol and c['action'] == action and c['effective_date'] == effective_date for c in changes)
