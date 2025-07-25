import pytest
import asyncio
import asyncpg
from config.environment import Environment, EnvironmentType
from dao.instrument_xrefs_dao import InstrumentXrefsDAO
from db.test_db_manager import unit_test_db

@pytest.mark.asyncio
async def test_instrument_xrefs_dao_crud(unit_test_db):
    env = Environment(EnvironmentType.TEST)
    env.get_database_url = lambda: unit_test_db
    dao = InstrumentXrefsDAO(env)

    # Insert a dummy instrument and vendor to satisfy FKs
    pool = await asyncpg.create_pool(unit_test_db)
    async with pool.acquire() as conn:
        instrument_id = (await conn.fetchrow(f"INSERT INTO {env.get_table_name('instruments')} (symbol) VALUES ('TESTSYM') RETURNING id")).get('id')
        vendor_id = (await conn.fetchrow(f"INSERT INTO {env.get_table_name('vendors')} (name) VALUES ('TestVendor') RETURNING vendor_id")).get('vendor_id')
    await pool.close()

    # Create xref
    from datetime import date
    today = date.today()
    xref_id = await dao.create_xref(instrument_id, vendor_id, 'TICK_XREF', 'primary', start_at=today, end_at=None)
    assert xref_id is not None

    # Get xref
    xref = await dao.get_xref(xref_id)
    assert xref is not None
    assert xref['instrument_id'] == instrument_id
    assert xref['vendor_id'] == vendor_id
    assert xref['symbol'] == 'TICK_XREF'
    assert xref['type'] == 'primary'
    assert xref['start_at'] == today
    assert xref['end_at'] is None

    # List xrefs for instrument
    xrefs_inst = await dao.list_xrefs_for_instrument(instrument_id)
    assert any(x['instrument_id'] == instrument_id and x['vendor_id'] == vendor_id and x['symbol'] == 'TICK_XREF' and x['start_at'] == today for x in xrefs_inst)

    # List xrefs for vendor
    xrefs_vendor = await dao.list_xrefs_for_vendor(vendor_id)
    assert any(x['instrument_id'] == instrument_id and x['vendor_id'] == vendor_id and x['symbol'] == 'TICK_XREF' and x['start_at'] == today for x in xrefs_vendor)

    # Find xref
    found = await dao.find_xref(vendor_id, 'TICK_XREF')
    assert found is not None
    assert found['instrument_id'] == instrument_id
    assert found['vendor_id'] == vendor_id
    assert found['symbol'] == 'TICK_XREF'
    assert found['type'] == 'primary'
    assert found['start_at'] == today
    assert found['end_at'] is None
