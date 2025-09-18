import pytest
import asyncpg
from datetime import date
from core.shared.utils.environment import Environment, EnvironmentType
from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_instrument_xrefs_mapping_debug(unit_test_db):
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    dao = InstrumentXrefsDAO(env)
    pool = await asyncpg.create_pool(env.get_database_url())
    async with pool.acquire() as conn:
        # Insert two instruments
        iid1 = (await conn.fetchrow(f"INSERT INTO {env.get_table_name('instruments')} (symbol) VALUES ('AAPL') RETURNING id")).get('id')
        iid2 = (await conn.fetchrow(f"INSERT INTO {env.get_table_name('instruments')} (symbol) VALUES ('TSLA') RETURNING id")).get('id')
        # Insert four vendors, including 'ticker' which is required by resolve_instrument_id_by_symbol
        vid_ticker = (await conn.fetchrow(f"INSERT INTO {env.get_table_name('vendors')} (name) VALUES ('ticker') RETURNING id")).get('id')
        vid1 = (await conn.fetchrow(f"INSERT INTO {env.get_table_name('vendors')} (name) VALUES ('test') RETURNING id")).get('id')
        vid2 = (await conn.fetchrow(f"INSERT INTO {env.get_table_name('vendors')} (name) VALUES ('polygon') RETURNING id")).get('id')
        vid3 = (await conn.fetchrow(f"INSERT INTO {env.get_table_name('vendors')} (name) VALUES ('tiingo') RETURNING id")).get('id')
        # Insert xrefs for all combinations, including 'ticker' vendor
        start_date = date(2000, 1, 1)
        await conn.execute(f"INSERT INTO {env.get_table_name('instrument_xrefs')} (instrument_id, vendor_id, symbol, start_at) VALUES ($1, $2, $3, $4)", iid1, vid_ticker, 'AAPL', start_date)
        await conn.execute(f"INSERT INTO {env.get_table_name('instrument_xrefs')} (instrument_id, vendor_id, symbol, start_at) VALUES ($1, $2, $3, $4)", iid2, vid_ticker, 'TSLA', start_date)
        await conn.execute(f"INSERT INTO {env.get_table_name('instrument_xrefs')} (instrument_id, vendor_id, symbol, start_at) VALUES ($1, $2, $3, $4)", iid1, vid2, 'AAPL', start_date)
        await conn.execute(f"INSERT INTO {env.get_table_name('instrument_xrefs')} (instrument_id, vendor_id, symbol, start_at) VALUES ($1, $2, $3, $4)", iid2, vid2, 'TSLA', start_date)
        await conn.execute(f"INSERT INTO {env.get_table_name('instrument_xrefs')} (instrument_id, vendor_id, symbol, start_at) VALUES ($1, $2, $3, $4)", iid1, vid3, 'AAPL', start_date)
        await conn.execute(f"INSERT INTO {env.get_table_name('instrument_xrefs')} (instrument_id, vendor_id, symbol, start_at) VALUES ($1, $2, $3, $4)", iid2, vid3, 'TSLA', start_date)
        await conn.execute(f"INSERT INTO {env.get_table_name('instrument_xrefs')} (instrument_id, vendor_id, symbol, start_at) VALUES ($1, $2, $3, $4)", iid1, vid1, 'AAPL', start_date)
        await conn.execute(f"INSERT INTO {env.get_table_name('instrument_xrefs')} (instrument_id, vendor_id, symbol, start_at) VALUES ($1, $2, $3, $4)", iid2, vid1, 'TSLA', start_date)
    await pool.close()
    # Debug: print all xrefs for AAPL
    pool = await asyncpg.create_pool(env.get_database_url())
    async with pool.acquire() as conn:
        xrefs = await conn.fetch(f"SELECT * FROM {env.get_table_name('instrument_xrefs')} WHERE symbol = 'AAPL'")
        print(f"[DEBUG][test] instrument_xrefs rows for AAPL: {xrefs}")
    await pool.close()
    # Test mapping logic
    iid = await dao.resolve_instrument_id('AAPL')
    print(f"[DEBUG][test] resolve_instrument_id('AAPL') returned: {iid}")
    assert iid == iid1, "AAPL should map to the first inserted instrument_id by default"
    # Test with vendor_id
    iid_poly = await dao.resolve_instrument_id('AAPL', vendor_id=vid2)
    assert iid_poly == iid1
    iid_tiingo = await dao.resolve_instrument_id('AAPL', vendor_id=vid3)
    assert iid_tiingo == iid1
    # Test with missing vendor
    iid_none = await dao.resolve_instrument_id('AAPL', vendor_id=9999)
    assert iid_none is None
