import os
import pytest
import asyncpg
from dotenv import load_dotenv

load_dotenv()

TEST_DB_URL = os.environ.get("TSDB_URL")

@pytest.mark.asyncio
async def test_instruments_table_schema():
    pool = await asyncpg.create_pool(TEST_DB_URL)
    async with pool.acquire() as conn:
        # Check columns
        cols = await conn.fetch("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'instruments'
        """)
        col_names = {row['column_name'] for row in cols}
        assert 'instrument_id' in col_names
        assert 'symbol' in col_names
        assert 'exchange' in col_names
        assert 'figi' in col_names
        assert 'active' in col_names
        assert 'created_at' in col_names
        assert 'updated_at' in col_names
        # Check unique constraint
        res = await conn.fetch("""
            SELECT COUNT(*) FROM information_schema.table_constraints
            WHERE table_name = 'instruments' AND constraint_type = 'UNIQUE'
        """)
        assert res[0]['count'] >= 4  # symbol, figi, isin, cusip
    await pool.close()

@pytest.mark.asyncio
async def test_insert_and_query_instrument():
    pool = await asyncpg.create_pool(TEST_DB_URL)
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM instruments WHERE symbol = 'TESTXYZ'")
        await conn.execute("""
            INSERT INTO instruments (symbol, exchange, name, type, currency, figi, isin, cusip, active)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """, 'TESTXYZ', 'NASDAQ', 'Test Instrument', 'Equity', 'USD', 'BBG000TEST', 'US000000TEST', '000000TEST', True)
        row = await conn.fetchrow("SELECT * FROM instruments WHERE symbol = 'TESTXYZ'")
        assert row['symbol'] == 'TESTXYZ'
        assert row['exchange'] == 'NASDAQ'
        assert row['active'] is True
        # Clean up
        await conn.execute("DELETE FROM instruments WHERE symbol = 'TESTXYZ'")
    await pool.close()

@pytest.mark.asyncio
async def test_instrument_aliases_and_metadata():
    pool = await asyncpg.create_pool(TEST_DB_URL)
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM instruments WHERE symbol = 'TESTALIAS'")
        await conn.execute("""
            INSERT INTO instruments (symbol, exchange, name, type, currency, figi, isin, cusip, active)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """, 'TESTALIAS', 'NYSE', 'Alias Instrument', 'Equity', 'USD', 'BBG000ALIAS', 'US000000ALIAS', '000000ALIAS', True)
        inst = await conn.fetchrow("SELECT instrument_id FROM instruments WHERE symbol = 'TESTALIAS'")
        inst_id = inst['instrument_id']
        # Insert alias
        await conn.execute("INSERT INTO instrument_aliases (instrument_id, alias, source) VALUES ($1, $2, $3)", inst_id, 'TESTAL', 'manual')
        alias = await conn.fetchrow("SELECT * FROM instrument_aliases WHERE instrument_id = $1", inst_id)
        assert alias['alias'] == 'TESTAL'
        # Insert metadata
        await conn.execute("INSERT INTO instrument_metadata (instrument_id, key, value) VALUES ($1, $2, $3)", inst_id, 'sector', 'Technology')
        meta = await conn.fetchrow("SELECT * FROM instrument_metadata WHERE instrument_id = $1", inst_id)
        assert meta['key'] == 'sector'
        assert meta['value'] == 'Technology'
        # Clean up
        await conn.execute("DELETE FROM instrument_metadata WHERE instrument_id = $1", inst_id)
        await conn.execute("DELETE FROM instrument_aliases WHERE instrument_id = $1", inst_id)
        await conn.execute("DELETE FROM instruments WHERE instrument_id = $1", inst_id)
    await pool.close()
