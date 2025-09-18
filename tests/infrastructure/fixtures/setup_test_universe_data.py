import pytest_asyncio
import asyncpg
from datetime import date, timedelta

@pytest_asyncio.fixture(scope='function')
async def setup_test_universe_data(unit_test_db):
    """
    Shared fixture: sets up a test universe, instrument, membership, and minimal price data for integration tests.
    Universe id=1, instrument id=1 (AAPL), membership from 2020-01-01, and daily prices for 2024-01-02 to 2024-01-07.
    """
    conn = await asyncpg.connect(unit_test_db)
    # Insert vendor and get id
    vendor_name = 'ticker'
    vendor_row = await conn.fetchrow("SELECT id FROM test_vendors WHERE name=$1", vendor_name)
    if vendor_row is None:
        vendor_id = (await conn.fetchrow("INSERT INTO test_vendors (name) VALUES ($1) RETURNING id", vendor_name))["id"]
    else:
        vendor_id = vendor_row["id"]
    # Insert universe
    await conn.execute("INSERT INTO test_universe (id, name, description) VALUES (1, 'Test Universe', 'test') ON CONFLICT (id) DO NOTHING")
    # Insert instrument
    await conn.execute('''
        INSERT INTO test_instruments (id, symbol, name, exchange, type, currency, active)
        VALUES (1, 'AAPL', 'Apple Inc.', 'NASDAQ', 'stock', 'USD', TRUE)
        ON CONFLICT (id) DO NOTHING;
    ''')
    # Insert xref with vendor_id
    await conn.execute('''
        INSERT INTO test_instrument_xrefs (instrument_id, symbol, vendor_id, start_at)
        VALUES ($1, $2, $3, now())
        ON CONFLICT (instrument_id, vendor_id, start_at) DO NOTHING;
    ''', 1, 'AAPL', vendor_id)
    # Insert membership
    await conn.execute("""
        INSERT INTO test_universe_membership (universe_id, instrument_id, symbol, start_at, end_at)
        VALUES (1, 1, 'AAPL', $1, NULL)
        ON CONFLICT (universe_id, instrument_id, start_at) DO NOTHING;
    """, date(2020, 1, 1))
    # Insert daily prices for AAPL for the full test date range into test_daily_price_polygon
    start = date(2024, 1, 1)
    end = date(2024, 2, 15)
    d = start
    while d <= end:
        await conn.execute('''
            INSERT INTO test_daily_price_polygon (date, instrument_id, open, high, low, close, volume)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (date, instrument_id) DO NOTHING;
        ''', d, 1, 100.0, 110.0, 90.0, 105.0, 1000)
        d += timedelta(days=1)
    await conn.close()
    yield
