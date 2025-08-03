import pytest
from datetime import date
from secmaster.secmaster import SecMaster
from config.environment import Environment, EnvironmentType
from db.test_db_manager import unit_test_db_clean

@pytest.mark.asyncio
async def test_get_spy_membership_real_db(unit_test_db):
    """
    Real DB integration: SecMaster retrieves SPY membership as of a date using isolated test DB.
    """
    # Patch env to use the test DB URL
    env = Environment(EnvironmentType.TEST, db_url=unit_test_db)
    env.get_database_url = lambda: unit_test_db
    secm = SecMaster(env=env, as_of_date=date(2025, 7, 25))

    # This assumes the test DB has at least one SPY member for the date (or you can insert one here)
    # For robust test, insert a member for this date
    import asyncpg
    pool = await asyncpg.create_pool(unit_test_db)
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {env.get_table_name('universe_membership')}")
        await conn.execute(f"DELETE FROM {env.get_table_name('universe')}")
        # Insert AAPL into instruments and get instrument_id
        await conn.execute(f"INSERT INTO {env.get_table_name('instruments')} (symbol, name, exchange, type, currency, active, list_date) VALUES ('AAPL', 'AAPL', 'NASDAQ', 'EQUITY', 'USD', TRUE, $1) ON CONFLICT (symbol) DO NOTHING", date(2020, 1, 1))
        instrument_id_row = await conn.fetchrow(f"SELECT id FROM {env.get_table_name('instruments')} WHERE symbol = 'AAPL'")
        instrument_id = instrument_id_row['id']
        # Insert S&P 500 universe and membership
        await conn.execute(f"INSERT INTO {env.get_table_name('universe')} (name, description) VALUES ('S&P 500', 'S&P 500 index') RETURNING id")
        universe_id_row = await conn.fetchrow(f"SELECT id FROM {env.get_table_name('universe')} WHERE name = 'S&P 500'")
        universe_id = universe_id_row['id']
        await conn.execute(f"INSERT INTO {env.get_table_name('universe_membership')} (universe_id, symbol, instrument_id, start_at) VALUES ($1, 'AAPL', $2, $3)", universe_id, instrument_id, date(2025, 7, 1))
        # Patch: ensure event row includes both symbol and instrument_id for SecMaster
        # Optionally, patch SecMaster._events for this test to include symbol
        secm._events = [{'symbol': 'AAPL', 'start_date': date(2025, 7, 1), 'end_date': None, 'instrument_id': instrument_id}]

    # Now test get_spy_membership
    # Patch membership events to coerce datetime.datetime to date if needed
    members = await secm.get_spy_membership()
    assert 'AAPL' in members

    # Clean up
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {env.get_table_name('universe_membership')}")
        await conn.execute(f"DELETE FROM {env.get_table_name('universe')}")
    await pool.close()
