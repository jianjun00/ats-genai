import pytest
import asyncpg
from datetime import date
from config.environment import get_environment

@pytest.fixture(scope="function")
async def spy_membership_fixture():
    env = get_environment()
    pool = await asyncpg.create_pool(env.get_database_url())
    universe_table = env.get_table_name('universe')
    membership_table = env.get_table_name('universe_membership')
    # Ensure S&P 500 universe exists
    async with pool.acquire() as conn:
        await conn.execute(f"""
            INSERT INTO {universe_table} (id, name, description)
            VALUES (100, 'S&P 500', 'Standard & Poor''s 500')
            ON CONFLICT (id) DO NOTHING
        """)
        # Insert two test members for a test date range
        await conn.execute(f"DELETE FROM {membership_table} WHERE universe_id=100")
        await conn.execute(f"INSERT INTO {membership_table} (universe_id, symbol, start_at, end_at) VALUES (100, 'AAPL', $1, NULL) ON CONFLICT DO NOTHING", date(2025, 1, 1))
        await conn.execute(f"INSERT INTO {membership_table} (universe_id, symbol, start_at, end_at) VALUES (100, 'TSLA', $1, NULL) ON CONFLICT DO NOTHING", date(2025, 1, 1))
    await pool.close()
    yield
    # Cleanup
    pool = await asyncpg.create_pool(env.get_database_url())
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {membership_table} WHERE universe_id=100")
    await pool.close()
