import pytest
import asyncpg
from datetime import date
from src.universe.universe_db import UniverseDB
from src.config.environment import Environment, EnvironmentType
from db.test_db_manager import unit_test_db

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

    # Add universe
    universe_name = "TEST_REALDB"
    universe_id = await db.add_universe(universe_name, "desc")
    assert isinstance(universe_id, int)

    # Add membership
    await db.add_universe_membership(universe_id, "AAPL", date(2025, 7, 25), None)

    # Retrieve members
    members = await db.get_universe_members(universe_id, date(2025, 7, 25))
    assert "AAPL" in members

    # Clean up
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {env.get_table_name('universe_membership')}")
        await conn.execute(f"DELETE FROM {env.get_table_name('universe')}")
    await pool.close()
