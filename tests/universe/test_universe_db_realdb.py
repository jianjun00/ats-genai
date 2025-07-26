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
    universe_name = "TEST_MULTI_DAY"
    universe_id = await db.add_universe(universe_name, "desc")
    # Day 1: Add AAPL & TSLA
    await db.add_universe_membership(universe_id, "AAPL", date(2025, 7, 1), None)
    await db.add_universe_membership(universe_id, "TSLA", date(2025, 7, 1), None)
    # Day 2: Remove AAPL (set end_at)
    async with pool.acquire() as conn:
        await conn.execute(f"UPDATE {env.get_table_name('universe_membership')} SET end_at=$1 WHERE universe_id=$2 AND symbol='AAPL'", date(2025, 7, 2), universe_id)
    # Day 3: Remove TSLA, add AAPL back
    await db.add_universe_membership(universe_id, "AAPL", date(2025, 7, 3), None)
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