import pytest
import asyncio
from datetime import date, timedelta
import asyncpg
import os
import uuid

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src/universe')))
from universe_db import UniverseDB

from config.environment import get_environment, set_environment, EnvironmentType
set_environment(EnvironmentType.INTEGRATION)
env = get_environment()
TSDB_URL = env.get_database_url()

@pytest.mark.asyncio
async def test_universe_db_crud():
    # Use a random universe name for isolation
    test_universe_name = f"TEST_UNIVERSE_{uuid.uuid4().hex[:8]}"
    db = UniverseDB(TSDB_URL)

    # Clean up if exists
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        universe_table = env.get_table_name("universe")
        universe_membership_table = env.get_table_name("universe_membership")
        await conn.execute(f'DELETE FROM {universe_membership_table} WHERE universe_id IN (SELECT id FROM {universe_table} WHERE name = $1)', test_universe_name)
        await conn.execute(f'DELETE FROM {universe_table} WHERE name = $1', test_universe_name)
    await pool.close()

    # Add universe
    universe_id = await db.add_universe(test_universe_name, description="Test universe")
    assert universe_id is not None
    fetched_id = await db.get_universe_id(test_universe_name)
    assert fetched_id == universe_id

    # Add members
    today = date.today()
    await db.add_universe_membership(universe_id, 'AAPL', today)
    await db.add_universe_membership(universe_id, 'TSLA', today, end_at=today + timedelta(days=2))
    # Membership for AAPL is open-ended, TSLA ends in 2 days

    # Query as of today
    members_today = await db.get_universe_members(universe_id, today)
    assert 'AAPL' in members_today
    assert 'TSLA' in members_today

    # Query after TSLA end
    after_end = today + timedelta(days=3)
    members_after = await db.get_universe_members(universe_id, after_end)
    assert 'AAPL' in members_after
    assert 'TSLA' not in members_after

    # Update TSLA to end today
    await db.update_universe_membership_end(universe_id, 'AAPL', today + timedelta(days=1))
    members_after_aapl_end = await db.get_universe_members(universe_id, today + timedelta(days=2))
    assert 'AAPL' not in members_after_aapl_end

    # Clean up
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        await conn.execute(f'DELETE FROM {universe_membership_table} WHERE universe_id = $1', universe_id)
        await conn.execute(f'DELETE FROM {universe_table} WHERE id = $1', universe_id)
    await pool.close()
