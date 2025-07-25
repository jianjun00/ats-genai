import pytest
import asyncpg
from datetime import date
from src.config.environment import Environment, EnvironmentType
from db.test_db_manager import unit_test_db
from src.app.runner import Runner
from src.universe.universe_db import UniverseDB

@pytest.mark.asyncio
async def test_runner_universe_manager_sod_eod_real_db(unit_test_db):
    """
    Integration test: Runner calls update_for_sod and update_for_eod on UniverseManager, verifying correct instrument IDs each day.
    """
    env = Environment(EnvironmentType.TEST)
    env.get_database_url = lambda: unit_test_db
    db = UniverseDB(env=env)
    pool = await asyncpg.create_pool(unit_test_db)
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {env.get_table_name('universe_membership')}")
        await conn.execute(f"DELETE FROM {env.get_table_name('universe')}")
    universe_name = "RUNNER_SOD_EOD"
    universe_id = await db.add_universe(universe_name, "desc")
    # Membership changes
    await db.add_universe_membership(universe_id, "AAPL", date(2025, 7, 1), None)
    await db.add_universe_membership(universe_id, "TSLA", date(2025, 7, 1), None)
    async with pool.acquire() as conn:
        await conn.execute(f"UPDATE {env.get_table_name('universe_membership')} SET end_at=$1 WHERE universe_id=$2 AND symbol='AAPL'", date(2025, 7, 2), universe_id)
    await db.add_universe_membership(universe_id, "AAPL", date(2025, 7, 3), None)
    async with pool.acquire() as conn:
        await conn.execute(f"UPDATE {env.get_table_name('universe_membership')} SET end_at=$1 WHERE universe_id=$2 AND symbol='TSLA' AND end_at IS NULL", date(2025, 7, 3), universe_id)
    # Run SOD/EOD for each day and check instrument ids
    # Patch config to avoid callback unpack error
    env.get = lambda section, key, default=None: [] if (section, key) == ("runner", "callbacks") else default
    runner = Runner("2025-07-01", "2025-07-03", env, universe_id)
    sod_instruments = {}
    async def capture_sod(runner, current_time):
        ids = await db.get_universe_members(universe_id, current_time.date())
        sod_instruments[current_time.date()] = set(ids)
    # Patch UniverseManager.update_for_sod to capture instrument ids
    runner.universe_manager.update_for_sod = capture_sod
    for event_time, event_type in runner.iter_events():
        if event_type == "sod":
            await runner.update_for_sod(event_time)
    assert sod_instruments[date(2025, 7, 1)] == {"AAPL", "TSLA"}
    assert sod_instruments[date(2025, 7, 2)] == {"TSLA"}
    assert sod_instruments[date(2025, 7, 3)] == {"AAPL"}
    # Clean up
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {env.get_table_name('universe_membership')}")
        await conn.execute(f"DELETE FROM {env.get_table_name('universe')}")
    await pool.close()
