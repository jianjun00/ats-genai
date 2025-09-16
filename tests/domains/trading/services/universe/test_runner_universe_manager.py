from state.universe_state_builder import UniverseStateIntervalBuilder
import app.runner

import pytest
import asyncpg
from datetime import date
from shared.utils.environment import Environment, EnvironmentType
from domains.trading.repositories.universe_dao import UniverseDAO
from domains.trading.repositories.universe_membership_dao import UniverseMembershipDAO

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_runner_universe_manager_sod_eod_real_db(unit_test_db):
    """
    Integration test: Runner calls update_for_sod and update_for_eod on UniverseManager, verifying correct instrument IDs each day.
    """
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    universe_dao = UniverseDAO(env)
    membership_dao = UniverseMembershipDAO(env)
    pool = await asyncpg.create_pool(unit_test_db)
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {env.get_table_name('universe_membership')}")
        await conn.execute(f"DELETE FROM {env.get_table_name('universe')}")
        await conn.execute(f"DELETE FROM {env.get_table_name('instrument_xrefs')}")
        await conn.execute(f"DELETE FROM {env.get_table_name('instruments')}")
        await conn.execute(f"DELETE FROM {env.get_table_name('vendors')}")
        # Insert vendor
        vendor_id = await conn.fetchval(f"INSERT INTO {env.get_table_name('vendors')} (name) VALUES ($1) RETURNING id", 'TESTVENDOR')
        # Insert instruments
        aapl_id = await conn.fetchval(f"INSERT INTO {env.get_table_name('instruments')} (symbol, name) VALUES ($1, $2) RETURNING id", 'AAPL', 'Apple Inc.')
        tsla_id = await conn.fetchval(f"INSERT INTO {env.get_table_name('instruments')} (symbol, name) VALUES ($1, $2) RETURNING id", 'TSLA', 'Tesla Inc.')
        # Insert instrument_xrefs
        await conn.execute(f"INSERT INTO {env.get_table_name('instrument_xrefs')} (instrument_id, vendor_id, symbol, type, start_at) VALUES ($1, $2, $3, $4, $5)", aapl_id, vendor_id, 'AAPL', 'primary', date(2025, 1, 1))
        await conn.execute(f"INSERT INTO {env.get_table_name('instrument_xrefs')} (instrument_id, vendor_id, symbol, type, start_at) VALUES ($1, $2, $3, $4, $5)", tsla_id, vendor_id, 'TSLA', 'primary', date(2025, 1, 1))
    universe_name = "RUNNER_SOD_EOD"
    universe_id = await universe_core.dao.create_universe(universe_name, "desc")
    # Build instrument_id to symbol mapping for assertions
    instrument_id_to_symbol = {aapl_id: 'AAPL', tsla_id: 'TSLA'}
    # Membership changes
    await membership_core.dao.add_membership_full(universe_id, aapl_id, start_at=date(2025, 7, 1))
    await membership_core.dao.add_membership_full(universe_id, tsla_id, start_at=date(2025, 7, 1))
    async with pool.acquire() as conn:
        await conn.execute(f"UPDATE {env.get_table_name('universe_membership')} SET end_at=$1 WHERE universe_id=$2 AND instrument_id=$3", date(2025, 7, 2), universe_id, aapl_id)
    await membership_core.dao.add_membership_full(universe_id, aapl_id, start_at=date(2025, 7, 3))
    async with pool.acquire() as conn:
        await conn.execute(f"UPDATE {env.get_table_name('universe_membership')} SET end_at=$1 WHERE universe_id=$2 AND instrument_id=$3 AND end_at IS NULL", date(2025, 7, 3), universe_id, tsla_id)
    # Run SOD/EOD for each day and check instrument ids
    env.get = lambda section, key, default=None: [] if (section, key) == ("runner", "callbacks") else default
    # Patch callbacks to provide required UniverseStateIntervalBuilder arguments
    callbacks = [lambda: UniverseStateIntervalBuilder(env=env, base_duration="1d", target_durations="1d")]
    runner_object = app.runner.Runner("2025-07-01", "2025-07-03", env, universe_id, callbacks=callbacks, base_duration="1d")
    # Patch runner.market_data_manager to use patched env (ensures correct DB URL)
    from domains.market_data.services.eod.daily_price_market_data_manager import DailyPriceMarketDataManager
    runner_object.market_data_manager = DailyPriceMarketDataManager(env=env)
    sod_instruments = {}
    async def capture_sod(runner_object, current_time):
        memberships = await membership_core.dao.get_active_memberships(universe_id, current_time.date())
        ids = [instrument_id_to_symbol.get(row['instrument_id']) for row in memberships]
        sod_instruments[current_time.date()] = set(ids)
    runner_object.universe_manager.update_for_sod = capture_sod
    for event_time, event_type in runner_object.iter_events():
        if event_type == "sod":
            await runner_object.update_for_sod(event_time)
    assert sod_instruments[date(2025, 7, 1)] == {"AAPL", "TSLA"}
    assert sod_instruments[date(2025, 7, 2)] == {"TSLA"}
    assert sod_instruments[date(2025, 7, 3)] == {"AAPL"}
    # Clean up
    async with pool.acquire() as conn:
        await conn.execute(f"DELETE FROM {env.get_table_name('universe_membership')}")
        await conn.execute(f"DELETE FROM {env.get_table_name('universe')}")
    await pool.close()
