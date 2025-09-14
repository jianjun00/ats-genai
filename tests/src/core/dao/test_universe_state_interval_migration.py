pytest_plugins = ["src.test.conftest"]

import pytest
import asyncpg
from datetime import datetime, timedelta

from shared.utils.environment import Environment, EnvironmentType
from domains.trading.repositories.universe_dao import UniverseDAO
from domains.trading.repositories.universe_state_interval_dao import UniverseStateIntervalDAO

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_universe_state_interval_has_no_interval_blob_after_migrations(unit_test_db):
    """
    Ensure that on a fresh, migrated test DB, the universe_state_interval table
    does not contain the legacy interval_blob column.
    This guards against NOT NULL violations seen previously.
    """
    db_url = unit_test_db

    # Verify column absence via information_schema
    conn = await asyncpg.connect(db_url)
    try:
        rows = await conn.fetch(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'test_universe_state_interval'
              AND column_name = 'interval_blob'
            """
        )
        assert len(rows) == 0, "interval_blob column should not exist after migration"
    finally:
        await conn.close()

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_universe_state_interval_dao_insert_succeeds_without_blob(unit_test_db):
    """
    Validate that inserting a universe_state_interval via DAO succeeds without
    setting any legacy blob column, using normalized schema.
    """
    db_url = unit_test_db
    env = Environment(env_type=EnvironmentType.TEST, db_url=db_url)

    # Obtain a universe_id to satisfy FK. Some migrations seed universes with explicit ids
    # without fixing the sequence, which can cause duplicate key on insert. Reuse existing if present.
    conn_chk = await asyncpg.connect(db_url)
    try:
        table_universe = env.get_table_name('universe')
        existing = await conn_chk.fetchrow(f"SELECT id FROM {table_universe} ORDER BY id ASC LIMIT 1")
        if existing:
            universe_id = existing["id"]
        else:
            # Ensure sequence is aligned before insert
            seq = await conn_chk.fetchval("SELECT pg_get_serial_sequence($1, 'id')", table_universe)
            max_id = await conn_chk.fetchval(f"SELECT COALESCE(MAX(id), 0) FROM {table_universe}")
            if seq:
                await conn_chk.execute("SELECT setval($1, $2, true)", seq, max_id)
            udao = UniverseDAO(env)
            universe_name = f"test_universe_{datetime.utcnow().timestamp()}"
            universe_id = await ucore.dao.create_universe(universe_name, description="for interval test")
    finally:
        await conn_chk.close()
    assert universe_id is not None

    # Insert an interval
    start_dt = datetime(2024, 1, 1, 0, 0, 0)
    end_dt = start_dt + timedelta(days=1)

    usi_dao = UniverseStateIntervalDAO(env)
    interval_id = await usi_core.dao.create(
        universe_id=universe_id,
        duration="1D",
        start_date_time=start_dt,
        end_date_time=end_dt,
    )
    assert interval_id is not None

    # Read back to confirm row exists and no interval_blob is involved
    conn = await asyncpg.connect(db_url)
    try:
        table = env.get_table_name('universe_state_interval')
        row = await conn.fetchrow(
            f"SELECT id, universe_id, duration, start_date_time, end_date_time FROM {table} WHERE id = $1",
            interval_id,
        )
        assert row is not None
        assert row["universe_id"] == universe_id
        assert row["duration"] == "1D"
    finally:
        await conn.close()
