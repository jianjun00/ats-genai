import pytest
from datetime import datetime
from core.platform.config_env.environment import Environment, EnvironmentType
from domains.instruments.repositories.instrument_interval_dao import InstrumentIntervalDAO

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_instrument_interval_dao_crud(unit_test_db):
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    dao = InstrumentIntervalDAO(env)
    # Insert a row
    universe_state_interval_id = 1  # you must ensure this exists or mock
    instrument_id = 101
    open_ = 100.0
    high = 110.0
    low = 90.0
    close = 105.0
    traded_volume = 1000.0
    traded_dollar = 105000.0
    status = "ok"
    market_cap = 1e9
    # For test, first insert a universe_state_interval row
    # Actually use UniverseStateIntervalDAO to insert parent
    from domains.trading.repositories.universe_state_interval_dao import UniverseStateIntervalDAO
    parent_dao = UniverseStateIntervalDAO(env)
    parent_id = await parent_core.dao.create(42, "5m", datetime(2025,8,7,9,30), datetime(2025,8,7,9,35))
    # Now insert child
    id = await dao.create(parent_id, instrument_id, open_, high, low, close, traded_volume, traded_dollar, status, market_cap)
    row = await dao.get(id)
    assert row is not None
    assert row["instrument_id"] == instrument_id
    assert row["open"] == open_
    # List
    rows = await dao.list(parent_id)
    assert any(r["id"] == id for r in rows)
    # Delete
    deleted = await dao.delete(id)
    assert deleted
