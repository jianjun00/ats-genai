import pytest
from shared.utils.environment import Environment, EnvironmentType
from db.test_db_manager import unit_test_db
from domains.instruments.repositories.instrument_indicator_interval_dao import InstrumentIndicatorIntervalDAO
from domains.instruments.repositories.instrument_interval_dao import InstrumentIntervalDAO
from domains.trading.repositories.universe_state_interval_dao import UniverseStateIntervalDAO
from datetime import datetime

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_instrument_indicator_interval_dao_crud(unit_test_db):
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    parent_dao = UniverseStateIntervalDAO(env)
    interval_id = await parent_core.dao.create(42, "5m", datetime(2025,8,7,9,30), datetime(2025,8,7,9,35))
    inst_dao = InstrumentIntervalDAO(env)
    inst_id = await inst_core.dao.create(interval_id, 101, 100.0, 110.0, 90.0, 105.0, 1000.0, 105000.0, "ok", 1e9)
    dao = InstrumentIndicatorIntervalDAO(env)
    # Insert
    ind_id = await dao.create(inst_id, "rsi", 55.5, "ok")
    row = await dao.get(ind_id)
    assert row is not None
    assert row["indicator_name"] == "rsi"
    # List
    rows = await dao.list(inst_id)
    assert any(r["id"] == ind_id for r in rows)
    # Delete
    deleted = await dao.delete(ind_id)
    assert deleted
