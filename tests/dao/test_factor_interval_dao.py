import pytest
from shared.utils.environment import Environment, EnvironmentType
from db.test_db_manager import unit_test_db
from domains.trading.repositories.factor_interval_dao import FactorIntervalDAO
from domains.trading.repositories.universe_state_interval_dao import UniverseStateIntervalDAO
from datetime import datetime

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_factor_interval_dao_crud(unit_test_db):
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    parent_dao = UniverseStateIntervalDAO(env)
    interval_id = await parent_core.dao.create(42, "5m", datetime(2025,8,7,9,30), datetime(2025,8,7,9,35))
    dao = FactorIntervalDAO(env)
    # Insert
    factor_id = await dao.create(interval_id, "momentum", 1.23)
    row = await dao.get(factor_id)
    assert row is not None
    assert row["factor_name"] == "momentum"
    # List
    rows = await dao.list(interval_id)
    assert any(r["id"] == factor_id for r in rows)
    # Delete
    deleted = await dao.delete(factor_id)
    assert deleted
