import pytest
from datetime import date
from unittest.mock import AsyncMock, MagicMock

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_get_universe_members(monkeypatch):
    # Patch InstrumentsDAO to return id->symbol mapping
    class FakeInstrumentsDAO:
        def __init__(self, env):
            self.env = env
            self.table_name = 'instruments'
    monkeypatch.setattr('core.dao.instruments_core.dao.InstrumentsDAO', FakeInstrumentsDAO)

    # Patch asyncpg.create_pool to return a pool that returns our fake symbol mapping
    class FakeConn:
        async def fetch(self, query, ids):
            return [
                {'id': 1, 'symbol': 'AAPL'},
                {'id': 2, 'symbol': 'TSLA'}
            ]
    fake_conn = FakeConn()
    class FakeAcquire:
        async def __aenter__(self): return fake_conn
        async def __aexit__(self, exc_type, exc, tb): pass
    class FakePool:
        def acquire(self): return FakeAcquire()
        async def close(self): pass
        async def __aenter__(self): return self
        async def __aexit__(self, exc_type, exc, tb): pass
    async def fake_create_pool(db_url): return FakePool()
    monkeypatch.setattr('asyncpg.create_pool', fake_create_pool)

    from core.platform.config.environment import Environment, EnvironmentType
    from domains.trading.services.universe_db import UniverseDB
    env = Environment(env_type=EnvironmentType.TEST, db_url="postgresql://test:test@localhost:5432/test_db_patch")
    db = UniverseDB(env=env)
    db.universe_membership_dao = MagicMock()
    # Memberships now return instrument_id only
    db.universe_membership_core.dao.get_active_memberships = AsyncMock(return_value=[{'instrument_id': 1}, {'instrument_id': 2}])

    members = await db.get_universe_members(1, date(2025, 7, 24))
    assert members == [1, 2]
    db.universe_membership_core.dao.get_active_memberships.assert_awaited_once_with(1, date(2025, 7, 24))
