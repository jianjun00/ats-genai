import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from datetime import date

# Example: test for UniverseDAO
from dao.universe_dao import UniverseDAO
from dao.universe_membership_dao import UniverseMembershipDAO
from config.environment import get_environment

@pytest.mark.asyncio
class DummyConn:
    def __init__(self, fetchrow_result=None, fetch_result=None, execute_result=None, raise_exc=None):
        self._fetchrow_result = fetchrow_result
        self._fetch_result = fetch_result
        self._execute_result = execute_result
        self._raise_exc = raise_exc
        self.fetchrow_calls = []
        self.fetch_calls = []
        self.execute_calls = []
    async def fetchrow(self, *args, **kwargs):
        self.fetchrow_calls.append((args, kwargs))
        if self._raise_exc: raise self._raise_exc
        return self._fetchrow_result
    async def fetch(self, *args, **kwargs):
        self.fetch_calls.append((args, kwargs))
        if self._raise_exc: raise self._raise_exc
        return self._fetch_result
    async def execute(self, *args, **kwargs):
        self.execute_calls.append((args, kwargs))
        if self._raise_exc: raise self._raise_exc
        return self._execute_result
    async def __aenter__(self): return self
    async def __aexit__(self, exc_type, exc, tb): pass

class DummyPool:
    def __init__(self, conn): self._conn = conn
    def acquire(self): return self._conn
    async def close(self): pass

import pytest
from unittest.mock import AsyncMock
from dao.universe_dao import UniverseDAO
from dao.universe_membership_dao import UniverseMembershipDAO
from config.environment import get_environment
from datetime import date

@pytest.mark.asyncio
async def test_universe_dao_get_universe_by_name_found(monkeypatch):
    env = get_environment()
    result_row = {'id': 1, 'name': 'TEST'}
    conn = DummyConn(fetchrow_result=result_row)
    pool = DummyPool(conn)
    monkeypatch.setattr('asyncpg.create_pool', AsyncMock(return_value=pool))
    dao = UniverseDAO(env)
    result = await dao.get_universe_by_name('TEST')
    assert result == result_row

@pytest.mark.asyncio
async def test_universe_dao_get_universe_by_name_not_found(monkeypatch):
    env = get_environment()
    conn = DummyConn(fetchrow_result=None)
    pool = DummyPool(conn)
    monkeypatch.setattr('asyncpg.create_pool', AsyncMock(return_value=pool))
    dao = UniverseDAO(env)
    result = await dao.get_universe_by_name('MISSING')
    assert result is None

@pytest.mark.asyncio
async def test_universe_dao_create_and_list(monkeypatch):
    env = get_environment()
    # create_universe returns id
    conn = DummyConn(fetchrow_result={'id': 5})
    pool = DummyPool(conn)
    monkeypatch.setattr('asyncpg.create_pool', AsyncMock(return_value=pool))
    dao = UniverseDAO(env)
    uid = await dao.create_universe('FOO', 'desc')
    assert uid == 5
    # list_universes returns list
    conn2 = DummyConn(fetch_result=[{'id': 1}, {'id': 2}])
    pool2 = DummyPool(conn2)
    monkeypatch.setattr('asyncpg.create_pool', AsyncMock(return_value=pool2))
    universes = await dao.list_universes()
    assert universes == [{'id': 1}, {'id': 2}]

@pytest.mark.asyncio
async def test_universe_dao_update_universe(monkeypatch):
    env = get_environment()
    conn = DummyConn(execute_result='UPDATE 1')
    pool = DummyPool(conn)
    monkeypatch.setattr('asyncpg.create_pool', AsyncMock(return_value=pool))
    dao = UniverseDAO(env)
    updated = await dao.update_universe(1, name='updated', description='desc')
    assert updated is True
    # No fields to update returns False
    updated = await dao.update_universe(1)
    assert updated is False

@pytest.mark.asyncio
async def test_universe_dao_get_universe(monkeypatch):
    env = get_environment()
    conn = DummyConn(fetchrow_result={'id': 2, 'name': 'BAR'})
    pool = DummyPool(conn)
    monkeypatch.setattr('asyncpg.create_pool', AsyncMock(return_value=pool))
    dao = UniverseDAO(env)
    result = await dao.get_universe(2)
    assert result == {'id': 2, 'name': 'BAR'}

@pytest.mark.asyncio
async def test_universe_membership_dao_add_and_remove(monkeypatch):
    env = get_environment()
    # add_membership returns id
    conn = DummyConn(fetchrow_result={'id': 11})
    pool = DummyPool(conn)
    monkeypatch.setattr('asyncpg.create_pool', AsyncMock(return_value=pool))
    dao = UniverseMembershipDAO(env)
    # add_membership expects instrument_id as int, not str
    mid = await dao.add_membership(1, instrument_id=101)
    assert mid is True
    # remove_membership returns True (use int for instrument_id)
    conn2 = DummyConn(execute_result='DELETE 1')
    pool2 = DummyPool(conn2)
    monkeypatch.setattr('asyncpg.create_pool', AsyncMock(return_value=pool2))
    # remove_membership expects symbol and start_at, not instrument_id
    removed = await dao.remove_membership(1, symbol=101, start_at=None)
    assert removed is True

@pytest.mark.asyncio
async def test_universe_membership_dao_update_and_add_full(monkeypatch):
    env = get_environment()
    conn = DummyConn(execute_result='UPDATE 1')
    pool = DummyPool(conn)
    monkeypatch.setattr('asyncpg.create_pool', AsyncMock(return_value=pool))
    dao = UniverseMembershipDAO(env)
    # update_membership_end
    await dao.update_membership_end(1, 'AAPL', date(2025, 7, 24))
    # add_membership_full
    await dao.add_membership_full(1, 'AAPL', date(2025, 7, 24), None)

@pytest.mark.asyncio
async def test_universe_membership_dao_getters(monkeypatch):
    env = get_environment()
    memberships = [{'symbol': 'AAPL'}, {'symbol': 'TSLA'}]
    conn = DummyConn(fetch_result=memberships)
    pool = DummyPool(conn)
    monkeypatch.setattr('asyncpg.create_pool', AsyncMock(return_value=pool))
    dao = UniverseMembershipDAO(env)
    # get_active_memberships
    result = await dao.get_active_memberships(1, date(2025, 7, 24))
    assert result == memberships
    # get_memberships_by_universe
    result2 = await dao.get_memberships_by_universe(1)
    assert result2 == memberships
    # get_memberships_by_instrument
    result3 = await dao.get_memberships_by_instrument(101)
    assert result3 == memberships

@pytest.mark.asyncio
async def test_universe_dao_db_error(monkeypatch):
    env = get_environment()
    exc = RuntimeError('DB fail')
    conn = DummyConn(raise_exc=exc)
    pool = DummyPool(conn)
    monkeypatch.setattr('asyncpg.create_pool', AsyncMock(return_value=pool))
    dao = UniverseDAO(env)
    with pytest.raises(RuntimeError, match='DB fail'):
        await dao.get_universe_by_name('ERR')
