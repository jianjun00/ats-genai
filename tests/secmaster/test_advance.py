import pytest
from datetime import date
from secmaster.secmaster import SecMaster

class DummyConn:
    def __init__(self, rows):
        self._rows = rows
    async def fetch(self, query, *args, **kwargs):
        return self._rows
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc, tb):
        pass

class DummyPool:
    def __init__(self, rows):
        self._rows = rows
    def acquire(self):
        return DummyConn(self._rows)
    async def close(self):
        pass

@pytest.mark.asyncio
async def test_advance_membership(monkeypatch):
    events = [
        {'added': 'TICK1', 'removed': None, 'event_date': date(2020,1,1)},
        {'added': None, 'removed': 'TICK1', 'event_date': date(2020,6,1)},
        {'added': 'TICK1', 'removed': None, 'event_date': date(2021,1,1)},
        {'added': 'TICK2', 'removed': None, 'event_date': date(2020,3,1)},
    ]
    async def dummy_create_pool(db_url):
        return DummyPool(events)
    monkeypatch.setattr('asyncpg.create_pool', dummy_create_pool)
    secm = SecMaster('dummy', as_of_date=date(2020,1,1))
    members = await secm.get_spy_membership()
    assert set(members) == {'TICK1'}

    # Advance to 2020-03-01 (TICK2 added)
    members = await secm.advance(date(2020,3,1))
    assert set(members) == {'TICK1', 'TICK2'}
    # Advance to 2020-06-01 (TICK1 removed)
    members = await secm.advance(date(2020,6,1))
    assert set(members) == {'TICK2'}
    # Advance to 2021-01-01 (TICK1 re-added)
    members = await secm.advance(date(2021,1,1))
    assert set(members) == {'TICK1', 'TICK2'}
