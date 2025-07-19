import pytest
import asyncio
from datetime import date
from secmaster.secmaster import SecMaster

class DummyConn:
    def __init__(self, rows):
        self._rows = rows
    async def fetch(self, query, as_of_date):
        # Only return events up to as_of_date, mimicking the DB filter
        return [row for row in self._rows if row['event_date'] <= as_of_date]
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc, tb):
        pass

class DummyPool:
    def __init__(self, rows):
        self._rows = rows
    def acquire(self):
        # Return DummyConn directly, not a coroutine
        return DummyConn(self._rows)
    async def close(self):
        pass

@pytest.mark.asyncio
async def test_membership_add_remove_logic(monkeypatch):
    # Simulate the following event sequence for TICK1:
    # 2020-01-01: add, 2020-06-01: remove, 2021-01-01: add
    # For TICK2: 2020-03-01: add, never removed
    events = [
        {'added': 'TICK1', 'removed': None, 'event_date': date(2020,1,1)},
        {'added': None, 'removed': 'TICK1', 'event_date': date(2020,6,1)},
        {'added': 'TICK1', 'removed': None, 'event_date': date(2021,1,1)},
        {'added': 'TICK2', 'removed': None, 'event_date': date(2020,3,1)},
    ]
    
    # Patch asyncpg.create_pool to return DummyPool
    async def dummy_create_pool(db_url):
        return DummyPool(events)
    monkeypatch.setattr('asyncpg.create_pool', dummy_create_pool)
    secm = SecMaster('dummy')

    # As of 2020-02-01 (before TICK2 is added)
    members = await secm.get_spy_membership(date(2020,2,1))
    assert 'TICK1' in members
    assert 'TICK2' not in members

    # As of 2020-03-01 (TICK2 just added, TICK1 still in)
    members = await secm.get_spy_membership(date(2020,3,1))
    assert 'TICK1' in members
    assert 'TICK2' in members

    # Before TICK1 re-added (after it was removed)
    members = await secm.get_spy_membership(date(2020,7,1))
    assert 'TICK1' not in members
    assert 'TICK2' in members

    # After TICK1 re-added
    members = await secm.get_spy_membership(date(2021,2,1))
    assert 'TICK1' in members
    assert 'TICK2' in members

    # Before any adds
    members = await secm.get_spy_membership(date(2019,12,31))
    assert members == []

    # After first add but before remove
    members = await secm.get_spy_membership(date(2020,2,1))
    assert 'TICK1' in members
    assert 'TICK2' not in members
