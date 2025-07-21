import pytest
from datetime import date
from secmaster.secmaster import SecMaster

class DummyConn:
    def __init__(self, rows):
        self._rows = rows
    async def fetch(self, query, *args, **kwargs):
        if 'universe_membership' in query:
            # Return membership intervals for S&P 500
            return self._rows
        if 'close' in query or 'market_cap' in query:
            # Return dummy close and market_cap for all current members
            return [
                {'symbol': 'TICK1', 'close': 100.0, 'market_cap': 1000000},
                {'symbol': 'TICK2', 'close': 200.0, 'market_cap': 2000000},
            ]
        return self._rows
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc, tb):
        pass
    async def fetchval(self, query, *args, **kwargs):
        # Mock ADV (average dollar volume) query for SecMaster.advance
        return 1234.56

class DummyPool:
    def __init__(self, rows):
        self._rows = rows
    def acquire(self):
        return DummyConn(self._rows)
    async def close(self):
        pass

@pytest.mark.asyncio
async def test_advance_membership(monkeypatch):
    # Membership intervals: (symbol, start_date, end_date)
    events = [
        {'symbol': 'TICK1', 'start_date': date(2020,1,1), 'end_date': date(2020,6,1)},
        {'symbol': 'TICK1', 'start_date': date(2021,1,1), 'end_date': None},
        {'symbol': 'TICK2', 'start_date': date(2020,3,1), 'end_date': None},
    ]
    async def dummy_create_pool(db_url):
        return DummyPool(events)
    monkeypatch.setattr('asyncpg.create_pool', dummy_create_pool)
    secm = SecMaster('dummy', as_of_date=date(2020,1,1))
    members = await secm.get_spy_membership()
    assert set(members) == {'TICK1'}

    # Advance to 2020-03-01 (TICK2 added)
    try:
        members = await secm.advance(date(2020,3,1))
    except KeyError as e:
        print(f"[DEBUG] KeyError: {e}")
        import traceback; traceback.print_exc()
        raise
    assert set(members) == {'TICK1', 'TICK2'}
    # Advance to 2020-06-01 (TICK1 removed)
    members = await secm.advance(date(2020,6,1))
    assert set(members) == {'TICK2'}
    # Advance to 2021-01-01 (TICK1 re-added)
    members = await secm.advance(date(2021,1,1))
    assert set(members) == {'TICK1', 'TICK2'}
