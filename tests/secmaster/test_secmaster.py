import pytest
import asyncio
from datetime import date
from secmaster.secmaster import SecMaster

class DummyConn:
    def __init__(self, rows):
        self._rows = rows
    async def fetch(self, query, *args, **kwargs):
        if 'universe_membership' in query:
            return self._rows
        # Fallback for other queries
        return self._rows
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
    # Membership intervals: (symbol, start_date, end_date)
    events = [
        {'symbol': 'TICK1', 'start_date': date(2020,1,1), 'end_date': date(2020,6,1)},
        {'symbol': 'TICK1', 'start_date': date(2021,1,1), 'end_date': None},
        {'symbol': 'TICK2', 'start_date': date(2020,3,1), 'end_date': None},
    ]
    
    # Patch asyncpg.create_pool to return DummyPool
    async def dummy_create_pool(db_url):
        return DummyPool(events)
    monkeypatch.setattr('asyncpg.create_pool', dummy_create_pool)
    # As of 2020-02-01 (before TICK2 is added)
    from unittest.mock import MagicMock
    mock_env = MagicMock()
    mock_env.get_table_name.side_effect = lambda name: name
    mock_env.get_database_url.return_value = 'postgresql://test/test'
    secm = SecMaster(mock_env, as_of_date=date(2020,2,1))
    members = await secm.get_spy_membership()
    assert 'TICK1' in members
    assert 'TICK2' not in members

    # As of 2020-03-01 (TICK2 just added, TICK1 still in)
    secm = SecMaster(mock_env, as_of_date=date(2020,3,1))
    members = await secm.get_spy_membership()
    assert 'TICK1' in members
    assert 'TICK2' in members

    # Before TICK1 re-added (after it was removed)
    secm = SecMaster(mock_env, as_of_date=date(2020,7,1))
    members = await secm.get_spy_membership()
    assert 'TICK1' not in members
    assert 'TICK2' in members

    # After TICK1 re-added
    from unittest.mock import MagicMock
    mock_env = MagicMock()
    mock_env.get_table_name.side_effect = lambda name: name
    mock_env.get_database_url.return_value = 'postgresql://test/test'
    secm = SecMaster(mock_env, as_of_date=date(2021,2,1))
    members = await secm.get_spy_membership()
    assert 'TICK1' in members
    assert 'TICK2' in members

    # Before any adds
    secm = SecMaster(mock_env, as_of_date=date(2019,12,31))
    members = await secm.get_spy_membership()
    assert members == []

    # After first add but before remove
    from unittest.mock import MagicMock
    mock_env = MagicMock()
    mock_env.get_table_name.side_effect = lambda name: name
    mock_env.get_database_url.return_value = 'postgresql://test/test'
    secm = SecMaster(mock_env, as_of_date=date(2020,2,1))
    members = await secm.get_spy_membership()
    assert 'TICK1' in members
    assert 'TICK2' not in members

@pytest.mark.asyncio
async def test_advance_membership_and_caches(monkeypatch):
    # Membership intervals: (symbol, start_date, end_date)
    events = [
        {'symbol': 'TICK1', 'start_date': date(2020,1,1), 'end_date': date(2020,6,1)},
        {'symbol': 'TICK1', 'start_date': date(2021,1,1), 'end_date': None},
        {'symbol': 'TICK2', 'start_date': date(2020,3,1), 'end_date': None},
    ]
    # Simulate daily_prices rows for cache checks
    daily_prices_rows = [
        {'symbol': 'TICK1', 'close': 100, 'market_cap': 1000000, 'date': date(2020,3,1)},
        {'symbol': 'TICK2', 'close': 50, 'market_cap': 500000, 'date': date(2020,3,1)},
        {'symbol': 'TICK2', 'close': 60, 'market_cap': 600000, 'date': date(2021,2,1)},
        {'symbol': 'TICK1', 'close': 110, 'market_cap': 1100000, 'date': date(2021,2,1)},
    ]
    adv_map = {
        ('TICK1', 30): 1234.0,
        ('TICK2', 30): 5678.0,
    }

    class DummyConnAdv(DummyConn):
        async def fetch(self, query, *args, **kwargs):
            if 'FROM daily_prices' in query and ('close' in query or 'market_cap' in query):
                date_arg = args[0]
                syms = set(args[1])
                return [r for r in daily_prices_rows if r['date'] == date_arg and r['symbol'] in syms]
            return []
        async def fetchval(self, query, *args, **kwargs):
            # For ADV
            sym = args[0]
            return adv_map.get((sym, 30), None)

    class DummyPoolAdv:
        def acquire(self):
            return DummyConnAdv(daily_prices_rows)
        async def close(self):
            pass

    async def dummy_create_pool(db_url):
        return DummyPoolAdv()
    monkeypatch.setattr('asyncpg.create_pool', dummy_create_pool)

    # Patch membership fetch
    async def dummy_load_events(self):
        self._events = [dict(row) for row in events]
    monkeypatch.setattr(SecMaster, 'load_all_membership_events', dummy_load_events)

    from unittest.mock import MagicMock
    mock_env = MagicMock()
    mock_env.get_table_name.side_effect = lambda name: name
    mock_env.get_database_url.return_value = 'postgresql://test/test'
    secm = SecMaster(mock_env, as_of_date=date(2020,2,1))
    await secm.load_all_membership_events()
    # Advance to 2020-03-01
    members = await secm.advance(date(2020,3,1))
    assert set(members) == {'TICK1', 'TICK2'}
    assert secm._last_close_price_cache['TICK1'] == 100
    assert secm._last_close_price_cache['TICK2'] == 50
    assert secm._market_cap_cache['TICK1'] == 1000000
    assert secm._market_cap_cache['TICK2'] == 500000
    assert secm._adv_cache[('TICK1', 30)] == 1234.0
    assert secm._adv_cache[('TICK2', 30)] == 5678.0

    # Advance to 2021-02-01 (TICK1 re-added, TICK2 updated)
    members = await secm.advance(date(2021,2,1))
    assert set(members) == {'TICK1', 'TICK2'}
    assert secm._last_close_price_cache['TICK1'] == 110
    assert secm._last_close_price_cache['TICK2'] == 60
    assert secm._market_cap_cache['TICK1'] == 1100000
    assert secm._market_cap_cache['TICK2'] == 600000
    assert secm._adv_cache[('TICK1', 30)] == 1234.0
    assert secm._adv_cache[('TICK2', 30)] == 5678.0