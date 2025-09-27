import pytest
from datetime import date
from domains.instruments.services.secmaster.secmaster import SecMaster

class DummyConn:
    def __init__(self, rows):
        self._rows = rows
    async def fetch(self, query, *args, **kwargs):
        if 'universe_membership' in query:
            # Return membership intervals for S&P 500, include all required fields
            return [
                {'symbol': 'TICK1', 'start_date': date(2020,1,1), 'end_date': date(2020,6,1), 'instrument_id': 1},
                {'symbol': 'TICK1', 'start_date': date(2021,1,1), 'end_date': None, 'instrument_id': 1},
                {'symbol': 'TICK2', 'start_date': date(2020,3,1), 'end_date': None, 'instrument_id': 2},
            ]
        if 'FROM daily_price_polygon' in query and ('close' in query or 'market_cap' in query):
            # Return dummy close and market_cap for all current members, include instrument_id and symbol
            daily_price_polygon_rows = [
                {'date': date(2020,1,1), 'symbol': 'TICK1', 'close': 100.0, 'market_cap': 1000000, 'instrument_id': 1},
                {'date': date(2020,1,1), 'symbol': 'TICK2', 'close': 200.0, 'market_cap': 2000000, 'instrument_id': 2},
                {'date': date(2021,1,1), 'symbol': 'TICK1', 'close': 150.0, 'market_cap': 1500000, 'instrument_id': 1},
                {'date': date(2021,1,1), 'symbol': 'TICK2', 'close': 250.0, 'market_cap': 2500000, 'instrument_id': 2},
            ]
            date_arg = args[0]
            syms = set(args[1])
            return [row for row in daily_price_polygon_rows if row['date'] == date_arg and row['symbol'] in syms]
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
    from unittest.mock import MagicMock
    mock_env = MagicMock()
    mock_env.get_table_name.side_effect = lambda name: name
    mock_env.get_database_url.return_value = 'postgresql://test/test'
    secm = SecMaster(mock_env, as_of_date=date(2020,1,1))
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
