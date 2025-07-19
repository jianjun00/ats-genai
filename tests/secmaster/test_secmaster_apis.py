import pytest
from datetime import date
from secmaster.secmaster import SecMaster

class DummyConn:
    def __init__(self, fetchval_map):
        self._fetchval_map = fetchval_map
        self._calls = []
    async def fetchval(self, query, *args):
        # Record call for assertion
        self._calls.append((query, args))
        # Use the query string to pick the result
        if 'FROM daily_prices' in query and 'ORDER BY date DESC LIMIT 1' in query:
            # get_last_close_price
            return self._fetchval_map.get('last_close', None)
        elif 'AVG(close * volume)' in query:
            # get_average_dollar_volume
            return self._fetchval_map.get('avg_dv', None)
        elif 'FROM daily_prices' in query and 'market_cap' in query:
            # get_market_cap
            return self._fetchval_map.get('market_cap', None)
        return None
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc, tb):
        pass

class DummyPool:
    def __init__(self, fetchval_map):
        self._fetchval_map = fetchval_map
    def acquire(self):
        return DummyConn(self._fetchval_map)
    async def close(self):
        pass

@pytest.mark.asyncio
async def test_get_last_close_price(monkeypatch):
    fetchval_map = {'last_close': 123.45}
    async def dummy_create_pool(db_url):
        return DummyPool(fetchval_map)
    monkeypatch.setattr('asyncpg.create_pool', dummy_create_pool)
    secm = SecMaster('dummy', as_of_date=date(2024, 7, 19))
    price = await secm.get_last_close_price('AAPL')
    assert price == 123.45

@pytest.mark.asyncio
async def test_get_average_dollar_volume(monkeypatch):
    fetchval_map = {'avg_dv': 1_000_000}
    async def dummy_create_pool(db_url):
        return DummyPool(fetchval_map)
    monkeypatch.setattr('asyncpg.create_pool', dummy_create_pool)
    secm = SecMaster('dummy', as_of_date=date(2024, 7, 19))
    avg_dv = await secm.get_average_dollar_volume('AAPL', window=30)
    assert avg_dv == 1_000_000

@pytest.mark.asyncio
async def test_get_market_cap(monkeypatch):
    fetchval_map = {'market_cap': 2_500_000_000_000}
    async def dummy_create_pool(db_url):
        return DummyPool(fetchval_map)
    monkeypatch.setattr('asyncpg.create_pool', dummy_create_pool)
    secm = SecMaster('dummy', as_of_date=date(2024, 7, 19))
    mc = await secm.get_market_cap('AAPL')
    assert mc == 2_500_000_000_000
