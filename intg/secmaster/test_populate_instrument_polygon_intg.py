import pytest
import asyncio
from unittest.mock import patch
from config.environment import Environment, EnvironmentType
import secmaster.populate_instrument_polygon as pip

@pytest.mark.asyncio
async def test_polygon_api_key_is_used(monkeypatch):
    """
    Integration test: Ensure POLYGON_API_KEY from Gin config is used in all Polygon API requests.
    """
    # Patch requests.get to capture URL
    requested_urls = []
    class FakeResp:
        def __init__(self, url):
            self.status_code = 200
            self._url = url
            if "/v3/reference/tickers?" in url:
                # Bulk endpoint
                self.text = '{"results": [{"ticker": "AAPL"}, {"ticker": "TSLA"}]}'
                self._json = {"results": [{"ticker": "AAPL"}, {"ticker": "TSLA"}]}
            else:
                # Detail endpoint
                self.text = '{"results": {"ticker": "AAPL", "list_date": "2020-01-01"}}'
                self._json = {"results": {"ticker": "AAPL", "list_date": "2020-01-01"}}
        def json(self):
            return self._json
    def fake_get(url, *args, **kwargs):
        requested_urls.append(url)
        return FakeResp(url)
    monkeypatch.setattr(pip.requests, "get", fake_get)
    # Patch upsert_instrument to avoid DB
    async def fake_upsert_instrument(pool, detail):
        pass
    monkeypatch.setattr(pip, "upsert_instrument", fake_upsert_instrument)
    # Patch asyncpg.create_pool
    class DummyPool:
        async def close(self):
            pass
    async def fake_create_pool(*args, **kwargs):
        return DummyPool()
    monkeypatch.setattr(pip.asyncpg, "create_pool", fake_create_pool)
    # Set up Gin config and POLYGON_API_KEY
    pip.POLYGON_API_KEY = "testkey123"
    # Patch env to a dummy Environment
    from unittest.mock import MagicMock
    pip.env = MagicMock()
    pip.env.get_database_url.return_value = "postgresql://test:test@localhost/test_db"
    pip.env.get_table_name.side_effect = lambda name: f"test_{name}"
    # Run fetch_and_store_instruments for a single ticker
    await pip.fetch_and_store_instruments(ticker="AAPL")
    # Check that apiKey param is present in all requests
    assert any("apiKey=testkey123" in url for url in requested_urls), f"apiKey not found in URLs: {requested_urls}"
    # Also test bulk endpoint
    await pip.fetch_and_store_instruments(start_ticker="A")
    assert any("apiKey=testkey123" in url for url in requested_urls), f"apiKey not found in bulk URLs: {requested_urls}"

import asyncpg

@pytest.mark.asyncio
async def test_ray_batched_upsert_integration(monkeypatch, unit_test_db):
    """
    Integration test: Run Ray-parallelized, batched upsert logic with integration test DB.
    """
    import secmaster.populate_instrument_polygon as pip
    # Patch requests.get to return fake detail for each ticker
    class FakeResp:
        def __init__(self, url):
            self.status_code = 200
            if "reference/tickers?" in url:
                # Bulk endpoint returns both tickers
                self._json = {"results": [{"ticker": "AAPL"}, {"ticker": "TSLA"}]}
            elif "AAPL" in url:
                self.status_code = 200
                self._json = {"results": {"ticker": "AAPL", "name": "Apple Inc", "primary_exchange": "NASDAQ", "type": "CS", "currency_name": "USD", "share_class_figi": "", "isin": "", "cusip": "", "composite_figi": "", "active": True, "list_date": "2021-06-10", "delisted_utc": "2022-01-01T00:00:00Z"}}
            elif "TSLA" in url:
                self.status_code = 200
                self._json = {"results": {"ticker": "TSLA", "name": "Tesla Inc", "primary_exchange": "NASDAQ", "type": "CS", "currency_name": "USD", "share_class_figi": "", "isin": "", "cusip": "", "composite_figi": "", "active": True, "list_date": "2020-01-01", "delisted_utc": None}}
            else:
                self.status_code = 200
                self._json = {"results": {}}
        def json(self):
            return self._json
    monkeypatch.setattr(pip.requests, "get", lambda url, *a, **k: FakeResp(url))
    # Set up Gin config and POLYGON_API_KEY
    pip.POLYGON_API_KEY = "testkey123"
    # Patch env to integration env
    from config.environment import Environment, EnvironmentType
    pip.env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    table_name = pip.env.get_table_name("instrument_polygon")
    # Clean table
    conn = await asyncpg.connect(unit_test_db)
    await conn.execute(f'DROP TABLE IF EXISTS {table_name} CASCADE;')
    await conn.close()
    # Run main workflow for two tickers (AAPL, TSLA)
    await pip.fetch_and_store_instruments(ticker=None, start_ticker="A")
    # Verify both tickers present in DB
    conn = await asyncpg.connect(unit_test_db)
    rows = await conn.fetch(f'SELECT symbol, name FROM {table_name} WHERE symbol IN ($1, $2)', "AAPL", "TSLA")
    symbols = {row["symbol"] for row in rows}
    assert "AAPL" in symbols and "TSLA" in symbols, f"Missing tickers in DB: {symbols}"
    await conn.close()
