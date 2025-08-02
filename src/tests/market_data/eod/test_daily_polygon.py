import pytest
import asyncio
from datetime import datetime
from config.environment import set_environment, EnvironmentType, get_environment
from db.test_db_manager import unit_test_db
from db.dao.instrument_polygon_dao import InstrumentPolygonDAO
from db.dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
from market_data.eod import daily_polygon

@pytest.mark.asyncio
async def test_daily_polygon_inserts_prices(unit_test_db, monkeypatch):
    set_environment(EnvironmentType.TEST)
    env = get_environment()
    instrument_dao = InstrumentPolygonDAO(env)
    prices_dao = DailyPricesPolygonDAO(env)

    # Insert a test instrument
    test_symbol = "AAPL"
    test_instrument_id = 1
    await instrument_dao.insert_instrument(
        instrument_id=test_instrument_id,
        symbol=test_symbol,
        name="Apple Inc.",
        exchange="NASDAQ",
        type_="CS",
        currency="USD",
        figi=None,
        isin=None,
        cusip=None,
        composite_figi=None,
        active=True,
        list_date=datetime(2010,1,1).date(),
        delist_date=None,
        raw=None
    )
    # Patch POLYGON_API_KEY and API calls
    monkeypatch.setenv("POLYGON_API_KEY", "testkey")
    def fake_download_prices_polygon(ticker, start, end, api_key):
        # Return a single fake price row
        return [{
            't': int(datetime(2023,1,3).timestamp()*1000),
            'o': 100.0, 'h': 110.0, 'l': 95.0, 'c': 105.0, 'v': 1000000
        }]
    monkeypatch.setattr(daily_polygon, "download_prices_polygon", fake_download_prices_polygon)
    # Patch shares outstanding API
    class FakeResp:
        status_code = 200
        def json(self):
            return {'results': {'share_class_shares_outstanding': 1000000000}}
    monkeypatch.setattr(daily_polygon.requests, "get", lambda url: FakeResp())
    # Run the ingestion logic directly
    await daily_polygon.run_ingestion(
        tickers=[test_symbol],
        start_date="2023-01-03",
        end_date="2023-01-03",
        environment="test",
        instrument_dao=instrument_dao,
        prices_dao=prices_dao,
        polygon_api_key="testkey"
    )
    # Check that a price was inserted
    rows = await prices_dao.list_prices(test_instrument_id)
    assert any(row['open'] == 100.0 and row['close'] == 105.0 for row in rows)
