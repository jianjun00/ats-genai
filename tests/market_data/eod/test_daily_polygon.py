import pytest
import asyncio
from datetime import datetime, timezone
from config.environment import Environment, EnvironmentType
from db.test_db_manager import unit_test_db
from dao.instrument_polygon_dao import InstrumentPolygonDAO
from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
from market_data.eod import daily_price_polygon

@pytest.mark.asyncio
async def test_daily_polygon_inserts_prices(unit_test_db, monkeypatch, polygon_vendor_id):
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    from dao.instruments_dao import InstrumentsDAO
    instrument_dao = InstrumentsDAO(env)
    prices_dao = DailyPricesPolygonDAO(env)

    # Insert a test instrument into the canonical instruments table
    test_symbol = "AAPL"
    test_instrument_id = await instrument_dao.create_instrument(
        symbol=test_symbol,
        name="Apple Inc.",
        exchange="NASDAQ",
        type_="CS",
        currency="USD",
        list_date=datetime(2010,1,1).date(),
        delist_date=None
    )
    assert test_instrument_id is not None, "Instrument insert failed"
    # Insert xref for AAPL/Polygon
    from dao.instrument_xrefs_dao import InstrumentXrefsDAO
    xrefs_dao = InstrumentXrefsDAO(env)
    await xrefs_dao.create_xref(
        instrument_id=test_instrument_id,
        vendor_id=polygon_vendor_id,
        symbol=test_symbol,
        start_at=datetime(2010,1,1).date()
    )

    # Patch POLYGON_API_KEY and API calls
    monkeypatch.setenv("POLYGON_API_KEY", "testkey")
    def fake_download_prices_polygon(ticker, start, end, api_key, **kwargs):
        # Return a single fake price row for 2023-01-03 UTC
        ts = int(datetime(2023,1,3,0,0,0, tzinfo=timezone.utc).timestamp()*1000)
        return [{
            't': ts,
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
        environment=env,
        instrument_dao=instrument_dao,
        prices_dao=prices_dao,
        polygon_api_key="testkey"
    )
    # Check that a price was inserted
    rows = await prices_dao.list_prices(test_instrument_id)
    assert any(row['open'] == 100.0 and row['close'] == 105.0 for row in rows)
