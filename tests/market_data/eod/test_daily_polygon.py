import sys
import os
# Add project root to sys.path so db.dao resolves to src/db/dao
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../..')))
import pytest
import asyncio
from datetime import datetime
from config.environment import set_environment, EnvironmentType, get_environment
from db.test_db_manager import unit_test_db
from dao.instrument_polygon_dao import InstrumentPolygonDAO
from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
from market_data.eod import daily_polygon

@pytest.mark.asyncio
async def test_daily_polygon_inserts_prices(unit_test_db, monkeypatch):
    from datetime import datetime, date
    today_str = datetime.now().strftime('%Y-%m-%d')
    print(f"[DEBUG] test setup today_str type: {type(today_str)}, value: {today_str}")
    print(f"[DEBUG] test setup today_date type: {type(date.today())}, value: {date.today()}")
    from config.environment import Environment
    env = Environment()
    env.config.set('database', 'database', unit_test_db.split('/')[-1])
    instrument_dao = InstrumentPolygonDAO(env)
    prices_dao = DailyPricesPolygonDAO(env)
    from dao.instrument_xrefs_dao import InstrumentXrefsDAO
    xrefs_dao = InstrumentXrefsDAO(env)

    # Insert a test instrument
    test_symbol = "AAPL"
    test_vendor_id = 1
    print(f"[DEBUG] insert_instrument list_date type: {type(datetime(2010, 1, 1).date())}, value: {datetime(2010, 1, 1).date()}")
    await instrument_dao.insert_instrument(
        test_symbol,
        name="Apple Inc.",
        exchange="NASDAQ",
        type_="CS",
        currency="USD",
        figi=None,
        isin=None,
        cusip=None,
        composite_figi=None,
        active=True,
        list_date=datetime(2010, 1, 1).date(),
        delist_date=None,
        raw=None
    )
    # Fetch instrument_id by symbol (if insert_instrument does not return id)
    instrument = await instrument_dao.get_instrument_by_symbol(test_symbol)
    instrument_id = instrument['id']
    # Insert xref for the test symbol using actual instrument_id
    await xrefs_dao.create_xref(
        instrument_id, test_vendor_id, test_symbol, datetime(2010, 1, 1).date()
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
    # Run the logic
    print(f"[DEBUG] run_ingestion start_date type: {type('2023-01-03')}, value: {'2023-01-03'}")
    print(f"[DEBUG] run_ingestion end_date type: {type('2023-01-03')}, value: {'2023-01-03'}")
    await daily_polygon.run_ingestion([test_symbol], "2023-01-03", "2023-01-03", instrument_dao, prices_dao)
    # Resolve instrument_id via xref for the test symbol
    resolved_id = await xrefs_dao.resolve_instrument_id(test_symbol)
    rows = await prices_dao.list_prices(resolved_id)
    assert any(row['open'] == 100.0 and row['close'] == 105.0 for row in rows)
