import pytest
from db.test_db_manager import integration_test_db
from dao.instrument_polygon_dao import InstrumentPolygonDAO
from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
from market_data.eod import daily_polygon
from config.environment import Environment, EnvironmentType
from datetime import datetime

@pytest.mark.asyncio
async def test_daily_polygon_inserts_prices(integration_test_db, monkeypatch):
    from config.environment import Environment, EnvironmentType
    from intg_tests.db.test_intg_db_base_intg import get_test_db_url
    env = Environment(env_type=EnvironmentType.INTEGRATION, db_url=get_test_db_url())
    instrument_dao = InstrumentPolygonDAO(env)
    prices_dao = DailyPricesPolygonDAO(env)

    # Insert a test instrument
    test_symbol = "AAPL"
    test_instrument_id = 1
    await instrument_dao.insert_instrument(
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
    # ... (rest of the test code as in the original file)
