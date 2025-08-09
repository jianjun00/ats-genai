import pytest
import asyncio
from datetime import datetime, date
from src.config.environment import Environment, EnvironmentType
from src.market_data.eod.unified_db_daily_price_market_data_manager import UnifiedDBDailyPriceMarketDataManager
from src.dao.daily_prices_tiingo_dao import DailyPricesTiingoDAO
from src.dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
from src.dao.instruments_dao import InstrumentsDAO
from src.dao.instrument_xrefs_dao import InstrumentXrefsDAO
from src.db.test_db_manager import unit_test_db

@pytest.mark.asyncio
async def test_unified_manager_returns_unified_price(unit_test_db):
    # Use the isolated test DB for all DAOs and managers
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    instruments_dao = InstrumentsDAO(env)
    xrefs_dao = InstrumentXrefsDAO(env)
    tiingo_dao = DailyPricesTiingoDAO(env)
    polygon_dao = DailyPricesPolygonDAO(env)

    # Create instrument and xref
    symbol = "AAPL"
    instrument_id = await instruments_dao.create_instrument(symbol=symbol)
    await xrefs_dao.create_xref(instrument_id=instrument_id, vendor_name="tiingo", symbol=symbol)
    await xrefs_dao.create_xref(instrument_id=instrument_id, vendor_name="polygon", symbol=symbol)

    # Insert prices into tiingo and polygon tables for the same date
    test_date = date(2025, 7, 18)
    await tiingo_dao.insert_price(
        date=test_date,
        instrument_id=instrument_id,
        open_=100.0, high=110.0, low=99.0, close=105.0,
        adj_close=105.0, volume=1000000, status_id=None
    )
    await polygon_dao.insert_price(
        date=test_date,
        instrument_id=instrument_id,
        open_=101.0, high=111.0, low=98.0, close=106.0,
        volume=999999, market_cap=None
    )

    # Create the unified manager
    mgr = await UnifiedDBDailyPriceMarketDataManager.create_async(env, symbols=[symbol])
    # Fetch unified OHLC
    result = await mgr.get_ohlc(instrument_id, datetime(2025, 7, 18), datetime(2025, 7, 18))
    assert result is not None, "Unified manager should return a price dict"
    assert result['open'] in (100.0, 101.0)
    assert result['close'] in (105.0, 106.0)
    assert result['traded_volume'] in (1000000, 999999)
    assert result['source'] in ("tiingo", "polygon", "both")
