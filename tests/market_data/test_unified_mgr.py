import pytest
from datetime import datetime, date
from shared.utils.environment import Environment, EnvironmentType
from domains.market_data.services.eod.unified_db_daily_price_market_data_manager import UnifiedDBDailyPriceMarketDataManager
from vendor.tiingo.core.dao.daily_prices_tiingo_dao import DailyPricesTiingoDAO
from vendor.polygon.core.dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
from domains.instruments.repositories.instruments_dao import InstrumentsDAO
from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_unified_mgr(unit_test_db):
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    instruments_dao = InstrumentsDAO(env)
    xrefs_dao = InstrumentXrefsDAO(env)
    tiingo_dao = DailyPricesTiingoDAO(env)
    polygon_dao = DailyPricesPolygonDAO(env)
    symbol = "AAPL"
    instrument_id = await instruments_core.dao.create_instrument(symbol=symbol)
    from dao.vendors_dao import VendorsDAO
    vendors_dao = VendorsDAO(env)
    tiingo_vendor = await vendors_core.dao.get_vendor_by_name("tiingo")
    if tiingo_vendor is None:
        await vendors_core.dao.create_vendor("tiingo")
        tiingo_vendor = await vendors_core.dao.get_vendor_by_name("tiingo")
    polygon_vendor = await vendors_core.dao.get_vendor_by_name("polygon")
    if polygon_vendor is None:
        await vendors_core.dao.create_vendor("polygon")
        polygon_vendor = await vendors_core.dao.get_vendor_by_name("polygon")
    test_date = date(2025, 7, 18)
    await xrefs_core.dao.create_xref(
        instrument_id=instrument_id,
        vendor_id=tiingo_vendor['id'],
        symbol=symbol,
        start_at=test_date
    )
    await xrefs_core.dao.create_xref(
        instrument_id=instrument_id,
        vendor_id=polygon_vendor['id'],
        symbol=symbol,
        start_at=test_date
    )
    # Add ticker xref for symbol resolution
    ticker_vendor = await vendors_core.dao.get_vendor_by_name("ticker")
    if ticker_vendor is None:
        await vendors_core.dao.create_vendor("ticker")
        ticker_vendor = await vendors_core.dao.get_vendor_by_name("ticker")
    await xrefs_core.dao.create_xref(
        instrument_id=instrument_id,
        vendor_id=ticker_vendor['id'],
        symbol=symbol,
        start_at=test_date,
        type="equity"
    )

    await tiingo_core.dao.insert_price(
        date=test_date,
        instrument_id=instrument_id,
        open_=100.0, high=110.0, low=99.0, close=105.0,
        adj_close=105.0, volume=1000000, status_id=None
    )
    await polygon_core.dao.insert_price(
        date=test_date,
        instrument_id=instrument_id,
        open_=101.0, high=111.0, low=98.0, close=106.0,
        volume=999999, market_cap=None
    )
    print("[TEST DEBUG] Creating UnifiedDBDailyPriceMarketDataManager...")
    mgr = await UnifiedDBDailyPriceMarketDataManager.create_async(env, symbols=[symbol])
    print(f"[TEST DEBUG] Manager created: {mgr}")
    print("[TEST DEBUG] Calling get_ohlc...")
    result = await mgr.get_ohlc(instrument_id, datetime(2025, 7, 18), datetime(2025, 7, 18))
    print(f"[TEST DEBUG] get_ohlc result: {result}")
    assert result is not None, "Unified manager should return a price dict"
    assert result['open'] in (100.0, 101.0)
    assert result['close'] in (105.0, 106.0)
    assert result['traded_volume'] in (1000000, 999999)
    assert result['source'] in ("tiingo", "polygon", "both")
