import pytest
from datetime import datetime
from shared.utils.environment import Environment, EnvironmentType
from domains.market_data.services.eod.unified_db_daily_price_market_data_manager import UnifiedDBDailyPriceMarketDataManager
from vendor.tiingo.core.dao.daily_price_polygon_tiingo_dao import DailyPricesTiingoDAO
from vendor.polygon.core.dao.daily_price_polygon_polygon_dao import DailyPricesPolygonDAO
from domains.instruments.repositories.instruments_dao import InstrumentsDAO
from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO
from core.dao.vendors_dao import VendorsDAO

async def get_or_create_vendor(vendors_dao, name, description=None):
    """Helper function to get a vendor by name or create it if it doesn't exist"""
    vendor = await vendors_core.dao.get_vendor_by_name(name)
    if not vendor:
        vendor_id = await vendors_core.dao.create_vendor(name=name, description=description)
        return {"id": vendor_id, "name": name}
    return vendor

@pytest.mark.asyncio
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
    instrument_id = await instruments_core.dao.create_instrument(symbol=symbol)

    # Get or create vendor IDs for tiingo, polygon, and ticker
    vendors_dao = VendorsDAO(env)
    tiingo_vendor = await get_or_create_vendor(vendors_dao, "tiingo", "Tiingo data provider")
    polygon_vendor = await get_or_create_vendor(vendors_dao, "polygon", "Polygon data provider")
    ticker_vendor = await get_or_create_vendor(vendors_dao, "ticker", "Standard ticker symbols")

    # Create xrefs with vendor_id and start_at
    from datetime import date
    today = date.today()
    await xrefs_core.dao.create_xref(instrument_id=instrument_id, vendor_id=tiingo_vendor['id'], symbol=symbol, start_at=today)
    await xrefs_core.dao.create_xref(instrument_id=instrument_id, vendor_id=polygon_vendor['id'], symbol=symbol, start_at=today)
    await xrefs_core.dao.create_xref(instrument_id=instrument_id, vendor_id=ticker_vendor['id'], symbol=symbol, start_at=today)

    # Insert prices into tiingo and polygon tables for the same date
    test_date = date(2025, 7, 18)
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

    # Create the unified manager
    mgr = await UnifiedDBDailyPriceMarketDataManager.create_async(env, symbols=[symbol])
    # Fetch unified OHLC
    result = await mgr.get_ohlc(instrument_id, datetime(2025, 7, 18), datetime(2025, 7, 18))
    assert result is not None, "Unified manager should return a price dict"
    assert result['open'] in (100.0, 101.0)
    assert result['close'] in (105.0, 106.0)
    assert result['traded_volume'] in (1000000, 999999)
    assert result['source'] in ("tiingo", "polygon", "both")
