import pytest
from datetime import date
from core.shared.utils.environment import Environment, EnvironmentType
from domains.market_data.services.eod.unify_daily_price_polygon import DatabaseDailyPricesUnifier
from domains.instruments.repositories.instruments_dao import InstrumentsDAO
from core.dao.vendors_dao import VendorsDAO
from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO
from vendor.tiingo.core.dao.daily_price_tiingo_dao import DailyPricesTiingoDAO
from vendor.polygon.core.dao.daily_price_polygon_dao import DailyPricesPolygonDAO

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_unify_daily_price_polygon_with_instrument_id(unit_test_db):
    """
    Test DatabaseDailyPricesUnifier.unify_daily_price_polygon fetches by instrument_id, not symbol.
    """
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    instruments_dao = InstrumentsDAO(env)
    xrefs_dao = InstrumentXrefsDAO(env)
    tiingo_dao = DailyPricesTiingoDAO(env)
    polygon_dao = DailyPricesPolygonDAO(env)
    vendors_dao = VendorsDAO(env)

    symbol = "AAPL"
    instrument_id = await instruments_core.dao.create_instrument(symbol=symbol)
    tiingo_vendor = await vendors_core.dao.get_vendor_by_name("tiingo")
    if not tiingo_vendor:
        await vendors_core.dao.create_vendor("tiingo")
        tiingo_vendor = await vendors_core.dao.get_vendor_by_name("tiingo")
    polygon_vendor = await vendors_core.dao.get_vendor_by_name("polygon")
    if not polygon_vendor:
        await vendors_core.dao.create_vendor("polygon")
        polygon_vendor = await vendors_core.dao.get_vendor_by_name("polygon")
    ticker_vendor = await vendors_core.dao.get_vendor_by_name("ticker")
    if not ticker_vendor:
        await vendors_core.dao.create_vendor("ticker")
        ticker_vendor = await vendors_core.dao.get_vendor_by_name("ticker")

    test_date = date(2025, 7, 18)
    # Add xrefs for tiingo, polygon, and ticker
    await xrefs_core.dao.create_xref(instrument_id, tiingo_vendor['id'], symbol, test_date)
    await xrefs_core.dao.create_xref(instrument_id, polygon_vendor['id'], symbol, test_date)
    await xrefs_core.dao.create_xref(instrument_id, ticker_vendor['id'], symbol, test_date, type="equity")

    # Insert prices using instrument_id only
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

    # Run the unifier
    unifier = DatabaseDailyPricesUnifier(env)
    results = await unifier.unify_daily_price_polygon(symbol, test_date, test_date)
    assert results, "unify_daily_price_polygon should return at least one result"
    row = results[0]
    assert row['date'] == test_date
    assert row['symbol'] == symbol
    assert row['open'] in (100.0, 101.0)
    assert row['close'] in (105.0, 106.0)
    assert row['volume'] in (1000000, 999999)
    assert row['source'] in ("tiingo", "polygon", "both")

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_unify_daily_price_polygon_missing_instrument_id(unit_test_db):
    """
    Test DatabaseDailyPricesUnifier.unify_daily_price_polygon returns [] if instrument_id cannot be resolved.
    """
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    unifier = DatabaseDailyPricesUnifier(env)
    # Use a symbol with no xref
    results = await unifier.unify_daily_price_polygon("NOEXIST", date(2025, 7, 18), date(2025, 7, 18))
    assert results == [], "Should return empty list if instrument_id cannot be resolved"
