import pytest
import asyncio
from datetime import date
from shared.utils.environment import Environment, EnvironmentType
from domains.market_data.services.eod.unify_daily_prices import DatabaseDailyPricesUnifier
from domains.instruments.repositories.instruments_dao import InstrumentsDAO
from core.dao.vendors_dao import VendorsDAO
from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO
from vendor.polygon.core.dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
from src.db.test_db_manager import unit_test_db

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_polygon_price_fields_are_not_none(unit_test_db):
    """
    Regression test: polygon price fields must not be None if present in DB.
    """
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    instruments_dao = InstrumentsDAO(env)
    xrefs_dao = InstrumentXrefsDAO(env)
    polygon_dao = DailyPricesPolygonDAO(env)
    vendors_dao = VendorsDAO(env)

    symbol = "A"
    instrument_id = await instruments_core.dao.create_instrument(symbol=symbol)
    polygon_vendor = await vendors_core.dao.get_vendor_by_name("polygon")
    if not polygon_vendor:
        await vendors_core.dao.create_vendor("polygon")
        polygon_vendor = await vendors_core.dao.get_vendor_by_name("polygon")
    ticker_vendor = await vendors_core.dao.get_vendor_by_name("ticker")
    if not ticker_vendor:
        await vendors_core.dao.create_vendor("ticker")
        ticker_vendor = await vendors_core.dao.get_vendor_by_name("ticker")

    test_date = date(2025, 7, 29)
    # Add xrefs for polygon and ticker
    await xrefs_core.dao.create_xref(instrument_id, polygon_vendor['id'], symbol, test_date)
    await xrefs_core.dao.create_xref(instrument_id, ticker_vendor['id'], symbol, test_date, type="equity")

    # Insert price using instrument_id only
    await polygon_core.dao.insert_price(
        date=test_date,
        instrument_id=instrument_id,
        open_=119.75, high=120.505, low=118.855, close=119.84,
        volume=1212204, market_cap=None
    )

    # Run the unifier
    unifier = DatabaseDailyPricesUnifier(env)
    results = await unifier.unify_daily_prices(symbol, test_date, test_date)
    assert results, "unify_daily_prices should return at least one result"
    row = results[0]
    assert row['date'] == test_date
    assert row['symbol'] == symbol
    # All price fields should match the DB and not be None
    assert row['open'] == 119.75, f"open field is {row['open']}" 
    assert row['high'] == 120.505, f"high field is {row['high']}"
    assert row['low'] == 118.855, f"low field is {row['low']}"
    assert row['close'] == 119.84, f"close field is {row['close']}"
    assert row['volume'] == 1212204, f"volume field is {row['volume']}"
    assert row['source'] == "polygon"
