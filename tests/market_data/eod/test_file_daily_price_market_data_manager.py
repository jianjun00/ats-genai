import os
import pytest
print(f"[IMPORT_DEBUG] Loaded test_file_daily_price_market_data_manager.py from {__file__}")
from datetime import datetime
from market_data.eod.file_daily_price_market_data_manager import FileDailyPriceMarketDataManager

@pytest.fixture(scope="module")
def vendors_dirs():
    # Use test data directories
    base = os.path.abspath(os.path.dirname(__file__))
    polygon_dir = os.path.join(base, "../../../tests/data/daily_prices_polygon")
    tiingo_dir = os.path.join(base, "../../../tests/data/daily_prices_tiingo")
    return {
        "polygon": polygon_dir,
        "tiingo": tiingo_dir,
    }

@pytest.fixture(scope="module")
def manager(vendors_dirs):
    # Only test AAPL and TSLA (should exist in both dirs)
    return FileDailyPriceMarketDataManager(vendors_dirs, symbols=["AAPL", "TSLA"])

def test_symbol_resolution(manager):
    assert manager.resolve_instrument_id("AAPL") == 1
    assert manager.resolve_instrument_id("TSLA") == 2
    assert manager.resolve_symbol(1) == "AAPL"
    assert manager.resolve_symbol(2) == "TSLA"

def test_get_all_symbols(manager):
    syms = manager._get_all_symbols()
    assert set(syms) == {"AAPL", "TSLA"}

def test_get_ohlc(manager):
    # Pick a known date in both fixtures
    dt = datetime(2024, 1, 2)
    iid = manager.resolve_instrument_id("AAPL")
    # Set the last SOD date to match the test date
    manager.set_last_sod_date(dt.date())
    ohlc = manager.get_ohlc(iid, dt, dt)
    assert ohlc is not None
    assert ohlc["open"] > 0
    assert ohlc["close"] > 0
    assert ohlc["traded_volume"] > 0


def test_get_ohlc_batch(manager):
    dt = datetime(2024, 1, 2)
    ids = [manager.resolve_instrument_id("AAPL"), manager.resolve_instrument_id("TSLA")]
    # Set the last SOD date to match the test date
    manager.set_last_sod_date(dt.date())
    batch = manager.get_ohlc_batch(ids, dt, dt)
    assert set(batch.keys()) == set(ids)
    for ohlc in batch.values():
        assert ohlc is not None
        assert ohlc["open"] > 0
        assert ohlc["close"] > 0
        assert ohlc["traded_volume"] > 0


def test_validate_date_no_warning(manager, caplog):
    # This test checks that no warning is logged when current_date matches the row date
    dt = datetime(2024, 1, 2)
    iid = manager.resolve_instrument_id("AAPL")
    manager.set_last_sod_date(dt.date())
    with caplog.at_level("WARNING"):
        ohlc = manager.get_ohlc(iid, dt, dt)
    assert ohlc is not None
    # Ensure no warning about date mismatch is present
    assert not any("validate_date: date" in r for r in caplog.text.splitlines())


