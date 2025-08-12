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

import pytest_asyncio
from config.environment import Environment, EnvironmentType
from dao.vendors_dao import VendorsDAO
from dao.instruments_dao import InstrumentsDAO
from dao.instrument_xrefs_dao import InstrumentXrefsDAO

@pytest_asyncio.fixture(scope="function")
async def manager(vendors_dirs, unit_test_db):
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
    env.get_table_name = lambda table: f"test_{table}"
    
    # Set up vendors, instruments, and xrefs
    vendors_dao = VendorsDAO(env)
    instruments_dao = InstrumentsDAO(env)
    xrefs_dao = InstrumentXrefsDAO(env)
    
    # Add ticker vendor
    ticker_vendor_id = await vendors_dao.create_vendor("ticker")
    
    # Add instruments
    aapl_id = await instruments_dao.create_instrument("AAPL", "Apple Inc.")
    tsla_id = await instruments_dao.create_instrument("TSLA", "Tesla Inc.")
    
    # Add xrefs for ticker vendor
    from datetime import date
    today = date.today()
    await xrefs_dao.create_xref(aapl_id, ticker_vendor_id, "AAPL", start_at=today)
    await xrefs_dao.create_xref(tsla_id, ticker_vendor_id, "TSLA", start_at=today)
    
    # Only test AAPL and TSLA (should exist in both dirs)
    mgr = await FileDailyPriceMarketDataManager.create_async(vendors_dirs, env, symbols=["AAPL", "TSLA"])
    return mgr

import pytest

import pytest
import pytest_asyncio

@pytest.mark.asyncio
async def test_symbol_resolution(manager):
    assert await manager.resolve_instrument_id("AAPL") == 1
    assert await manager.resolve_instrument_id("TSLA") == 2
    assert await manager.resolve_symbol(1) == "AAPL"
    assert await manager.resolve_symbol(2) == "TSLA"

@pytest.mark.asyncio
async def test_get_all_symbols(manager):
    syms = manager._get_all_symbols()
    assert set(syms) == {"AAPL", "TSLA"}

@pytest.mark.asyncio
async def test_get_ohlc(manager):
    # Pick a known date in both fixtures
    dt = datetime(2024, 1, 2)
    iid = await manager.resolve_instrument_id("AAPL")
    # Set the last SOD date to match the test date
    manager.set_last_sod_date(dt.date())
    ohlc = await manager.get_ohlc(iid, dt, dt)
    assert ohlc is not None
    assert ohlc["open"] > 0
    assert ohlc["close"] > 0
    assert ohlc["traded_volume"] > 0


@pytest.mark.asyncio
async def test_get_ohlc_batch(manager):
    dt = datetime(2024, 1, 2)
    # Await the async resolve_instrument_id calls
    aapl_id = await manager.resolve_instrument_id("AAPL")
    tsla_id = await manager.resolve_instrument_id("TSLA")
    ids = [aapl_id, tsla_id]
    # Set the last SOD date to match the test date
    manager.set_last_sod_date(dt.date())
    batch = await manager.get_ohlc_batch(ids, dt, dt)
    assert set(batch.keys()) == set(ids)
    for ohlc in batch.values():
        assert ohlc is not None
        assert ohlc["open"] > 0
        assert ohlc["close"] > 0
        assert ohlc["traded_volume"] > 0


@pytest.mark.asyncio
async def test_tiingo_list_date_parsing(tmp_path, unit_test_db):
    import json
    from unittest.mock import AsyncMock, patch, MagicMock
    
    print(f"[DEBUG][test_tiingo_list_date_parsing] Starting test with tmp_path={tmp_path}, unit_test_db={unit_test_db}")
    
    try:
        # 1. Normal bars (control)
        normal = {'date': '2025-01-02T00:00:00.000Z', 'open': 10, 'high': 15, 'low': 8, 'close': 12, 'volume': 1000}
        # 2. Malformed date
        malformed_date = {'date': 'bad-date-format', 'open': 10, 'high': 15, 'low': 8, 'close': 12, 'volume': 1000}
        # 3. Missing date
        missing_date = {'open': 10, 'high': 15, 'low': 8, 'close': 12, 'volume': 1000}
        # 4. Missing open/close
        missing_open = {'date': '2025-01-04T00:00:00.000Z', 'high': 15, 'low': 8, 'close': 12, 'volume': 1000}
        missing_close = {'date': '2025-01-05T00:00:00.000Z', 'open': 10, 'high': 15, 'low': 8, 'volume': 1000}
        # 5. Extra/unexpected fields
        extra_fields = {'date': '2025-01-06T00:00:00.000Z', 'open': 10, 'high': 15, 'low': 8, 'close': 12, 'volume': 1000, 'foo': 123}
        # 6. Non-dict row
        non_dict = "notadict"
        # 7. Row with 't' field (should be ignored for tiingo, but test mixed)
        t_field_row = {'t': 1735776000000, 'open': 10, 'high': 15, 'low': 8, 'close': 12, 'volume': 1000}  # 2025-01-03
        # 8. Empty list
        tiingo_data = [normal, malformed_date, missing_date, missing_open, missing_close, extra_fields, non_dict, t_field_row]
        
        # Create test data files
        tiingo_dir = tmp_path / "tiingo"
        tiingo_dir.mkdir()
        tiingo_file = tiingo_dir / "tiingo_aapl_response.json"
        with open(tiingo_file, "w") as f:
            json.dump(tiingo_data, f)
        
        polygon_dir = tmp_path / "polygon"
        polygon_dir.mkdir()
        vendors_dirs = {"polygon": str(polygon_dir), "tiingo": str(tiingo_dir)}
        
        print(f"[DEBUG][test_tiingo_list_date_parsing] Created test files in {tmp_path}")
        
        # Set up environment with test database
        from config.environment import Environment, EnvironmentType
        env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        env.get_table_name = lambda table: f"test_{table}"
        
        # Mock all database interactions
        mock_xrefs_dao = MagicMock()
        mock_xrefs_dao.resolve_instrument_id_by_symbol = AsyncMock(return_value=1)
        mock_xrefs_dao.get_symbol_by_instrument_id_vendor_name = AsyncMock(return_value="AAPL")
        
        mock_vendors_dao = MagicMock()
        mock_vendors_dao.get_vendor_by_name = AsyncMock(return_value={"id": 1, "name": "ticker"})
        
        print("[DEBUG][test_tiingo_list_date_parsing] Setting up mocks...")
        
        with patch('dao.instrument_xrefs_dao.InstrumentXrefsDAO', return_value=mock_xrefs_dao), \
             patch('dao.vendors_dao.VendorsDAO', return_value=mock_vendors_dao):
            
            print("[DEBUG][test_tiingo_list_date_parsing] Creating FileDailyPriceMarketDataManager...")
            mgr = await FileDailyPriceMarketDataManager.create_async(vendors_dirs, env, symbols=["AAPL"])
            
            print("[DEBUG][test_tiingo_list_date_parsing] Verifying data...")
            data = mgr.vendor_data["tiingo"]["AAPL"]
            
            # Verify the data was loaded correctly
            assert isinstance(data, dict), f"Expected dict, got {type(data)}"
            
            # Check that keys are dates for valid rows (normal, missing_open, missing_close, extra_fields)
            expected_dates = ["2025-01-02", "2025-01-04", "2025-01-05", "2025-01-06"]
            for d in expected_dates:
                dt = datetime.strptime(d, "%Y-%m-%d").date()
                assert dt in data, f"Expected date {d} not found in data: {data.keys()}"
            
            # The row with a malformed date should not be present
            assert not any(k for k in data if isinstance(k, str) and k == "bad-date-format"), \
                f"Found malformed date key in data: {data.keys()}"
            
            # The row that's not a dict, or missing date/t, should not cause a crash
            # There should be no key for None
            assert None not in data, f"Found None key in data: {data.keys()}"
            
            print("[DEBUG][test_tiingo_list_date_parsing] Test completed successfully")
    
    except Exception as e:
        print(f"[DEBUG][test_tiingo_list_date_parsing] Test failed with error: {str(e)}")
        import traceback
        print(f"[DEBUG][test_tiingo_list_date_parsing] Traceback: {traceback.format_exc()}")
        raise
        # The loader should ignore non-dict rows and missing date/t rows gracefully

@pytest.mark.asyncio
async def test_validate_date_no_warning(manager, caplog):
    # This test checks that no warning is logged when current_date matches the row date
    dt = datetime(2024, 1, 2)
    iid = manager.resolve_instrument_id("AAPL")
    manager.set_last_sod_date(dt.date())
    with caplog.at_level("WARNING"):
        ohlc = manager.get_ohlc(iid, dt, dt)
    assert ohlc is not None
    # Ensure no warning about date mismatch is present
    assert not any("validate_date: date" in r for r in caplog.text.splitlines())


