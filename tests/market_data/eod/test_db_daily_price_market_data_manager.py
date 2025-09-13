import pytest
from datetime import datetime
from unittest.mock import patch

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../src')))

from domains.market_data.services.eod.db_daily_price_market_data_manager import DBDailyPriceMarketDataManager
from shared.utils.environment import Environment, EnvironmentType

class DummyVendorsDAO:
    def __init__(self, env):
        self.env = env

    async def get_vendor_by_name(self, name):
        return {'id': 1, 'name': 'ticker'}

class DummyXrefsDAO:
    def __init__(self, env):
        self.env = env
        self.symbol_map = {'AAPL': 1, 'TSLA': 2}
        self.vendor_id = 1

    async def resolve_instrument_id(self, symbol, vendor_id=None, at_date=None):
        return self.symbol_map.get(symbol.upper())

    async def resolve_instrument_id_by_symbol(self, symbol, at_date=None):
        return self.symbol_map.get(symbol.upper())

    async def get_symbol_by_instrument_id_vendor_name(self, instrument_id, vendor_name="ticker"):
        reverse_map = {v: k for k, v in self.symbol_map.items()}
        return reverse_map.get(instrument_id)

class DummyPricesDAO:
    def __init__(self, env):
        self.env = env
    async def list_prices_for_instruments_and_date(self, instrument_ids, as_of_date):
        # Return dummy data for AAPL and TSLA
        rows = []
        for iid in instrument_ids:
            if iid == 1:
                rows.append({'instrument_id': 1, 'open': 10, 'high': 15, 'low': 9, 'close': 14, 'volume': 1000})
            elif iid == 2:
                rows.append({'instrument_id': 2, 'open': 20, 'high': 25, 'low': 19, 'close': 24, 'volume': 2000})
        return rows

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_db_daily_price_manager_symbol_mapping(monkeypatch, unit_test_db):
    # Create a proper test environment with the test database
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)

    # Set up the mocks
    mock_xrefs_dao = DummyXrefsDAO(env)
    mock_vendors_dao = DummyVendorsDAO(env)

    # Patch the DAO constructors to return our mocks
    with patch('market_data.eod.db_daily_price_market_data_manager.InstrumentXrefsDAO', return_value=mock_xrefs_dao), \
         patch('core.dao.vendors_core.dao.VendorsDAO', return_value=mock_vendors_dao):

        # Initialize the manager
        mgr = DBDailyPriceMarketDataManager(env, symbols=['AAPL', 'TSLA'])

        # Test direct resolution using the DAO
        assert await mgr.resolve_instrument_id('AAPL') == 1
        assert await mgr.resolve_instrument_id('TSLA') == 2

        # Test symbol resolution
        assert await mgr.resolve_symbol(1) == 'AAPL'
        assert await mgr.resolve_symbol(2) == 'TSLA'

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_db_daily_price_manager_get_ohlc(monkeypatch, unit_test_db):
    # Create a proper test environment with the test database
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)

    # Set up the mocks
    mock_xrefs_dao = DummyXrefsDAO(env)
    mock_vendors_dao = DummyVendorsDAO(env)
    mock_prices_dao = DummyPricesDAO(env)

    # Patch the DAO constructors to return our mocks
    with patch('market_data.eod.db_daily_price_market_data_manager.InstrumentXrefsDAO', return_value=mock_xrefs_dao), \
         patch('core.dao.vendors_core.dao.VendorsDAO', return_value=mock_vendors_dao), \
         patch('market_data.eod.db_daily_price_market_data_manager.DailyPricesDAO', return_value=mock_prices_dao):

        # Initialize the manager
        mgr = DBDailyPriceMarketDataManager(env, symbols=['AAPL'])

        # Get OHLC data
        ohlc = await mgr.get_ohlc(1, datetime(2025, 8, 8), datetime(2025, 8, 8))

        # Verify the data
        assert ohlc['open'] == 10
        assert ohlc['high'] == 15
        assert ohlc['low'] == 9
        assert ohlc['close'] == 14
        assert ohlc['traded_volume'] == 1000
        assert ohlc['traded_dollar'] == 14000

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_db_daily_price_manager_get_ohlc_batch(monkeypatch, unit_test_db):
    # Create a proper test environment with the test database
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)

    # Set up the mocks
    mock_xrefs_dao = DummyXrefsDAO(env)
    mock_vendors_dao = DummyVendorsDAO(env)
    mock_prices_dao = DummyPricesDAO(env)

    # Patch the DAO constructors to return our mocks
    with patch('market_data.eod.db_daily_price_market_data_manager.InstrumentXrefsDAO', return_value=mock_xrefs_dao), \
         patch('core.dao.vendors_core.dao.VendorsDAO', return_value=mock_vendors_dao), \
         patch('market_data.eod.db_daily_price_market_data_manager.DailyPricesDAO', return_value=mock_prices_dao):

        # Initialize the manager
        mgr = DBDailyPriceMarketDataManager(env, symbols=['AAPL', 'TSLA'])

        # Get batch OHLC data
        batch = await mgr.get_ohlc_batch([1, 2], datetime(2025, 8, 8), datetime(2025, 8, 8))

        # Verify the data
        assert batch[1]['close'] == 14
        assert batch[2]['close'] == 24
        assert batch[1]['traded_dollar'] == 14000
        assert batch[2]['traded_dollar'] == 48000

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_db_daily_price_manager_missing_symbol(monkeypatch, unit_test_db):
    # Create a proper test environment with the test database
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)

    # Set up the mocks
    mock_xrefs_dao = DummyXrefsDAO(env)
    mock_vendors_dao = DummyVendorsDAO(env)
    mock_prices_dao = DummyPricesDAO(env)

    # Patch the DAO constructors to return our mocks
    with patch('market_data.eod.db_daily_price_market_data_manager.InstrumentXrefsDAO', return_value=mock_xrefs_dao), \
         patch('core.dao.vendors_core.dao.VendorsDAO', return_value=mock_vendors_dao), \
         patch('market_data.eod.db_daily_price_market_data_manager.DailyPricesDAO', return_value=mock_prices_dao):

        # Initialize the manager
        mgr = DBDailyPriceMarketDataManager(env, symbols=['AAPL'])

        # Try to get OHLC data for an unmapped instrument
        ohlc = await mgr.get_ohlc(999, datetime(2025, 8, 8), datetime(2025, 8, 8))

        # Should return None for unmapped instrument
        assert ohlc is None

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_db_daily_price_manager_missing_price(monkeypatch, unit_test_db):
    # Create a proper test environment with the test database
    env = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)

    # Set up the mocks
    mock_xrefs_dao = DummyXrefsDAO(env)
    mock_vendors_dao = DummyVendorsDAO(env)

    # Create a mock DAO that returns no prices
    class EmptyPricesDAO(DummyPricesDAO):
        async def list_prices_for_instruments_and_date(self, instrument_ids, as_of_date):
            return []

    mock_prices_dao = EmptyPricesDAO(env)

    # Patch the DAO constructors to return our mocks
    with patch('market_data.eod.db_daily_price_market_data_manager.InstrumentXrefsDAO', return_value=mock_xrefs_dao), \
         patch('core.dao.vendors_core.dao.VendorsDAO', return_value=mock_vendors_dao), \
         patch('market_data.eod.db_daily_price_market_data_manager.DailyPricesDAO', return_value=mock_prices_dao):

        # Initialize the manager
        mgr = DBDailyPriceMarketDataManager(env, symbols=['AAPL'])

        # Try to get OHLC data (should return None since no prices are available)
        ohlc = await mgr.get_ohlc(1, datetime(2025, 8, 8), datetime(2025, 8, 8))

        # Should return None when no prices are available
        assert ohlc is None
