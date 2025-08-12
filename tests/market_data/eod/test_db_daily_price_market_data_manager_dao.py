import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta

from market_data.eod.db_daily_price_market_data_manager import DBDailyPriceMarketDataManager
from dao.instrument_xrefs_dao import InstrumentXrefsDAO


class TestDBDailyPriceMarketDataManagerDAO:
    """Test that DBDailyPriceMarketDataManager correctly uses InstrumentXrefsDAO."""

    @pytest.fixture
    def mock_xrefs_dao(self):
        """Create a mock InstrumentXrefsDAO."""
        mock_dao = AsyncMock(spec=InstrumentXrefsDAO)
        mock_dao.resolve_instrument_id_by_symbol.return_value = 1
        mock_dao.get_symbol_by_instrument_id_vendor_name.return_value = "AAPL"
        return mock_dao

    @pytest.fixture
    def mock_env(self):
        """Create a mock Environment."""
        mock_env = MagicMock()
        mock_env.db_url = "postgresql://postgres:password@localhost:5432/test_db"
        return mock_env
        
    @pytest.fixture
    def mock_db_manager(self, mock_xrefs_dao, mock_env):
        """Create a mock DBDailyPriceMarketDataManager with mocked dependencies."""
        with patch('market_data.eod.db_daily_price_market_data_manager.DailyPricesDAO') as mock_prices_dao:
            # Create an AsyncMock for the prices_dao
            mock_prices_dao_instance = AsyncMock()
            mock_prices_dao.return_value = mock_prices_dao_instance
            
            manager = DBDailyPriceMarketDataManager(
                env=mock_env,
                symbols=["AAPL", "TSLA"]
            )
            # Replace the DAOs with our mocks
            manager.xrefs_dao = mock_xrefs_dao
            manager.prices_dao = mock_prices_dao_instance
            return manager

    @pytest.mark.asyncio
    async def test_resolve_instrument_id_uses_dao(self, mock_db_manager, mock_xrefs_dao):
        """Test that resolve_instrument_id uses the InstrumentXrefsDAO."""
        symbol = "AAPL"
        instrument_id = await mock_db_manager.resolve_instrument_id(symbol)
        
        # Verify DAO was called with correct parameters
        mock_xrefs_dao.resolve_instrument_id_by_symbol.assert_called_once_with(symbol)
        assert instrument_id == 1

    @pytest.mark.asyncio
    async def test_resolve_symbol_uses_dao(self, mock_db_manager, mock_xrefs_dao):
        """Test that resolve_symbol uses the InstrumentXrefsDAO."""
        instrument_id = 1
        symbol = await mock_db_manager.resolve_symbol(instrument_id)
        
        # Verify DAO was called with correct parameters
        mock_xrefs_dao.get_symbol_by_instrument_id_vendor_name.assert_called_once_with(
            instrument_id, vendor_name="ticker"
        )
        assert symbol == "AAPL"

    @pytest.mark.asyncio
    async def test_get_ohlc_resolves_symbol_with_dao(self, mock_db_manager, mock_xrefs_dao):
        """Test that get_ohlc resolves symbols using the InstrumentXrefsDAO."""
        # Setup mock data
        mock_db_manager.prices_dao.list_prices_for_instruments_and_date.return_value = [{
            'instrument_id': 1,
            'date': date(2025, 1, 1),
            'open': 150.0,
            'high': 155.0,
            'low': 148.0,
            'close': 152.0,
            'volume': 1000000
        }]
        
        # Call get_ohlc with an instrument ID
        start_date = datetime(2025, 1, 1)
        end_date = datetime(2025, 1, 10)
        result = await mock_db_manager.get_ohlc(1, start_date, end_date)
        
        # Verify DAO was used for symbol resolution
        mock_xrefs_dao.get_symbol_by_instrument_id_vendor_name.assert_called_once_with(1, vendor_name="ticker")
        
        # Verify price DAO was called with resolved instrument ID
        mock_db_manager.prices_dao.list_prices_for_instruments_and_date.assert_called_once_with(
            [1], start_date.date()
        )
        
        # Verify result
        assert result is not None
        assert result['close'] == 152.0

    @pytest.mark.asyncio
    async def test_get_ohlc_batch_resolves_symbols_with_dao(self, mock_db_manager, mock_xrefs_dao):
        """Test that get_ohlc_batch resolves symbols using the InstrumentXrefsDAO."""
        # Setup mock data for list_prices_for_instruments_and_date
        mock_db_manager.prices_dao.list_prices_for_instruments_and_date.return_value = [
            {
                'instrument_id': 1,
                'date': date(2025, 1, 1),
                'open': 150.0,
                'high': 155.0,
                'low': 148.0,
                'close': 152.0,
                'volume': 1000000
            },
            {
                'instrument_id': 2,
                'date': date(2025, 1, 1),
                'open': 800.0,
                'high': 850.0,
                'low': 780.0,
                'close': 820.0,
                'volume': 500000
            }
        ]
        
        # Call get_ohlc_batch with instrument IDs
        instrument_ids = [1, 2]
        start_date = datetime(2025, 1, 1)
        end_date = datetime(2025, 1, 10)
        result = await mock_db_manager.get_ohlc_batch(instrument_ids, start_date, end_date)
        
        # Verify price DAO was called with instrument IDs
        mock_db_manager.prices_dao.list_prices_for_instruments_and_date.assert_called_once_with(
            instrument_ids, start_date.date()
        )
        
        # Verify result contains data for both instruments
        assert 1 in result
        assert 2 in result
        assert result[1]['close'] == 152.0
        assert result[2]['close'] == 820.0
