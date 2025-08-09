import pytest
import asyncio
from datetime import datetime, date
from unittest.mock import AsyncMock, patch

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../src')))

from market_data.eod.db_daily_price_market_data_manager import DBDailyPriceMarketDataManager
from config.environment import Environment

class DummyXrefsDAO:
    def __init__(self, env):
        self.env = env
    async def resolve_instrument_id(self, symbol):
        return {'AAPL': 1, 'TSLA': 2}.get(symbol.upper())

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
async def test_db_daily_price_manager_symbol_mapping(monkeypatch):
    env = Environment()
    monkeypatch.setattr('market_data.eod.db_daily_price_market_data_manager.InstrumentXrefsDAO', DummyXrefsDAO)
    monkeypatch.setattr('market_data.eod.db_daily_price_market_data_manager.DailyPricesDAO', DummyPricesDAO)
    mgr = DBDailyPriceMarketDataManager(env, symbols=['AAPL', 'TSLA'])
    await mgr._load_symbol_mappings()
    assert mgr.resolve_instrument_id('AAPL') == 1
    assert mgr.resolve_instrument_id('TSLA') == 2
    assert mgr.resolve_symbol(1) == 'AAPL'
    assert mgr.resolve_symbol(2) == 'TSLA'

@pytest.mark.asyncio
async def test_db_daily_price_manager_get_ohlc(monkeypatch):
    env = Environment()
    monkeypatch.setattr('market_data.eod.db_daily_price_market_data_manager.InstrumentXrefsDAO', DummyXrefsDAO)
    monkeypatch.setattr('market_data.eod.db_daily_price_market_data_manager.DailyPricesDAO', DummyPricesDAO)
    mgr = DBDailyPriceMarketDataManager(env, symbols=['AAPL'])
    await mgr._load_symbol_mappings()
    ohlc = await mgr.get_ohlc(1, datetime(2025, 8, 8), datetime(2025, 8, 8))
    assert ohlc['open'] == 10
    assert ohlc['high'] == 15
    assert ohlc['low'] == 9
    assert ohlc['close'] == 14
    assert ohlc['traded_volume'] == 1000
    assert ohlc['traded_dollar'] == 14000

@pytest.mark.asyncio
async def test_db_daily_price_manager_get_ohlc_batch(monkeypatch):
    env = Environment()
    monkeypatch.setattr('market_data.eod.db_daily_price_market_data_manager.InstrumentXrefsDAO', DummyXrefsDAO)
    monkeypatch.setattr('market_data.eod.db_daily_price_market_data_manager.DailyPricesDAO', DummyPricesDAO)
    mgr = DBDailyPriceMarketDataManager(env, symbols=['AAPL', 'TSLA'])
    await mgr._load_symbol_mappings()
    batch = await mgr.get_ohlc_batch([1, 2], datetime(2025, 8, 8), datetime(2025, 8, 8))
    assert batch[1]['close'] == 14
    assert batch[2]['close'] == 24
    assert batch[1]['traded_dollar'] == 14000
    assert batch[2]['traded_dollar'] == 48000

@pytest.mark.asyncio
async def test_db_daily_price_manager_missing_symbol(monkeypatch):
    env = Environment()
    monkeypatch.setattr('market_data.eod.db_daily_price_market_data_manager.InstrumentXrefsDAO', DummyXrefsDAO)
    monkeypatch.setattr('market_data.eod.db_daily_price_market_data_manager.DailyPricesDAO', DummyPricesDAO)
    mgr = DBDailyPriceMarketDataManager(env, symbols=['AAPL'])
    await mgr._load_symbol_mappings()
    # 999 is not mapped
    ohlc = await mgr.get_ohlc(999, datetime(2025, 8, 8), datetime(2025, 8, 8))
    assert ohlc is None

@pytest.mark.asyncio
async def test_db_daily_price_manager_missing_price(monkeypatch):
    env = Environment()
    monkeypatch.setattr('market_data.eod.db_daily_price_market_data_manager.InstrumentXrefsDAO', DummyXrefsDAO)
    class EmptyPricesDAO(DummyPricesDAO):
        async def list_prices_for_instruments_and_date(self, instrument_ids, as_of_date):
            return []
    monkeypatch.setattr('market_data.eod.db_daily_price_market_data_manager.DailyPricesDAO', EmptyPricesDAO)
    mgr = DBDailyPriceMarketDataManager(env, symbols=['AAPL'])
    await mgr._load_symbol_mappings()
    ohlc = await mgr.get_ohlc(1, datetime(2025, 8, 8), datetime(2025, 8, 8))
    assert ohlc is None
