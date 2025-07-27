import pytest
from datetime import date, datetime
from market_data.daily_price_market_data_manager import DailyPriceMarketDataManager
from config.environment import Environment

class DummyDAO:
    def __init__(self, prices):
        self._prices = prices
        self._calls = []
    async def list_prices_for_symbols_and_date(self, symbols, as_of_date):
        self._calls.append((tuple(symbols), as_of_date))
        # Return dummy rows for each symbol
        return [
            {
                'symbol': symbol,
                'open': 10.0,
                'high': 12.0,
                'low': 9.5,
                'close': 11.0,
                'volume': 1000
            }
            for symbol in symbols
        ]
    async def list_prices_for_instruments_and_date(self, instrument_ids, as_of_date):
        # Return dummy rows for each instrument_id, mapping 1->AAPL, 2->TSLA for test
        symbol_map = {1: 'AAPL', 2: 'TSLA'}
        return [
            {
                'instrument_id': iid,
                'symbol': symbol_map.get(iid, f'SYM{iid}'),
                'open': 10.0,
                'high': 12.0,
                'low': 9.5,
                'close': 11.0,
                'volume': 1000
            }
            for iid in instrument_ids
        ]

import pytest

@pytest.mark.asyncio
async def test_update_for_sod_and_get_ohlc(monkeypatch):
    env = Environment()
    # Patch DAO and calendar
    prices = {}
    manager = DailyPriceMarketDataManager(env)
    monkeypatch.setattr(manager, 'dao', DummyDAO(prices))
    monkeypatch.setattr(manager, '_get_all_symbols', lambda: ['AAPL', 'TSLA'])
    async def fake_resolve_instrument_id(symbol, vendor_id=None, at_date=None):
        return 1 if symbol == 'AAPL' else 2
    monkeypatch.setattr(manager.instrument_xref_dao, 'resolve_instrument_id', fake_resolve_instrument_id)
    monkeypatch.setattr(manager, '_get_exchange_open_close', lambda d: (
        datetime(2024, 1, 2, 9, 30), datetime(2024, 1, 2, 16, 0)))
    # SOD
    await manager.update_for_sod(None, datetime(2024, 1, 2, 9, 30))
    # Check intervals
    for iid in [1, 2]:
        ohlc = manager.get_ohlc(iid, datetime(2024, 1, 2, 9, 30), datetime(2024, 1, 2, 16, 0))
        assert ohlc['open'] == 10.0
        assert ohlc['high'] == 12.0
        assert ohlc['low'] == 9.5
        assert ohlc['close'] == 11.0
        assert ohlc['volume'] == 1000
        assert ohlc['traded_dollar'] == 11000.0
    # EOD clears intervals
    await manager.update_for_eod(None, datetime(2024, 1, 2, 16, 0))
    assert manager._intervals == {}
