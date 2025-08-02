import os
import json
from datetime import datetime, date, time
from typing import List, Dict, Optional, Any
from market_data.market_data_manager import MarketDataManager
from state.instrument_interval import InstrumentInterval
from .unify_daily_prices import FileDailyPricesUnifier

class FileDailyPriceMarketDataManager(MarketDataManager):
    """
    Loads daily prices from file-based vendor directories and reconciles using FileDailyPricesUnifier.
    Accepts a dict of vendor -> directory with daily price request/response files.
    Example usage:
        vendors_dirs = {
            'polygon': 'tests/data/daily_prices_polygon',
            'tiingo': 'tests/data/daily_prices_tiingo'
        }
        mgr = FileDailyPriceMarketDataManager(vendors_dirs)
    """
    def __init__(self, vendors_dirs: Dict[str, str], symbols: Optional[List[str]] = None):
        self.vendors_dirs = vendors_dirs
        self.symbols = symbols  # If None, will infer from files
        self._intervals: Dict[int, InstrumentInterval] = {}
        self._last_prices: Dict[int, Dict[str, float]] = {}
        self._symbol_to_id: Dict[str, int] = {}
        self._id_to_symbol: Dict[int, str] = {}
        self._load_symbol_mappings()
        self._load_vendor_data()
        self.unifier = FileDailyPricesUnifier(
            environment=None,  # Not needed for file-based
            tiingo_data=self.vendor_data.get('tiingo', {}),
            polygon_data=self.vendor_data.get('polygon', {})
        )
    def _load_symbol_mappings(self):
        # For now, assign instrument_id as 1, 2, ... for each symbol
        if self.symbols is None:
            # Infer symbols from files (look for *_response.json)
            found = set()
            for vendor, d in self.vendors_dirs.items():
                for fname in os.listdir(d):
                    if fname.endswith('_response.json'):
                        parts = fname.split('_')
                        symbol = parts[1].upper() if len(parts) > 1 else None
                        if symbol:
                            found.add(symbol)
            self.symbols = sorted(found)
        self._symbol_to_id = {s: i+1 for i, s in enumerate(self.symbols)}
        self._id_to_symbol = {i+1: s for i, s in enumerate(self.symbols)}

    def _load_vendor_data(self):
        self.vendor_data = {}
        for vendor, d in self.vendors_dirs.items():
            data = {}
            for fname in os.listdir(d):
                if fname.endswith('_response.json'):
                    parts = fname.split('_')
                    symbol = parts[1].upper() if len(parts) > 1 else None
                    if not symbol:
                        continue
                    fpath = os.path.join(d, fname)
                    with open(fpath) as f:
                        if vendor == 'polygon':
                            resp = json.load(f)
                            # Polygon format: { 'results': [ {o,h,l,c,v,t,...}, ... ] }
                            for row in resp.get('results', []):
                                dt = datetime.utcfromtimestamp(row['t']/1000).date()
                                data.setdefault(symbol, {})[dt] = {
                                    'open': row['o'],
                                    'high': row['h'],
                                    'low': row['l'],
                                    'close': row['c'],
                                    'volume': row['v']
                                }
                        elif vendor == 'tiingo':
                            resp = json.load(f)
                            # Tiingo format: [ { 'date': ..., 'open':..., ... }, ... ]
                            for row in resp:
                                dt = datetime.strptime(row['date'][:10], '%Y-%m-%d').date()
                                data.setdefault(symbol, {})[dt] = {
                                    'open': row['open'],
                                    'high': row['high'],
                                    'low': row['low'],
                                    'close': row['close'],
                                    'volume': row['volume']
                                }
            self.vendor_data[vendor] = data

    def _get_all_symbols(self) -> List[str]:
        return self.symbols

    def resolve_instrument_id(self, symbol: str) -> Optional[int]:
        return self._symbol_to_id.get(symbol.upper())

    def resolve_symbol(self, instrument_id: int) -> Optional[str]:
        return self._id_to_symbol.get(instrument_id)

    def get_ohlc(self, instrument_id: int, start: datetime, end: datetime) -> Optional[Dict[str, float]]:
        symbol = self.resolve_symbol(instrument_id)
        if not symbol:
            return None
        # Use reconciled result for the date
        dt = start.date()
        # Use FileDailyPricesUnifier to get reconciled price
        results = self.unifier.unify_daily_prices_sync(symbol, dt)
        if not results:
            return None
        row = results[0]
        return {
            'open': row.get('open'),
            'high': row.get('high'),
            'low': row.get('low'),
            'close': row.get('close'),
            'volume': row.get('volume'),
            'traded_dollar': row.get('close', 0.0) * row.get('volume', 0.0)
        }

    def get_ohlc_batch(self, instrument_ids: List[int], start: datetime, end: datetime) -> Dict[int, Optional[Dict[str, float]]]:
        return {iid: self.get_ohlc(iid, start, end) for iid in instrument_ids}

    # Synchronous wrapper for FileDailyPricesUnifier
    # (for integration with existing code expecting sync get_ohlc)
    def unify_daily_prices_sync(self, symbol, asof):
        import asyncio
        coro = self.unifier.unify_daily_prices(symbol, asof)
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        if loop.is_running():
            # Running inside an event loop (e.g., pytest-asyncio)
            import nest_asyncio
            nest_asyncio.apply()
            fut = asyncio.ensure_future(coro)
            return loop.run_until_complete(fut)
        else:
            return loop.run_until_complete(coro)
