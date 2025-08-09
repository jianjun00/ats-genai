import os
import json
print(f"[IMPORT_DEBUG] Loaded file_daily_price_market_data_manager.py from {__file__}")
from datetime import datetime, date, time
from typing import List, Dict, Optional, Any
from .base_daily_price_market_data_manager import BaseDailyPriceMarketDataManager
from state.instrument_interval import InstrumentInterval
from .unify_daily_prices import FileDailyPricesUnifier

from dao.instrument_xrefs_dao import InstrumentXrefsDAO
from config.environment import Environment

class FileDailyPriceMarketDataManager(BaseDailyPriceMarketDataManager):
    """
    Loads daily prices from file-based vendor directories and reconciles using FileDailyPricesUnifier.
    Accepts a dict of vendor -> directory with daily price request/response files.
    Example usage:
        vendors_dirs = {
            'polygon': 'tests/data/daily_prices_polygon',
            'tiingo': 'tests/data/daily_prices_tiingo'
        }
        mgr = await FileDailyPriceMarketDataManager.create_async(vendors_dirs, env)
    """
    def __init__(self, vendors_dirs: Dict[str, str], env: Environment, symbols: Optional[List[str]] = None):
        super().__init__(symbols)
        print(f"[DEBUG][FileDailyPriceMarketDataManager.__init__2] vendors_dirs={vendors_dirs}, symbols={symbols}")
        self.vendors_dirs = vendors_dirs
        self.env = env
        print(f"[DEBUG][FileDailyPriceMarketDataManager.__init__] _last_sod_date initialized to None, id(self)={id(self)}")

    @classmethod
    async def create_async(cls, vendors_dirs: Dict[str, str], env: Environment, symbols: Optional[List[str]] = None):
        print(f"[DEBUG][create_async] Called with vendors_dirs={vendors_dirs}, env={env}, symbols={symbols}")
        self = cls.__new__(cls)
        cls.__init__(self, vendors_dirs, env, symbols)
        await self._load_symbol_mappings()
        print(f"[DEBUG][_load_symbol_mappings] _symbol_to_id: {self._symbol_to_id}, _id_to_symbol: {self._id_to_symbol}")
        self._load_vendor_data()
        print(f"[DEBUG][_load_vendor_data] Loaded data for symbols: {list(self.vendor_data.keys())}")
        self.unifier = FileDailyPricesUnifier(
            environment=None,  # Not needed for file-based
            tiingo_data=self.vendor_data.get('tiingo', {}),
            polygon_data=self.vendor_data.get('polygon', {})
        )
        return self

    async def _load_symbol_mappings(self):
        print(f"[DEBUG][_load_symbol_mappings] Called. vendors_dirs={self.vendors_dirs}, symbols={self.symbols}")
        """
        Always use InstrumentXrefsDAO to resolve instrument IDs for each symbol.
        This is the only supported mapping method for correctness and testability.
        """
        xrefs_dao = InstrumentXrefsDAO(self.env)
        if self.symbols is None:
            found = set()
            for vendor, d in self.vendors_dirs.items():
                for fname in os.listdir(d):
                    if fname.endswith('_response.json'):
                        parts = fname.split('_')
                        symbol = parts[1].upper() if len(parts) > 1 else None
                        if symbol:
                            found.add(symbol)
            self.symbols = sorted(found)
        self._symbol_to_id = {}
        self._id_to_symbol = {}
        print(f"[DEBUG][_load_symbol_mappings] self.symbols before mapping: {self.symbols}")
        if not self.symbols:
            print(f"[DEBUG][_load_symbol_mappings] WARNING: self.symbols is empty, skipping mapping.")
        # Look up vendor_id for 'ticker' using VendorsDAO
        from dao.vendors_dao import VendorsDAO
        vendors_dao = VendorsDAO(self.env)
        vendor_row = await vendors_dao.get_vendor_by_name('ticker')
        ticker_vendor_id = vendor_row['id'] if vendor_row else None
        print(f"[DEBUG][_load_symbol_mappings] Using ticker_vendor_id={ticker_vendor_id}")
        for s in self.symbols:
            print(f"[DEBUG][_load_symbol_mappings] Attempting to resolve instrument_id for symbol: {s} with vendor_id={ticker_vendor_id}")
            instrument_id = await xrefs_dao.resolve_instrument_id_by_symbol(s)
            if instrument_id is not None:
                self._symbol_to_id[s] = instrument_id
                self._id_to_symbol[instrument_id] = s
            else:
                print(f"[DEBUG][_load_symbol_mappings] WARNING: Could not resolve instrument_id for symbol {s} and vendor_id={ticker_vendor_id}")
        print(f"[DEBUG][_load_symbol_mappings] Loaded symbols: {list(self._symbol_to_id.keys())}")
        print(f"[DEBUG][_load_symbol_mappings] FINAL MAPPINGS: symbol_to_id={self._symbol_to_id}, id_to_symbol={self._id_to_symbol}")

    def _load_vendor_data(self):
        print(f"[DEBUG][_load_vendor_data] Called. vendors_dirs={self.vendors_dirs}")
        print(f"[DEBUG][_load_vendor_data] vendors_dirs={self.vendors_dirs}")
        self.vendor_data = {}
        for vendor, d in self.vendors_dirs.items():
            print(f"[DEBUG][_load_vendor_data] Processing vendor={vendor}, dir={d}")
            data = {}
            files = os.listdir(d)
            print(f"[DEBUG][_load_vendor_data] Files in {d}: {files}")
            for fname in files:
                print(f"[DEBUG][_load_vendor_data] Checking file: {fname}")
                if fname.endswith('_response.json'):
                    print(f"[DEBUG][_load_vendor_data] Found response file: {fname}")
                    parts = fname.split('_')
                    symbol = parts[1].upper() if len(parts) > 1 else None
                    print(f"[DEBUG][_load_vendor_data] Parsed symbol: {symbol}")
                    # Print the date range in this file
                    try:
                        with open(os.path.join(d, fname), 'r') as f_tmp:
                            resp_tmp = json.load(f_tmp)
                            print(f"[DEBUG][_load_vendor_data] {fname}: type={type(resp_tmp)}, keys/len={list(resp_tmp.keys()) if isinstance(resp_tmp, dict) else len(resp_tmp) if isinstance(resp_tmp, list) else 'N/A'}")
                            dates = []
                            if isinstance(resp_tmp, dict) and 'results' in resp_tmp:
                                for row in resp_tmp.get('results', []):
                                    print(f"[DEBUG][_load_vendor_data] {fname}: row={row} (type={type(row)})")
                                    t_val = row.get('t')
                                    if t_val is not None:
                                        dt = datetime.utcfromtimestamp(t_val / 1000).date()
                                        dates.append(dt)
                            elif isinstance(resp_tmp, list):
                                for row in resp_tmp:
                                    print(f"[DEBUG][_load_vendor_data] {fname}: row={row} (type={type(row)})")
                                    t_val = row.get('t') if isinstance(row, dict) else None
                                    if t_val is not None:
                                        dt = datetime.utcfromtimestamp(t_val / 1000).date()
                                        dates.append(dt)
                                    elif isinstance(row, dict) and 'date' in row:
                                        try:
                                            dt = datetime.strptime(row['date'][:10], '%Y-%m-%d').date()
                                            dates.append(dt)
                                        except Exception as e:
                                            print(f"[DEBUG][_load_vendor_data] {fname}: Failed to parse date from {row['date']}: {e}")
                            else:
                                print(f"[DEBUG][_load_vendor_data] {fname}: UNSUPPORTED FORMAT for date debug")
                            if dates:
                                print(f"[DEBUG][_load_vendor_data] {fname}: {symbol} covers {min(dates)} to {max(dates)} ({len(dates)} bars)")
                            else:
                                print(f"[DEBUG][_load_vendor_data] {fname}: {symbol} has NO bars")
                    except Exception as e:
                        print(f"[DEBUG][_load_vendor_data] Error reading {fname} for date range: {e}")
                    if not symbol:
                        continue
                    fpath = os.path.join(d, fname)
                    with open(fpath) as f:
                        if vendor == 'polygon':
                            resp = json.load(f)
                            print(f"[DEBUG][_load_vendor_data] Loaded polygon JSON for {symbol}, {len(resp.get('results', []))} bars")
                            for row in resp.get('results', []):
                                t_val = row.get('t')
                                if t_val is not None:
                                    dt = datetime.utcfromtimestamp(t_val / 1000).date()
                                elif isinstance(t_val, str):
                                    try:
                                        dt = datetime.strptime(t_val[:10], '%Y-%m-%d').date()
                                    except Exception:
                                        dt = datetime.utcnow().date()  # fallback
                                elif isinstance(t_val, datetime):
                                    dt = t_val.date()
                                elif isinstance(t_val, date):
                                    dt = t_val
                                else:
                                    dt = datetime.utcnow().date()  # fallback
                                data.setdefault(symbol, {})[dt] = {
                                    'open': row['o'],
                                    'high': row['h'],
                                    'low': row['l'],
                                    'close': row['c'],
                                    'volume': row['v']
                                }
                        elif vendor == 'tiingo':
                            resp = json.load(f)
                            # Accept both dict (with 'bars') and list at top-level for tiingo
                            if isinstance(resp, dict) and 'bars' in resp:
                                rows = resp['bars']
                            elif isinstance(resp, list):
                                rows = resp
                            else:
                                print(f"[DEBUG][_load_vendor_data] Unexpected tiingo format in {fname}: {type(resp)}")
                                rows = []
                            for row in rows:
                                if not isinstance(row, dict):
                                    print(f"[DEBUG][_load_vendor_data] Skipping non-dict row: {row}")
                                    continue
                                # Accept either 'date' or 't' (for mixed or malformed test data)
                                dt = None
                                if 'date' in row:
                                    date_val = row['date']
                                    try:
                                        if isinstance(date_val, datetime):
                                            dt = date_val.date()
                                        elif isinstance(date_val, date):
                                            dt = date_val
                                        else:
                                            dt = datetime.strptime(str(date_val)[:10], '%Y-%m-%d').date()
                                    except Exception as e:
                                        print(f"[DEBUG][_load_vendor_data] Skipping row with malformed date: {row}, error: {e}")
                                        continue
                                elif 't' in row:
                                    t_val = row['t']
                                    try:
                                        dt = datetime.utcfromtimestamp(t_val / 1000).date()
                                    except Exception as e:
                                        print(f"[DEBUG][_load_vendor_data] Skipping row with malformed t: {row}, error: {e}")
                                        continue
                                else:
                                    print(f"[DEBUG][_load_vendor_data] Skipping row missing date/t: {row}")
                                    continue
                                # Defensive extraction of OHLCV fields
                                try:
                                    data.setdefault(symbol, {})[dt] = {
                                        'open': row.get('open'),
                                        'high': row.get('high'),
                                        'low': row.get('low'),
                                        'close': row.get('close'),
                                        'volume': row.get('volume')
                                    }
                                except Exception as e:
                                    print(f"[DEBUG][_load_vendor_data] Error extracting fields from row: {row}, error: {e}")
                                    continue
            self.vendor_data[vendor] = data

    def _get_all_symbols(self) -> List[str]:
        return self.symbols

    async def resolve_instrument_id(self, symbol: str) -> Optional[int]:
        xrefs_dao = InstrumentXrefsDAO(self.env)
        return await xrefs_dao.resolve_instrument_id_by_symbol(symbol)

    async def resolve_symbol(self, instrument_id: int) -> Optional[str]:
        xrefs_dao = InstrumentXrefsDAO(self.env)
        return await xrefs_dao.get_symbol_by_instrument_id_vendor_name(instrument_id, vendor_name="ticker")

    async def get_ohlc(self, instrument_id: int, start: datetime, end: datetime, current_date: Optional[date] = None) -> Optional[Dict[str, float]]:
        assert isinstance(instrument_id, int), f"instrument_id must be int, got {type(instrument_id)}: {instrument_id}"
        symbol = await self.resolve_symbol(instrument_id)
        print(f"[DEBUG][get_ohlc] Lookup instrument_id={instrument_id}, symbol={symbol}, date={start.date()} to {end.date()}")
        sod_date = current_date if current_date is not None else self._last_sod_date
        if sod_date is None:
            sod_date = start.date()  # fallback for legacy/test, but should be set
        results = self.unifier.unify_daily_prices_sync(symbol, start.date(), sod_date)
        if not results:
            print(f"[DEBUG][get_ohlc] No OHLC data for instrument_id={instrument_id} ({symbol}) at {start.date()}")
            return None
        print(f"[DEBUG][get_ohlc] Found OHLC for instrument_id={instrument_id} ({symbol}) at {start.date()}: {results}")
        close = results[0]['close']
        volume = results[0]['volume']
        traded_dollar = close * volume if close is not None and volume is not None else None
        return {
            'open': results[0]['open'],
            'high': results[0]['high'],
            'low': results[0]['low'],
            'close': close,
            'traded_volume': volume,
            'traded_dollar': traded_dollar,
        }


    def get_ohlc_batch(self, instrument_ids: List[int], start: datetime, end: datetime, current_date: Optional[date] = None) -> Dict[int, Optional[Dict[str, float]]]:
        print(f"[DEBUG][get_ohlc_batch] instrument_ids={instrument_ids}, date={start.date()} to {end.date()}")
        for iid in instrument_ids:
            assert isinstance(iid, int), f"instrument_ids must be ints, got {type(iid)}: {iid}"
        batch = {}
        for iid in instrument_ids:
            ohlc = self.get_ohlc(iid, start, end, current_date=current_date)
            print(f"[DEBUG][get_ohlc_batch] iid={iid}, ohlc={ohlc}")
            if ohlc:
                close = ohlc.get('close')
                volume = ohlc.get('traded_volume')
                print(f"[DEBUG][get_ohlc_batch] iid={iid}, close={close}, volume={volume}")
                traded_dollar = close * volume if close is not None and volume is not None else None
                print(f"[DEBUG][get_ohlc_batch] iid={iid}, traded_dollar={traded_dollar}")
                ohlc['traded_dollar'] = traded_dollar
            else:
                print(f"[DEBUG][get_ohlc_batch] iid={iid}, ohlc is None")
            batch[iid] = ohlc
        print(f"[DEBUG][get_ohlc_batch] batch result: {batch}")
        return batch

    # Synchronous wrapper for FileDailyPricesUnifier
    # (for integration with existing code expecting sync get_ohlc)
    def unify_daily_prices_sync(self, symbol, asof, current_date: Optional[date] = None):
        import asyncio
        sod_date = current_date if current_date is not None else self._last_sod_date
        if sod_date is None:
            # fallback for legacy/test
            if isinstance(asof, (tuple, list)):
                sod_date = asof[-1] if asof else None
            else:
                sod_date = asof
        coro = self.unifier.unify_daily_prices(symbol, asof, sod_date)
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

