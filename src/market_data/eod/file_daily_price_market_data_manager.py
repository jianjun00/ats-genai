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
        print("\n" + "="*80)
        print("STARTING _load_vendor_data")
        print(f"[DEBUG][_load_vendor_data] Called. vendors_dirs={self.vendors_dirs}, symbols={self.symbols}")
        print(f"[DEBUG][_load_vendor_data] Current working directory: {os.getcwd()}")
        print(f"[DEBUG][_load_vendor_data] Full PYTHONPATH: {os.environ.get('PYTHONPATH', 'Not set')}")
        
        # Print environment variables that might affect file loading
        print("\n[DEBUG] Environment variables:")
        for var in ['PWD', 'HOME', 'VIRTUAL_ENV', 'CONDA_PREFIX']:
            print(f"  {var}: {os.environ.get(var, 'Not set')}")
            
        self.vendor_data = {}
        
        # First, collect all available files and their symbols
        vendor_files = {}
        for vendor, d in self.vendors_dirs.items():
            print("\n" + "-"*60)
            print(f"[DEBUG][_load_vendor_data] Processing vendor: {vendor}, directory: {d}")
            
            # Check if directory exists and is accessible
            if not os.path.exists(d):
                print(f"[ERROR] Directory does not exist: {d}")
                # Try to find the directory relative to the project root
                project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
                alt_path = os.path.join(project_root, d)
                if os.path.exists(alt_path):
                    print(f"[DEBUG] Found alternative path: {alt_path}")
                    d = alt_path
                    self.vendors_dirs[vendor] = d  # Update the path for future use
                else:
                    print(f"[ERROR] Could not find directory {d} in any location")
                    continue
            
            # Resolve any relative paths to absolute paths
            abs_d = os.path.abspath(d)
            print(f"[DEBUG][_load_vendor_data] Resolved path for {vendor}: {abs_d}")
            
            # Check directory existence and permissions
            if not os.path.exists(abs_d):
                print(f"[ERROR] Directory does not exist: {abs_d}")
                # Try to find the directory in the project structure
                project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
                possible_paths = [
                    os.path.join(project_root, 'tests', 'data', f'daily_prices_{vendor}'),
                    os.path.join(project_root, 'data', f'daily_prices_{vendor}'),
                    os.path.join(project_root, f'daily_prices_{vendor}')
                ]
                
                found = False
                for path in possible_paths:
                    if os.path.exists(path):
                        print(f"[DEBUG] Found alternative path: {path}")
                        abs_d = path
                        self.vendors_dirs[vendor] = abs_d
                        found = True
                        break
                        
                if not found:
                    print(f"[ERROR] Could not find directory for {vendor} in any location")
                    print("Searched in:", possible_paths)
                    continue
                
            # Initialize vendor data structure
            self.vendor_data[vendor] = {}
            vendor_files[vendor] = {}
            
            # List directory contents
            try:
                dir_contents = os.listdir(abs_d)
                print(f"[DEBUG] Directory contents of {abs_d}:")
                for item in dir_contents:
                    item_path = os.path.join(abs_d, item)
                    print(f"  - {item} (file: {os.path.isfile(item_path)}, dir: {os.path.isdir(item_path)})")
            except Exception as e:
                print(f"[ERROR] Failed to list directory {abs_d}: {e}\nPermissions: {os.access(abs_d, os.R_OK)}, Exists: {os.path.exists(abs_d)}")
                continue
                
            # Process each file in the directory
            for f in os.listdir(abs_d):
                if not f.endswith('.json'):
                    print(f"[DEBUG] Skipping non-JSON file: {f}")
                    continue
                    
                file_path = os.path.join(abs_d, f)
                print(f"\n[DEBUG] Processing file: {file_path}")
                print(f"[DEBUG] File exists: {os.path.exists(file_path)}")
                print(f"[DEBUG] File readable: {os.access(file_path, os.R_OK)}")
                print(f"[DEBUG] File size: {os.path.getsize(file_path) if os.path.exists(file_path) else 0} bytes")
                
                # Extract symbol from filename
                try:
                    if vendor == 'tiingo':
                        # Handle both patterns:
                        # 1. tiingo_aapl_response.json -> aapl
                        # 2. tiingo_aapl_2025-01-01_2025-01-07_response.json -> aapl
                        parts = f.split('_')
                        print(f"[DEBUG] Processing Tiingo file: {f}, parts: {parts}")
                        if len(parts) < 3:  # Need at least vendor_symbol_response.json
                            print(f"[WARNING][_load_vendor_data] Invalid filename format for {vendor}: {f}")
                            continue
                        # The symbol is always the second part
                        symbol = parts[1].upper()
                        # Clean up any .json suffix if present (for the case of vendor_symbol.json)
                        symbol = symbol.split('.')[0]
                        print(f"[DEBUG] Extracted symbol {symbol} from {f}")
                        
                        # Store the file with its symbol
                        if symbol not in vendor_files[vendor]:
                            vendor_files[vendor][symbol] = []
                        vendor_files[vendor][symbol].append((f, file_path))
                        print(f"[DEBUG] Added {f} to {vendor} files for symbol {symbol}")
                        
                    elif vendor == 'polygon':
                        # polygon_tsla_response.json -> tsla
                        parts = f.split('_')
                        print(f"[DEBUG] Processing Polygon file: {f}, parts: {parts}")
                        if len(parts) < 3:  # Need at least vendor_symbol_response.json
                            print(f"[WARNING][_load_vendor_data] Invalid filename format for {vendor}: {f}")
                            continue
                        symbol = parts[1].upper()
                        # Clean up any .json suffix if present
                        symbol = symbol.split('.')[0]
                        print(f"[DEBUG] Extracted symbol {symbol} from {f}")
                        
                        # Store the file with its symbol
                        if symbol not in vendor_files[vendor]:
                            vendor_files[vendor][symbol] = []
                        vendor_files[vendor][symbol].append((f, file_path))
                        print(f"[DEBUG] Added {f} to {vendor} files for symbol {symbol}")
                    else:
                        print(f"[WARNING][_load_vendor_data] Unknown vendor: {vendor}")
                        continue
                        
                except Exception as e:
                    print(f"[ERROR] Error extracting symbol from {f}: {e}")
                    print(f"[DEBUG] Vendor: {vendor}, Filename: {f}")
                    continue
                
          # Process each vendor's files
        for vendor, symbol_files in vendor_files.items():
            print(f"\n{'='*60}")
            print(f"[DEBUG][_load_vendor_data] Processing {sum(len(files) for files in symbol_files.values())} files for vendor: {vendor}")
            print(f"[DEBUG][_load_vendor_data] Symbols found: {list(symbol_files.keys())}")
            data = {}
            
            # Process each symbol's files
            for symbol, files in symbol_files.items():
                print(f"\n[DEBUG][_load_vendor_data] Processing {len(files)} files for symbol {symbol}")
                
                for fname, fpath in files:
                    print(f"[DEBUG][_load_vendor_data] Processing {vendor} file: {fname}")
                    print(f"[DEBUG][_load_vendor_data] Full path: {fpath}")
                    print(f"[DEBUG][_load_vendor_data] File exists: {os.path.exists(fpath)}")
                    print(f"[DEBUG][_load_vendor_data] File readable: {os.access(fpath, os.R_OK)}")
                
                try:
                    print(f"[DEBUG][_load_vendor_data] Opening file: {fpath}")
                    with open(fpath, 'r') as f:
                        file_content = f.read()
                        print(f"[DEBUG][_load_vendor_data] Raw file content (first 500 chars):\n{file_content[:500]}...")
                        
                        try:
                            resp = json.loads(file_content)
                            print(f"[DEBUG][_load_vendor_data] Successfully parsed JSON for {vendor} {symbol}")
                            print(f"[DEBUG][_load_vendor_data] JSON type: {type(resp)}")
                            if isinstance(resp, dict):
                                print(f"[DEBUG][_load_vendor_data] JSON keys: {list(resp.keys())}")
                            elif isinstance(resp, list):
                                print(f"[DEBUG][_load_vendor_data] JSON list length: {len(resp)}")
                                if len(resp) > 0:
                                    print(f"[DEBUG][_load_vendor_data] First item type: {type(resp[0])}")
                                    if isinstance(resp[0], dict):
                                        print(f"[DEBUG][_load_vendor_data] First item keys: {list(resp[0].keys())}")
                        except json.JSONDecodeError as je:
                            print(f"[ERROR][_load_vendor_data] Failed to parse JSON from {fpath}: {je}")
                            print(f"[ERROR][_load_vendor_data] File content (first 1000 chars):\n{file_content[:1000]}")
                            raise
                        
                        if vendor == 'polygon':
                            rows = resp.get('results', [])
                            print(f"[DEBUG][_load_vendor_data] Found {len(rows)} price bars in {fname}")
                            
                            for i, row in enumerate(rows, 1):
                                if not isinstance(row, dict):
                                    print(f"[DEBUG][_load_vendor_data] Skipping non-dict row {i} in {fname}")
                                    continue
                                    
                                t_val = row.get('t')
                                if t_val is not None:
                                    try:
                                        dt = datetime.utcfromtimestamp(t_val / 1000).date()
                                        print(f"[DEBUG][_load_vendor_data] Processed row {i}: date={dt}, ohlc=({row.get('o')}, {row.get('h')}, {row.get('l')}, {row.get('c')})")
                                        
                                        data.setdefault(symbol, {})[dt] = {
                                            'open': row.get('o'),
                                            'high': row.get('h'),
                                            'low': row.get('l'),
                                            'close': row.get('c'),
                                            'volume': row.get('v')
                                        }
                                    except Exception as e:
                                        print(f"[DEBUG][_load_vendor_data] Error processing row {i} in {fname}: {e}"
                                              f"\nRow data: {row}")
                                else:
                                    print(f"[DEBUG][_load_vendor_data] Skipping row {i} with no timestamp in {fname}")
                            
                        elif vendor == 'tiingo':
                            rows = resp if isinstance(resp, list) else resp.get('bars', []) if isinstance(resp, dict) else []
                            print(f"[DEBUG][_load_vendor_data] Found {len(rows)} price bars in {fname}")
                            
                            for i, row in enumerate(rows, 1):
                                if not isinstance(row, dict):
                                    print(f"[DEBUG][_load_vendor_data] Skipping non-dict row {i} in {fname}")
                                    continue
                                    
                                dt = None
                                if 'date' in row:
                                    date_val = row['date']
                                    try:
                                        if isinstance(date_val, (datetime, date)):
                                            dt = date_val.date() if hasattr(date_val, 'date') else date_val
                                        else:
                                            dt = datetime.strptime(str(date_val)[:10], '%Y-%m-%d').date()
                                    except Exception as e:
                                        print(f"[DEBUG][_load_vendor_data] Error parsing date in row {i}: {e}"
                                              f"\nDate value: {date_val}, type: {type(date_val)}")
                                        continue
                                elif 't' in row:
                                    try:
                                        dt = datetime.utcfromtimestamp(row['t'] / 1000).date()
                                    except Exception as e:
                                        print(f"[DEBUG][_load_vendor_data] Error parsing timestamp in row {i}: {e}"
                                              f"\nTimestamp value: {row['t']}")
                                        continue
                                
                                if dt is not None:
                                    print(f"[DEBUG][_load_vendor_data] Processed row {i}: date={dt}, ohlc=({row.get('open')}, {row.get('high')}, {row.get('low')}, {row.get('close')})")
                                    
                                    data.setdefault(symbol, {})[dt] = {
                                        'open': row.get('open'),
                                        'high': row.get('high'),
                                        'low': row.get('low'),
                                        'close': row.get('close'),
                                        'volume': row.get('volume')
                                    }
                                else:
                                    print(f"[DEBUG][_load_vendor_data] Skipping row {i} with no valid date in {fname}")
                
                except Exception as e:
                    print(f"[DEBUG][_load_vendor_data] Error processing {vendor} file {fname}: {e}")
                    import traceback
                    traceback.print_exc()
            
            # Log summary for this vendor
            self.vendor_data[vendor] = data
            print(f"[DEBUG][_load_vendor_data] Loaded data for {len(data)} symbols from {vendor}")
            for sym, prices in data.items():
                if prices:
                    dates = sorted(prices.keys())
                    print(f"[DEBUG][_load_vendor_data]   {sym}: {len(prices)} price bars from {dates[0]} to {dates[-1]}")
                else:
                    print(f"[DEBUG][_load_vendor_data]   {sym}: NO PRICE BARS LOADED")

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
        
        if not symbol:
            print(f"[ERROR][get_ohlc] Could not resolve instrument_id {instrument_id} to a symbol")
            return None
            
        # Debug: Print available vendor data for this symbol
        print(f"[DEBUG][get_ohlc] Available vendor data for {symbol}:")
        for vendor, data in self.vendor_data.items():
            if symbol in data:
                dates = list(data[symbol].keys())
                print(f"  - {vendor}: {len(dates)} dates from {min(dates) if dates else 'N/A'} to {max(dates) if dates else 'N/A'}")
                
                # Print data for the requested date range
                for dt, ohlcv in data[symbol].items():
                    if start.date() <= dt <= end.date():
                        print(f"    - {dt}: {ohlcv}")
            else:
                print(f"  - {vendor}: No data for {symbol}")
        
        # Debug: Print all available symbols and vendors
        print(f"[DEBUG][get_ohlc] Vendor data keys: {list(self.vendor_data.keys())}")
        for vendor, data in self.vendor_data.items():
            print(f"[DEBUG][get_ohlc] Vendor {vendor} has symbols: {list(data.keys())}")
        
        # Get OHLC data from all vendors
        all_ohlc = []
        for vendor, data in self.vendor_data.items():
            if symbol in data:
                print(f"[DEBUG][get_ohlc] Found data for symbol {symbol} in vendor {vendor}")
                print(f"[DEBUG][get_ohlc] Available dates for {symbol} in {vendor}: {sorted(data[symbol].keys())}")
                
                # Find the closest date to the requested range
                available_dates = sorted(data[symbol].keys())
                if not available_dates:
                    print(f"[DEBUG][get_ohlc] No dates available for {symbol} in {vendor}")
                    continue
                    
                # Find the first date that's on or after the start date
                matching_dates = [d for d in available_dates if d >= start.date() and d <= end.date()]
                
                if not matching_dates:
                    # If no exact matches, find the closest date before the end date
                    matching_dates = [d for d in available_dates if d <= end.date()]
                    if matching_dates:
                        print(f"[DEBUG][get_ohlc] No exact date match, using closest date {matching_dates[-1]} before end date")
                
                for dt in matching_dates[:1]:  # Just take the first matching date
                    try:
                        ohlcv = data[symbol][dt]
                        print(f"[DEBUG][get_ohlc] Using data for {symbol} on {dt}: {ohlcv}")
                        all_ohlc.append({
                            'date': dt,
                            'open': ohlcv['open'],
                            'high': ohlcv['high'],
                            'low': ohlcv['low'],
                            'close': ohlcv['close'],
                            'volume': ohlcv.get('volume', 0),
                            'vendor': vendor
                        })
                    except Exception as e:
                        print(f"[ERROR][get_ohlc] Error processing data for {symbol} on {dt}: {e}\nData: {ohlcv}")
        
        if not all_ohlc:
            print(f"[WARNING][get_ohlc] No OHLC data found for symbol {symbol} in date range {start.date()} to {end.date()}")
            print(f"[DEBUG][get_ohlc] Available symbols in vendor_data: {list(self.vendor_data.get('tiingo', {}).keys()) + list(self.vendor_data.get('polygon', {}).keys())}")
            
            # Print available date ranges for the symbol if found in any vendor
            for vendor, data in self.vendor_data.items():
                if symbol in data:
                    dates = sorted(data[symbol].keys())
                    print(f"[DEBUG][get_ohlc] Available dates for {symbol} in {vendor}: {dates}")
                    
                    # Print first few data points for debugging
                    print(f"[DEBUG][get_ohlc] Sample data for {symbol} in {vendor}:")
                    for i, (dt, ohlcv) in enumerate(list(data[symbol].items())[:3]):
                        print(f"  {dt}: {ohlcv}")
            return None
        
        # Sort by date and get the most recent data point
        all_ohlc.sort(key=lambda x: x['date'])
        result = all_ohlc[-1]  # Get the most recent data point
        
        # Convert to the expected format
        ohlc = {
            'open': result['open'],
            'high': result['high'],
            'low': result['low'],
            'close': result['close'],
            'traded_volume': result['volume'],
            'traded_dollar': result['close'] * result['volume'] if result['volume'] is not None and result['close'] is not None else None
        }
        
        print(f"[DEBUG][get_ohlc] Returning OHLC data for {symbol} on {result['date']}: {ohlc}")
        return ohlc
        for i, result in enumerate(results):
            print(f"[DEBUG][get_ohlc] Result {i}: {result}")
            
        if not results[0] or 'close' not in results[0] or results[0]['close'] is None:
            print(f"[DEBUG][get_ohlc] WARNING: First result is missing close price: {results[0]}")
            return None
            
        close = results[0]['close']
        volume = results[0].get('volume', 0)
        traded_dollar = close * volume if close is not None and volume is not None else None
        
        print(f"[DEBUG][get_ohlc] Returning OHLC for {symbol} at {start.date()}: open={results[0]['open']}, high={results[0]['high']}, low={results[0]['low']}, close={close}, volume={volume}")
        
        return {
            'open': results[0]['open'],
            'high': results[0]['high'],
            'low': results[0]['low'],
            'close': close,
            'traded_volume': volume,
            'traded_dollar': traded_dollar,
        }


    async def get_ohlc_batch(self, instrument_ids: List[int], start: datetime, end: datetime, current_date: Optional[date] = None) -> Dict[int, Optional[Dict[str, float]]]:
        print(f"[DEBUG][get_ohlc_batch] instrument_ids={instrument_ids}, date={start.date()} to {end.date()}")
        for iid in instrument_ids:
            assert isinstance(iid, int), f"instrument_ids must be ints, got {type(iid)}: {iid}"
        batch = {}
        for iid in instrument_ids:
            ohlc = await self.get_ohlc(iid, start, end, current_date=current_date)
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
        print(f"[DEBUG][unify_daily_prices_sync] Calling unifier for symbol={symbol}, asof={asof}, current_date={current_date}")
        result = self.unifier.unify_daily_prices_sync(symbol, asof, current_date)
        print(f"[DEBUG][unify_daily_prices_sync] Unifier result for {symbol}: "
              f"{len(result) if result else 0} records, sample: {result[0] if result else 'None'}")
        return result
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

