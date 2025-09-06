import asyncpg
import numpy as np
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
from core.config.environment import Environment

load_dotenv()

CLOSE_THRESHOLD = 0.01

class DailyPricesUnifierBase:
    def __init__(self, environment):
        self.environment = environment
        self.close_history = []  # rolling window of closes

    def close_enough(self, a, b):
        if a is None or b is None:
            return False
        return abs(a - b) <= max(abs(a), abs(b)) * CLOSE_THRESHOLD

    def validate_row(self, row):
        msgs = []
        vals = {k: row.get(k) for k in ['open','high','low','close']}
        for k, v in vals.items():
            try:
                if v is None or float(v) <= 0:
                    msgs.append(f"{k} <= 0")
            except Exception:
                msgs.append(f"{k} invalid: {v}")
        try:
            h, l, c, o = map(float, [row.get('high'), row.get('low'), row.get('close'), row.get('open')])
            if not (h >= l and h >= c and h >= o):
                msgs.append(f"high < one of low/close/open")
            if not (l <= h and l <= c and l <= o):
                msgs.append(f"low > one of high/close/open")
        except Exception:
            msgs.append(f"HLCO comparison failed")
        return msgs

    def validate_date(self, dt_obj, current_date):
        msgs = []
        try:
            # Patch: compare only date part if either is datetime
            d1 = dt_obj.date() if hasattr(dt_obj, 'date') else dt_obj
            d2 = current_date.date() if hasattr(current_date, 'date') else current_date
            if d1 != d2:
                from logging import getLogger
                logger = getLogger(__name__)
                logger.warning(f"validate_date: date {dt_obj} does not match tracked current_date {current_date}")
                msgs.append(f"date {dt_obj} does not match tracked current_date {current_date}")
        except Exception as e:
            msgs.append(f"date validation failed: {str(e)}")
        finally:
            return msgs

    def validate_sigma(self, close):
        msgs = []
        if len(self.close_history) >= 20:
            arr = np.array(self.close_history[-20:])
            mu = np.mean(arr)
            sigma = np.std(arr)
            try:
                c = float(close)
                if sigma > 0 and (abs(c - mu) > 6 * sigma):
                    msgs.append(f"close {c} outlier: mu={mu:.2f}, sigma={sigma:.2f}")
            except Exception:
                msgs.append("close sigma check failed")
        return msgs

    def validate_diff(self, t, p):
        msgs = []
        for k in ['open','high','low','close','volume']:
            t_val = t.get(k) if t else None
            p_val = p.get({'open':'o','high':'h','low':'l','close':'c','volume':'v'}[k]) if p else None
            if t_val is not None and p_val is not None:
                try:
                    t_val = float(t_val)
                    p_val = float(p_val)
                    if abs(t_val - p_val) > max(abs(t_val), abs(p_val)) * 0.01:
                        msgs.append(f"{k} diff >1%: tiingo={t_val}, polygon={p_val}")
                except Exception:
                    msgs.append(f"{k} diff check failed: tiingo={t_val}, polygon={p_val}")
        return msgs

    def update_close_history(self, close):
        try:
            if close is not None:
                self.close_history.append(float(close))
        except Exception:
            pass

class DatabaseDailyPricesUnifier(DailyPricesUnifierBase):
    def __init__(self, environment):
        super().__init__(environment)

    async def fetch_prices_by_instrument_id(self, conn, table, instrument_id, start_date, end_date):
        print(f"[DEBUG][fetch_prices_by_instrument_id] Querying table={table} for instrument_id={instrument_id}, start_date={start_date}, end_date={end_date}")
        rows = await conn.fetch(
            f"""
            SELECT date, open, high, low, close, volume FROM {table}
            WHERE instrument_id = $1 AND date >= $2 AND date <= $3
            """,
            instrument_id, start_date, end_date
        )
        print(f"[DEBUG][fetch_prices_by_instrument_id] Got {len(rows)} rows for instrument_id={instrument_id} in {table}")
        for idx, row in enumerate(rows):
            print(f"[DEBUG][fetch_prices_by_instrument_id] Row {idx}: {dict(row)} (types: {[type(v) for v in dict(row).values()]})")
        if not rows:
            # Print all available dates for this instrument_id in this table
            all_dates = await conn.fetch(f"SELECT date FROM {table} WHERE instrument_id = $1 ORDER BY date", instrument_id)
            print(f"[DEBUG][fetch_prices_by_instrument_id] Available dates for instrument_id={instrument_id} in {table}: {[r['date'] for r in all_dates]}")
        return {row['date']: dict(row) for row in rows}

    async def unify_daily_prices(self, symbol, asof, current_date):
        print(f"[DEBUG][unify_daily_prices] Called with symbol={symbol}, asof={asof}, current_date={current_date}")
        # asof can be date or (start, end)
        if isinstance(asof, (tuple, list)):
            start_date, end_date = asof
        else:
            start_date = end_date = asof

        from core.dao.instrument_xrefs_dao import InstrumentXrefsDAO
        xrefs_dao = InstrumentXrefsDAO(self.environment)
        
        # Use the standard method which uses 'ticker' vendor
        instrument_id = await xrefs_dao.resolve_instrument_id_by_symbol(symbol)
        print(f"[DEBUG][unify_daily_prices] Resolved instrument_id={instrument_id} for symbol={symbol}")
        if instrument_id is None:
            print(f"[DEBUG][unify_daily_prices] Could not resolve instrument_id for symbol={symbol}")
            return []

        pool = await asyncpg.create_pool(self.environment.get_database_url())
        results = []
        async with pool.acquire() as conn:
            # Fetch by instrument_id, not symbol
            tiingo = await self.fetch_prices_by_instrument_id(conn, self.environment.get_table_name('daily_prices_tiingo'), instrument_id, start_date, end_date)
            polygon = await self.fetch_prices_by_instrument_id(conn, self.environment.get_table_name('daily_prices_polygon'), instrument_id, start_date, end_date)
            print(f"[DEBUG][unify_daily_prices] tiingo keys: {list(tiingo.keys())}, polygon keys: {list(polygon.keys())}")
            # Only process dates in the requested range
            requested_dates = set()
            curr = start_date
            while curr <= end_date:
                requested_dates.add(curr)
                curr += timedelta(days=1)
            all_dates = sorted((set(tiingo.keys()) | set(polygon.keys())) & requested_dates)
            for d in all_dates:
                t = tiingo.get(d)
                p = polygon.get(d)
                status = 'valid'
                note = ''
                try:
                    dt_obj = d if isinstance(d, date) else datetime.strptime(str(d), "%Y-%m-%d").date()
                except Exception:
                    status = 'invalid'; note = f"Invalid date format: {d}"
                    dt_obj = None
                row = t if t else p
                row_msgs = self.validate_row(row) if row else []
                date_msgs = self.validate_date(dt_obj, current_date) if dt_obj else []
                sigma_msgs = self.validate_sigma(row['close']) if row and 'close' in row and row['close'] is not None else []
                diff_msgs = self.validate_diff(t, p) if t and p else []
                all_msgs = row_msgs + date_msgs + sigma_msgs + diff_msgs
                if all_msgs:
                    status = 'invalid' if row_msgs or date_msgs or sigma_msgs else 'conflict'
                    note = "; ".join(all_msgs)
                print(f"[DEBUG][unify_daily_prices] For date={d}: t={t}, p={p}, row={row}")
                open_, high, low, close, volume = [row.get(k) if row else None for k in ['open','high','low','close','volume']]
                print(f"[DEBUG][unify_daily_prices] Date={d}, open={open_}, high={high}, low={low}, close={close}, volume={volume}, status={status}, note={note}")
                await conn.execute(
                    f"""
                    INSERT INTO {self.environment.get_table_name('daily_prices')} (date, instrument_id, symbol, open, high, low, close, volume, source, status, note)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (date, instrument_id) DO UPDATE SET symbol=EXCLUDED.symbol, open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume, source=EXCLUDED.source, status=EXCLUDED.status, note=EXCLUDED.note
                    """,
                    d, instrument_id, symbol, open_, high, low, close, volume,
                    ('tiingo' if t and not p else 'polygon' if p and not t else 'both'),
                    status, note
                )
                self.update_close_history(close)
                results.append({
                    'date': d,
                    'symbol': symbol,
                    'open': open_,
                    'high': high,
                    'low': low,
                    'close': close,
                    'volume': volume,
                    'source': ('tiingo' if t and not p else 'polygon' if p and not t else 'both'),
                    'status': status,
                    'note': note
                })
        await pool.close()
        print(f"[DEBUG][unify_daily_prices] Final results for symbol={symbol}: {results}")
        return results

    async def unify_daily_prices_batch(self, symbols, asof, current_date):
        """
        Optimized batch processing for multiple symbols at once.
        Performance improvement: Reduces database connections and query overhead.
        """
        print(f"[DEBUG][unify_daily_prices_batch] Processing {len(symbols)} symbols: {symbols}")
        
        # asof can be date or (start, end)
        if isinstance(asof, (tuple, list)):
            start_date, end_date = asof
        else:
            start_date = end_date = asof

        from core.dao.instrument_xrefs_dao import InstrumentXrefsDAO
        xrefs_dao = InstrumentXrefsDAO(self.environment)
        
        # Resolve all instrument IDs in batch
        symbol_to_id = {}
        for symbol in symbols:
            instrument_id = await xrefs_dao.resolve_instrument_id_by_symbol(symbol)
            if instrument_id:
                symbol_to_id[symbol] = instrument_id
        
        print(f"[DEBUG][unify_daily_prices_batch] Resolved {len(symbol_to_id)} instrument IDs")
        
        if not symbol_to_id:
            return {symbol: [] for symbol in symbols}

        pool = await asyncpg.create_pool(self.environment.get_database_url())
        all_results = {}
        
        async with pool.acquire() as conn:
            # Batch fetch from both tables
            instrument_ids = list(symbol_to_id.values())
            
            # Fetch all Tiingo data in one query
            tiingo_rows = await conn.fetch(
                f"""
                SELECT date, instrument_id, open, high, low, close, volume 
                FROM {self.environment.get_table_name('daily_prices_tiingo')}
                WHERE instrument_id = ANY($1) AND date >= $2 AND date <= $3
                ORDER BY instrument_id, date
                """,
                instrument_ids, start_date, end_date
            )
            
            # Fetch all Polygon data in one query  
            polygon_rows = await conn.fetch(
                f"""
                SELECT date, instrument_id, open, high, low, close, volume
                FROM {self.environment.get_table_name('daily_prices_polygon')} 
                WHERE instrument_id = ANY($1) AND date >= $2 AND date <= $3
                ORDER BY instrument_id, date
                """,
                instrument_ids, start_date, end_date
            )
            
            print(f"[DEBUG][unify_daily_prices_batch] Fetched {len(tiingo_rows)} Tiingo rows, {len(polygon_rows)} Polygon rows")
            
            # Organize data by symbol and date
            tiingo_data = {}
            polygon_data = {}
            id_to_symbol = {v: k for k, v in symbol_to_id.items()}
            
            for row in tiingo_rows:
                symbol = id_to_symbol.get(row['instrument_id'])
                if symbol:
                    if symbol not in tiingo_data:
                        tiingo_data[symbol] = {}
                    tiingo_data[symbol][row['date']] = dict(row)
            
            for row in polygon_rows:
                symbol = id_to_symbol.get(row['instrument_id'])
                if symbol:
                    if symbol not in polygon_data:
                        polygon_data[symbol] = {}
                    polygon_data[symbol][row['date']] = dict(row)
            
            # Process each symbol's data
            for symbol in symbols:
                results = []
                if symbol not in symbol_to_id:
                    all_results[symbol] = results
                    continue
                    
                tiingo_symbol = tiingo_data.get(symbol, {})
                polygon_symbol = polygon_data.get(symbol, {})
                
                # Get all dates for this symbol
                requested_dates = set()
                curr = start_date
                while curr <= end_date:
                    requested_dates.add(curr)
                    curr += timedelta(days=1)
                
                all_dates = sorted((set(tiingo_symbol.keys()) | set(polygon_symbol.keys())) & requested_dates)
                
                for d in all_dates:
                    t = tiingo_symbol.get(d)
                    p = polygon_symbol.get(d) 
                    
                    status = 'valid'
                    note = ''
                    
                    try:
                        dt_obj = d if isinstance(d, date) else datetime.strptime(str(d), "%Y-%m-%d").date()
                    except Exception:
                        status = 'invalid'
                        note = f"Invalid date format: {d}"
                        dt_obj = None
                    
                    row = t if t else p
                    row_msgs = self.validate_row(row) if row else []
                    date_msgs = self.validate_date(dt_obj, current_date) if dt_obj else []
                    sigma_msgs = self.validate_sigma(row['close']) if row and 'close' in row and row['close'] is not None else []
                    diff_msgs = self.validate_diff(t, p) if t and p else []
                    
                    all_msgs = row_msgs + date_msgs + sigma_msgs + diff_msgs
                    if all_msgs:
                        status = 'invalid' if row_msgs or date_msgs or sigma_msgs else 'conflict'
                        note = "; ".join(all_msgs)
                    
                    open_, high, low, close, volume = [row.get(k) if row else None for k in ['open','high','low','close','volume']]
                    
                    results.append({
                        'date': d,
                        'symbol': symbol,
                        'open': open_,
                        'high': high,
                        'low': low,
                        'close': close,
                        'volume': volume,
                        'source': ('tiingo' if t and not p else 'polygon' if p and not t else 'both'),
                        'status': status,
                        'note': note
                    })
                
                all_results[symbol] = results
        
        await pool.close()
        print(f"[DEBUG][unify_daily_prices_batch] Returning results for {len(all_results)} symbols")
        return all_results

class FileDailyPricesUnifier(DailyPricesUnifierBase):
    def unify_daily_prices_sync(self, symbol, asof, current_date):
        import asyncio
        import logging
        logging.getLogger(__name__)
        
        # Debug: Log input parameters
        print(f"[DEBUG][unify_daily_prices_sync] ENTER: symbol={symbol}, asof={asof}, current_date={current_date}")
        print(f"[DEBUG][unify_daily_prices_sync] tiingo_data keys: {list(self.tiingo_data.keys())}")
        print(f"[DEBUG][unify_daily_prices_sync] polygon_data keys: {list(self.polygon_data.keys())}")
        
        # Debug: Check if symbol exists in either data source
        if symbol not in self.tiingo_data and symbol not in self.polygon_data:
            print(f"[ERROR][unify_daily_prices_sync] Symbol {symbol} not found in tiingo or polygon data")
            print(f"[DEBUG][unify_daily_prices_sync] tiingo_data: {self.tiingo_data}")
            print(f"[DEBUG][unify_daily_prices_sync] polygon_data: {self.polygon_data}")
            return []
        
        # Debug: Log data for the symbol if found
        if symbol in self.tiingo_data:
            print(f"[DEBUG][unify_daily_prices_sync] Found {symbol} in tiingo_data with {len(self.tiingo_data[symbol])} records")
            dates = sorted(self.tiingo_data[symbol].keys())
            if dates:
                print(f"[DEBUG][unify_daily_prices_sync] Tiingo date range for {symbol}: {dates[0]} to {dates[-1]} ({len(dates)} days)")
        
        if symbol in self.polygon_data:
            print(f"[DEBUG][unify_daily_prices_sync] Found {symbol} in polygon_data with {len(self.polygon_data[symbol])} records")
            dates = sorted(self.polygon_data[symbol].keys())
            if dates:
                print(f"[DEBUG][unify_daily_prices_sync] Polygon date range for {symbol}: {dates[0]} to {dates[-1]} ({len(dates)} days)")
        
        coro = self.unify_daily_prices(symbol, asof, current_date)
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        if loop.is_running():
            import nest_asyncio
            nest_asyncio.apply()
            fut = asyncio.ensure_future(coro)
            result = loop.run_until_complete(fut)
        else:
            result = loop.run_until_complete(coro)
            
        # Debug: Log results
        print(f"[DEBUG][unify_daily_prices_sync] Results for {symbol} on {asof}:")
        if not result:
            print("[DEBUG][unify_daily_prices_sync]   No results returned from unify_daily_prices")
        else:
            print(f"[DEBUG][unify_daily_prices_sync]   Found {len(result)} records")
            for i, r in enumerate(result[:3], 1):
                print(f"[DEBUG][unify_daily_prices_sync]   Result {i}: {r}")
            if len(result) > 3:
                print(f"[DEBUG][unify_daily_prices_sync]   ... and {len(result) - 3} more records")
        
        return result

    def __init__(self, environment, tiingo_data, polygon_data):
        super().__init__(environment)
        self.tiingo_data = tiingo_data
        self.polygon_data = polygon_data

    async def unify_daily_prices(self, symbol, asof, current_date):
        import logging
        from datetime import timedelta
        logging.getLogger(__name__)
        
        # Debug: Log input parameters
        print(f"[DEBUG][unify_daily_prices] ENTER: symbol={symbol}, asof={asof}, current_date={current_date}")
        
        # asof can be date or (start, end)
        if isinstance(asof, (tuple, list)):
            start_date, end_date = asof
        else:
            start_date = end_date = asof
            
        # Ensure dates are date objects, not datetime
        if hasattr(start_date, 'date'):
            start_date = start_date.date()
        if hasattr(end_date, 'date'):
            end_date = end_date.date()
            
        # Debug: Log date range
        print(f"[DEBUG][unify_daily_prices] Processing date range: {start_date} to {end_date}")
        
        tiingo = self.tiingo_data.get(symbol, {})
        polygon = self.polygon_data.get(symbol, {})
        
        # Debug: Log available dates in each data source
        print(f"[DEBUG][unify_daily_prices] Tiingo data for {symbol}: {len(tiingo)} dates")
        if tiingo:
            tiingo_dates = sorted(tiingo.keys())
            print(f"[DEBUG][unify_daily_prices] Tiingo date range: {tiingo_dates[0]} to {tiingo_dates[-1]} ({len(tiingo_dates)} days)")
            
        print(f"[DEBUG][unify_daily_prices] Polygon data for {symbol}: {len(polygon)} dates")
        if polygon:
            polygon_dates = sorted(polygon.keys())
            print(f"[DEBUG][unify_daily_prices] Polygon date range: {polygon_dates[0]} to {polygon_dates[-1]} ({len(polygon_dates)} days)")
        
        # Debug: Check if we have data for the requested date range
        all_dates = set()
        if tiingo:
            all_dates.update(tiingo.keys())
        if polygon:
            all_dates.update(polygon.keys())
            
        if not all_dates:
            print(f"[ERROR][unify_daily_prices] No price data available for {symbol} in any data source")
            return []
            
        # Generate all dates in the requested range
        date_range = []
        current = start_date
        while current <= end_date:
            date_range.append(current)
            current += timedelta(days=1)
            
        print(f"[DEBUG][unify_daily_prices] Checking {len(date_range)} dates from {start_date} to {end_date}")
        
        # Check which dates have data
        missing_dates = [d for d in date_range if d not in all_dates]
        if missing_dates:
            print(f"[DEBUG][unify_daily_prices] Missing data for {len(missing_dates)}/{len(date_range)} dates")
            if len(missing_dates) <= 10:
                print(f"[DEBUG][unify_daily_prices] Missing dates: {missing_dates}")
            else:
                print(f"[DEBUG][unify_daily_prices] Missing dates (first 10): {missing_dates[:10]}...")
        else:
            print(f"[DEBUG][unify_daily_prices] Data available for all {len(date_range)} dates")
        # Only process dates within the requested interval
        # Emit a row for every calendar day in the requested interval (not just trading days)
        from datetime import timedelta
        num_days = (end_date - start_date).days + 1
        all_days = [start_date + timedelta(days=i) for i in range(num_days)]
        results = []
        for d in all_days:
            t = tiingo.get(d)
            p = polygon.get(d)
            if not t and not p:
                # No vendor data for this trading day
                print(f"[DEBUG][unify_daily_prices] No vendor data for date={d}, symbol={symbol}")
                results.append({
                    'date': d,
                    'symbol': symbol,
                    'open': None, 'high': None, 'low': None, 'close': None, 'volume': None,
                    'source': None,
                    'status': 'missing',
                    'note': 'No vendor data'
                })
                continue
            status = 'valid'
            note = ''
            try:
                if isinstance(d, date):
                    dt_obj = d
                elif isinstance(d, datetime):
                    dt_obj = d.date()
                else:
                    dt_obj = datetime.strptime(str(d)[:10], "%Y-%m-%d").date()
            except Exception:
                status = 'invalid'; note = f"Invalid date format: {d}"
                dt_obj = None
            row = t if t else p
            row_msgs = self.validate_row(row) if row else []
            date_msgs = self.validate_date(dt_obj, current_date) if dt_obj else []
            sigma_msgs = self.validate_sigma(row['close']) if row and 'close' in row and row['close'] is not None else []
            diff_msgs = self.validate_diff(t, p) if t and p else []
            all_msgs = row_msgs + date_msgs + sigma_msgs + diff_msgs
            if all_msgs:
                status = 'invalid' if row_msgs or date_msgs or sigma_msgs else 'conflict'
                note = "; ".join(all_msgs)
            open_, high, low, close, volume = [row.get(k) if row else None for k in ['open','high','low','close','volume']]
            if not t and p:
                open_, high, low, close, volume = [p.get('o'), p.get('h'), p.get('l'), p.get('c'), p.get('v')]
            # Log if any OHLC is NaN or None
            if any(x is None or (isinstance(x, float) and np.isnan(x)) for x in [open_, high, low, close]):
                print(f"[DEBUG][unify_daily_prices] NaN/None OHLC for date={d}, symbol={symbol}: open={open_}, high={high}, low={low}, close={close}, volume={volume}")
            results.append({
                'date': d,
                'symbol': symbol,
                'open': open_, 'high': high, 'low': low, 'close': close, 'volume': volume,
                'source': ('tiingo' if t and not p else 'polygon' if p and not t else 'both'),
                'status': status,
                'note': note
            })
            self.update_close_history(close)
        print(f"[DEBUG][unify_daily_prices] Final results for symbol={symbol}: {results}")
        return results


if __name__ == "__main__":
    import argparse
    import asyncio
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', required=True)
    parser.add_argument('--start_date', required=True)
    parser.add_argument('--end_date', required=True)
    args = parser.parse_args()
    from core.config.environment import Environment
    environment = Environment()
    unifier = DatabaseDailyPricesUnifier(environment)
    asyncio.run(unifier.unify_daily_prices(args.symbol, (args.start_date, args.end_date)))
