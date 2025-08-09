import os
import asyncpg
import numpy as np
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
from config.environment import Environment

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

        from dao.instrument_xrefs_dao import InstrumentXrefsDAO
        xrefs_dao = InstrumentXrefsDAO(self.environment)
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

class FileDailyPricesUnifier(DailyPricesUnifierBase):
    def unify_daily_prices_sync(self, symbol, asof, current_date):
        import asyncio
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
            return loop.run_until_complete(fut)
        else:
            return loop.run_until_complete(coro)

    def __init__(self, environment, tiingo_data, polygon_data):
        super().__init__(environment)
        self.tiingo_data = tiingo_data
        self.polygon_data = polygon_data

    async def unify_daily_prices(self, symbol, asof, current_date):
        # asof can be date or (start, end)
        if isinstance(asof, (tuple, list)):
            start_date, end_date = asof
        else:
            start_date = end_date = asof
        tiingo = self.tiingo_data.get(symbol, {})
        polygon = self.polygon_data.get(symbol, {})
        # Only process dates within the requested interval
        # Emit a row for every calendar day in the requested interval (not just trading days)
        from datetime import timedelta
        num_days = (end_date - start_date).days + 1
        all_days = [start_date + timedelta(days=i) for i in range(num_days)]
        results = []
        seen_dates = set()
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
    from config.environment import Environment
    environment = Environment()
    unifier = DatabaseDailyPricesUnifier(environment)
    asyncio.run(unifier.unify_daily_prices(args.symbol, (args.start_date, args.end_date)))
