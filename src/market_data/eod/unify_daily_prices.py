import os
import asyncpg
import numpy as np
from datetime import datetime, date
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

    def validate_date(self, dt_obj, asof):
        msgs = []
        try:
            def to_date(val):
                if isinstance(val, date):
                    return val
                elif isinstance(val, datetime):
                    return val.date()
                else:
                    return datetime.strptime(str(val)[:10], "%Y-%m-%d").date()
            if not isinstance(asof, (tuple, list)):
                s = e = to_date(asof)
            else:
                s = to_date(asof[0])
                e = to_date(asof[1])
            if not (s <= dt_obj <= e):
                msgs.append(f"date {dt_obj} not in range {s} to {e}")
        except Exception:
            msgs.append("date range check failed")
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

    async def fetch_prices(self, conn, table, symbol, start_date, end_date):
        rows = await conn.fetch(
            f"""
            SELECT date, open, high, low, close, volume FROM {table}
            WHERE symbol = $1 AND date >= $2 AND date <= $3
            """,
            symbol, start_date, end_date
        )
        return {row['date']: row for row in rows}

    async def unify_daily_prices(self, symbol, asof):
        # asof can be date or (start, end)
        if isinstance(asof, (tuple, list)):
            start_date, end_date = asof
        else:
            start_date = end_date = asof
        pool = await asyncpg.create_pool(self.environment.get_database_url())
        async with pool.acquire() as conn:
            tiingo = await self.fetch_prices(conn, self.environment.get_table_name('daily_prices_tiingo'), symbol, start_date, end_date)
            polygon = await self.fetch_prices(conn, self.environment.get_table_name('daily_prices_polygon'), symbol, start_date, end_date)
            all_dates = sorted(set(tiingo.keys()) | set(polygon.keys()))
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
                date_msgs = self.validate_date(dt_obj, (start_date, end_date)) if dt_obj else []
                sigma_msgs = self.validate_sigma(row['close']) if row and 'close' in row and row['close'] is not None else []
                diff_msgs = self.validate_diff(t, p) if t and p else []
                all_msgs = row_msgs + date_msgs + sigma_msgs + diff_msgs
                if all_msgs:
                    status = 'invalid' if row_msgs or date_msgs or sigma_msgs else 'conflict'
                    note = "; ".join(all_msgs)
                open_, high, low, close, volume = [row.get(k) if row else None for k in ['open','high','low','close','volume']]
                if not t and p:
                    open_, high, low, close, volume = [p.get('o'), p.get('h'), p.get('l'), p.get('c'), p.get('v')]
                await conn.execute(
                    f"""
                    INSERT INTO {self.environment.get_table_name('daily_prices')} (date, symbol, open, high, low, close, volume, source, status, note)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (date, symbol) DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume, source=EXCLUDED.source, status=EXCLUDED.status, note=EXCLUDED.note
                    """,
                    d, symbol, open_, high, low, close, volume,
                    ('tiingo' if t and not p else 'polygon' if p and not t else 'both'),
                    status, note
                )
                self.update_close_history(close)
        await pool.close()

class FileDailyPricesUnifier(DailyPricesUnifierBase):
    def unify_daily_prices_sync(self, symbol, asof):
        import asyncio
        coro = self.unify_daily_prices(symbol, asof)
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

    async def unify_daily_prices(self, symbol, asof):
        # asof can be date or (start, end)
        if isinstance(asof, (tuple, list)):
            start_date, end_date = asof
        else:
            start_date = end_date = asof
        tiingo = self.tiingo_data.get(symbol, {})
        polygon = self.polygon_data.get(symbol, {})
        all_dates = sorted(set(tiingo.keys()) | set(polygon.keys()))
        results = []
        for d in all_dates:
            t = tiingo.get(d)
            p = polygon.get(d)
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
            date_msgs = self.validate_date(dt_obj, (start_date, end_date)) if dt_obj else []
            sigma_msgs = self.validate_sigma(row['close']) if row and 'close' in row and row['close'] is not None else []
            diff_msgs = self.validate_diff(t, p) if t and p else []
            all_msgs = row_msgs + date_msgs + sigma_msgs + diff_msgs
            if all_msgs:
                status = 'invalid' if row_msgs or date_msgs or sigma_msgs else 'conflict'
                note = "; ".join(all_msgs)
            open_, high, low, close, volume = [row.get(k) if row else None for k in ['open','high','low','close','volume']]
            if not t and p:
                open_, high, low, close, volume = [p.get('o'), p.get('h'), p.get('l'), p.get('c'), p.get('v')]
            results.append({
                'date': d,
                'symbol': symbol,
                'open': open_, 'high': high, 'low': low, 'close': close, 'volume': volume,
                'source': ('tiingo' if t and not p else 'polygon' if p and not t else 'both'),
                'status': status,
                'note': note
            })
            self.update_close_history(close)
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
