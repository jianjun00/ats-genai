import os
import asyncpg
from datetime import date
from dotenv import load_dotenv
from src.config.environment import get_environment

load_dotenv()

env = get_environment()

# Threshold for "close enough" (e.g. 1% difference)
CLOSE_THRESHOLD = 0.01

async def fetch_prices(conn, table, symbol, start_date, end_date):
    rows = await conn.fetch(
        f"""
        SELECT date, open, high, low, close, volume FROM {table}
        WHERE symbol = $1 AND date >= $2 AND date <= $3
        """,
        symbol, start_date, end_date
    )
    return {row['date']: row for row in rows}

def close_enough(a, b):
    if a is None or b is None:
        return False
    return abs(a - b) <= max(abs(a), abs(b)) * CLOSE_THRESHOLD

import numpy as np
from datetime import datetime, timedelta

async def unify_daily_prices(symbol, start_date, end_date, environment):
    pool = await asyncpg.create_pool(environment.get_database_url())
    async with pool.acquire() as conn:
        tiingo = await fetch_prices(conn, environment.get_table_name('daily_prices_tiingo'), symbol, start_date, end_date)
        polygon = await fetch_prices(conn, environment.get_table_name('daily_prices_polygon'), symbol, start_date, end_date)
        all_dates = sorted(set(tiingo.keys()) | set(polygon.keys()))
        close_history = []  # for rolling 20-day sigma
        for d in all_dates:
            t = tiingo.get(d)
            p = polygon.get(d)
            status = 'valid'
            note = ''
            # Helper: parse date
            try:
                dt_obj = d if isinstance(d, date) else datetime.strptime(str(d), "%Y-%m-%d").date()
            except Exception:
                status = 'invalid'; note = f"Invalid date format: {d}"
                dt_obj = None
            # 1. Value threshold validation
            def validate_row(row):
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
            # 2. Date in range
            def validate_date(dt_obj):
                msgs = []
                try:
                    s = datetime.strptime(str(start_date), "%Y-%m-%d").date()
                    e = datetime.strptime(str(end_date), "%Y-%m-%d").date()
                    if not (s <= dt_obj <= e):
                        msgs.append(f"date {dt_obj} not in range {s} to {e}")
                except Exception:
                    msgs.append("date range check failed")
                return msgs
            # 3. 6 sigma outlier (close)
            def validate_sigma(close):
                msgs = []
                if len(close_history) >= 20:
                    arr = np.array(close_history[-20:])
                    mu = np.mean(arr)
                    sigma = np.std(arr)
                    try:
                        c = float(close)
                        if sigma > 0 and (abs(c - mu) > 6 * sigma):
                            msgs.append(f"close {c} outlier: mu={mu:.2f}, sigma={sigma:.2f}")
                    except Exception:
                        msgs.append("close sigma check failed")
                return msgs
            # 4. Tiingo vs Polygon diff
            def validate_diff(t, p):
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
            # Compose validation
            row = t if t else p
            row_msgs = validate_row(row) if row else []
            date_msgs = validate_date(dt_obj) if dt_obj else []
            sigma_msgs = validate_sigma(row['close']) if row and 'close' in row and row['close'] is not None else []
            diff_msgs = validate_diff(t, p) if t and p else []
            # Compose status/note
            all_msgs = row_msgs + date_msgs + sigma_msgs + diff_msgs
            if all_msgs:
                status = 'invalid' if row_msgs or date_msgs or sigma_msgs else 'conflict'
                note = "; ".join(all_msgs)
            # Insert to DB
            open_, high, low, close, volume = [row.get(k) if row else None for k in ['open','high','low','close','volume']]
            # For polygon, remap keys
            if not t and p:
                open_, high, low, close, volume = [p.get('o'), p.get('h'), p.get('l'), p.get('c'), p.get('v')]
            await conn.execute(
                f"""
                INSERT INTO {environment.get_table_name('daily_prices')} (date, symbol, open, high, low, close, volume, source, status, note)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (date, symbol) DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume, source=EXCLUDED.source, status=EXCLUDED.status, note=EXCLUDED.note
                """,
                d, symbol, open_, high, low, close, volume,
                ('tiingo' if t and not p else 'polygon' if p and not t else 'both'),
                status, note
            )
            # Update close history for sigma
            try:
                if close is not None:
                    close_history.append(float(close))
            except Exception:
                pass
    await pool.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', required=True)
    parser.add_argument('--start_date', required=True)
    parser.add_argument('--end_date', required=True)
    args = parser.parse_args()
    import asyncio
    from src.config.environment import get_environment
    environment = get_environment()
    asyncio.run(unify_daily_prices(args.symbol, args.start_date, args.end_date, environment))
