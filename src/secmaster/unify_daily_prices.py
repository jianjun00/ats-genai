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

async def unify_daily_prices(symbol, start_date, end_date, environment):
    pool = await asyncpg.create_pool(environment.get_database_url())
    async with pool.acquire() as conn:
        tiingo = await fetch_prices(conn, environment.get_table_name('daily_prices_tiingo'), symbol, start_date, end_date)
        polygon = await fetch_prices(conn, environment.get_table_name('daily_prices_polygon'), symbol, start_date, end_date)
        all_dates = set(tiingo.keys()) | set(polygon.keys())
        for d in sorted(all_dates):
            t = tiingo.get(d)
            p = polygon.get(d)
            if t and not p:
                # Only Tiingo
                await conn.execute(
                    f"""
                    INSERT INTO {environment.get_table_name('daily_prices')} (date, symbol, open, high, low, close, volume, source, status, note)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, 'tiingo', 'valid', NULL)
                    ON CONFLICT (date, symbol) DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume, source='tiingo', status='valid', note=NULL
                    """,
                    d, symbol, t['open'], t['high'], t['low'], t['close'], t['volume']
                )
            elif p and not t:
                # Only Polygon
                await conn.execute(
                    f"""
                    INSERT INTO {environment.get_table_name('daily_prices')} (date, symbol, open, high, low, close, volume, source, status, note)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, 'polygon', 'valid', NULL)
                    ON CONFLICT (date, symbol) DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume, source='polygon', status='valid', note=NULL
                    """,
                    d, symbol, p['open'], p['high'], p['low'], p['close'], p['volume']
                )
            elif t and p:
                # Both have data; compare
                diffs = []
                for k in ['open','high','low','close','volume']:
                    if not close_enough(t[k], p[k]):
                        diffs.append(f"{k}: tiingo={t[k]}, polygon={p[k]}")
                if not diffs:
                    # Close enough
                    await conn.execute(
                        f"""
                        INSERT INTO {environment.get_table_name('daily_prices')} (date, symbol, open, high, low, close, volume, source, status, note)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, 'both', 'valid', NULL)
                        ON CONFLICT (date, symbol) DO UPDATE SET open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low, close=EXCLUDED.close, volume=EXCLUDED.volume, source='both', status='valid', note=NULL
                        """,
                        d, symbol, t['open'], t['high'], t['low'], t['close'], t['volume']
                    )
                else:
                    # Conflict
                    await conn.execute(
                        f"""
                        INSERT INTO {environment.get_table_name('daily_prices')} (date, symbol, open, high, low, close, volume, source, status, note)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, 'both', 'conflict', $8)
                        ON CONFLICT (date, symbol) DO UPDATE SET source='both', status='conflict', note=$8
                        """,
                        d, symbol, t['open'], t['high'], t['low'], t['close'], t['volume'], "; ".join(diffs)
                    )
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
