import os
import asyncio
import asyncpg
import aiohttp
from datetime import datetime, timedelta
import pandas as pd
import argparse

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")
TIINGO_API_KEY = os.getenv("TIINGO_API_KEY")
TIINGO_BASE_URL = "https://api.tiingo.com/tiingo/daily/{symbol}/prices"

CREATE_DAILY_PRICES_TIINGO_SQL = """
CREATE TABLE IF NOT EXISTS daily_prices_tiingo (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    adjClose DOUBLE PRECISION,
    volume BIGINT,
    PRIMARY KEY (date, symbol)
);
"""

INSERT_SQL = """
INSERT INTO daily_prices_tiingo (date, symbol, open, high, low, close, adjClose, volume)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (date, symbol) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    adjClose = EXCLUDED.adjClose,
    volume = EXCLUDED.volume;
"""

async def get_spy_members(pool):
    async with pool.acquire() as conn:
        universe_row = await conn.fetchrow("SELECT id FROM universe WHERE name = 'S&P 500'")
        if not universe_row:
            print("[ERROR] Could not find S&P 500 universe in universe table.")
            return []
        universe_id = universe_row['id']
        rows = await conn.fetch("SELECT DISTINCT symbol FROM universe_membership WHERE universe_id = $1", universe_id)
        return [row['symbol'] for row in rows]

def tiingo_url(symbol, start_date, end_date):
    return (
        f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
        f"?startDate={start_date}&endDate={end_date}&format=json&token={TIINGO_API_KEY}"
    )

async def get_existing_dates(pool, symbol, start_date, end_date):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT date FROM daily_prices_tiingo WHERE symbol = $1 AND date >= $2 AND date <= $3",
            symbol, start_date, end_date
        )
    return set(row['date'] for row in rows)

from calendars.exchange_calendar import ExchangeCalendar

def get_missing_date_ranges(existing_dates, start_date, end_date):
    # Returns a list of (range_start, range_end) for missing contiguous NYSE trading dates
    nyse_cal = ExchangeCalendar('NYSE')
    trading_days = nyse_cal.all_trading_days(start_date, end_date)
    missing = [d for d in trading_days if d not in existing_dates]
    if not missing:
        return []
    # Group into contiguous ranges
    ranges = []
    range_start = missing[0]
    prev = missing[0]
    for d in missing[1:]:

        if (d - prev).days > 1:
            ranges.append((range_start, prev))
            range_start = d
        prev = d
    ranges.append((range_start, prev))
    return ranges

from calendars.exchange_calendar import ExchangeCalendar

async def fetch_and_insert_symbol(pool, session, symbol, start_date, end_date):
    # Always use datetime.date for DB and date math
    if isinstance(start_date, str):
        start_date_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    else:
        start_date_dt = start_date
    if isinstance(end_date, str):
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    else:
        end_date_dt = end_date
    # Get NYSE trading days only
    nyse_cal = ExchangeCalendar('NYSE')
    trading_days = set(nyse_cal.all_trading_days(start_date_dt, end_date_dt))
    existing_dates = await get_existing_dates(pool, symbol, start_date_dt, end_date_dt)
    missing_ranges = get_missing_date_ranges(existing_dates, start_date_dt, end_date_dt)
    if not missing_ranges:
        print(f"[DEBUG] All data exists for {symbol} in {start_date} to {end_date}, skipping fetch.")
        return
    for range_start, range_end in missing_ranges:
        url = tiingo_url(symbol, range_start, range_end)
        print(f"[DEBUG] Fetching {symbol} from URL: {url}")
        async with session.get(url) as resp:
            print(f"[DEBUG] HTTP status for {symbol}: {resp.status}")
            if resp.status != 200:
                print(f"[ERROR] Failed to fetch {symbol}: HTTP {resp.status}")
                continue
            data = await resp.json()
            if not data:
                print(f"[WARNING] No data returned for {symbol} from {range_start} to {range_end}")
                continue
            rows = []
            for row in data:
                # Robustly parse Tiingo ISO date string (e.g., '2020-01-02T00:00:00.000Z')
                date_val = pd.to_datetime(row['date']).date()
                if date_val not in trading_days:
                    continue  # Only insert if NYSE is open
                rows.append((
                    date_val,
                    symbol,
                    row.get('open'),
                    row.get('high'),
                    row.get('low'),
                    row.get('close'),
                    row.get('adjClose'),
                    row.get('volume'),
                ))
            async with pool.acquire() as conn:
                await conn.executemany(INSERT_SQL, rows)
            print(f"[INFO] Inserted {len(rows)} rows for {symbol} from {range_start} to {range_end}")

# --- MAIN FUNCTION AND ENTRYPOINT ---
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--ticker', type=str, default=None, help='Process only this ticker (optional)')
    args = parser.parse_args()

    if not TIINGO_API_KEY:
        print("[ERROR] TIINGO_API_KEY environment variable not set.")
        return

    pool = await asyncpg.create_pool(TSDB_URL, min_size=1, max_size=4)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_DAILY_PRICES_TIINGO_SQL)
        # WARNING: Disabling SSL verification is NOT recommended for production. Use only for debugging local SSL issues.
    import ssl
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        if args.ticker:
            tickers = [args.ticker]
        else:
            tickers = await get_spy_members(pool)
            if not tickers:
                print("[ERROR] No tickers found to process.")
                await pool.close()
                return
        for symbol in tickers:
            try:
                print(f"[INFO] Processing {symbol}...")
                await fetch_and_insert_symbol(pool, session, symbol, args.start_date, args.end_date)
            except Exception as e:
                print(f"[ERROR] Exception for {symbol}: {e}")
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
