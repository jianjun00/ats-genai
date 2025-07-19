import pandas as pd
import yfinance as yf
import asyncpg
import asyncio
from datetime import datetime, timedelta
from spy_universe import SPYUniverse
import os

TSDB_URL = os.getenv('TSDB_URL', 'postgresql://localhost:5432/trading_db')
BATCH_SIZE = 1000  # Number of rows per insert batch

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS daily_prices (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    PRIMARY KEY (date, symbol)
);
"""

INSERT_SQL = """
INSERT INTO daily_prices (date, symbol, open, high, low, close, volume)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (date, symbol) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume;
"""

async def fetch_and_insert_symbol(pool, symbol, periods):
    for period in periods:
        start = period['effective_date']
        end = period['removal_date'] or datetime.utcnow().date()
        # yfinance expects string dates
        df = yf.download(symbol, start=start, end=end + timedelta(days=1), progress=False, auto_adjust=False)
        if df.empty:
            continue
        df = df.reset_index()
        rows = []
        for _, row in df.iterrows():
            rows.append((row['Date'].date(), symbol, row['Open'], row['High'], row['Low'], row['Close'], int(row['Volume'])))
            if len(rows) >= BATCH_SIZE:
                await pool.executemany(INSERT_SQL, rows)
                rows = []
        if rows:
            await pool.executemany(INSERT_SQL, rows)

async def main():
    # Load membership periods
    df = pd.read_csv('spy_membership.csv', parse_dates=['effective_date', 'removal_date'])
    # Group periods by symbol
    symbol_periods = {}
    for _, row in df.iterrows():
        symbol = row['symbol']
        eff = row['effective_date'].date()
        rem = row['removal_date'].date() if pd.notna(row['removal_date']) else None
        symbol_periods.setdefault(symbol, []).append({'effective_date': eff, 'removal_date': rem})
    # Connect to TimescaleDB
    pool = await asyncpg.create_pool(TSDB_URL, min_size=1, max_size=4)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_TABLE_SQL)
    # Download and insert for each symbol
    tasks = []
    for symbol, periods in symbol_periods.items():
        tasks.append(fetch_and_insert_symbol(pool, symbol, periods))
    await asyncio.gather(*tasks)
    await pool.close()
    print('Done loading daily prices for all SPY members.')

if __name__ == '__main__':
    asyncio.run(main())
