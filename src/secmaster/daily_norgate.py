import os
import asyncio
import asyncpg
import norgatedata
import pandas as pd
from datetime import datetime
import argparse

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")

CREATE_DAILY_PRICES_NORGATE_SQL = """
CREATE TABLE IF NOT EXISTS daily_prices_norgate (
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

def get_norgate_daily_prices(symbol, start_date=None, end_date=None):
    # Returns a pandas DataFrame with columns: Date, Open, High, Low, Close, Volume
    df = norgatedata.price_timeseries(symbol)
    df = df.reset_index()  # Ensure 'Date' is a column
    if start_date:
        df = df[df['Date'] >= pd.to_datetime(start_date)]
    if end_date:
        df = df[df['Date'] <= pd.to_datetime(end_date)]
    # Drop rows with any NA in price columns
    df = df.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume'])
    return df

async def insert_prices(pool, df, symbol):
    if df.empty:
        print(f"No data for {symbol}")
        return
    async with pool.acquire() as conn:
        await conn.execute(CREATE_DAILY_PRICES_NORGATE_SQL)
        await conn.executemany(
            "INSERT INTO daily_prices_norgate (date, symbol, open, high, low, close, volume) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT DO NOTHING",
            [(
                row['Date'].date(),
                symbol,
                float(row['Open']),
                float(row['High']),
                float(row['Low']),
                float(row['Close']),
                int(row['Volume'])
            ) for _, row in df.iterrows()]
        )
    print(f"Inserted {len(df)} rows for {symbol}")

def get_sp500_symbols():
    # This function assumes you have a list of S&P 500 tickers in spy_membership.csv
    # You can adapt this to your own universe
    csv_path = os.path.join(os.path.dirname(__file__), '../../spy_membership.csv')
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"{csv_path} not found. Please provide a list of tickers.")
    df = pd.read_csv(csv_path)
    return df['symbol'].unique().tolist()

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ticker', type=str, default=None, help='Process only this ticker (optional)')
    parser.add_argument('--start', type=str, default=None, help='Start date (YYYY-MM-DD, optional)')
    parser.add_argument('--end', type=str, default=None, help='End date (YYYY-MM-DD, optional)')
    args = parser.parse_args()

    if args.ticker:
        tickers = [args.ticker]
    else:
        tickers = get_sp500_symbols()

    pool = await asyncpg.create_pool(TSDB_URL)
    for symbol in tickers:
        print(f"Downloading {symbol} from Norgate Data...")
        try:
            df = get_norgate_daily_prices(symbol, args.start, args.end)
            await insert_prices(pool, df, symbol)
        except Exception as e:
            print(f"Error with {symbol}: {e}")
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
