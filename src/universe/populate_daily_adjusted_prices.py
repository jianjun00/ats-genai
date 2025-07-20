import os
import asyncio
import asyncpg
import pandas as pd
from datetime import datetime

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")

CREATE_DAILY_ADJUSTED_PRICES_SQL = """
CREATE TABLE IF NOT EXISTS daily_adjusted_prices (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    market_cap DOUBLE PRECISION,
    original_open DOUBLE PRECISION,
    original_high DOUBLE PRECISION,
    original_low DOUBLE PRECISION,
    original_close DOUBLE PRECISION,
    split_numerator DOUBLE PRECISION,
    split_denominator DOUBLE PRECISION,
    dividend_amount DOUBLE PRECISION,
    adjustment_factor DOUBLE PRECISION,
    PRIMARY KEY (date, symbol)
);
"""

async def fetch_prices(pool, symbol):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM daily_prices WHERE symbol = $1 ORDER BY date ASC", symbol
        )
        return pd.DataFrame([dict(row) for row in rows])

async def fetch_splits(pool, symbol):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT split_date, numerator, denominator FROM stock_splits WHERE symbol = $1 ORDER BY split_date ASC", symbol
        )
        return pd.DataFrame([dict(row) for row in rows])

async def fetch_dividends(pool, symbol):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT ex_date, amount FROM dividends WHERE symbol = $1 ORDER BY ex_date ASC", symbol
        )
        return pd.DataFrame([dict(row) for row in rows])

async def insert_adjusted_prices(pool, df, symbol):
    if df.empty:
        print(f"No data for {symbol}")
        return
    async with pool.acquire() as conn:
        await conn.execute(CREATE_DAILY_ADJUSTED_PRICES_SQL)
        await conn.executemany(
            """
            INSERT INTO daily_adjusted_prices (
                date, symbol, open, high, low, close, volume, market_cap,
                original_open, original_high, original_low, original_close,
                split_numerator, split_denominator, dividend_amount, adjustment_factor
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12,
                $13, $14, $15, $16
            ) ON CONFLICT DO NOTHING
            """,
            [(
                row['date'], row['symbol'], row['adj_open'], row['adj_high'], row['adj_low'], row['adj_close'],
                row['volume'], row.get('market_cap'),
                row['open'], row['high'], row['low'], row['close'],
                row.get('split_numerator'), row.get('split_denominator'), row.get('dividend_amount'), row['adjustment_factor']
            ) for _, row in df.iterrows()]
        )
    print(f"Inserted {len(df)} adjusted rows for {symbol}")

def calculate_adjusted_prices(prices_df, splits_df, dividends_df):
    df = prices_df.copy()
    df['adj_open'] = df['open']
    df['adj_high'] = df['high']
    df['adj_low'] = df['low']
    df['adj_close'] = df['close']
    df['split_numerator'] = None
    df['split_denominator'] = None
    df['dividend_amount'] = 0.0
    df['adjustment_factor'] = 1.0
    # Prepare splits
    splits = splits_df.set_index('split_date') if not splits_df.empty else pd.DataFrame()
    dividends = dividends_df.set_index('ex_date') if not dividends_df.empty else pd.DataFrame()
    # Calculate cumulative adjustment factor for each date
    adj_factor = 1.0
    adj_factors = []
    for idx, row in df.iterrows():
        date = row['date']
        # Split adjustment
        if not splits.empty and date in splits.index:
            s = splits.loc[date]
            if isinstance(s, pd.DataFrame) or (isinstance(s, pd.Series) and hasattr(s, '__len__') and len(s.shape) > 0 and s.shape[0] > 1):
                numerators = s['numerator'] if isinstance(s, pd.DataFrame) else s['numerator']
                denominators = s['denominator'] if isinstance(s, pd.DataFrame) else s['denominator']
                ratio = (denominators.astype(float) / numerators.astype(float)).prod()
                numerator = numerators.prod()
                denominator = denominators.prod()
            else:
                ratio = float(s['denominator']) / float(s['numerator'])
                numerator = s['numerator'] if not isinstance(s, pd.DataFrame) else s['numerator'].iloc[0]
                denominator = s['denominator'] if not isinstance(s, pd.DataFrame) else s['denominator'].iloc[0]
            adj_factor *= ratio
            df.at[idx, 'split_numerator'] = numerator
            df.at[idx, 'split_denominator'] = denominator
        # Dividend adjustment
        if not dividends.empty and date in dividends.index:
            d = dividends.loc[date]
            # If multiple dividends on the same date, sum the amounts
            if isinstance(d, pd.DataFrame) or (isinstance(d, pd.Series) and hasattr(d, '__len__') and len(d.shape) > 0 and d.shape[0] > 1):
                amount = d['amount'].sum()
            else:
                amount = d['amount'] if not isinstance(d, pd.DataFrame) else d['amount'].iloc[0]
            if row['close'] > 0:
                adj_factor *= (row['close'] - amount) / row['close']
            df.at[idx, 'dividend_amount'] = amount
        adj_factors.append(adj_factor)
    df['adjustment_factor'] = adj_factors
    # Apply adjustment to all prior prices (back-adjusted)
    for col in ['open', 'high', 'low', 'close']:
        df[f'adj_{col}'] = df[col] * df['adjustment_factor']
    return df

async def get_all_symbols(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT symbol FROM daily_prices")
        return [row['symbol'] for row in rows]

async def main():
    symbols = None
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--ticker', type=str, default=None, help='Process only this ticker (optional)')
    args = parser.parse_args()
    pool = await asyncpg.create_pool(TSDB_URL)
    if args.ticker:
        symbols = [args.ticker]
    else:
        symbols = await get_all_symbols(pool)
    for symbol in symbols:
        print(f"Processing {symbol}...")
        prices = await fetch_prices(pool, symbol)
        if prices.empty:
            print(f"No prices for {symbol}")
            continue
        print(f"Fetched columns for {symbol}: {list(prices.columns)}")
        if 'open' not in prices.columns:
            raise ValueError(f"Column 'open' not found in prices DataFrame. Columns: {list(prices.columns)}")
        splits = await fetch_splits(pool, symbol)
        dividends = await fetch_dividends(pool, symbol)
        adj_df = calculate_adjusted_prices(prices, splits, dividends)
        await insert_adjusted_prices(pool, adj_df, symbol)
    await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
