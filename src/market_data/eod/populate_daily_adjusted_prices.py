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

async def fetch_prices(pool, symbol=None, instrument_id=None):
    async with pool.acquire() as conn:
        if instrument_id is not None:
            rows = await conn.fetch(
                "SELECT * FROM daily_prices WHERE instrument_id = $1 ORDER BY date ASC", instrument_id
            )
        elif symbol is not None:
            rows = await conn.fetch(
                "SELECT * FROM daily_prices WHERE symbol = $1 ORDER BY date ASC", symbol
            )
        else:
            raise ValueError("Must provide symbol or instrument_id")
        return pd.DataFrame([dict(row) for row in rows])

async def resolve_instrument_id(conn, symbol, vendor_id=None, at_date=None):
    q = "SELECT instrument_id FROM instrument_xref WHERE symbol = $1"
    params = [symbol]
    if vendor_id is not None:
        q += " AND vendor_id = $2"
        params.append(vendor_id)
    if at_date is not None:
        if vendor_id is not None:
            q += " AND (start_at <= $3 AND (end_at IS NULL OR end_at >= $3))"
            params.append(at_date)
        else:
            q += " AND (start_at <= $2 AND (end_at IS NULL OR end_at >= $2))"
            params.append(at_date)
    q += " ORDER BY start_at DESC LIMIT 1"
    row = await conn.fetchrow(q, *params)
    if not row:
        raise ValueError(f"No instrument_id found for symbol={symbol}, vendor_id={vendor_id}, at_date={at_date}")
    return row['instrument_id']

async def fetch_splits(pool, symbol=None, instrument_id=None):
    async with pool.acquire() as conn:
        if instrument_id is not None:
            rows = await conn.fetch(
                "SELECT split_date, numerator, denominator FROM stock_splits WHERE instrument_id = $1 ORDER BY split_date ASC", instrument_id
            )
        elif symbol is not None:
            rows = await conn.fetch(
                "SELECT split_date, numerator, denominator FROM stock_splits WHERE symbol = $1 ORDER BY split_date ASC", symbol
            )
        else:
            raise ValueError("Must provide symbol or instrument_id")
        return pd.DataFrame([dict(row) for row in rows])

async def fetch_dividends(pool, symbol=None, instrument_id=None):
    async with pool.acquire() as conn:
        if instrument_id is not None:
            rows = await conn.fetch(
                "SELECT ex_date, amount FROM dividends WHERE instrument_id = $1 ORDER BY ex_date ASC", instrument_id
            )
        elif symbol is not None:
            rows = await conn.fetch(
                "SELECT ex_date, amount FROM dividends WHERE symbol = $1 ORDER BY ex_date ASC", symbol
            )
        else:
            raise ValueError("Must provide symbol or instrument_id")
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
                # Multiple splits on same date
                for _, split in s.iterrows():
                    adj_factor *= split['denominator'] / split['numerator']
                    row['split_numerator'] = split['numerator']
                    row['split_denominator'] = split['denominator']
            else:
                adj_factor *= s['denominator'] / s['numerator']
                row['split_numerator'] = s['numerator']
                row['split_denominator'] = s['denominator']
        # Dividend adjustment
        if not dividends.empty and date in dividends.index:
            d = dividends.loc[date]
            if isinstance(d, pd.DataFrame) or (isinstance(d, pd.Series) and hasattr(d, '__len__') and len(d.shape) > 0 and d.shape[0] > 1):
                for _, div in d.iterrows():
                    row['dividend_amount'] = div['amount']
                    adj_factor *= (row['adj_close'] - div['amount']) / row['adj_close']
            else:
                row['dividend_amount'] = d['amount']
                adj_factor *= (row['adj_close'] - d['amount']) / row['adj_close']
        row['adjustment_factor'] = adj_factor
        row['adj_open'] *= adj_factor
        row['adj_high'] *= adj_factor
        row['adj_low'] *= adj_factor
        row['adj_close'] *= adj_factor
        adj_factors.append(adj_factor)
    return df
