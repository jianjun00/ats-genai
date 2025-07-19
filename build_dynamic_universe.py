import asyncpg
import pandas as pd
import asyncio
from datetime import datetime, timedelta
import os

TSDB_URL = os.getenv('TSDB_URL', 'postgresql://localhost:5432/trading_db')
ROLLING_DAYS = 30
MIN_CLOSE = 5
MIN_AVG_VOLUME = 50000000
MIN_MARKET_CAP = 500000000
OUT_CSV = 'dynamic_universe.csv'

QUERY = f"""
SELECT
    p.date,
    p.symbol,
    p.close,
    p.volume,
    m.market_cap
FROM daily_prices p
JOIN daily_market_cap m ON p.date = m.date AND p.symbol = m.symbol
WHERE p.date >= $1 AND p.date <= $2
ORDER BY p.symbol, p.date
"""

async def fetch_data():
    start_date = (datetime.utcnow() - timedelta(days=365*10)).date()
    end_date = datetime.utcnow().date()
    pool = await asyncpg.create_pool(TSDB_URL, min_size=1, max_size=4)
    async with pool.acquire() as conn:
        rows = await conn.fetch(QUERY, start_date, end_date)
    await pool.close()
    df = pd.DataFrame(rows, columns=['date', 'symbol', 'close', 'volume', 'market_cap'])
    return df

def compute_membership_periods(df):
    # Ensure correct types
    df['date'] = pd.to_datetime(df['date']).dt.date
    df = df.sort_values(['symbol', 'date'])
    universe_periods = []
    for symbol, sdf in df.groupby('symbol'):
        sdf = sdf.reset_index(drop=True)
        sdf['avg_volume'] = sdf['volume'].rolling(ROLLING_DAYS, min_periods=1).mean()
        in_universe = False
        eff_date = None
        for i, row in sdf.iterrows():
            qualifies = (
                row['close'] > MIN_CLOSE and
                row['avg_volume'] > MIN_AVG_VOLUME and
                row['market_cap'] > MIN_MARKET_CAP
            )
            if qualifies and not in_universe:
                eff_date = row['date']
                in_universe = True
            elif not qualifies and in_universe:
                universe_periods.append({'symbol': symbol, 'effective_date': eff_date, 'removal_date': row['date']})
                in_universe = False
        if in_universe:
            universe_periods.append({'symbol': symbol, 'effective_date': eff_date, 'removal_date': None})
    return pd.DataFrame(universe_periods)

async def main():
    df = await fetch_data()
    periods_df = compute_membership_periods(df)
    periods_df = periods_df.sort_values(['symbol', 'effective_date']).reset_index(drop=True)
    periods_df.to_csv(OUT_CSV, index=False, date_format='%Y-%m-%d')
    print(f"Wrote dynamic universe membership to {OUT_CSV} ({len(periods_df)} periods)")

if __name__ == '__main__':
    asyncio.run(main())
