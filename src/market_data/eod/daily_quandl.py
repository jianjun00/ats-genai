import os
import asyncio
from src.config.environment import get_environment, set_environment, EnvironmentType
from market_data.eod.daily_prices_quandl_dao import DailyPricesQuandlDAO
import requests
from datetime import datetime, timedelta
import time
import argparse

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")
QUANDL_API_KEY = os.getenv("QUANDL_API_KEY")  # Set this in your environment
START_DATE = (datetime.now() - timedelta(days=365*10)).strftime("%Y-%m-%d")
END_DATE = datetime.now().strftime("%Y-%m-%d")

# Example: Sharadar US Daily Prices (SFP) dataset on Quandl
# https://data.nasdaq.com/data/SF1
QUANDL_DATASET = "SHARADAR/SFP"  # Change as needed

async def get_all_spy_tickers():
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT added FROM spy_membership_change WHERE added IS NOT NULL")
        removed = await conn.fetch("SELECT DISTINCT removed FROM spy_membership_change WHERE removed IS NOT NULL")
    await pool.close()
    tickers = set([row['added'] for row in rows] + [row['removed'] for row in removed])
    tickers.discard(None)
    return sorted(tickers)

def download_prices_quandl(ticker, start, end, api_key):
    url = f"https://data.nasdaq.com/api/v3/datasets/{QUANDL_DATASET}.json?api_key={api_key}&ticker={ticker}&start_date={start}&end_date={end}&order=asc"
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Failed to fetch {ticker}: {resp.status_code} {resp.text}")
        return []
    data = resp.json()
    dataset = data.get('dataset', {})
    if not dataset or not dataset.get('data'):
        print(f"No results for {ticker}: {data}")
        return []
    columns = dataset['column_names']
    idx = {col: i for i, col in enumerate(columns)}
    prices = []
    for row in dataset['data']:
        prices.append({
            'date': datetime.strptime(row[idx['date']], "%Y-%m-%d").date(),
            'open': row[idx.get('open', 0)],
            'high': row[idx.get('high', 0)],
            'low': row[idx.get('low', 0)],
            'close': row[idx.get('close', 0)],
            'volume': row[idx.get('volume', 0)],
        })
    return prices

CREATE_DAILY_PRICES_QUANDL_SQL = """
CREATE TABLE IF NOT EXISTS daily_prices_quandl (
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

async def insert_prices(dao: DailyPricesQuandlDAO, prices, ticker):
    if not prices:
        return
    await dao.batch_insert_prices(prices, ticker)

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ticker', type=str, default=None, help='Process only this ticker (optional)')
    parser.add_argument('--start', type=str, default=START_DATE, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, default=END_DATE, help='End date (YYYY-MM-DD)')
    parser.add_argument('--environment', type=str, default='intg', choices=['test', 'intg', 'prod'], help='Environment to use (test, intg, prod)')
    args = parser.parse_args()
    set_environment(EnvironmentType(args.environment))
    env = get_environment()
    dao = DailyPricesQuandlDAO(env)
    if not QUANDL_API_KEY:
        raise Exception("Please set your QUANDL_API_KEY environment variable.")
    if args.ticker:
        tickers = [args.ticker]
    else:
        tickers = await get_all_spy_tickers()
    for ticker in tickers:
        print(f"Downloading {ticker} from Quandl...")
        try:
            prices = download_prices_quandl(ticker, args.start, args.end, QUANDL_API_KEY)
            await insert_prices(dao, prices, ticker)
            print(f"Inserted {len(prices)} rows for {ticker}")
            time.sleep(1.0)  # Be gentle to API
        except Exception as e:
            print(f"Error with {ticker}: {e}")

if __name__ == "__main__":
    asyncio.run(main())
