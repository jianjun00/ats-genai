import os
import asyncio
import asyncpg
import requests
from datetime import datetime, timedelta

TSDB_URL = os.getenv("TSDB_URL", "postgresql://postgres:postgres@localhost:5432/trading_db")

START_DATE = (datetime.now() - timedelta(days=365*10)).strftime("%Y-%m-%d")
END_DATE = datetime.now().strftime("%Y-%m-%d")

# get_all_spy_tickers is obsolete, use InstrumentPolygonDAO.get_all_symbols instead.

def fetch_splits_polygon(ticker, api_key):
    url = f"https://api.polygon.io/v3/reference/splits?ticker={ticker}&apiKey={api_key}"
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Failed to fetch splits for {ticker}: {resp.status_code} {resp.text}")
        return []
    data = resp.json()
    return data.get('results', [])

def fetch_dividends_polygon(ticker, api_key):
    url = f"https://api.polygon.io/v3/reference/dividends?ticker={ticker}&apiKey={api_key}"
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Failed to fetch dividends for {ticker}: {resp.status_code} {resp.text}")
        return []
    data = resp.json()
    return data.get('results', [])

async def insert_splits(splits, ticker):
    if not splits:
        return
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO stock_splits (symbol, split_date, numerator, denominator) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
            [(
                ticker,
                datetime.strptime(split['execution_date'], "%Y-%m-%d").date(),
                float(split['split_from']),
                float(split['split_to'])
            ) for split in splits if split.get('execution_date') and split.get('split_from') and split.get('split_to')]
        )
    await pool.close()

async def insert_dividends(dividends, ticker):
    if not dividends:
        return
    pool = await asyncpg.create_pool(TSDB_URL)
    async with pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO dividends (symbol, ex_date, amount) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
            [(
                ticker,
                datetime.strptime(div['ex_dividend_date'], "%Y-%m-%d").date(),
                float(div['cash_amount'])
            ) for div in dividends if div.get('ex_dividend_date') and div.get('cash_amount')]
        )
    await pool.close()

from dao.instrument_polygon_dao import InstrumentPolygonDAO


async def main():
    env = Environment()
    api_key = env.get_api_key('polygon')
    if not api_key:
        raise Exception("Please set your POLYGON_API_KEY in your environment or config.")
    dao = InstrumentPolygonDAO(env)
    tickers = await dao.get_all_symbols()
    for ticker in tickers:
        print(f"Processing splits and dividends for {ticker}...")
        splits = fetch_splits_polygon(ticker, api_key)
        await insert_splits(splits, ticker)
        dividends = fetch_dividends_polygon(ticker, api_key)
        await insert_dividends(dividends, ticker)
        print(f"Inserted splits and dividends for {ticker}")

if __name__ == "__main__":
    asyncio.run(main())
