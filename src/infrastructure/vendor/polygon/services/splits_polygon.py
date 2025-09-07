import asyncio
import requests
from datetime import datetime, date
from infrastructure.database.repositories.stock_splits_polygon_dao import StockSplitsPolygonDAO


# get_all_spy_tickers is obsolete, use InstrumentPolygonDAO.get_all_symbols instead.

def fetch_splits_polygon(ticker, api_key):
    url = f"https://api.polygon.io/v3/reference/splits?ticker={ticker}&apiKey={api_key}"
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Failed to fetch splits for {ticker}: {resp.status_code} {resp.text}")
        return []
    data = resp.json()
    return data.get('results', [])

async def insert_splits_polygon(splits, ticker, dao=None):
    if dao is None:
        
        from infrastructure.database.repositories.stock_splits_polygon_dao import StockSplitsPolygonDAO
        env = Environment()
        dao = StockSplitsPolygonDAO(env)
    if not splits:
        return
    for split in splits:
        def parse_date(val):
            if isinstance(val, date):
                return val
            if val is None:
                return None
            return datetime.strptime(val, "%Y-%m-%d").date()
        split_row = {
            'symbol': ticker,
            'execution_date': parse_date(split.get('execution_date')),
            'split_from': split.get('split_from'),
            'split_to': split.get('split_to'),
            'cash_amount': split.get('cash_amount'),
            'declaration_date': parse_date(split.get('declaration_date')),
            'payment_date': parse_date(split.get('payment_date')),
            'record_date': parse_date(split.get('record_date')),
            'description': split.get('description'),
            'refid': split.get('refid'),
        }
        if split_row['execution_date'] and split_row['split_from'] is not None and split_row['split_to'] is not None:
            await dao.insert_split(split_row)

from vendor.polygon.dao.instrument_polygon_dao import InstrumentPolygonDAO
import argparse

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--environment', type=str, default='intg', choices=['test', 'intg', 'prod'], help='Environment to use (test, intg, prod)')
    parser.add_argument('--start_ticker', type=str, default='', help='Only process tickers > start_ticker (lexical order)')
    args = parser.parse_args()
    
    env = Environment()
    api_key = env.get_api_key('polygon')
    if not api_key:
        raise Exception("Please set your POLYGON_API_KEY in your environment or config.")
    instrument_dao = InstrumentPolygonDAO(env)
    splits_dao = StockSplitsPolygonDAO(env)
    tickers = await instrument_dao.get_all_symbols()
    start_ticker = args.start_ticker
    if start_ticker:
        tickers = [t for t in tickers if t > start_ticker]
    for ticker in tickers:
        print(f"Processing splits for {ticker}...")
        splits = fetch_splits_polygon(ticker, api_key)
        await insert_splits_polygon(splits, ticker, splits_dao)
        print(f"Inserted splits for {ticker}")

if __name__ == "__main__":
    asyncio.run(main())
