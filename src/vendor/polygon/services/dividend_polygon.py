import asyncio
import requests
from datetime import datetime, date
from infrastructure.database.repositories.dividend_polygon_dao import DividendPolygonDAO


# get_all_spy_tickers is obsolete, use InstrumentPolygonDAO.get_all_symbols instead.

def fetch_dividends_polygon(ticker, api_key):
    url = f"https://api.polygon.io/v3/reference/dividends?ticker={ticker}&apiKey={api_key}"
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Failed to fetch dividends for {ticker}: {resp.status_code} {resp.text}")
        return []
    data = resp.json()
    return data.get('results', [])

async def insert_dividends_polygon(dividends, ticker, dao=None):
    if dao is None:
        
        from infrastructure.database.repositories.dividend_polygon_dao import DividendPolygonDAO
        env = Environment()
        dao = DividendPolygonDAO(env)
    if not dividends:
        return
    for div in dividends:
        # Convert date fields to datetime.date if they are strings
        def parse_date(val):
            if isinstance(val, date):
                return val
            if val is None:
                return None
            return datetime.strptime(val, "%Y-%m-%d").date()
        dividend_row = {
            'symbol': ticker,
            'ex_dividend_date': parse_date(div.get('ex_dividend_date')),
            'cash_amount': div.get('cash_amount'),
            'declaration_date': parse_date(div.get('declaration_date')),
            'payment_date': parse_date(div.get('payment_date')),
            'record_date': parse_date(div.get('record_date')),
            'description': div.get('description'),
            'refid': div.get('refid'),
        }
        if dividend_row['ex_dividend_date'] and dividend_row['cash_amount'] is not None:
            await dao.insert_dividend(dividend_row)

from vendor.polygon.dao.instrument_polygon_dao import InstrumentPolygonDAO
from shared.utils.environment import Environment, EnvironmentType
import argparse

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--environment', type=str, default='intg', choices=['test', 'intg', 'prod'], help='Environment to use (test, intg, prod)')
    parser.add_argument('--start_ticker', type=str, default='', help='Only process tickers > start_ticker (lexical order)')
    args = parser.parse_args()
    env = Environment(env_type=EnvironmentType(args.environment))
    api_key = env.get_api_key('polygon')
    if not api_key:
        raise Exception("Please set your POLYGON_API_KEY in your environment or config.")
    instrument_dao = InstrumentPolygonDAO(env)
    dividend_dao = DividendPolygonDAO(env)
    tickers = await instrument_dao.get_all_symbols()
    start_ticker = args.start_ticker
    if start_ticker:
        tickers = [t for t in tickers if t > start_ticker]
    for ticker in tickers:
        print(f"Processing dividends for {ticker}...")
        dividends = fetch_dividends_polygon(ticker, api_key)
        await insert_dividends_polygon(dividends, ticker, dividend_dao)
        print(f"Inserted dividends for {ticker}")

if __name__ == "__main__":
    asyncio.run(main())
