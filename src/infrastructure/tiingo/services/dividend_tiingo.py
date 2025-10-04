import argparse
import requests
from datetime import datetime, date
from core.platform.config_env.environment import Environment, EnvironmentType
from infrastructure.database.repositories.dividend_tiingo_dao import DividendTiingoDAO


def parse_date(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    return datetime.strptime(val, "%Y-%m-%d").date()


def fetch_tiingo_dividends(symbol, api_key, start_date, end_date):
    url = f"https://api.tiingo.com/iex/{symbol}/dividends?startDate={start_date}&endDate={end_date}"
    headers = {"Authorization": f"Token {api_key}"}
    print(f"[DEBUG] Requesting Tiingo dividends: {url}")
    resp = requests.get(url, headers=headers)
    print(f"[DEBUG] Response status: {resp.status_code}")
    if resp.status_code == 404:
        print(f"No dividend data found for symbol {symbol} (404 Not Found)")
        return []
    if resp.status_code != 200:
        print(f"Failed to fetch dividends for {symbol}: {resp.status_code} {resp.text}")
        return []
    return resp.json()


def map_tiingo_dividend(div):
    def pd(val):
        return parse_date(div.get(val))
    return {
        'symbol': div.get('ticker') or div.get('symbol'),
        'ex_dividend_date': pd('exDate'),
        'cash_amount': div.get('amount'),
        'declaration_date': pd('declaredDate'),
        'payment_date': pd('paymentDate'),
        'record_date': pd('recordDate'),
        'description': div.get('description'),
        'refid': div.get('id'),
        'qualified': div.get('qualified'),
        'flag': div.get('flag'),
        'currency': div.get('currency'),
        'frequency': div.get('frequency'),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--environment', type=str, default='intg', choices=['test', 'intg', 'prod'], help='Environment to use (test, intg, prod)')
    parser.add_argument('--ticker', type=str, required=True, help='Ticker symbol (e.g., AAPL)')
    parser.add_argument('--start_date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='End date (YYYY-MM-DD)')
    args = parser.parse_args()
    env = Environment(env_type=EnvironmentType(args.environment))
    api_key = env.get_api_key('tiingo')
    if not api_key:
        raise Exception("Please set your TIINGO_API_KEY in your environment or config.")
    dao = DividendTiingoDAO(env)
    divs = fetch_tiingo_dividends(args.ticker, api_key, args.start_date, args.end_date)
    inserted = 0
    for div in divs:
        mapped = map_tiingo_dividend(div)
        if mapped['symbol'] and mapped['ex_dividend_date'] and mapped['cash_amount'] is not None:
            dao.insert_dividend_sync(mapped)  # Use sync insert for CLI
            inserted += 1
    print(f"Inserted {inserted} dividends for {args.ticker}.")


if __name__ == "__main__":
    main()
