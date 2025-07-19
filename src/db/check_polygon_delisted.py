import os
import requests
from datetime import datetime

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
REFERENCE_URL = "https://api.polygon.io/v3/reference/tickers?ticker={ticker}&apiKey={api_key}"
PRICE_URL = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}"

def check_ticker_polygon(ticker, start_date, end_date):
    # Check reference metadata
    ref_url = REFERENCE_URL.format(ticker=ticker, api_key=POLYGON_API_KEY)
    ref_resp = requests.get(ref_url)
    if ref_resp.status_code != 200:
        print(f"Reference API error for {ticker}: {ref_resp.status_code} {ref_resp.text}")
        return
    ref_data = ref_resp.json()
    if not ref_data.get('results'):
        print(f"Ticker {ticker} not found in Polygon reference API.")
        return
    status = ref_data['results'][0].get('active', None)
    print(f"Ticker {ticker} status: {'active' if status else 'delisted or inactive'}")
    # Check price data
    price_url = PRICE_URL.format(ticker=ticker, start=start_date, end=end_date, api_key=POLYGON_API_KEY)
    price_resp = requests.get(price_url)
    if price_resp.status_code != 200:
        print(f"Price API error for {ticker}: {price_resp.status_code} {price_resp.text}")
        return
    price_data = price_resp.json()
    results = price_data.get('results', [])
    print(f"Ticker {ticker} daily price bars found: {len(results)}")
    if len(results) == 0:
        print(f"No price data for {ticker} in Polygon for given date range.")
    else:
        print(f"First date: {datetime.utcfromtimestamp(results[0]['t']/1000).date()} | Last date: {datetime.utcfromtimestamp(results[-1]['t']/1000).date()}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--ticker', type=str, required=True, help='Ticker symbol to check (e.g., YHOO)')
    parser.add_argument('--start', type=str, default='2010-01-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, default=datetime.now().strftime('%Y-%m-%d'), help='End date (YYYY-MM-DD)')
    args = parser.parse_args()
    if not POLYGON_API_KEY:
        print("Please set your POLYGON_API_KEY environment variable.")
    else:
        check_ticker_polygon(args.ticker, args.start, args.end)
