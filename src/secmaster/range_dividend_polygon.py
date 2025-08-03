import argparse
import asyncio
import requests
from datetime import datetime, date, timedelta
from config.environment import Environment, EnvironmentType
from dao.dividend_polygon_dao import DividendPolygonDAO

def fetch_dividends_polygon(start_date, end_date, api_key):
    url = (
        f"https://api.polygon.io/v3/reference/dividends?ex_dividend_date.gte={start_date}" \
        f"&ex_dividend_date.lte={end_date}&apiKey={api_key}"
    )
    print(f"[DEBUG] Requesting Polygon dividends: {url}")
    resp = requests.get(url)
    print(f"[DEBUG] Response status: {resp.status_code}")
    print(f"[DEBUG] Response headers: {dict(resp.headers)}")
    print(f"[DEBUG] Response body (first 500 chars): {resp.text[:500]}")
    if resp.status_code != 200:
        print(f"Failed to fetch dividends: {resp.status_code} {resp.text}")
        return []
    data = resp.json()
    return data.get('results', [])

def parse_date(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    return datetime.strptime(val, "%Y-%m-%d").date()

def date_chunks(start_date, end_date, chunk_days=5):
    start = parse_date(start_date)
    end = parse_date(end_date)
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days-1), end)
        yield current.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d')
        current = chunk_end + timedelta(days=1)

async def insert_dividends_polygon(dividends, dao):
    if not dividends:
        print("No dividends to insert.")
        return
    inserted = 0
    for div in dividends:
        def pd(val):
            return parse_date(div.get(val))
        dividend_row = {
            'symbol': div.get('ticker'),
            'ex_dividend_date': pd('ex_dividend_date'),
            'cash_amount': div.get('cash_amount'),
            'declaration_date': pd('declaration_date'),
            'payment_date': pd('payment_date'),
            'record_date': pd('record_date'),
            'description': div.get('description'),
            'refid': div.get('refid'),
        }
        if dividend_row['symbol'] and dividend_row['ex_dividend_date'] and dividend_row['cash_amount'] is not None:
            await dao.insert_dividend(dividend_row)
            inserted += 1
    print(f"Inserted {inserted} dividends.")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--environment', type=str, default='intg', choices=['test', 'intg', 'prod'], help='Environment to use (test, intg, prod)')
    parser.add_argument('--start_date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='End date (YYYY-MM-DD)')
    args = parser.parse_args()
    env = Environment(env_type=EnvironmentType(args.environment))
    api_key = env.get_api_key('polygon')
    if not api_key:
        raise Exception("Please set your POLYGON_API_KEY in your environment or config.")
    all_dividends = []
    import time
    last_request_time = None
    for chunk_start, chunk_end in date_chunks(args.start_date, args.end_date, chunk_days=5):
        now = time.monotonic()
        if last_request_time is not None:
            elapsed = now - last_request_time
            if elapsed < 0.2:
                await asyncio.sleep(0.2 - elapsed)
        print(f"[DEBUG] Fetching dividends for {chunk_start} to {chunk_end}")
        chunk_divs = fetch_dividends_polygon(chunk_start, chunk_end, api_key)
        last_request_time = time.monotonic()
        print(f"[DEBUG] Fetched {len(chunk_divs)} dividends for {chunk_start} to {chunk_end}")
        all_dividends.extend(chunk_divs)
    print(f"Fetched {len(all_dividends)} total dividends from Polygon API.")
    div_dao = DividendPolygonDAO(env)
    await insert_dividends_polygon(all_dividends, div_dao)

if __name__ == "__main__":
    asyncio.run(main())
