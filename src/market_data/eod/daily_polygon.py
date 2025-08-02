import os
import asyncio
import requests
from datetime import datetime, timedelta
import time

from config.environment import get_environment, set_environment, EnvironmentType
from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
import asyncpg
import argparse

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")  # Set this in your environment
BASE_URL = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}"
START_DATE = (datetime.now() - timedelta(days=365*10)).strftime("%Y-%m-%d")
END_DATE = datetime.now().strftime("%Y-%m-%d")

from dao.instrument_polygon_dao import InstrumentPolygonDAO

async def get_all_polygon_tickers(env):
    instrument_dao = InstrumentPolygonDAO(env)
    return await instrument_dao.get_all_symbols()

def download_prices_polygon(ticker, start, end, api_key):
    url = BASE_URL.format(ticker=ticker, start=start, end=end, api_key=api_key)
    resp = requests.get(url)
    # Log request/response for AAPL/TSLA in date range
    from datetime import datetime
    import json, os
    log_tickers = {"AAPL", "TSLA"}
    log_start = datetime(2020, 1, 10)
    log_end = datetime(2024, 12, 31)
    def in_log_range(s, e):
        try:
            sdt = datetime.strptime(str(s), "%Y-%m-%d")
            edt = datetime.strptime(str(e), "%Y-%m-%d")
            return not (edt < log_start or sdt > log_end)
        except Exception:
            return False
    if ticker.upper() in log_tickers and in_log_range(start, end):
        os.makedirs("test/data", exist_ok=True)
        req_path = f"test/data/polygon_{ticker.lower()}_{start}_{end}_request.json"
        resp_path = f"test/data/polygon_{ticker.lower()}_{start}_{end}_response.json"
        with open(req_path, "w") as f:
            json.dump({"url": url}, f, indent=2)
        try:
            with open(resp_path, "w") as f:
                json.dump(resp.json(), f, indent=2)
        except Exception as e:
            with open(resp_path, "w") as f:
                f.write(f"[ERROR] Could not serialize response: {e}\n")
    if resp.status_code != 200:
        print(f"Failed to fetch {ticker}: {resp.status_code} {resp.text}")
        return []
    data = resp.json()
    if 'results' not in data:
        print(f"No results for {ticker}: {data}")
        return []
    return data['results']


async def insert_prices(prices, instrument_id, shares_outstanding, dao: DailyPricesPolygonDAO):
    if not prices:
        return
    for row in prices:
        date_val = datetime.utcfromtimestamp(row['t']/1000).date()
        print(f"[DEBUG] insert_prices date_val type: {type(date_val)}, value: {date_val}")
        await dao.insert_price(
            date=date_val,
            instrument_id=instrument_id,
            open_=row['o'],
            high=row['h'],
            low=row['l'],
            close=row['c'],
            volume=row['v'],
            market_cap=(row['c'] * shares_outstanding if shares_outstanding else None)
        )

import argparse

from calendars.exchange_calendar import ExchangeCalendar

async def get_existing_dates_polygon(dao: DailyPricesPolygonDAO, instrument_id, start_date, end_date):
    all_prices = await dao.list_prices(instrument_id)
    return set(row['date'] for row in all_prices if start_date <= row['date'] <= end_date)

def group_contiguous_dates(dates):
    # Given a sorted list of dates, group into contiguous ranges
    if not dates:
        return []
    dates = sorted(dates)
    ranges = []
    range_start = dates[0]
    prev = dates[0]
    for d in dates[1:]:
        if (d - prev).days > 1:
            ranges.append((range_start, prev))
            range_start = d
        prev = d
    ranges.append((range_start, prev))
    return ranges

async def run_ingestion(tickers, start_date, end_date, environment=None, instrument_dao=None, prices_dao=None, polygon_api_key=None, xrefs_dao=None):
    print(f"[DEBUG] run_ingestion start_date type: {type(start_date)}, value: {start_date}")
    print(f"[DEBUG] run_ingestion end_date type: {type(end_date)}, value: {end_date}")
    if environment:
        set_environment(EnvironmentType(environment))
    env = get_environment()
    if not polygon_api_key:
        polygon_api_key = os.getenv("POLYGON_API_KEY")
    if not polygon_api_key:
        raise Exception("Please set your POLYGON_API_KEY environment variable.")
    if instrument_dao is None:
        instrument_dao = InstrumentPolygonDAO(env)
    if prices_dao is None:
        prices_dao = DailyPricesPolygonDAO(env)
    if xrefs_dao is None:
        from dao.instrument_xrefs_dao import InstrumentXrefsDAO
        xrefs_dao = InstrumentXrefsDAO(env)
    nyse_cal = ExchangeCalendar('NYSE')
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
    trading_days = set(nyse_cal.all_trading_days(start_date, end_date))
    for ticker in tickers:
        print(f"Processing {ticker}...")
        try:
            instrument_id = await xrefs_dao.resolve_instrument_id(ticker)
            if not instrument_id:
                print(f"[WARN] No instrument_id for {ticker}, skipping.")
                continue
            # Fetch shares outstanding
            url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={polygon_api_key}"
            resp = requests.get(url)
            if resp.status_code == 200:
                ref_data = resp.json()
                shares_outstanding = ref_data.get('results', {}).get('share_class_shares_outstanding', None)
            else:
                print(f"Failed to fetch shares outstanding for {ticker}: {resp.status_code} {resp.text}")
                shares_outstanding = None
            existing_dates = await get_existing_dates_polygon(prices_dao, instrument_id, start_date, end_date)
            print(f"[DEBUG] trading_days: {sorted(trading_days)}")
            print(f"[DEBUG] existing_dates: {sorted(existing_dates)}")
            missing_days = [d for d in trading_days if d not in existing_dates]
            print(f"[DEBUG] missing_days: {sorted(missing_days)}")
            if not missing_days:
                print(f"[DEBUG] All data exists for {ticker} in {start_date} to {end_date}, skipping fetch.")
                continue
            # Group into contiguous ranges for efficient API calls
            ranges = group_contiguous_dates(missing_days)
            total_inserted = 0
            for range_start, range_end in ranges:
                prices = download_prices_polygon(ticker, range_start.strftime("%Y-%m-%d"), range_end.strftime("%Y-%m-%d"), polygon_api_key)
                print(f"[DEBUG] prices returned: {prices}")
                for row in prices:
                    row_date = datetime.utcfromtimestamp(row['t']/1000).date()
                    print(f"[DEBUG] price row date: {row_date}, in missing_days: {row_date in missing_days}")
                filtered_prices = [row for row in prices if datetime.utcfromtimestamp(row['t']/1000).date() in missing_days]
                print(f"[DEBUG] filtered_prices: {filtered_prices}")
                print(f"[DEBUG] instrument_id used for insert: {instrument_id}")
                await insert_prices(filtered_prices, instrument_id, shares_outstanding, prices_dao)
                total_inserted += len(filtered_prices)
                print(f"Inserted {len(filtered_prices)} rows for {ticker} from {range_start} to {range_end}")
                time.sleep(0.8)
        except Exception as e:
            print(f"Error with {ticker}: {e}")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--ticker', type=str, default=None, help='Process only this ticker (optional, maps to instrument_id)')
    parser.add_argument('--start_date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--environment', type=str, default='intg', choices=['test', 'intg', 'prod'], help='Environment to use (test, intg, prod)')
    args = parser.parse_args()
    set_environment(EnvironmentType(args.environment))
    env = get_environment()
    instrument_dao = InstrumentPolygonDAO(env)
    from dao.instrument_xrefs_dao import InstrumentXrefsDAO
    xrefs_dao = InstrumentXrefsDAO(env)
    if args.ticker:
        instrument_id = await xrefs_dao.resolve_instrument_id(args.ticker)
        if instrument_id is None:
            print(f"[ERROR] Could not resolve instrument_id for ticker {args.ticker}. Exiting.")
            return
        tickers = [args.ticker]
    else:
        tickers = await instrument_dao.get_all_symbols()
    await run_ingestion(tickers, args.start_date, args.end_date, args.environment, instrument_dao=instrument_dao, polygon_api_key=POLYGON_API_KEY)


if __name__ == "__main__":
    asyncio.run(main())
