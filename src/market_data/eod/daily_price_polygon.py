import os
from venv import logger

import asyncio
import requests
import datetime as dt
import time

from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
import argparse

# POLYGON_API_KEY is now managed via Gin and set_polygon_api_key
BASE_URL = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}"
START_DATE = (dt.datetime.now() - dt.timedelta(days=365*10)).strftime("%Y-%m-%d")
END_DATE = dt.datetime.now().strftime("%Y-%m-%d")

from dao.instrument_polygon_dao import InstrumentPolygonDAO

async def get_all_polygon_tickers(env):
    instrument_dao = InstrumentPolygonDAO(env)
    return await instrument_dao.get_all_symbols()

def download_prices_polygon(ticker, start, end, api_key, logging=False, log_start=None, log_end=None, log_tickers=None, log_dir=None):
    url = BASE_URL.format(ticker=ticker, start=start, end=end, api_key=api_key)
    resp = requests.get(url)
    import json, os
    log_tickers = set(log_tickers) if log_tickers else set()
    def in_log_range(s, e):
        if log_start is None or log_end is None:
            return False
        try:
            sdt = dt.datetime.strptime(str(s), "%Y-%m-%d")
            edt = dt.datetime.strptime(str(e), "%Y-%m-%d")
            return not (edt < log_start or sdt > log_end)
        except Exception:
            return False
    if logging and ticker.upper() in log_tickers and in_log_range(start, end):
        log_dir = log_dir or "test/data/daily_prices_polygon"
        os.makedirs(log_dir, exist_ok=True)
        req_path = os.path.join(log_dir, f"polygon_{ticker.lower()}_{start}_{end}_request.json")
        resp_path = os.path.join(log_dir, f"polygon_{ticker.lower()}_{start}_{end}_response.json")
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


async def insert_prices(prices, instrument_id, shares_outstanding, dao: DailyPricesPolygonDAO, env=None):
    if env is None:
        from config.environment import Environment
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--gin_config', type=str, default='config/app.gin')
        known_args, _ = parser.parse_known_args()
        env = Environment(gin_config_path=getattr(known_args, 'gin_config', 'config/app.gin'))
    env.get_table_name('daily_prices_polygon')
    if not prices:
        return
    # Batch version for efficiency
    batch_rows = []
    for row in prices:
        date_val = dt.datetime.utcfromtimestamp(row['t']/1000).date()
        batch_rows.append({
            'date': date_val,
            'instrument_id': instrument_id,
            'open': row['o'],
            'high': row['h'],
            'low': row['l'],
            'close': row['c'],
            'volume': row['v'],
            'market_cap': (row['c'] * shares_outstanding if shares_outstanding else None)
        })
    if batch_rows:
        await dao.batch_insert_prices(batch_rows)


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

async def run_ingestion(tickers, start_date, end_date, environment=None, instrument_dao=None, prices_dao=None, polygon_api_key=None, xrefs_dao=None, logging=False, log_tickers=None, log_dir=None):
    """
    Run Polygon daily price ingestion in parallel using Ray.
    For each ticker, launches a Ray task to fetch and insert prices.
    Args and logic match previous run_ingestion, but ingestion is fully parallelized.
    """
    import ray
    from market_data.eod.daily_polygon_ray_utils import ray_ingest_polygon_instrument
    from config.environment import Environment
    import requests

    if environment:
        env = environment
    else:
        env = Environment()
    if not polygon_api_key:
        polygon_api_key = env.get_polygon_api_key() or os.getenv("POLYGON_API_KEY")
    if not polygon_api_key:
        raise Exception("Please set your POLYGON_API_KEY environment variable or Gin config.")
    if instrument_dao is None:
        instrument_dao = InstrumentPolygonDAO(env)
    if xrefs_dao is None:
        from dao.instrument_xrefs_dao import InstrumentXrefsDAO
        xrefs_dao = InstrumentXrefsDAO(env)
    ray.init(ignore_reinit_error=True)
    env_dict = env.__dict__
    ray_tasks = []
    for ticker in tickers:
        instrument_id = await xrefs_dao.resolve_instrument_id(ticker)
        if instrument_id is None:
            print(f"[ERROR] Could not resolve instrument_id for ticker {ticker}. Skipping.")
            continue
        # Fetch shares outstanding
        url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={polygon_api_key}"
        print(f"[DEBUG] Fetching shares outstanding for {ticker}: URL={url}, API_KEY={polygon_api_key}")
        resp = requests.get(url)
        if resp.status_code == 200:
            ref_data = resp.json()
            shares_outstanding = ref_data.get('results', {}).get('share_class_shares_outstanding', None)
        else:
            print(f"Failed to fetch shares outstanding for {ticker}: {resp.status_code} {resp.text}")
            shares_outstanding = None
        ray_tasks.append(ray_ingest_polygon_instrument.remote(
            env_dict, ticker, instrument_id, shares_outstanding, start_date, end_date, polygon_api_key, logging, log_tickers, log_dir
        ))
    results = ray.get(ray_tasks)
    print(f"[INFO] Ray Polygon ingestion complete. Results: {results}")
    ray.shutdown()
    return results

    if environment:
        env = environment
    else:
        env = Environment()
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
        start_date = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
    if isinstance(end_date, str):
        end_date = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
    trading_days = set(nyse_cal.all_trading_days(start_date, end_date))
    for ticker in tickers:
        print(f"Processing {ticker}...")
        try:
            if log_dir:
                print(f"[DEBUG] Early skip: log_dir set ({log_dir}), skipping DB insert for {ticker}.")
                # If log_dir is specified, skip filtering and DAO insertion, just log all requested data
                from datetime import datetime
                log_start = datetime.strptime(str(start_date), "%Y-%m-%d") if logging else None
                log_end = datetime.strptime(str(end_date), "%Y-%m-%d") if logging else None
                prices = download_prices_polygon(
                    ticker,
                    start_date.strftime("%Y-%m-%d"),
                    end_date.strftime("%Y-%m-%d"),
                    polygon_api_key,
                    logging=logging,
                    log_start=log_start,
                    log_end=log_end,
                    log_tickers=log_tickers,
                    log_dir=log_dir
                )
                print(f"[DEBUG] (log_dir set) prices returned: {prices}")
                continue
            instrument_id = await xrefs_dao.resolve_instrument_id(ticker)
            if not instrument_id:
                print(f"[DEBUG] Early skip: No instrument_id for {ticker}, skipping.")
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
                print(f"[DEBUG] Early skip: All data exists for {ticker} in {start_date} to {end_date}, skipping fetch.")
                continue
            # Group into contiguous ranges for efficient API calls
            ranges = group_contiguous_dates(missing_days)
            total_inserted = 0
            for range_start, range_end in ranges:
                from datetime import datetime
                log_start = datetime.strptime(str(start_date), "%Y-%m-%d") if logging else None
                log_end = datetime.strptime(str(end_date), "%Y-%m-%d") if logging else None
                prices = download_prices_polygon(
                    ticker,
                    range_start.strftime("%Y-%m-%d"),
                    range_end.strftime("%Y-%m-%d"),
                    polygon_api_key,
                    logging=logging,
                    log_start=log_start,
                    log_end=log_end,
                    log_tickers=log_tickers,
                    log_dir=log_dir
                )
                print(f"[DEBUG] prices returned: {prices}")
                for row in prices:
                    row_date = datetime.utcfromtimestamp(row['t']/1000).date()
                    print(f"[DEBUG] price row date: {row_date}, in missing_days: {row_date in missing_days}")
                filtered_prices = [row for row in prices if datetime.utcfromtimestamp(row['t']/1000).date() in missing_days]
                print(f"[DEBUG] filtered_prices: {filtered_prices}")
                print(f"[DEBUG] instrument_id used for insert: {instrument_id}")
                await insert_prices(filtered_prices, instrument_id, shares_outstanding, prices_dao, env=env)
                total_inserted += len(filtered_prices)
                print(f"Inserted {len(filtered_prices)} rows for {ticker} from {range_start} to {range_end}")
                time.sleep(0.8)
        except Exception as e:
            print(f"Error with {ticker}: {e}")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tickers', type=str, default=None, help='Comma-separated list of tickers to process and log (optional, maps to instrument_id)')
    parser.add_argument('--start_date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--environment', type=str, default='intg', choices=['test', 'intg', 'prod', 'dev'], help='Environment to use (test, intg, prod, dev)')
    parser.add_argument('--logging', action='store_true', help='Enable logging of Polygon API requests/responses for specified tickers in date range')
    parser.add_argument('--log_dir', type=str, default='test/data/daily_prices_polygon', help='Directory to store Polygon API logs (default: test/data/daily_prices_polygon)')
    parser.add_argument('--gin_config', type=str, default='config/app.gin', help='Path to Gin config file (default: config/app.gin)')
    args = parser.parse_args()

    print(f"[DEBUG] CLI arg --gin_config: {args.gin_config}")
    from config.environment import Environment
    env = Environment(gin_config_path=args.gin_config)
    print(f"[DEBUG] env.get_polygon_api_key(): {env.get_polygon_api_key()}")
    instrument_dao = InstrumentPolygonDAO(env)
    from dao.instrument_xrefs_dao import InstrumentXrefsDAO
    xrefs_dao = InstrumentXrefsDAO(env)
    # Determine tickers to process
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(',') if t.strip()]
    else:
        tickers = await instrument_dao.get_all_symbols()
    logger.info(f"[DEBUG] tickers: {tickers}")
    # Resolve instrument_ids in one pass
    ticker_to_instrument_id = {}
    missing = []
    for t in tickers:
        instrument_id = await xrefs_dao.resolve_instrument_id(t)
        logger.info(f"[DEBUG] instrument_id: {instrument_id}")
        if instrument_id is None:
            missing.append(t)
        else:
            ticker_to_instrument_id[t] = instrument_id
    if missing:
        print(f"[ERROR] Could not resolve instrument_id for tickers: {', '.join(missing)}. Skipping these.")
    if not ticker_to_instrument_id:
        print("[ERROR] No valid tickers with instrument_id found. Exiting.")
        return

    from dao.daily_prices_polygon_dao import DailyPricesPolygonDAO
    prices_dao = DailyPricesPolygonDAO(env)
    total_success = 0
    total_fail = 0
    for ticker, instrument_id in ticker_to_instrument_id.items():
        polygon_api_key = env.get_polygon_api_key() or os.getenv("POLYGON_API_KEY")
        if not polygon_api_key:
            print(f"[ERROR] No Polygon API key found in Gin config or environment. Please set 'polygon_api_key' in your Gin config or POLYGON_API_KEY env var.")
            continue
        try:
            print(f"[INFO] Processing {ticker} (instrument_id={instrument_id})...")
            prices = download_prices_polygon(
                ticker,
                args.start_date,
                args.end_date,
                polygon_api_key,
                logging=args.logging,
                log_tickers=tickers if args.tickers else None,
                log_dir=args.log_dir
            )
            if not prices:
                print(f"[WARN] No prices fetched for {ticker}")
                total_fail += 1
                continue
            await insert_prices(prices, instrument_id, None, prices_dao, env=env)
            print(f"[INFO] Inserted {len(prices)} prices for {ticker}")
            total_success += 1
        except Exception as e:
            print(f"[ERROR] Failed to process {ticker}: {e}")
            total_fail += 1
    print(f"[INFO] Polygon ingestion complete. Success: {total_success}, Failures: {total_fail}")


if __name__ == "__main__":
    asyncio.run(main())
