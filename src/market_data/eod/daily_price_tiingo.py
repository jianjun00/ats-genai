import os
import gin
try:
    gin.parse_config_file('config/app.gin')
except Exception as e:
    print(f'[WARN] Could not parse gin config: {e}')

import asyncio
import asyncpg
import datetime as dt
import pandas as pd
import argparse
from config.environment import Environment, EnvironmentType
from vendor.tiingo.dao.daily_prices_tiingo_dao import DailyPricesTiingoDAO
from dao.instrument_xrefs_dao import InstrumentXrefsDAO

def parse_env_type(env_str):
    env_map = {
        'test': EnvironmentType.TEST,
        'intg': EnvironmentType.INTEGRATION,
        'prod': EnvironmentType.PRODUCTION,
        'dev': EnvironmentType.DEV,
    }
    return env_map.get(env_str.lower(), EnvironmentType.INTEGRATION)

TIINGO_API_KEY = os.getenv("TIINGO_API_KEY")
TIINGO_BASE_URL = "https://api.tiingo.com/tiingo/daily/{symbol}/prices"

def get_env_and_table_name(environment, gin_config_path=None):
    env = Environment(gin_config_path=gin_config_path)
    table_name = env.get_table_name('daily_prices_tiingo')
    print(f"[DEBUG] ENVIRONMENT at start of main: {env.env_type.value}, table: {table_name}")
    return env, table_name

def tiingo_url(symbol, start_date, end_date):
    return (
        f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
        f"?startDate={start_date}&endDate={end_date}&format=json&token={TIINGO_API_KEY}"
    )

async def get_existing_dates(dao: DailyPricesTiingoDAO, instrument_id, start_date, end_date):
    all_prices = await dao.list_prices(instrument_id)
    return set(row['date'] for row in all_prices if start_date <= row['date'] <= end_date)

from calendars.exchange_calendar import ExchangeCalendar

def get_missing_date_ranges(existing_dates, start_date, end_date):
    # Returns a list of (range_start, range_end) for missing contiguous NYSE trading dates
    nyse_cal = ExchangeCalendar('NYSE')
    trading_days = nyse_cal.all_trading_days(start_date, end_date)
    missing = [d for d in trading_days if d not in existing_dates]
    if not missing:
        return []
    # Group into contiguous ranges
    ranges = []
    range_start = missing[0]
    prev = missing[0]
    for d in missing[1:]:

        if (d - prev).days > 1:
            ranges.append((range_start, prev))
            range_start = d
        prev = d
    ranges.append((range_start, prev))
    return ranges

from calendars.exchange_calendar import ExchangeCalendar

async def get_status_id(pool, code, env):
    status_code_table = env.get_table_name('status_code')
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT id FROM {status_code_table} WHERE code = $1",
            code
        )
        if not row:
            raise ValueError(f"Status code '{code}' not found in {status_code_table} table.")
        return row['id']

async def fetch_and_insert_symbol(dao: DailyPricesTiingoDAO, session, instrument_id, symbol, start_date, end_date, ok_status_id, no_data_status_id):
    # Always use datetime.date for DB and date math
    
    env = dao.env
    table_name = env.get_table_name('daily_prices_tiingo')
    print(f"[DEBUG] Inserting into table: {table_name}, ENVIRONMENT: {env.env_type.value}")
    if isinstance(start_date, str):
        start_date_dt = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
    else:
        start_date_dt = start_date
    if isinstance(end_date, str):
        end_date_dt = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
    else:
        end_date_dt = end_date
    # Get NYSE trading days only
    nyse_cal = ExchangeCalendar('NYSE')
    trading_days = set(nyse_cal.all_trading_days(start_date_dt, end_date_dt))
    existing_dates = await get_existing_dates(dao, instrument_id, start_date_dt, end_date_dt)
    missing_ranges = get_missing_date_ranges(existing_dates, start_date_dt, end_date_dt)
    if not missing_ranges:
        print(f"[DEBUG] All data exists for {symbol} in {start_date} to {end_date}, skipping fetch.")
        return
    for range_start, range_end in missing_ranges:
        url = tiingo_url(symbol, range_start, range_end)
        print(f"[DEBUG] Fetching {symbol} from URL: {url}")
        import aiohttp
        max_retries = 5
        retry_delay = 2
        data = None
        for attempt in range(max_retries):
            try:
                async with session.get(url) as resp:
                    if resp.status == 429:
                        print(f"[ERROR] HTTP 429 for {symbol} ({range_start} to {range_end}), attempt {attempt+1}/{max_retries}. Retrying in {retry_delay} seconds...")
                        await asyncio.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                    data = await resp.json()
                    break  # Success!
            except aiohttp.ClientError as e:
                print(f"[ERROR] Client connection error for {symbol}: {e}. Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                retry_delay *= 2
        else:
            print(f"[ERROR] Failed to fetch {symbol} after {max_retries} retries. Skipping.")
            continue
        # After successful response, process as before
        # Log request/response for AAPL/TSLA in date range
        
        log_start = dt.datetime(2020, 1, 10)
        log_end = dt.datetime(2024, 12, 31)
        def in_log_range(s, e):
            try:
                sdt = dt.datetime.strptime(str(s), "%Y-%m-%d")
                edt = dt.datetime.strptime(str(e), "%Y-%m-%d")
                return not (edt < log_start or sdt > log_end)
            except Exception:
                return False
        print(f"[DEBUG] HTTP status for {symbol}: {resp.status}")
        if resp.status != 200:
            print(f"[ERROR] Failed to fetch {symbol}: HTTP {resp.status}")
            continue
        data = await resp.json()
        if not data:
            print(f"[WARNING] No data returned for {symbol} from {range_start} to {range_end}")
            # Insert a row for each missing trading day in this range with status NO_DATA
            missing_days = [d for d in trading_days if range_start <= d <= range_end]
            no_data_rows = [
                {
                    'date': d,
                    'instrument_id': instrument_id,
                    'open': None,
                    'high': None,
                    'low': None,
                    'close': None,
                    'adjClose': None,
                    'volume': None,
                    'status_id': no_data_status_id
                }
                for d in missing_days
            ]
            await dao.batch_insert_prices(no_data_rows)
            print(f"[INFO] Inserted {len(no_data_rows)} NO_DATA rows for {symbol} from {range_start} to {range_end}")
            continue
        ok_data_rows = []
        for row in data:
            # Robustly parse Tiingo ISO date string (e.g., '2020-01-02T00:00:00.000Z')
            date_val = pd.to_datetime(row['date']).date()
            if date_val not in trading_days:
                continue  # Only insert if NYSE is open
            ok_data_rows.append({
                'date': date_val,
                'instrument_id': instrument_id,
                'open': row.get('open'),
                'high': row.get('high'),
                'low': row.get('low'),
                'close': row.get('close'),
                'adjClose': row.get('adjClose'),
                'volume': row.get('volume'),
                'status_id': ok_status_id
            })
        if ok_data_rows:
            await dao.batch_insert_prices(ok_data_rows)
            print(f"[INFO] Inserted {len(ok_data_rows)} price rows for {symbol} from {range_start} to {range_end}")

async def ingest_ticker(env, dao, instrument_id, ticker, start_date, end_date, ok_status_id, no_data_status_id):
    import aiohttp
    async with aiohttp.ClientSession() as session:
        await fetch_and_insert_symbol(dao, session, instrument_id, ticker, start_date, end_date, ok_status_id, no_data_status_id)
    # Get NYSE trading days only
    nyse_cal = ExchangeCalendar('NYSE')
    start_date_dt = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date_dt = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
    trading_days = set(nyse_cal.all_trading_days(start_date_dt, end_date_dt))
    existing_dates = await get_existing_dates(dao, instrument_id, start_date_dt, end_date_dt)
    missing_ranges = get_missing_date_ranges(existing_dates, start_date_dt, end_date_dt)
    if not missing_ranges:
        print(f"[DEBUG] All data exists for {ticker} in {start_date} to {end_date}, skipping fetch.")
        return
    for range_start, range_end in missing_ranges:
        url = tiingo_url(ticker, range_start, range_end)
        print(f"[DEBUG] Fetching {ticker} from URL: {url}")
        async with session.get(url) as resp:
            # Log request/response for AAPL/TSLA in date range
            import json, os
            log_tickers = {"AAPL", "TSLA"}
            log_start = dt.datetime(2020, 1, 10)
            log_end = dt.datetime(2024, 12, 31)
            def in_log_range(s, e):
                try:
                    sdt = dt.datetime.strptime(str(s), "%Y-%m-%d")
                    edt = dt.datetime.strptime(str(e), "%Y-%m-%d")
                    return not (edt < log_start or sdt > log_end)
                except Exception:
                    return False
            if symbol.upper() in log_tickers and in_log_range(range_start, range_end):
                os.makedirs("test/data", exist_ok=True)
                req_path = f"test/data/tiingo_{symbol.lower()}_{range_start}_{range_end}_request.json"
                resp_path = f"test/data/tiingo_{symbol.lower()}_{range_start}_{range_end}_response.json"
                with open(req_path, "w") as f:
                    json.dump({"url": url}, f, indent=2)
                try:
                    resp_text = await resp.text()
                    try:
                        resp_json = json.loads(resp_text)
                        with open(resp_path, "w") as f:
                            json.dump(resp_json, f, indent=2)
                    except Exception:
                        with open(resp_path, "w") as f:
                            f.write(resp_text)
                except Exception as e:
                    with open(resp_path, "w") as f:
                        f.write(f"[ERROR] Could not serialize response: {e}\n")
            print(f"[DEBUG] HTTP status for {symbol}: {resp.status}")
            if resp.status != 200:
                print(f"[ERROR] Failed to fetch {symbol}: HTTP {resp.status}")
                continue
            data = await resp.json()
            if not data:
                print(f"[WARNING] No data returned for {symbol} from {range_start} to {range_end}")
                # Insert a row for each missing trading day in this range with status NO_DATA
                missing_days = [d for d in trading_days if range_start <= d <= range_end]
                no_data_rows = [
                    {
                        'date': d,
                        'instrument_id': instrument_id,
                        'open': None,
                        'high': None,
                        'low': None,
                        'close': None,
                        'adjClose': None,
                        'volume': None,
                        'status_id': no_data_status_id
                    }
                    for d in missing_days
                ]
                await dao.batch_insert_prices(no_data_rows)
                print(f"[INFO] Inserted {len(no_data_rows)} NO_DATA rows for {symbol} from {range_start} to {range_end}")
                continue
            ok_data_rows = []
            for row in data:
                # Robustly parse Tiingo ISO date string (e.g., '2020-01-02T00:00:00.000Z')
                date_val = pd.to_datetime(row['date']).date()
                if date_val not in trading_days:
                    continue  # Only insert if NYSE is open
                ok_data_rows.append({
                    'date': date_val,
                    'instrument_id': instrument_id,
                    'open': row.get('open'),
                    'high': row.get('high'),
                    'low': row.get('low'),
                    'close': row.get('close'),
                    'adjClose': row.get('adjClose'),
                    'volume': row.get('volume'),
                    'status_id': ok_status_id
                })
            if ok_data_rows:
                await dao.batch_insert_prices(ok_data_rows)
                print(f"[INFO] Inserted {len(ok_data_rows)} price rows for {symbol} from {range_start} to {range_end}")

# --- MAIN FUNCTION AND ENTRYPOINT ---
async def get_instrument_dates(env, instrument_id):
    # Returns (list_date, delist_date) as date objects or None
    from dao.instruments_dao import InstrumentsDAO
    instruments_dao = InstrumentsDAO(env)
    row = await instruments_dao.get_instrument(instrument_id)
    print(f"[DEBUG] get_instrument_dates row for {instrument_id}: {row}")
    if not row:
        print(f"[DEBUG] No instrument found for {instrument_id}")
        return None, None
    list_date = row['list_date']
    delist_date = row['delist_date']
    print(f"[DEBUG] list_date type: {type(list_date)}, value: {list_date}")
    print(f"[DEBUG] delist_date type: {type(delist_date)}, value: {delist_date}")
    # Convert to date if not None
    if list_date is not None and isinstance(list_date, str):
        list_date = dt.dt.datetime.strptime(list_date, "%Y-%m-%d").date()
    if delist_date is not None and isinstance(delist_date, str):
        delist_date = dt.dt.datetime.strptime(delist_date[:10], "%Y-%m-%d").date()
    return list_date, delist_date

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--tickers', type=str, default=None, help='Comma-separated list of tickers to process and log (optional, maps to instrument_id)')
    parser.add_argument('--environment', type=str, default='intg', choices=['test', 'intg', 'prod', 'dev'], help='Environment to use (test, intg, prod, dev)')
    parser.add_argument('--logging', action='store_true', help='Enable logging of Tiingo API requests/responses for specified tickers in date range')
    parser.add_argument('--log_dir', type=str, default='test/data/daily_prices_tiingo', help='Directory to store Tiingo API logs (default: test/data/daily_prices_tiingo)')
    parser.add_argument('--gin_config', type=str, default='config/app.gin', help='Path to Gin config file (default: config/app.gin)')
    args = parser.parse_args()

    env, _ = get_env_and_table_name(args.environment, gin_config_path=args.gin_config)

    if not TIINGO_API_KEY:
        print("[DEBUG] Early return: TIINGO_API_KEY environment variable not set.")
        return

    # Use DAO for all daily_prices_tiingo operations
    dao = DailyPricesTiingoDAO(env)
    xrefs_dao = InstrumentXrefsDAO(env)

    # Handle tickers argument (comma-separated)
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(',') if t.strip()]
    else:
        tickers = await xrefs_dao.get_all_symbols()

    # For each ticker, resolve instrument_id and run ingestion
    # Create DB pool and fetch status ids before ingestion loop
    pool = await asyncpg.create_pool(env.get_database_url(), min_size=1, max_size=4)
    ok_status_id = await get_status_id(pool, 'OK', env)
    no_data_status_id = await get_status_id(pool, 'NO_DATA', env)

    # Sequential processing (avoid Ray runtime env issues)
    for ticker in tickers:
        instrument_id = await xrefs_dao.resolve_instrument_id(ticker)
        if instrument_id is None:
            print(f"[ERROR] Could not resolve instrument_id for ticker {ticker}. Skipping.")
            continue
        print(f"[INFO] Running ingestion for {ticker} (instrument_id={instrument_id}) from {args.start_date} to {args.end_date}")
        await ingest_ticker(env, dao, instrument_id, ticker, args.start_date, args.end_date, ok_status_id, no_data_status_id)
        print(f"[INFO] Completed ingestion for {ticker}")
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
