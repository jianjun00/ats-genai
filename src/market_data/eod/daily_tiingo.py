import os
import asyncio
import aiohttp
import asyncpg
from datetime import datetime, timedelta
import pandas as pd
import argparse
from config.environment import get_environment, set_environment, EnvironmentType
from dao.daily_prices_tiingo_dao import DailyPricesTiingoDAO

def parse_env_type(env_str):
    env_map = {
        'test': EnvironmentType.TEST,
        'intg': EnvironmentType.INTEGRATION,
        'prod': EnvironmentType.PRODUCTION,
    }
    return env_map.get(env_str.lower(), EnvironmentType.INTEGRATION)

TIINGO_API_KEY = os.getenv("TIINGO_API_KEY")
TIINGO_BASE_URL = "https://api.tiingo.com/tiingo/daily/{symbol}/prices"

def get_env_and_table_name(environment):
    set_environment(parse_env_type(environment))
    env = get_environment()
    table_name = env.get_table_name('daily_prices_tiingo')
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
    if isinstance(start_date, str):
        start_date_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    else:
        start_date_dt = start_date
    if isinstance(end_date, str):
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
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
        async with session.get(url) as resp:
            print(f"[DEBUG] HTTP status for {symbol}: {resp.status}")
            if resp.status != 200:
                print(f"[ERROR] Failed to fetch {symbol}: HTTP {resp.status}")
                continue
            data = await resp.json()
            if not data:
                print(f"[WARNING] No data returned for {symbol} from {range_start} to {range_end}")
                # Insert a row for each missing trading day in this range with status NO_DATA
                missing_days = [d for d in trading_days if range_start <= d <= range_end]
                for d in missing_days:
                    await dao.insert_price(
                        date=d,
                        instrument_id=instrument_id,
                        open_=None,
                        high=None,
                        low=None,
                        close=None,
                        adj_close=None,
                        volume=None,
                        status_id=no_data_status_id
                    )
                print(f"[INFO] Inserted {len(missing_days)} NO_DATA rows for {symbol} from {range_start} to {range_end}")
                continue
            for row in data:
                # Robustly parse Tiingo ISO date string (e.g., '2020-01-02T00:00:00.000Z')
                date_val = pd.to_datetime(row['date']).date()
                if date_val not in trading_days:
                    continue  # Only insert if NYSE is open
                await dao.insert_price(
                    date=date_val,
                    instrument_id=instrument_id,
                    open_=row.get('open'),
                    high=row.get('high'),
                    low=row.get('low'),
                    close=row.get('close'),
                    adj_close=row.get('adjClose'),
                    volume=row.get('volume'),
                    status_id=ok_status_id
                )
            print(f"[INFO] Inserted {len(data)} rows for {symbol} from {range_start} to {range_end}")

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
        list_date = datetime.strptime(list_date, "%Y-%m-%d").date()
    if delist_date is not None and isinstance(delist_date, str):
        delist_date = datetime.strptime(delist_date[:10], "%Y-%m-%d").date()
    return list_date, delist_date

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--start_date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--instrument_id', type=str, default=None, help='Process only this instrument_id (optional)')
    parser.add_argument('--environment', type=str, default='intg', choices=['test', 'intg', 'prod'], help='Environment to use (test, intg, prod)')
    args = parser.parse_args()

    env, table_name = get_env_and_table_name(args.environment)

    if not TIINGO_API_KEY:
        print("[ERROR] TIINGO_API_KEY environment variable not set.")
        return

    # Use DAO for all daily_prices_tiingo operations
    dao = DailyPricesTiingoDAO(env)
    import ssl

    # Determine instrument_ids to process
    pool = await asyncpg.create_pool(env.get_database_url(), min_size=1, max_size=4)
    from dao.instrument_xrefs_dao import InstrumentXrefsDAO
    xrefs_dao = InstrumentXrefsDAO(env)
    if args.instrument_id:
        instrument_ids = [int(args.instrument_id)]
    else:
        # Get all unique instrument_ids with a symbol in instrument_xrefs
        xrefs_rows = await xrefs_dao.list_xrefs_for_vendor(vendor_id=1)  # None = all vendors
        instrument_ids = list({row['instrument_id'] for row in xrefs_rows if row.get('instrument_id')})
    # For each instrument_id, map to symbol, then get existing dates and fetch/insert missing
    ok_status_id = await get_status_id(pool, 'OK', env)
    no_data_status_id = await get_status_id(pool, 'NO_DATA', env)
    async with aiohttp.ClientSession() as session:
        for instrument_id in instrument_ids:
            # Map instrument_id to symbol for Tiingo API
            xrefs = await xrefs_dao.list_xrefs_for_instrument(instrument_id)
            symbol = None
            if xrefs:
                # Choose the first symbol (could be improved to select by vendor if needed)
                symbol = xrefs[0]['symbol']
            if not symbol:
                print(f"[ERROR] Could not resolve symbol for instrument_id {instrument_id}. Skipping.")
                continue
            list_date, delist_date = await get_instrument_dates(env, instrument_id)
            if not list_date:
                print(f"[INFO] Skipping {symbol} (no list_date)")
                continue
            print(f"[DEBUG] Instrument_id {instrument_id} maps to symbol {symbol}")
            start_date_dt = datetime.strptime(args.start_date, "%Y-%m-%d").date()
            end_date_dt = datetime.strptime(args.end_date, "%Y-%m-%d").date()

            effective_start = max(list_date, start_date_dt)
            effective_end = delist_date if delist_date else end_date_dt
            if effective_start > effective_end:
                print(f"[INFO] Skipping {symbol} (effective_start {effective_start} > effective_end {effective_end})")
                continue
            print(f"[INFO] Processing {symbol} from {effective_start} to {effective_end}")
            existing_dates = await get_existing_dates(dao, instrument_id, effective_start, effective_end)
            missing_ranges = get_missing_date_ranges(existing_dates, effective_start, effective_end)


            if not missing_ranges:
                print(f"[INFO] No missing dates for {symbol} (instrument_id {instrument_id}) in {args.start_date} to {args.end_date}")
                continue
            for range_start, range_end in missing_ranges:
                print(f"[INFO] Processing {symbol} (instrument_id {instrument_id}) from {range_start} to {range_end}")
                try:
                    await fetch_and_insert_symbol(dao, session, instrument_id, symbol, range_start, range_end, ok_status_id, no_data_status_id)
                except Exception as e:
                    print(f"[ERROR] Exception for {symbol} (instrument_id {instrument_id}): {e}")
    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
