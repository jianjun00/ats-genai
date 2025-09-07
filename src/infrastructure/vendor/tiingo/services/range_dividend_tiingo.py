import argparse
import asyncio
import requests
import time
from datetime import datetime, date
from shared.utils.environment import Environment, EnvironmentType
from shared.utils.vendor_api_keys import get_tiingo_api_key
from shared.utils.backfill_framework import BackfillStats, VendorRateLimiters
from infrastructure.database.repositories.dividend_tiingo_dao import DividendTiingoDAO
import asyncpg

def parse_date(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    return datetime.strptime(val, "%Y-%m-%d").date()

def fetch_tiingo_dividends(symbol, api_key, start_date, end_date, stats=None, rate_limiter=None):
    url = f"https://api.tiingo.com/iex/{symbol}/dividends?startDate={start_date}&endDate={end_date}"
    headers = {"Authorization": f"Token {api_key}"}
    print(f"[DEBUG] Requesting Tiingo dividends: {url}")

    start_time = time.time()
    resp = requests.get(url, headers=headers)
    response_time = time.time() - start_time

    print(f"[DEBUG] Response status: {resp.status_code}")
    print(f"[DEBUG] Response headers: {dict(resp.headers)}")
    print(f"[DEBUG] Response body (first 500 chars): {resp.text[:500]}")

    # Record API call statistics
    if stats:
        stats.record_api_call(success=(resp.status_code == 200), response_time=response_time)

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
        'cash_amount': div.get('cashAmount'),
        'declaration_date': pd('declarationDate'),
        'payment_date': pd('payDate'),
        'record_date': pd('recordDate'),
        'description': div.get('description'),
        'refid': div.get('id'),
        'qualified': div.get('qualified'),
        'flag': div.get('flag'),
        'currency': div.get('currency'),
        'frequency': div.get('frequency'),
    }

async def get_symbols_from_dividend_polygon(env, start_date, end_date):
    db_url = env.get_database_url()
    table_name = env.get_table_name('dividend_polygon')
    start = parse_date(start_date)
    end = parse_date(end_date)
    pool = await asyncpg.create_pool(db_url)
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"SELECT DISTINCT symbol FROM {table_name} WHERE ex_dividend_date >= $1 AND ex_dividend_date <= $2", start, end)
    await pool.close()
    return [row['symbol'] for row in rows]

async def insert_dividends_tiingo(dividends, dao):
    if not dividends:
        print("No dividends to insert.")
        return
    inserted = 0
    for div in dividends:
        mapped = map_tiingo_dividend(div)
        if mapped['symbol'] and mapped['ex_dividend_date'] and mapped['cash_amount'] is not None:
            await dao.insert_dividend(mapped)
            inserted += 1
    print(f"Inserted {inserted} dividends.")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--environment', type=str, default='intg', choices=['test', 'intg', 'prod'], help='Environment to use (test, intg, prod)')
    parser.add_argument('--start_date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='End date (YYYY-MM-DD)')
    args = parser.parse_args()
    env = Environment(env_type=EnvironmentType(args.environment))

    # Use enhanced API key resolution from shared utilities
    api_key = get_tiingo_api_key(env=env)
    if not api_key:
        raise Exception("Please set your TIINGO_API_KEY in your environment or config.")

    # Initialize shared utilities for comprehensive monitoring
    stats = BackfillStats()
    rate_limiter = VendorRateLimiters.tiingo()

    symbols = await get_symbols_from_dividend_polygon(env, args.start_date, args.end_date)
    print(f"Found {len(symbols)} symbols with dividends in dividend_polygon between {args.start_date} and {args.end_date}")

    div_dao = DividendTiingoDAO(env)
    for i, symbol in enumerate(symbols):
        print(f"Processing {symbol} ({i+1}/{len(symbols)})")

        # Use enhanced fetch with statistics and rate limiting
        tiingo_divs = fetch_tiingo_dividends(symbol, api_key, args.start_date, args.end_date,
                                           stats=stats, rate_limiter=rate_limiter)
        await insert_dividends_tiingo(tiingo_divs, div_dao)

        # Apply rate limiting between requests
        await rate_limiter.wait_if_needed()

        # Progress reporting every 10 symbols
        if (i + 1) % 10 == 0:
            stats.log_progress(print)

    # Final comprehensive statistics report
    print("\n=== COMPREHENSIVE DIVIDEND PROCESSING STATISTICS ===")
    stats.log_progress(print)

if __name__ == "__main__":
    asyncio.run(main())
