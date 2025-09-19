import argparse
import asyncio
import requests
import time
from datetime import datetime, date
from core.platform.config.environment import Environment, EnvironmentType
from core.shared.utils.vendor_api_keys import get_tiingo_api_key
from core.shared.utils.backfill_framework import BackfillStats, VendorRateLimiters
from core.shared.utils.http_response_handlers import handle_vendor_response
from core.shared.utils.data_transformers import transform_vendor_dividend, parse_vendor_date
from core.shared.utils.validation_utils import validate_dividend_data, validate_date_range
from core.shared.utils.config_utils import get_api_key_with_fallback
from infrastructure.database.repositories.dividend_tiingo_dao import DividendTiingoDAO
import asyncpg

def fetch_tiingo_dividends(symbol, api_key, start_date, end_date, stats=None, rate_limiter=None):
    """Fetch dividend data from Tiingo API using shared HTTP handling."""
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

    # Use shared HTTP response handler
    result = handle_vendor_response(resp, symbol, vendor='tiingo')
    
    if result['success']:
        return result['data']
    else:
        print(f"Failed to fetch dividends for {symbol}: {result['error']}")
        return []

async def get_symbols_from_dividend_polygon(env, start_date, end_date):
    """Get distinct symbols from dividend_polygon table using shared date parsing."""
    db_url = env.get_database_url()
    table_name = env.get_table_name('dividend_polygon')
    start = parse_vendor_date(start_date, vendor='tiingo')
    end = parse_vendor_date(end_date, vendor='tiingo')
    pool = await asyncpg.create_pool(db_url)
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"SELECT DISTINCT symbol FROM {table_name} WHERE ex_dividend_date >= $1 AND ex_dividend_date <= $2", start, end)
    await pool.close()
    return [row['symbol'] for row in rows]

async def insert_dividends_tiingo(dividends, dao):
    """Insert dividends using shared data transformation and validation."""
    if not dividends:
        print("No dividends to insert.")
        return
    inserted = 0
    for div in dividends:
        # Use shared data transformation
        transformed = transform_vendor_dividend(div, vendor='tiingo')
        
        # Use shared validation
        validation_result = validate_dividend_data(transformed)
        
        if validation_result.is_valid:
            await dao.insert_dividend(transformed)
            inserted += 1
        else:
            print(f"Validation failed for dividend: {validation_result.errors}")
    
    print(f"Inserted {inserted} dividends.")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--environment', type=str, default='intg', choices=['test', 'intg', 'prod'], help='Environment to use (test, intg, prod)')
    parser.add_argument('--start_date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='End date (YYYY-MM-DD)')
    args = parser.parse_args()
    env = Environment(env_type=EnvironmentType(args.environment))

    # Validate date range using shared validation
    date_validation = validate_date_range(args.start_date, args.end_date, max_range_days=365)
    if not date_validation.is_valid:
        print(f"Invalid date range: {date_validation.errors}")
        return

    # Use enhanced API key resolution from shared utilities with fallback
    api_key = get_api_key_with_fallback('tiingo', env_instance=env)
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
