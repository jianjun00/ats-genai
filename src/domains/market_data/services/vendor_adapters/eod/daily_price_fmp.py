#!/usr/bin/env python3
"""
Financial Modeling Prep Daily Price Ingestion
Fetches daily stock prices from Financial Modeling Prep API and stores in database.
"""

import os
import asyncio
import aiohttp
import datetime as dt
import argparse
import logging
from typing import List, Dict, Any

from shared.utils.environment import Environment, EnvironmentType
from vendor.fmp.dao.daily_prices_fmp_dao import DailyPricesFmpDAO
from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO
from core.calendars.exchange_calendar import ExchangeCalendar

logger = logging.getLogger(__name__)

# Financial Modeling Prep API configuration
FMP_API_KEY = os.getenv("FMP_API_KEY")
FMP_BASE_URL = "https://financialmodelingprep.com/api/v3"

def parse_env_type(env_str):
    env_map = {
        'test': EnvironmentType.TEST,
        'intg': EnvironmentType.INTEGRATION,
        'prod': EnvironmentType.PRODUCTION,
        'dev': EnvironmentType.DEV,
    }
    return env_map.get(env_str.lower(), EnvironmentType.INTEGRATION)

async def get_existing_dates(dao: DailyPricesFmpDAO, instrument_id, start_date, end_date):
    """Get existing dates for an instrument in the given date range."""
    all_prices = await dao.list_prices(instrument_id)
    return set(row['date'] for row in all_prices if start_date <= row['date'] <= end_date)

def get_missing_date_ranges(existing_dates, start_date, end_date):
    """Returns a list of (range_start, range_end) for missing contiguous NYSE trading dates."""
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

async def fetch_fmp_historical_prices(session: aiohttp.ClientSession, symbol: str,
                                    start_date: dt.date, end_date: dt.date) -> List[Dict[str, Any]]:
    """
    Fetch historical stock prices from Financial Modeling Prep API.

    Args:
        session: aiohttp session
        symbol: Stock symbol (e.g., 'AAPL')
        start_date: Start date for data
        end_date: End date for data

    Returns:
        List of price data dictionaries
    """
    if not FMP_API_KEY:
        logger.error("FMP_API_KEY not available")
        return []

    # FMP uses historical-price-full endpoint with date range
    url = f"{FMP_BASE_URL}/historical-price-full/{symbol}"
    params = {
        'from': start_date.strftime('%Y-%m-%d'),
        'to': end_date.strftime('%Y-%m-%d'),
        'apikey': FMP_API_KEY
    }

    try:
        async with session.get(url, params=params) as response:
            if response.status == 429:  # Rate limit exceeded
                logger.warning(f"Rate limit exceeded for {symbol}, sleeping 60 seconds")
                await asyncio.sleep(60)
                return []

            if response.status != 200:
                error_text = await response.text()
                logger.error(f"FMP API error for {symbol}: {response.status} - {error_text}")
                return []

            data = await response.json()

            # Check for API errors
            if "Error Message" in data:
                logger.error(f"FMP error for {symbol}: {data['Error Message']}")
                return []

            # FMP returns data in 'historical' array
            if 'historical' not in data:
                logger.error(f"No historical data found for {symbol}")
                return []

            historical_data = data['historical']
            prices = []

            for price_data in historical_data:
                try:
                    date_obj = dt.datetime.strptime(price_data['date'], "%Y-%m-%d").date()

                    price_record = {
                        'date': date_obj,
                        'open_price': float(price_data.get('open', 0)),
                        'high_price': float(price_data.get('high', 0)),
                        'low_price': float(price_data.get('low', 0)),
                        'close': float(price_data.get('close', 0)),
                        'adj_close': float(price_data.get('adjClose', price_data.get('close', 0))),
                        'volume': int(price_data.get('volume', 0))
                    }

                    prices.append(price_record)

                except (ValueError, KeyError) as e:
                    logger.warning(f"Error parsing price data for {symbol} on {price_data.get('date', 'unknown')}: {e}")
                    continue

            logger.info(f"Fetched {len(prices)} price records for {symbol}")
            return prices

    except aiohttp.ClientError as e:
        logger.error(f"Connection error fetching {symbol} from FMP: {e}")
        return []

async def process_symbol(dao: DailyPricesFmpDAO, session: aiohttp.ClientSession,
                        symbol: str, instrument_id: int, start_date: dt.date, end_date: dt.date):
    """Process a single symbol - fetch and store price data."""

    logger.info(f"Processing {symbol} (instrument_id={instrument_id})")

    # Check what we already have
    existing_dates = await get_existing_dates(dao, instrument_id, start_date, end_date)
    missing_ranges = get_missing_date_ranges(existing_dates, start_date, end_date)

    if not missing_ranges:
        logger.info(f"All data exists for {symbol} in date range, skipping")
        return True

    # FMP API works best with larger date ranges, so combine ranges
    overall_start = min(r[0] for r in missing_ranges)
    overall_end = max(r[1] for r in missing_ranges)

    # Fetch data for the overall range
    prices = await fetch_fmp_historical_prices(session, symbol, overall_start, overall_end)
    if not prices:
        logger.warning(f"No price data received for {symbol}")
        return False

    # Filter to only the missing dates we need
    needed_dates = set()
    for range_start, range_end in missing_ranges:
        nyse_cal = ExchangeCalendar('NYSE')
        range_trading_days = nyse_cal.all_trading_days(range_start, range_end)
        needed_dates.update(range_trading_days)

    # Filter prices to only what we need and is in our date range
    filtered_prices = []
    for price in prices:
        if start_date <= price['date'] <= end_date and price['date'] in needed_dates:
            price['instrument_id'] = instrument_id
            filtered_prices.append(price)

    if filtered_prices:
        await dao.batch_insert_prices(filtered_prices)
        logger.info(f"Inserted {len(filtered_prices)} price records for {symbol}")
        return True
    else:
        logger.info(f"No new price data to insert for {symbol}")
        return True

async def main():
    parser = argparse.ArgumentParser(description="Financial Modeling Prep Daily Price Ingestion")
    parser.add_argument('--start_date', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', type=str, required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--tickers', type=str, default=None, help='Comma-separated list of tickers (optional)')
    parser.add_argument('--environment', type=str, default='dev', choices=['test', 'intg', 'prod', 'dev'], help='Environment to use')
    parser.add_argument('--gin_config', type=str, default='config/app_dev.gin', help='Path to Gin config file')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of symbols to process')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    args = parser.parse_args()

    # Configure logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if not FMP_API_KEY:
        logger.error("FMP_API_KEY environment variable not set")
        return

    # Initialize environment and DAOs
    env = Environment(gin_config_path=args.gin_config)
    dao = DailyPricesFmpDAO(env)
    xrefs_dao = InstrumentXrefsDAO(env)

    # Parse dates
    start_date = dt.datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = dt.datetime.strptime(args.end_date, "%Y-%m-%d").date()

    # Get tickers to process
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(',') if t.strip()]
    else:
        tickers = await xrefs_dao.get_all_symbols()

    if args.limit:
        tickers = tickers[:args.limit]

    logger.info(f"Processing {len(tickers)} symbols from {start_date} to {end_date}")

    success_count = 0
    error_count = 0

    # FMP allows 250 requests per minute for free tier
    # Process symbols with appropriate delays
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(timeout=timeout) as session:
        for i, ticker in enumerate(tickers):
            try:
                # Resolve instrument ID
                instrument_id = await xrefs_dao.resolve_instrument_id(ticker)
                if not instrument_id:
                    logger.error(f"Could not resolve instrument_id for {ticker}")
                    error_count += 1
                    continue

                # Process the symbol
                success = await process_symbol(dao, session, ticker, instrument_id, start_date, end_date)

                if success:
                    success_count += 1
                else:
                    error_count += 1

                # Rate limiting: FMP allows 250 calls per minute = ~0.25 seconds between calls
                if i < len(tickers) - 1:  # Don't sleep after the last symbol
                    await asyncio.sleep(0.3)  # 300ms between calls = 200 calls per minute (safe margin)

            except Exception as e:
                logger.error(f"Error processing {ticker}: {e}")
                error_count += 1

    logger.info(f"FMP ingestion complete. Success: {success_count}, Errors: {error_count}")

if __name__ == "__main__":
    asyncio.run(main())