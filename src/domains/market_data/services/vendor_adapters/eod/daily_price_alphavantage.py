#!/usr/bin/env python3
"""
Alpha Vantage Daily Price Ingestion
Fetches daily stock prices from Alpha Vantage API and stores in database.
"""

import os
import asyncio
import aiohttp
import datetime as dt
import argparse
import logging
from typing import List, Dict, Any

from shared.utils.environment import Environment, EnvironmentType
from domains.market_data.repositories.daily_prices_alphavantage_dao import DailyPricesAlphaVantageDAO
from domains.instruments.repositories.instrument_xrefs_dao import InstrumentXrefsDAO
from core.business.calendars.exchange_calendar import ExchangeCalendar

logger = logging.getLogger(__name__)

# Alpha Vantage API configuration
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"

def parse_env_type(env_str):
    env_map = {
        'test': EnvironmentType.TEST,
        'intg': EnvironmentType.INTEGRATION,
        'prod': EnvironmentType.PRODUCTION,
        'dev': EnvironmentType.DEV,
    }
    return env_map.get(env_str.lower(), EnvironmentType.INTEGRATION)

async def get_existing_dates(dao: DailyPricesAlphaVantageDAO, instrument_id, start_date, end_date):
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

async def fetch_alphavantage_daily_prices(session: aiohttp.ClientSession, symbol: str) -> List[Dict[str, Any]]:
    """
    Fetch daily stock prices from Alpha Vantage API.
    
    Args:
        session: aiohttp session
        symbol: Stock symbol (e.g., 'AAPL')
        
    Returns:
        List of price data dictionaries
    """
    if not ALPHA_VANTAGE_API_KEY:
        logger.error("ALPHA_VANTAGE_API_KEY not available")
        return []
    
    params = {
        'function': 'TIME_SERIES_DAILY',
        'symbol': symbol,
        'outputsize': 'full',  # Get full historical data (20+ years)
        'apikey': ALPHA_VANTAGE_API_KEY
    }
    
    try:
        async with session.get(ALPHA_VANTAGE_BASE_URL, params=params) as response:
            if response.status == 429:  # Rate limit exceeded
                logger.warning(f"Rate limit exceeded for {symbol}, sleeping 60 seconds")
                await asyncio.sleep(60)
                return []
            
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"Alpha Vantage API error for {symbol}: {response.status} - {error_text}")
                return []
            
            data = await response.json()
            
            # Check for API errors
            if "Error Message" in data:
                logger.error(f"Alpha Vantage error for {symbol}: {data['Error Message']}")
                return []
            
            if "Note" in data:
                logger.warning(f"Alpha Vantage rate limit note for {symbol}: {data['Note']}")
                await asyncio.sleep(60)  # Back off for rate limiting
                return []
            
            # Parse the time series data
            time_series_key = "Time Series (Daily)"
            if time_series_key not in data:
                logger.error(f"No time series data found for {symbol}")
                return []
            
            time_series = data[time_series_key]
            prices = []
            
            for date_str, price_data in time_series.items():
                try:
                    date_obj = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
                    
                    price_record = {
                        'date': date_obj,
                        'open_price': float(price_data['1. open']),
                        'high_price': float(price_data['2. high']),
                        'low_price': float(price_data['3. low']),
                        'close': float(price_data['4. close']),
                        'adj_close': float(price_data['4. close']),  # Free tier doesn't have adjusted close, use close
                        'volume': int(price_data['5. volume'])
                    }
                    
                    prices.append(price_record)
                    
                except (ValueError, KeyError) as e:
                    logger.warning(f"Error parsing price data for {symbol} on {date_str}: {e}")
                    continue
            
            logger.info(f"Fetched {len(prices)} price records for {symbol}")
            return prices
            
    except aiohttp.ClientError as e:
        logger.error(f"Connection error fetching {symbol} from Alpha Vantage: {e}")
        return []

async def process_symbol(dao: DailyPricesAlphaVantageDAO, session: aiohttp.ClientSession, 
                        symbol: str, instrument_id: int, start_date: dt.date, end_date: dt.date):
    """Process a single symbol - fetch and store price data."""
    
    logger.info(f"Processing {symbol} (instrument_id={instrument_id})")
    
    # Check what we already have
    existing_dates = await get_existing_dates(dao, instrument_id, start_date, end_date)
    missing_ranges = get_missing_date_ranges(existing_dates, start_date, end_date)
    
    if not missing_ranges:
        logger.info(f"All data exists for {symbol} in date range, skipping")
        return True
    
    # Fetch all data from Alpha Vantage (they provide full historical data)
    prices = await fetch_alphavantage_daily_prices(session, symbol)
    if not prices:
        logger.warning(f"No price data received for {symbol}")
        return False
    
    # Filter to only the missing date ranges we need
    needed_dates = set()
    for range_start, range_end in missing_ranges:
        nyse_cal = ExchangeCalendar('NYSE')
        range_trading_days = nyse_cal.all_trading_days(range_start, range_end)
        needed_dates.update(range_trading_days)
    
    # Filter prices to only what we need
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
    parser = argparse.ArgumentParser(description="Alpha Vantage Daily Price Ingestion")
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
    
    if not ALPHA_VANTAGE_API_KEY:
        logger.error("ALPHA_VANTAGE_API_KEY environment variable not set")
        return

    # Initialize environment and DAOs
    env = Environment(gin_config_path=args.gin_config)
    dao = DailyPricesAlphaVantageDAO(env)
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
    
    # Alpha Vantage has strict rate limits (5 calls per minute for free tier)
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

                # Rate limiting: Alpha Vantage free tier allows 5 calls per minute
                if i < len(tickers) - 1:  # Don't sleep after the last symbol
                    logger.info(f"Rate limiting: sleeping 12 seconds before next symbol")
                    await asyncio.sleep(12)  # 12 seconds between calls = 5 calls per minute
                
            except Exception as e:
                logger.error(f"Error processing {ticker}: {e}")
                error_count += 1

    logger.info(f"Alpha Vantage ingestion complete. Success: {success_count}, Errors: {error_count}")

if __name__ == "__main__":
    asyncio.run(main())