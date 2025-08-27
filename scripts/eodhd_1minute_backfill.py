#!/usr/bin/env python3
"""
EODHD 1-Minute Bar Backfill Script for 30-Year Historical Data

Fetches 1-minute OHLCV data from EODHD API and stores it on D: drive using
the existing file-based storage infrastructure.

Based on existing patterns:
- src/market_data/agent/eodhd_minute_adapter.py
- src/storage/file_based_minute_manager.py
- src/market_data/backfill/enhanced_minute_backfill_orchestrator.py

Data Storage: /mnt/d/ats-data/minute-bars/
"""

import os
import sys
import asyncio
import logging
import argparse
import gin
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Any, Set
from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import numpy as np

# Add src to Python path
sys.path.insert(0, '/workspace/src')

from config.environment import Environment, EnvironmentType
from config.database import Database

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("eodhd_1minute_backfill")

@dataclass
class EODHDMinuteBar:
    """EODHD 1-minute OHLCV bar data structure (reusing existing pattern)."""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    vendor: str = "eodhd"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DataFrame."""
        return {
            'symbol': self.symbol,
            'timestamp': self.timestamp,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': self.volume,
            'vendor': self.vendor
        }

class EODHDMinuteBackfiller:
    """
    EODHD 1-minute data backfiller using file-based storage on D: drive.
    
    Features:
    - 30-year historical data collection
    - D: drive monthly Parquet file storage
    - Rate limiting (20 calls/minute for free tier)
    - Resume capability with existing data detection
    - Comprehensive error handling
    """
    
    def __init__(self, api_key: str, base_path: str = "/mnt/d/ats-data"):
        self.api_key = api_key
        self.base_path = Path(base_path)
        self.minute_data_path = self.base_path / "minute-bars"
        
        # Rate limiting configuration (EODHD free tier)
        self.rate_limit_calls_per_minute = 20
        self.request_delay = 60.0 / self.rate_limit_calls_per_minute  # 3 seconds
        
        # Session management
        self.session = None
        
        # Statistics
        self.stats = {
            'total_symbols': 0,
            'processed_symbols': 0,
            'total_bars': 0,
            'files_created': 0,
            'api_calls': 0,
            'errors': 0
        }
        
        # Ensure storage directories exist
        self.setup_storage()
        
        logger.info(f"📊 EODHD 1-Minute Backfiller initialized:")
        logger.info(f"   Base path: {self.base_path}")
        logger.info(f"   Minute data path: {self.minute_data_path}")
        logger.info(f"   Rate limit: {self.rate_limit_calls_per_minute} calls/minute")
    
    def setup_storage(self):
        """Set up storage directory structure on D: drive."""
        try:
            # Create main directories
            directories = [
                self.base_path,
                self.minute_data_path,
                self.minute_data_path / "metadata",
                self.minute_data_path / "logs"
            ]
            
            for directory in directories:
                directory.mkdir(parents=True, exist_ok=True)
                logger.info(f"📁 Created directory: {directory}")
                
        except Exception as e:
            logger.error(f"❌ Failed to create storage directories: {e}")
            raise

    def get_minute_bars_url(self, symbol: str, date_str: str) -> str:
        """Construct URL for 1-minute intraday data (reusing existing pattern)."""
        return (
            f"https://eodhistoricaldata.com/api/intraday/{symbol}.US"
            f"?api_token={self.api_key}&interval=1m&from={date_str}&to={date_str}&fmt=json"
        )

    async def fetch_minute_bars_for_date(self, symbol: str, target_date: date) -> List[EODHDMinuteBar]:
        """
        Fetch 1-minute bars for a specific date (reusing existing EODHD pattern).
        """
        import aiohttp
        
        date_str = target_date.strftime('%Y-%m-%d')
        url = self.get_minute_bars_url(symbol, date_str)
        
        try:
            async with self.session.get(url) as response:
                self.stats['api_calls'] += 1
                
                if response.status == 200:
                    data = await response.json()
                    return self._parse_minute_bars(symbol, data)
                elif response.status == 429:
                    # Rate limit exceeded
                    logger.warning(f"Rate limit exceeded for {symbol} on {date_str}")
                    await asyncio.sleep(60)  # Wait 1 minute
                    return await self.fetch_minute_bars_for_date(symbol, target_date)
                elif response.status == 404:
                    # No data available (weekends, holidays, etc.)
                    logger.debug(f"No data for {symbol} on {date_str}")
                    return []
                elif response.status == 402:
                    # API limit exceeded
                    logger.error(f"API limit exceeded for {symbol} on {date_str}")
                    return []
                else:
                    logger.error(f"EODHD API error for {symbol} on {date_str}: {response.status}")
                    self.stats['errors'] += 1
                    return []
                    
        except Exception as e:
            logger.error(f"Error fetching data for {symbol} on {date_str}: {e}")
            self.stats['errors'] += 1
            return []

    def _parse_minute_bars(self, symbol: str, data: List[Dict]) -> List[EODHDMinuteBar]:
        """Parse EODHD API response into EODHDMinuteBar objects (reusing existing pattern)."""
        bars = []
        
        if not data or not isinstance(data, list):
            return bars
        
        for item in data:
            try:
                # EODHD timestamp format: "2024-01-01 09:30:00"
                timestamp_str = f"{item['datetime']}"
                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
                
                bar = EODHDMinuteBar(
                    symbol=symbol,
                    timestamp=timestamp,
                    open=float(item['open']),
                    high=float(item['high']),
                    low=float(item['low']),
                    close=float(item['close']),
                    volume=int(item.get('volume', 0))
                )
                bars.append(bar)
                
            except (KeyError, ValueError, TypeError) as e:
                logger.warning(f"Error parsing bar for {symbol}: {e}")
                continue
        
        logger.debug(f"Parsed {len(bars)} minute bars for {symbol}")
        return bars

    def get_monthly_file_path(self, symbol: str, year: int, month: int) -> Path:
        """Get file path for monthly Parquet file (following existing pattern)."""
        return self.minute_data_path / symbol / str(year) / f"{month:02d}.parquet"

    def save_monthly_data(self, symbol: str, year: int, month: int, bars: List[EODHDMinuteBar]) -> bool:
        """
        Save minute bars to monthly Parquet file (following existing file storage pattern).
        """
        if not bars:
            return False
            
        try:
            # Convert bars to DataFrame
            df_data = [bar.to_dict() for bar in bars]
            df = pd.DataFrame(df_data)
            
            # Sort by timestamp
            df = df.sort_values('timestamp').reset_index(drop=True)
            
            # Get file path and ensure directory exists
            file_path = self.get_monthly_file_path(symbol, year, month)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Check if file already exists and merge data
            if file_path.exists():
                logger.info(f"📄 Existing file found: {file_path}")
                existing_df = pd.read_parquet(file_path)
                
                # Merge with existing data (avoid duplicates)
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['timestamp'], keep='last')
                combined_df = combined_df.sort_values('timestamp').reset_index(drop=True)
                df = combined_df
                
                logger.info(f"📊 Merged data: {len(existing_df)} existing + {len(bars)} new = {len(df)} total")
            
            # Save to Parquet file
            df.to_parquet(file_path, compression='snappy', index=False)
            self.stats['files_created'] += 1
            self.stats['total_bars'] += len(bars)
            
            logger.info(f"💾 Saved {len(bars)} bars to {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to save data for {symbol} {year}-{month:02d}: {e}")
            self.stats['errors'] += 1
            return False

    async def backfill_symbol(self, symbol: str, start_date: date, end_date: date) -> Dict[str, Any]:
        """
        Backfill 1-minute data for a single symbol across date range.
        """
        logger.info(f"🔄 Starting backfill for {symbol}: {start_date} to {end_date}")
        
        symbol_stats = {
            'symbol': symbol,
            'days_processed': 0,
            'bars_collected': 0,
            'files_created': 0,
            'api_calls': 0,
            'errors': 0
        }
        
        # Group dates by month for efficient storage
        monthly_data = {}  # (year, month) -> List[EODHDMinuteBar]
        
        current_date = start_date
        while current_date <= end_date:
            # Skip weekends (EODHD has no weekend data)
            if current_date.weekday() >= 5:  # Saturday=5, Sunday=6
                current_date += timedelta(days=1)
                continue
            
            logger.debug(f"📅 Processing {symbol} for {current_date}")
            
            # Fetch minute bars for this date
            bars = await self.fetch_minute_bars_for_date(symbol, current_date)
            
            if bars:
                # Group by month
                year_month = (current_date.year, current_date.month)
                if year_month not in monthly_data:
                    monthly_data[year_month] = []
                monthly_data[year_month].extend(bars)
                
                symbol_stats['bars_collected'] += len(bars)
                logger.debug(f"✅ Collected {len(bars)} bars for {symbol} on {current_date}")
            else:
                logger.debug(f"⚠️ No data for {symbol} on {current_date}")
            
            symbol_stats['days_processed'] += 1
            symbol_stats['api_calls'] += 1
            
            # Rate limiting delay
            await asyncio.sleep(self.request_delay)
            current_date += timedelta(days=1)
        
        # Save monthly data files
        for (year, month), bars in monthly_data.items():
            if self.save_monthly_data(symbol, year, month, bars):
                symbol_stats['files_created'] += 1
        
        logger.info(f"✅ Completed {symbol}: {symbol_stats['days_processed']} days, "
                   f"{symbol_stats['bars_collected']} bars, {symbol_stats['files_created']} files")
        
        return symbol_stats

    async def get_symbols_for_backfill(self, pool: Optional[Any], limit: Optional[int] = None) -> List[str]:
        """Get list of symbols for backfill from database or default list."""
        
        if pool:
            try:
                async with pool.acquire() as conn:
                    # Get active instruments on major US exchanges
                    limit_clause = f"LIMIT {limit}" if limit else ""
                    
                    instruments = await conn.fetch(f"""
                        SELECT DISTINCT symbol 
                        FROM dev_instruments 
                        WHERE active = true 
                          AND symbol IS NOT NULL 
                          AND symbol != ''
                          AND exchange IN ('NASDAQ', 'NYSE', 'NYSE ARCA', 'BATS', 'XNYS', 'NYSE MKT', 'XNAS', 'AMEX', 'NYSE NAT')
                        ORDER BY symbol
                        {limit_clause}
                    """)
                    
                    symbols = [row['symbol'] for row in instruments]
                    logger.info(f"📊 Found {len(symbols)} symbols from database")
                    return symbols
            except Exception as e:
                logger.warning(f"⚠️ Database query failed: {e}")
        
        # Fallback to major symbols if database unavailable
        major_symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'ADBE', 'CRM']
        logger.info(f"📊 Using fallback symbols: {major_symbols}")
        return major_symbols[:limit] if limit else major_symbols

    async def run_backfill(self, symbols: List[str], start_date: date, end_date: date):
        """
        Run the complete 1-minute backfill process.
        """
        import aiohttp
        
        logger.info("🚀 Starting EODHD 1-minute backfill process...")
        logger.info(f"📊 Symbols: {len(symbols)}")
        logger.info(f"📅 Date range: {start_date} to {end_date}")
        logger.info(f"💾 Storage path: {self.minute_data_path}")
        
        self.stats['total_symbols'] = len(symbols)
        
        # Create aiohttp session
        timeout = aiohttp.ClientTimeout(total=300)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            self.session = session
            
            for symbol in symbols:
                try:
                    symbol_stats = await self.backfill_symbol(symbol, start_date, end_date)
                    self.stats['processed_symbols'] += 1
                    
                    # Update global stats
                    self.stats['total_bars'] += symbol_stats['bars_collected']
                    self.stats['api_calls'] += symbol_stats['api_calls']
                    self.stats['errors'] += symbol_stats['errors']
                    
                    logger.info(f"📈 Progress: {self.stats['processed_symbols']}/{self.stats['total_symbols']} symbols completed")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to process {symbol}: {e}")
                    self.stats['errors'] += 1
                    continue

    def log_final_summary(self):
        """Log comprehensive final summary."""
        logger.info("=" * 80)
        logger.info("🎉 EODHD 1-MINUTE BACKFILL COMPLETE")
        logger.info("=" * 80)
        logger.info(f"📊 PROCESSING SUMMARY:")
        logger.info(f"  Total Symbols: {self.stats['total_symbols']:,}")
        logger.info(f"  Processed Symbols: {self.stats['processed_symbols']:,}")
        logger.info(f"  Total Bars Collected: {self.stats['total_bars']:,}")
        logger.info(f"  Files Created: {self.stats['files_created']:,}")
        logger.info(f"  API Calls Made: {self.stats['api_calls']:,}")
        logger.info(f"  Errors: {self.stats['errors']:,}")
        logger.info("")
        logger.info(f"💾 STORAGE SUMMARY:")
        logger.info(f"  Base Path: {self.base_path}")
        logger.info(f"  Minute Data Path: {self.minute_data_path}")
        
        success_rate = ((self.stats['processed_symbols']) / self.stats['total_symbols'] * 100) if self.stats['total_symbols'] > 0 else 0
        logger.info(f"✅ Success Rate: {success_rate:.1f}%")
        logger.info("=" * 80)

async def main():
    parser = argparse.ArgumentParser(description="EODHD 1-minute backfill to D: drive")
    parser.add_argument('--environment', type=str, default='dev', choices=['test', 'intg', 'prod', 'dev'], 
                       help='Environment to use (default: dev)')
    parser.add_argument('--gin_config', type=str, default=None, help='Path to Gin config file (optional)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--limit', type=int, default=int(os.getenv('LIMIT', '10')), help='Limit number of instruments to process')
    parser.add_argument('--years', type=int, default=int(os.getenv('YEARS', '30')), help='Number of years of historical data to fetch')
    parser.add_argument('--start_date', type=str, default=None, help='Start date (YYYY-MM-DD), overrides --years')
    parser.add_argument('--end_date', type=str, default=None, help='End date (YYYY-MM-DD), defaults to today')
    parser.add_argument('--base_path', type=str, default='/mnt/d/ats-data', help='Base storage path (default: /mnt/d/ats-data)')
    
    args = parser.parse_args()
    
    # Set up logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    try:
        # Get EODHD API key
        eodhd_api_key = os.environ.get("EODHD_API_KEY")
        if not eodhd_api_key:
            logger.error("No EODHD API key found. Set EODHD_API_KEY environment variable.")
            sys.exit(1)
        
        logger.info("EODHD API key found")
        
        # Calculate date range
        if args.start_date:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        else:
            start_date = (datetime.now() - timedelta(days=365 * args.years)).date()
        
        if args.end_date:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        else:
            end_date = datetime.now().date()
        
        logger.info(f"Backfilling 1-minute data from {start_date} to {end_date}")
        
        # Initialize backfiller
        backfiller = EODHDMinuteBackfiller(
            api_key=eodhd_api_key,
            base_path=args.base_path
        )
        
        # Try to connect to database for symbols (optional)
        pool = None
        try:
            if args.gin_config:
                gin_config_path = args.gin_config
            else:
                gin_config_map = {
                    'test': 'config/app_test.gin',
                    'intg': 'config/app_intg.gin', 
                    'prod': 'config/app_prod.gin',
                    'dev': 'config/app_dev.gin',
                }
                gin_config_path = gin_config_map.get(args.environment)
            
            if gin_config_path and os.path.exists(gin_config_path):
                gin.parse_config_file(gin_config_path)
                env_type = EnvironmentType(args.environment)
                env = Environment(gin_config_path=gin_config_path, env_type=env_type)
                pool = await Database.create_connection_pool(max_retries=1, initial_delay=1.0, timeout=5.0)
                logger.info("Connected to database for symbol list")
        except Exception as e:
            logger.warning(f"Database connection failed, using fallback symbols: {e}")
        
        # Get symbols to process
        symbols = await backfiller.get_symbols_for_backfill(pool, limit=args.limit)
        
        # Run backfill
        await backfiller.run_backfill(symbols, start_date, end_date)
        
        # Log final summary
        backfiller.log_final_summary()
        
        if pool:
            await pool.close()
        
        logger.info("EODHD 1-minute backfill complete")
        
    except Exception as e:
        logger.error(f"Failed to run EODHD 1-minute backfill: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())