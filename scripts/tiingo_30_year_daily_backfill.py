#!/usr/bin/env python3
"""
Tiingo 30-Year Daily Price Backfill

Comprehensive backfill of historical daily price data for all active instruments
using Tiingo API with 30-year historical depth. Based on existing working patterns
with enhanced idempotent operations.
"""

import sys
sys.path.append('/workspace/src')

import os
import asyncio
import asyncpg
import requests
import logging
from datetime import datetime, timedelta, date
import time
import json
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("tiingo_30_year_daily_backfill")

class Tiingo30YearBackfiller:
    """
    Tiingo 30-year daily price backfiller with idempotent operations.
    
    Features:
    - 30-year historical data collection
    - Idempotent UPSERT operations
    - Rate limiting (1000 requests/hour for paid Tiingo)
    - Resume capability with existing data detection
    - Uses dev_instruments table for symbol list
    """
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.tiingo.com/tiingo/daily"
        
        # Rate limiting (Tiingo allows ~1000 requests/hour for paid plans)
        self.request_delay = 3.6  # seconds between requests (safer than 3.6 limit)
        
        # Statistics
        self.stats = {
            'total_instruments': 0,
            'processed_instruments': 0,
            'total_records': 0,
            'api_calls': 0,
            'errors': 0,
            'skipped_instruments': 0
        }
        
        logger.info(f"📊 Tiingo 30-Year Backfiller initialized")
        logger.info(f"   Rate limit: {3600/self.request_delay:.1f} requests/hour")

    async def get_database_connection(self):
        """Get database connection."""
        db_host = os.getenv('DB_HOST', 'postgres')
        db_port = int(os.getenv('DB_PORT', '5432'))
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', 'dev_password')
        db_name = os.getenv('DB_NAME', 'dev_db')
        
        return await asyncpg.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password,
            database=db_name
        )

    async def get_instruments_for_backfill(self, conn, limit=None):
        """Get active instruments from dev_instruments table."""
        limit_clause = f"LIMIT {limit}" if limit else ""
        
        instruments = await conn.fetch(f"""
            SELECT id, symbol, name, exchange, active
            FROM dev_instruments 
            WHERE active = true 
              AND symbol IS NOT NULL 
              AND symbol != ''
              AND exchange IN ('NASDAQ', 'NYSE', 'NYSE ARCA', 'BATS', 'XNYS', 'NYSE MKT', 'XNAS', 'AMEX', 'NYSE NAT')
            ORDER BY symbol
            {limit_clause}
        """)
        
        self.stats['total_instruments'] = len(instruments)
        logger.info(f"📊 Found {len(instruments)} instruments for 30-year backfill")
        return instruments

    def download_tiingo_daily_prices(self, symbol, start_date, end_date):
        """Download daily prices from Tiingo API."""
        url = f"{self.base_url}/{symbol}/prices"
        params = {
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'format': 'json',
            'token': self.api_key
        }
        
        try:
            response = requests.get(url, params=params)
            self.stats['api_calls'] += 1
            
            if response.status_code == 200:
                data = response.json()
                logger.debug(f"✅ Downloaded {len(data)} records for {symbol}")
                return data
            elif response.status_code == 404:
                logger.debug(f"⚠️ No data available for {symbol}")
                return []
            elif response.status_code == 429:
                logger.warning(f"⚠️ Rate limit hit for {symbol}, waiting...")
                time.sleep(60)  # Wait 1 minute
                return self.download_tiingo_daily_prices(symbol, start_date, end_date)
            else:
                logger.error(f"❌ Tiingo API error for {symbol}: {response.status_code}")
                self.stats['errors'] += 1
                return []
                
        except Exception as e:
            logger.error(f"❌ Error downloading {symbol}: {e}")
            self.stats['errors'] += 1
            return []

    async def insert_daily_prices_idempotent(self, conn, instrument_id, symbol, prices):
        """Insert daily prices with idempotent UPSERT operations."""
        if not prices:
            return 0
        
        # Prepare data for insertion
        rows = []
        for price in prices:
            try:
                # Parse Tiingo date format
                if isinstance(price['date'], str):
                    date_val = datetime.strptime(price['date'][:10], '%Y-%m-%d').date()
                else:
                    date_val = price['date']
                
                rows.append((
                    date_val,
                    symbol,
                    price.get('open'),
                    price.get('high'),
                    price.get('low'),
                    price.get('close'),
                    price.get('volume', 0),
                    instrument_id
                ))
            except Exception as e:
                logger.error(f"❌ Error processing price record for {symbol}: {e}")
                continue
        
        if not rows:
            return 0
        
        # Insert with idempotent UPSERT (using existing Tiingo table schema)
        try:
            result = await conn.executemany("""
                INSERT INTO dev_daily_prices_tiingo 
                (date, symbol, open, high, low, close, volume, instrument_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (date, instrument_id) DO UPDATE SET
                    symbol = EXCLUDED.symbol,
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume
            """, rows)
            
            logger.info(f"💾 Inserted {len(rows)} price records for {symbol}")
            self.stats['total_records'] += len(rows)
            return len(rows)
            
        except Exception as e:
            logger.error(f"❌ Database error inserting prices for {symbol}: {e}")
            self.stats['errors'] += 1
            return 0

    async def check_existing_data(self, conn, instrument_id, start_date, end_date):
        """Check if instrument already has data in the date range."""
        count = await conn.fetchval("""
            SELECT COUNT(*) FROM dev_daily_prices_tiingo
            WHERE instrument_id = $1 AND date BETWEEN $2 AND $3
        """, instrument_id, start_date, end_date)
        
        return count

    async def backfill_instrument(self, conn, instrument, start_date, end_date, skip_existing=True):
        """Backfill daily prices for a single instrument."""
        instrument_id = instrument['id']
        symbol = instrument['symbol']
        
        try:
            # Check if we should skip existing data
            if skip_existing:
                existing_count = await self.check_existing_data(conn, instrument_id, start_date, end_date)
                if existing_count > 0:
                    logger.info(f"⏭️ Skipping {symbol} - already has {existing_count} records")
                    self.stats['skipped_instruments'] += 1
                    return 0
            
            logger.info(f"📈 Processing {symbol} (ID: {instrument_id}) for 30-year backfill...")
            
            # Download data from Tiingo
            prices = self.download_tiingo_daily_prices(symbol, start_date, end_date)
            
            if not prices:
                logger.warning(f"⚠️ No price data for {symbol}")
                return 0
            
            # Insert data idempotently
            inserted_count = await self.insert_daily_prices_idempotent(conn, instrument_id, symbol, prices)
            
            logger.info(f"✅ Completed {symbol}: {inserted_count} records inserted")
            self.stats['processed_instruments'] += 1
            
            # Rate limiting delay
            time.sleep(self.request_delay)
            
            return inserted_count
            
        except Exception as e:
            logger.error(f"❌ Failed to process {symbol}: {e}")
            self.stats['errors'] += 1
            return 0

    async def run_backfill(self, start_date, end_date, limit=None, skip_existing=True):
        """Run the complete 30-year backfill process."""
        logger.info("🚀 Starting Tiingo 30-year daily price backfill...")
        logger.info(f"📅 Date range: {start_date} to {end_date}")
        
        conn = await self.get_database_connection()
        
        try:
            # Get instruments to process
            instruments = await self.get_instruments_for_backfill(conn, limit)
            
            if not instruments:
                logger.warning("❌ No instruments found for backfill")
                return
            
            logger.info(f"📊 Processing {len(instruments)} instruments")
            
            # Process each instrument
            for i, instrument in enumerate(instruments, 1):
                try:
                    await self.backfill_instrument(conn, instrument, start_date, end_date, skip_existing)
                    
                    # Progress logging
                    if i % 100 == 0 or i == len(instruments):
                        progress = (i / len(instruments)) * 100
                        logger.info(f"📊 Progress: {i:,}/{len(instruments):,} ({progress:.1f}%) - "
                                  f"{self.stats['total_records']:,} total records")
                        
                except Exception as e:
                    logger.error(f"❌ Critical error processing instrument {instrument.get('symbol', 'unknown')}: {e}")
                    continue
            
        finally:
            await conn.close()
    
    def log_final_summary(self):
        """Log comprehensive final summary."""
        logger.info("=" * 80)
        logger.info("🎉 TIINGO 30-YEAR DAILY PRICE BACKFILL COMPLETE")
        logger.info("=" * 80)
        logger.info(f"📊 PROCESSING SUMMARY:")
        logger.info(f"  Total Instruments: {self.stats['total_instruments']:,}")
        logger.info(f"  Processed Instruments: {self.stats['processed_instruments']:,}")
        logger.info(f"  Skipped Instruments: {self.stats['skipped_instruments']:,}")
        logger.info(f"  Total Records Inserted: {self.stats['total_records']:,}")
        logger.info(f"  API Calls Made: {self.stats['api_calls']:,}")
        logger.info(f"  Errors: {self.stats['errors']:,}")
        logger.info("")
        
        success_rate = ((self.stats['processed_instruments']) / self.stats['total_instruments'] * 100) if self.stats['total_instruments'] > 0 else 0
        logger.info(f"✅ Success Rate: {success_rate:.1f}%")
        
        avg_records = self.stats['total_records'] / max(1, self.stats['processed_instruments'])
        logger.info(f"📈 Average Records per Instrument: {avg_records:.1f}")
        logger.info("=" * 80)

async def main():
    parser = argparse.ArgumentParser(description="Tiingo 30-year daily price backfill")
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--limit', type=int, default=int(os.getenv('LIMIT', '0')) or None, 
                       help='Limit number of instruments to process')
    parser.add_argument('--years', type=int, default=int(os.getenv('YEARS', '30')), 
                       help='Number of years of historical data to fetch (default: 30)')
    parser.add_argument('--start_date', type=str, default=None, 
                       help='Start date (YYYY-MM-DD), overrides --years')
    parser.add_argument('--end_date', type=str, default=None, 
                       help='End date (YYYY-MM-DD), defaults to today')
    parser.add_argument('--skip_existing', action='store_true', default=True, 
                       help='Skip instruments that already have price data')
    
    args = parser.parse_args()
    
    # Set up logging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
    
    try:
        # Get Tiingo API key
        tiingo_api_key = os.environ.get("TIINGO_API_KEY")
        if not tiingo_api_key:
            logger.error("❌ TIINGO_API_KEY environment variable not set")
            sys.exit(1)
        
        logger.info("✅ Tiingo API key found")
        
        # Calculate date range
        if args.start_date:
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        else:
            start_date = (datetime.now() - timedelta(days=365 * args.years)).date()
        
        if args.end_date:
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
        else:
            end_date = datetime.now().date()
        
        logger.info(f"📅 Backfilling Tiingo prices from {start_date} to {end_date}")
        
        # Initialize backfiller
        backfiller = Tiingo30YearBackfiller(tiingo_api_key)
        
        # Run backfill
        await backfiller.run_backfill(
            start_date, end_date, 
            limit=args.limit, 
            skip_existing=args.skip_existing
        )
        
        # Log final summary
        backfiller.log_final_summary()
        
        logger.info("✅ Tiingo 30-year daily price backfill complete")
        
    except Exception as e:
        logger.error(f"❌ Failed to run Tiingo 30-year backfill: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())