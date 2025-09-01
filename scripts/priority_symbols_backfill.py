#!/usr/bin/env python3
"""
Priority Symbols Daily Price Backfill

This script fills missing daily price data for priority symbols (AAPL, TSLA) 
using the working database connection pattern from run_dev.py.
"""

import os
import sys
import asyncio
import asyncpg
import requests
import logging
from datetime import datetime, timedelta, date
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("priority_symbols_backfill")

# Priority symbols to backfill
PRIORITY_SYMBOLS = ['AAPL', 'TSLA', 'MSFT', 'GOOGL', 'AMZN']

class PrioritySymbolsBackfiller:
    """Backfill daily prices for priority symbols using working database connection."""
    
    def __init__(self):
        # API keys
        self.tiingo_api_key = "5f40b4f36e171405746304ec0e5a6f3aa9ca77e5"
        self.polygon_api_key = "wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD"
        self.eodhd_api_key = "68aa0c7d2fe831.67386369"
        
        # Database connection (using Docker network settings)
        self.db_url = "postgresql://postgres:dev_password@ats-dev-postgres:5432/dev_db"
        
        self.stats = {
            'total_symbols': 0,
            'successful_symbols': 0,
            'failed_symbols': 0,
            'total_records': 0
        }
        
    async def get_database_connection(self):
        """Get database connection using run_dev.py compatible settings."""
        try:
            return await asyncpg.connect(self.db_url)
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def download_tiingo_daily_prices(self, symbol, start_date, end_date):
        """Download daily prices from Tiingo API."""
        url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
        params = {
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'format': 'json',
            'token': self.tiingo_api_key
        }
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                logger.info(f"✅ Downloaded {len(data)} records for {symbol} from Tiingo")
                return data
            elif response.status_code == 404:
                logger.warning(f"⚠️ No Tiingo data available for {symbol}")
                return []
            else:
                logger.error(f"❌ Tiingo API error for {symbol}: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"❌ Error downloading Tiingo data for {symbol}: {e}")
            return []
    
    async def get_instrument_id(self, conn, symbol):
        """Get instrument ID for symbol."""
        result = await conn.fetchval(
            "SELECT id FROM dev_instruments WHERE symbol = $1 LIMIT 1",
            symbol
        )
        return result
    
    async def check_existing_data_count(self, conn, instrument_id, vendor):
        """Check how much data already exists."""
        table_name = f"dev_daily_prices_{vendor}"
        count = await conn.fetchval(
            f"SELECT COUNT(*) FROM {table_name} WHERE instrument_id = $1",
            instrument_id
        )
        return count
    
    async def insert_tiingo_prices(self, conn, instrument_id, symbol, prices):
        """Insert Tiingo daily prices with conflict resolution."""
        if not prices:
            return 0
            
        rows = []
        for price in prices:
            try:
                date_val = datetime.strptime(price['date'][:10], '%Y-%m-%d').date()
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
                logger.error(f"Error processing price record for {symbol}: {e}")
                continue
                
        if not rows:
            return 0
            
        try:
            await conn.executemany("""
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
            
            logger.info(f"💾 Inserted {len(rows)} Tiingo records for {symbol}")
            return len(rows)
        except Exception as e:
            logger.error(f"❌ Database error inserting Tiingo prices for {symbol}: {e}")
            return 0
    
    async def backfill_symbol(self, conn, symbol):
        """Backfill daily prices for a single symbol."""
        logger.info(f"🔄 Processing {symbol}...")
        
        # Get instrument ID
        instrument_id = await self.get_instrument_id(conn, symbol)
        if not instrument_id:
            logger.warning(f"❌ No instrument found for symbol {symbol}")
            return False
            
        logger.info(f"📊 Found instrument ID {instrument_id} for {symbol}")
        
        # Check existing data
        existing_tiingo = await self.check_existing_data_count(conn, instrument_id, 'tiingo')
        logger.info(f"📈 {symbol} existing Tiingo records: {existing_tiingo}")
        
        # Define backfill date range (last 2 years for testing)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=730)  # 2 years
        
        success_count = 0
        
        # Backfill Tiingo data
        if existing_tiingo < 500:  # Only backfill if we don't have much data
            logger.info(f"🔽 Downloading Tiingo data for {symbol} from {start_date} to {end_date}...")
            tiingo_prices = self.download_tiingo_daily_prices(symbol, start_date, end_date)
            if tiingo_prices:
                inserted = await self.insert_tiingo_prices(conn, instrument_id, symbol, tiingo_prices)
                if inserted > 0:
                    success_count += 1
                    self.stats['total_records'] += inserted
        else:
            logger.info(f"⏭️ Skipping {symbol} Tiingo backfill - already has {existing_tiingo} records")
            success_count += 1
        
        # Add small delay to be respectful to APIs
        time.sleep(1)
        
        return success_count > 0
    
    async def run_backfill(self):
        """Run the priority symbols backfill."""
        logger.info("🚀 Starting priority symbols backfill...")
        logger.info(f"🎯 Priority symbols: {', '.join(PRIORITY_SYMBOLS)}")
        
        conn = await self.get_database_connection()
        
        try:
            self.stats['total_symbols'] = len(PRIORITY_SYMBOLS)
            
            for symbol in PRIORITY_SYMBOLS:
                try:
                    success = await self.backfill_symbol(conn, symbol)
                    if success:
                        self.stats['successful_symbols'] += 1
                        logger.info(f"✅ Successfully processed {symbol}")
                    else:
                        self.stats['failed_symbols'] += 1
                        logger.warning(f"❌ Failed to process {symbol}")
                        
                except Exception as e:
                    logger.error(f"❌ Critical error processing {symbol}: {e}")
                    self.stats['failed_symbols'] += 1
                    
        finally:
            await conn.close()
            
        # Log summary
        logger.info("=" * 60)
        logger.info("🎉 PRIORITY SYMBOLS BACKFILL COMPLETE")
        logger.info("=" * 60)
        logger.info(f"📊 Total symbols processed: {self.stats['total_symbols']}")
        logger.info(f"✅ Successful: {self.stats['successful_symbols']}")
        logger.info(f"❌ Failed: {self.stats['failed_symbols']}")
        logger.info(f"💾 Total records inserted: {self.stats['total_records']}")
        logger.info("=" * 60)

async def main():
    try:
        backfiller = PrioritySymbolsBackfiller()
        await backfiller.run_backfill()
        logger.info("✅ Priority symbols backfill completed successfully")
    except Exception as e:
        logger.error(f"❌ Backfill failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())