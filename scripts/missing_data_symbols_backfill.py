#!/usr/bin/env python3
"""
Missing Data Symbols Backfill

This script identifies and fills symbols that have NO daily price data
across Tiingo, EODHD, and Polygon vendors.
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
logger = logging.getLogger("missing_data_backfill")

class MissingDataBackfiller:
    """Backfill daily prices for symbols with no existing data."""
    
    def __init__(self):
        # API keys
        self.tiingo_api_key = "5f40b4f36e171405746304ec0e5a6f3aa9ca77e5"
        self.polygon_api_key = "wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD"
        self.eodhd_api_key = "68aa0c7d2fe831.67386369"
        
        # Database connection
        self.db_url = "postgresql://postgres:dev_password@ats-dev-postgres:5432/dev_db"
        
        self.stats = {
            'symbols_found': 0,
            'symbols_processed': 0,
            'tiingo_successful': 0,
            'eodhd_successful': 0,
            'polygon_successful': 0,
            'total_records': 0
        }
        
    async def get_database_connection(self):
        """Get database connection."""
        return await asyncpg.connect(self.db_url)
    
    async def find_symbols_with_no_data(self, conn, limit=10):
        """Find symbols that have no data in any vendor table."""
        
        query = """
        SELECT DISTINCT i.id, i.symbol, i.name, i.exchange
        FROM dev_instruments i
        LEFT JOIN dev_daily_prices_tiingo t ON i.id = t.instrument_id
        LEFT JOIN dev_daily_prices_eodhd e ON i.id = e.instrument_id  
        LEFT JOIN dev_daily_prices_polygon p ON i.id = p.instrument_id
        WHERE i.active = true 
          AND i.symbol IS NOT NULL 
          AND i.symbol != ''
          AND i.symbol ~ '^[A-Z]{1,5}$'  -- Only standard symbols
          AND t.instrument_id IS NULL    -- No Tiingo data
          AND e.instrument_id IS NULL    -- No EODHD data  
          AND p.instrument_id IS NULL    -- No Polygon data
        ORDER BY i.symbol
        LIMIT $1
        """
        
        results = await conn.fetch(query, limit)
        logger.info(f"📊 Found {len(results)} symbols with NO data across all vendors")
        
        if results:
            sample_symbols = [r['symbol'] for r in results[:5]]
            logger.info(f"🎯 Sample symbols: {', '.join(sample_symbols)}")
            
        return results
    
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
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if data:
                    logger.info(f"✅ Tiingo: Downloaded {len(data)} records for {symbol}")
                    return data
                else:
                    logger.debug(f"⚠️ Tiingo: No data for {symbol}")
                    return []
            elif response.status_code == 404:
                logger.debug(f"⚠️ Tiingo: {symbol} not found")
                return []
            elif response.status_code == 429:
                logger.warning(f"⚠️ Tiingo rate limit hit for {symbol}")
                time.sleep(30)
                return []
            else:
                logger.warning(f"⚠️ Tiingo API error for {symbol}: {response.status_code}")
                return []
        except Exception as e:
            logger.warning(f"⚠️ Tiingo error for {symbol}: {e}")
            return []
    
    def download_eodhd_daily_prices(self, symbol, start_date, end_date):
        """Download daily prices from EODHD API."""
        url = f"https://eodhd.com/api/eod/{symbol}.US"
        params = {
            'from': start_date.strftime('%Y-%m-%d'),
            'to': end_date.strftime('%Y-%m-%d'),
            'api_token': self.eodhd_api_key,
            'fmt': 'json'
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, list) and data:
                    logger.info(f"✅ EODHD: Downloaded {len(data)} records for {symbol}")
                    return data
                else:
                    logger.debug(f"⚠️ EODHD: No data for {symbol}")
                    return []
            else:
                logger.warning(f"⚠️ EODHD API error for {symbol}: {response.status_code}")
                return []
        except Exception as e:
            logger.warning(f"⚠️ EODHD error for {symbol}: {e}")
            return []
    
    async def insert_tiingo_prices(self, conn, instrument_id, symbol, prices):
        """Insert Tiingo daily prices."""
        if not prices:
            return 0
            
        rows = []
        for price in prices:
            try:
                date_val = datetime.strptime(price['date'][:10], '%Y-%m-%d').date()
                rows.append((
                    date_val, symbol, price.get('open'), price.get('high'), 
                    price.get('low'), price.get('close'), price.get('volume', 0), instrument_id
                ))
            except:
                continue
                
        if rows:
            try:
                await conn.executemany("""
                    INSERT INTO dev_daily_prices_tiingo 
                    (date, symbol, open, high, low, close, volume, instrument_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (date, instrument_id) DO NOTHING
                """, rows)
                return len(rows)
            except Exception as e:
                logger.error(f"❌ Database error inserting Tiingo data for {symbol}: {e}")
        return 0
    
    async def insert_eodhd_prices(self, conn, instrument_id, symbol, prices):
        """Insert EODHD daily prices."""
        if not prices:
            return 0
            
        rows = []
        for price in prices:
            try:
                date_val = datetime.strptime(price['date'], '%Y-%m-%d').date()
                rows.append((
                    date_val, symbol, price.get('open'), price.get('high'),
                    price.get('low'), price.get('close'), price.get('volume', 0), instrument_id
                ))
            except:
                continue
                
        if rows:
            try:
                await conn.executemany("""
                    INSERT INTO dev_daily_prices_eodhd 
                    (date, symbol, open, high, low, close, volume, instrument_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (date, instrument_id) DO NOTHING
                """, rows)
                return len(rows)
            except Exception as e:
                logger.error(f"❌ Database error inserting EODHD data for {symbol}: {e}")
        return 0
    
    async def backfill_symbol(self, conn, instrument):
        """Backfill data for a single symbol across all vendors."""
        symbol = instrument['symbol']
        instrument_id = instrument['id']
        
        logger.info(f"🔄 Processing {symbol} (ID: {instrument_id})...")
        
        # Date range: last 2 years for testing
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=730)
        
        inserted_counts = {'tiingo': 0, 'eodhd': 0}
        
        # Try Tiingo first (usually has good coverage)
        logger.info(f"🔽 Downloading Tiingo data for {symbol}...")
        tiingo_prices = self.download_tiingo_daily_prices(symbol, start_date, end_date)
        if tiingo_prices:
            inserted_counts['tiingo'] = await self.insert_tiingo_prices(conn, instrument_id, symbol, tiingo_prices)
            if inserted_counts['tiingo'] > 0:
                self.stats['tiingo_successful'] += 1
                logger.info(f"💾 Inserted {inserted_counts['tiingo']} Tiingo records for {symbol}")
        
        # Add delay between API calls
        time.sleep(2)
        
        # Try EODHD if we didn't get much from Tiingo
        if inserted_counts['tiingo'] < 100:
            logger.info(f"🔽 Downloading EODHD data for {symbol}...")
            eodhd_prices = self.download_eodhd_daily_prices(symbol, start_date, end_date)
            if eodhd_prices:
                inserted_counts['eodhd'] = await self.insert_eodhd_prices(conn, instrument_id, symbol, eodhd_prices)
                if inserted_counts['eodhd'] > 0:
                    self.stats['eodhd_successful'] += 1
                    logger.info(f"💾 Inserted {inserted_counts['eodhd']} EODHD records for {symbol}")
        
        total_inserted = sum(inserted_counts.values())
        self.stats['total_records'] += total_inserted
        
        if total_inserted > 0:
            logger.info(f"✅ Successfully backfilled {symbol}: {total_inserted} total records")
        else:
            logger.warning(f"⚠️ No data found for {symbol} from any vendor")
            
        # Add delay between symbols
        time.sleep(1)
        
        return total_inserted > 0
    
    async def run_backfill(self, limit=10):
        """Run the missing data backfill."""
        logger.info("🚀 Starting missing data backfill...")
        logger.info(f"📊 Processing up to {limit} symbols with no existing data")
        
        conn = await self.get_database_connection()
        
        try:
            # Find symbols with no data
            symbols_to_process = await self.find_symbols_with_no_data(conn, limit)
            
            if not symbols_to_process:
                logger.info("🎉 No symbols found with completely missing data!")
                return
                
            self.stats['symbols_found'] = len(symbols_to_process)
            
            for instrument in symbols_to_process:
                try:
                    success = await self.backfill_symbol(conn, instrument)
                    self.stats['symbols_processed'] += 1
                    
                    if success:
                        logger.info(f"✅ Success for {instrument['symbol']}")
                    else:
                        logger.warning(f"⚠️ No data found for {instrument['symbol']}")
                        
                except Exception as e:
                    logger.error(f"❌ Error processing {instrument['symbol']}: {e}")
                    
        finally:
            await conn.close()
            
        # Log summary
        logger.info("=" * 60)
        logger.info("🎉 MISSING DATA BACKFILL COMPLETE")
        logger.info("=" * 60) 
        logger.info(f"📊 Symbols found with no data: {self.stats['symbols_found']}")
        logger.info(f"🔄 Symbols processed: {self.stats['symbols_processed']}")
        logger.info(f"✅ Tiingo successful: {self.stats['tiingo_successful']}")
        logger.info(f"✅ EODHD successful: {self.stats['eodhd_successful']}")
        logger.info(f"💾 Total records inserted: {self.stats['total_records']:,}")
        
        if self.stats['total_records'] > 0:
            avg_records = self.stats['total_records'] / max(1, self.stats['symbols_processed'])
            logger.info(f"📈 Average records per symbol: {avg_records:.0f}")
        
        logger.info("=" * 60)

async def main():
    """Main function."""
    try:
        # Process 20 symbols with no data
        backfiller = MissingDataBackfiller()
        await backfiller.run_backfill(limit=20)
        logger.info("✅ Missing data backfill completed successfully")
    except Exception as e:
        logger.error(f"❌ Backfill failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())