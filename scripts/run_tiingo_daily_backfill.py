#!/usr/bin/env python3
"""
Comprehensive Tiingo Daily Price Backfill

Backfills historical daily price data for all active Tiingo instruments
using the fixed instrument population (11,642 active instruments).
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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("run_tiingo_daily_backfill")

async def get_active_tiingo_instruments():
    """Get all active Tiingo instruments from the database."""
    db_host = os.getenv('DB_HOST', 'postgres')
    db_port = os.getenv('DB_PORT', '5432')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'dev_password')
    db_name = os.getenv('DB_NAME', 'dev_db')
    
    conn = await asyncpg.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name
    )
    
    try:
        # Get all active Tiingo instruments (end_date IS NULL = active after fix)
        instruments = await conn.fetch("""
            SELECT symbol, name, start_date, exchange
            FROM dev_instrument_tiingo
            WHERE end_date IS NULL
            ORDER BY symbol
        """)
        
        logger.info(f"Found {len(instruments)} active Tiingo instruments")
        return [dict(inst) for inst in instruments]
        
    finally:
        await conn.close()

def download_tiingo_daily_prices(symbol, start_date, end_date, api_key):
    """Download daily prices from Tiingo API."""
    url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices?startDate={start_date}&endDate={end_date}&format=json&token={api_key}"
    
    try:
        response = requests.get(url, timeout=30)
        
        if response.status_code == 429:
            logger.warning(f"Rate limit hit for {symbol}, waiting 60 seconds...")
            time.sleep(60)
            return download_tiingo_daily_prices(symbol, start_date, end_date, api_key)
        
        if response.status_code == 404:
            logger.info(f"No data available for {symbol} (404)")
            return []
        
        if response.status_code != 200:
            logger.error(f"Failed to fetch {symbol}: {response.status_code} - {response.text[:200]}")
            return []
        
        data = response.json()
        
        if not data:
            logger.info(f"No price data for {symbol}")
            return []
        
        logger.info(f"Downloaded {len(data)} price records for {symbol}")
        return data
        
    except Exception as e:
        logger.error(f"Error downloading {symbol}: {e}")
        return []

async def get_instrument_id_for_tiingo_symbol(symbol):
    """Get instrument_id for a Tiingo symbol from dev_instruments table."""
    db_host = os.getenv('DB_HOST', 'postgres')
    db_port = os.getenv('DB_PORT', '5432')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'dev_password')
    db_name = os.getenv('DB_NAME', 'dev_db')
    
    conn = await asyncpg.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name
    )
    
    try:
        # Try to find existing instrument_id by symbol
        row = await conn.fetchrow("""
            SELECT id FROM dev_instruments WHERE symbol = $1
        """, symbol)
        
        if row:
            return row['id']
        
        # If not found, create a new instrument entry based on Tiingo data
        tiingo_row = await conn.fetchrow("""
            SELECT * FROM dev_instrument_tiingo WHERE symbol = $1
        """, symbol)
        
        if tiingo_row:
            # Create instrument in main table
            instrument_id = await conn.fetchval("""
                INSERT INTO dev_instruments 
                (symbol, name, exchange, type, currency, active, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
                RETURNING id
            """, 
            tiingo_row['symbol'],
            tiingo_row['name'],
            tiingo_row['exchange'],
            tiingo_row['asset_type'] or 'stock',
            tiingo_row['currency'] or 'USD',
            True  # Active since we're only processing active Tiingo instruments
            )
            
            logger.info(f"Created instrument_id {instrument_id} for {symbol}")
            return instrument_id
        
        logger.error(f"Could not find or create instrument_id for {symbol}")
        return None
        
    finally:
        await conn.close()

async def insert_tiingo_daily_prices(symbol, prices, instrument_id):
    """Insert daily prices into dev_daily_prices_tiingo table."""
    if not prices or not instrument_id:
        return 0
    
    db_host = os.getenv('DB_HOST', 'postgres')
    db_port = os.getenv('DB_PORT', '5432')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'dev_password')
    db_name = os.getenv('DB_NAME', 'dev_db')
    
    conn = await asyncpg.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name
    )
    
    try:
        # Table already exists with different schema, no need to create
        
        # Prepare price records for insertion
        # Match existing table schema: date, symbol, open, high, low, close, adjclose, volume, status_id, instrument_id
        records = []
        for price in prices:
            # Parse date from ISO format
            price_date = datetime.strptime(price['date'][:10], '%Y-%m-%d').date()
            
            record = (
                price_date,                           # date
                symbol,                               # symbol  
                float(price.get('open', 0)),          # open
                float(price.get('high', 0)),          # high
                float(price.get('low', 0)),           # low
                float(price.get('close', 0)),         # close
                float(price.get('adjClose', 0)),      # adjclose (note: different column name)
                int(price.get('volume', 0)),          # volume
                None,                                 # status_id (set to NULL)
                instrument_id                         # instrument_id
            )
            records.append(record)
        
        # Insert with conflict handling (upsert)
        await conn.executemany("""
            INSERT INTO dev_daily_prices_tiingo 
            (date, symbol, open, high, low, close, adjclose, volume, status_id, instrument_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (date, instrument_id) DO UPDATE SET
                symbol = EXCLUDED.symbol,
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                adjclose = EXCLUDED.adjclose,
                volume = EXCLUDED.volume
        """, records)
        
        logger.info(f"Inserted {len(records)} price records for {symbol}")
        return len(records)
        
    except Exception as e:
        logger.error(f"Error inserting prices for {symbol}: {e}")
        return 0
    finally:
        await conn.close()

async def backfill_tiingo_instrument_prices(instrument, start_date, end_date, api_key):
    """Backfill prices for a single Tiingo instrument."""
    symbol = instrument['symbol']
    
    try:
        # Get or create instrument_id
        instrument_id = await get_instrument_id_for_tiingo_symbol(symbol)
        if not instrument_id:
            logger.error(f"Could not resolve instrument_id for {symbol}")
            return 0
        
        # Download prices from Tiingo
        prices = download_tiingo_daily_prices(
            symbol, 
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d'),
            api_key
        )
        
        if not prices:
            return 0
        
        # Insert prices into database
        inserted = await insert_tiingo_daily_prices(symbol, prices, instrument_id)
        return inserted
        
    except Exception as e:
        logger.error(f"Failed to backfill {symbol}: {e}")
        return 0

async def main():
    """Main Tiingo daily price backfill function."""
    # Get API key
    api_key = os.getenv('TIINGO_API_KEY')
    if not api_key:
        logger.error("❌ TIINGO_API_KEY environment variable not set")
        return False
    
    # Calculate date range - default to 5 years for comprehensive backfill
    end_date = date.today()
    start_date = end_date - timedelta(days=365 * 5)  # 5 years back
    
    logger.info(f"🚀 Starting comprehensive Tiingo daily price backfill")
    logger.info(f"📅 Date range: {start_date} to {end_date}")
    
    # Get all active instruments
    instruments = await get_active_tiingo_instruments()
    if not instruments:
        logger.error("No active Tiingo instruments found")
        return False
    
    logger.info(f"📊 Processing {len(instruments)} active Tiingo instruments")
    
    # Process instruments with rate limiting
    total_success = 0
    total_records = 0
    batch_size = 50  # Process in batches for better progress tracking
    
    for i in range(0, len(instruments), batch_size):
        batch = instruments[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (len(instruments) + batch_size - 1) // batch_size
        
        logger.info(f"📦 Processing batch {batch_num}/{total_batches} ({len(batch)} instruments)")
        
        batch_success = 0
        batch_records = 0
        
        for instrument in batch:
            symbol = instrument['symbol']
            
            logger.info(f"📈 Processing {symbol}...")
            
            try:
                records_inserted = await backfill_tiingo_instrument_prices(
                    instrument, start_date, end_date, api_key
                )
                
                if records_inserted > 0:
                    batch_success += 1
                    batch_records += records_inserted
                    logger.info(f"✅ {symbol}: {records_inserted} records")
                else:
                    logger.warning(f"⚠️  {symbol}: No data inserted")
                
                # Rate limiting - Tiingo allows more requests than Polygon
                time.sleep(1)  # 1 second between calls
                
            except Exception as e:
                logger.error(f"❌ Failed to process {symbol}: {e}")
                continue
        
        total_success += batch_success
        total_records += batch_records
        
        logger.info(f"📊 Batch {batch_num} completed: {batch_success}/{len(batch)} successful, {batch_records} total records")
        
        # Longer delay between batches to be respectful of API limits
        if batch_num < total_batches:
            logger.info("⏸️  Pausing 30 seconds between batches...")
            time.sleep(30)
    
    logger.info(f"🎉 Tiingo daily price backfill complete!")
    logger.info(f"📊 Successfully processed: {total_success}/{len(instruments)} instruments")
    logger.info(f"📈 Total price records inserted: {total_records:,}")
    
    # Final statistics
    if total_success > 0:
        avg_records_per_instrument = total_records / total_success
        logger.info(f"📊 Average records per successful instrument: {avg_records_per_instrument:.1f}")
    
    return total_success > 0

if __name__ == "__main__":
    success = asyncio.run(main())
    if not success:
        sys.exit(1)