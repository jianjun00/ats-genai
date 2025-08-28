#!/usr/bin/env python3
"""
Direct 30-Year Polygon Daily Price Backfill

Simplified approach that works directly with dev_instrument_polygon table
and populates dev_daily_prices_polygon table without complex xref lookups.
"""

import sys
sys.path.append('/workspace/src')

import os
import asyncio
import logging
import requests
import asyncpg
from datetime import datetime, timedelta
import time
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("run_polygon_backfill_direct")

async def get_polygon_instruments():
    """Get all Polygon instruments from the database."""
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
        # Get all active Polygon instruments
        instruments = await conn.fetch("""
            SELECT symbol, name, active, list_date
            FROM dev_instrument_polygon
            WHERE active = true
            ORDER BY symbol
        """)
        
        logger.info(f"Found {len(instruments)} active Polygon instruments")
        return [dict(inst) for inst in instruments]
        
    finally:
        await conn.close()

def download_polygon_daily_prices(symbol, start_date, end_date, api_key):
    """Download daily prices from Polygon API."""
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}?adjusted=true&sort=asc&limit=50000&apiKey={api_key}"
    
    try:
        response = requests.get(url, timeout=30)
        
        if response.status_code == 429:
            logger.warning(f"Rate limit hit for {symbol}, waiting 60 seconds...")
            time.sleep(60)
            return download_polygon_daily_prices(symbol, start_date, end_date, api_key)
        
        if response.status_code != 200:
            logger.error(f"Failed to fetch {symbol}: {response.status_code} - {response.text[:200]}")
            return []
        
        data = response.json()
        
        if 'results' not in data or not data['results']:
            logger.info(f"No price data for {symbol}")
            return []
        
        prices = data['results']
        logger.info(f"Downloaded {len(prices)} price records for {symbol}")
        return prices
        
    except Exception as e:
        logger.error(f"Error downloading {symbol}: {e}")
        return []

async def get_instrument_id(symbol):
    """Get instrument_id for a symbol from dev_instruments table."""
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
        # Try to find existing instrument_id
        row = await conn.fetchrow("""
            SELECT id FROM dev_instruments WHERE symbol = $1
        """, symbol)
        
        if row:
            return row['id']
        
        # If not found, create a new instrument entry based on Polygon data
        polygon_row = await conn.fetchrow("""
            SELECT * FROM dev_instrument_polygon WHERE symbol = $1
        """, symbol)
        
        if polygon_row:
            # Create instrument in main table
            instrument_id = await conn.fetchval("""
                INSERT INTO dev_instruments 
                (symbol, name, exchange, type, currency, figi, isin, cusip, composite_figi, active, list_date, delist_date, created_at, updated_at, sector)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW(), NOW(), $13)
                RETURNING id
            """, 
            polygon_row['symbol'],
            polygon_row['name'],
            polygon_row['exchange'],
            polygon_row['type'],
            polygon_row['currency'],
            polygon_row['figi'],
            polygon_row['isin'],
            polygon_row['cusip'],
            polygon_row['composite_figi'],
            polygon_row['active'],
            polygon_row['list_date'],
            polygon_row['delist_date'],
            polygon_row['sector']
            )
            
            logger.info(f"Created instrument_id {instrument_id} for {symbol}")
            return instrument_id
        
        logger.error(f"Could not find or create instrument_id for {symbol}")
        return None
        
    finally:
        await conn.close()

async def insert_daily_prices(symbol, prices, instrument_id):
    """Insert daily prices into dev_daily_prices_polygon table."""
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
        # Prepare price records for insertion
        records = []
        for price in prices:
            # Convert timestamp from milliseconds to date
            date = datetime.fromtimestamp(price['t'] / 1000).date()
            
            record = (
                date,                     # date
                symbol,                   # symbol  
                float(price['o']),        # open
                float(price['h']),        # high
                float(price['l']),        # low
                float(price['c']),        # close
                int(price['v']),          # volume
                None,                     # market_cap (not available from price data)
                instrument_id             # instrument_id
            )
            records.append(record)
        
        # Insert with conflict handling (upsert)
        await conn.executemany("""
            INSERT INTO dev_daily_prices_polygon 
            (date, symbol, open, high, low, close, volume, market_cap, instrument_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (date, instrument_id) DO UPDATE SET
                symbol = EXCLUDED.symbol,
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                market_cap = EXCLUDED.market_cap
        """, records)
        
        logger.info(f"Inserted {len(records)} price records for {symbol}")
        return len(records)
        
    except Exception as e:
        logger.error(f"Error inserting prices for {symbol}: {e}")
        return 0
    finally:
        await conn.close()

async def backfill_instrument_prices(instrument, start_date, end_date, api_key):
    """Backfill prices for a single instrument."""
    symbol = instrument['symbol']
    
    try:
        # Get or create instrument_id
        instrument_id = await get_instrument_id(symbol)
        if not instrument_id:
            logger.error(f"Could not resolve instrument_id for {symbol}")
            return 0
        
        # Download prices from Polygon
        prices = download_polygon_daily_prices(
            symbol, 
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d'),
            api_key
        )
        
        if not prices:
            return 0
        
        # Insert prices into database
        inserted = await insert_daily_prices(symbol, prices, instrument_id)
        return inserted
        
    except Exception as e:
        logger.error(f"Failed to backfill {symbol}: {e}")
        return 0

async def main():
    """Main backfill function."""
    # Get API key
    api_key = os.getenv('POLYGON_API_KEY')
    if not api_key:
        logger.error("❌ POLYGON_API_KEY environment variable not set")
        return False
    
    # Calculate 30-year date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=365 * 30)  # 30 years back
    
    logger.info(f"🚀 Starting direct 30-year Polygon backfill")
    logger.info(f"📅 Date range: {start_date} to {end_date}")
    
    # Get all instruments
    instruments = await get_polygon_instruments()
    if not instruments:
        logger.error("No instruments found")
        return False
    
    logger.info(f"📊 Processing {len(instruments)} instruments")
    
    # Process instruments with rate limiting
    total_success = 0
    total_records = 0
    
    for i, instrument in enumerate(instruments, 1):
        symbol = instrument['symbol']
        
        logger.info(f"📈 [{i}/{len(instruments)}] Processing {symbol}...")
        
        try:
            records_inserted = await backfill_instrument_prices(
                instrument, start_date, end_date, api_key
            )
            
            if records_inserted > 0:
                total_success += 1
                total_records += records_inserted
                logger.info(f"✅ {symbol}: {records_inserted} records")
            else:
                logger.warning(f"⚠️  {symbol}: No data inserted")
            
            # Rate limiting - 5 API calls per minute for free tier
            time.sleep(12)  # 12 seconds between calls = 5 calls per minute
            
        except Exception as e:
            logger.error(f"❌ Failed to process {symbol}: {e}")
            continue
    
    logger.info(f"🎉 Backfill complete!")
    logger.info(f"📊 Successfully processed: {total_success}/{len(instruments)} instruments")
    logger.info(f"📈 Total price records inserted: {total_records}")
    
    return total_success > 0

if __name__ == "__main__":
    success = asyncio.run(main())
    if not success:
        sys.exit(1)