#!/usr/bin/env python3
"""
Priority Polygon backfill for major stocks missing data
"""

import asyncio
import asyncpg
import requests
import time
from datetime import datetime, date, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

POLYGON_API_KEY = "wfrcZNX3ZJJt55Or_CmBXda8G8e8tABD"
BASE_URL = "https://api.polygon.io/v2/aggs/ticker"

# Priority symbols to backfill
PRIORITY_SYMBOLS = [
    'MSFT', 'GOOGL', 'GOOG', 'META', 'TSLA', 'NVDA', 'BRK.A', 'BRK.B',
    'AMZN', 'AAPL', 'UNH', 'JNJ', 'JPM', 'V', 'PG', 'MA', 'HD', 'CVX',
    'LLY', 'ABBV', 'BAC', 'AVGO', 'PFE', 'KO', 'PEP', 'TMO', 'COST',
    'MRK', 'WMT', 'ABT', 'ACN', 'DHR', 'VZ', 'CSCO', 'DIS', 'ADBE',
    'SPY', 'QQQ', 'IWM', 'EFA', 'VTI', 'EEM', 'TLT', 'GLD', 'SLV'
]

async def get_db_connection():
    """Get database connection."""
    return await asyncpg.connect(
        host="ats-dev-postgres",
        port=5432,
        user="postgres", 
        password="dev_password",
        database="dev_db"
    )

async def get_missing_symbols():
    """Get priority symbols that are missing Polygon data."""
    conn = await get_db_connection()
    
    # Get symbols that exist in instruments but missing in Polygon
    query = """
    SELECT i.id, i.symbol, i.name 
    FROM dev_instruments i 
    WHERE i.symbol = ANY($1) 
    AND i.active = true
    AND i.id NOT IN (SELECT DISTINCT instrument_id FROM dev_daily_prices_polygon)
    ORDER BY i.symbol
    """
    
    missing = await conn.fetch(query, PRIORITY_SYMBOLS)
    await conn.close()
    
    return missing

async def fetch_polygon_data(symbol, from_date, to_date):
    """Fetch daily data from Polygon API."""
    url = f"{BASE_URL}/{symbol}/range/1/day/{from_date}/{to_date}"
    params = {
        "adjusted": "true",
        "sort": "asc", 
        "limit": 50000,
        "apikey": POLYGON_API_KEY
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if data.get("status") == "OK" and "results" in data:
            return data["results"]
        else:
            logger.warning(f"No data returned for {symbol}: {data.get('message', 'Unknown error')}")
            return []
            
    except Exception as e:
        logger.error(f"API error for {symbol}: {e}")
        return []

async def insert_polygon_data(conn, instrument_id, symbol, data):
    """Insert Polygon data into database."""
    if not data:
        return 0
        
    records = []
    for item in data:
        # Convert timestamp (ms) to date
        date_val = date.fromtimestamp(item['t'] / 1000)
        
        record = (
            date_val,
            symbol,
            float(item['o']),  # open
            float(item['h']),  # high  
            float(item['l']),  # low
            float(item['c']),  # close
            int(item['v']),    # volume
            None,              # market_cap
            instrument_id
        )
        records.append(record)
    
    if records:
        # Insert with conflict handling
        insert_query = """
        INSERT INTO dev_daily_prices_polygon 
        (date, symbol, open, high, low, close, volume, market_cap, instrument_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (date, instrument_id) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            updated_at = CURRENT_TIMESTAMP
        """
        
        await conn.executemany(insert_query, records)
        logger.info(f"✅ Inserted {len(records)} records for {symbol}")
        
    return len(records)

async def backfill_symbol(symbol, instrument_id, name):
    """Backfill data for a single symbol."""
    logger.info(f"🔄 Backfilling {symbol} ({name})...")
    
    # Use 10 year range (2015-2025) for faster backfill
    from_date = "2015-01-01"
    to_date = "2025-08-30"
    
    conn = await get_db_connection()
    
    try:
        # Fetch data from Polygon
        data = await fetch_polygon_data(symbol, from_date, to_date)
        
        if data:
            # Insert into database
            inserted = await insert_polygon_data(conn, instrument_id, symbol, data)
            logger.info(f"✅ {symbol}: {inserted} records inserted")
            return inserted
        else:
            logger.warning(f"⚠️ {symbol}: No data available")
            return 0
            
    except Exception as e:
        logger.error(f"❌ {symbol}: {e}")
        return 0
    finally:
        await conn.close()

async def main():
    """Main backfill function."""
    logger.info("🚀 Starting priority Polygon backfill...")
    
    # Get missing symbols
    missing_symbols = await get_missing_symbols()
    logger.info(f"📊 Found {len(missing_symbols)} priority symbols missing Polygon data")
    
    total_inserted = 0
    
    for record in missing_symbols:
        instrument_id = record['id']
        symbol = record['symbol']
        name = record['name']
        
        try:
            inserted = await backfill_symbol(symbol, instrument_id, name)
            total_inserted += inserted
            
            # Rate limit: 5 requests per minute
            await asyncio.sleep(12)  # 12 seconds between requests
            
        except Exception as e:
            logger.error(f"Failed to backfill {symbol}: {e}")
            continue
    
    logger.info(f"🎉 Priority backfill completed! Total records: {total_inserted}")

if __name__ == "__main__":
    asyncio.run(main())