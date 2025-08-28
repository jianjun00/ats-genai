#!/usr/bin/env python3
"""
Simple Tiingo 30-Year Daily Price Backfill

Fixed version with correct database connection for Docker environment.
"""

import sys
sys.path.append('/workspace/src')

import os
import asyncio
import asyncpg
import requests
import logging
from datetime import datetime, timedelta
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("tiingo_simple")

async def main():
    # Get API key
    api_key = os.getenv("TIINGO_API_KEY")
    if not api_key:
        logger.error("TIINGO_API_KEY environment variable not set")
        return 1
    
    # Database connection (Docker-compatible)
    try:
        conn = await asyncpg.connect(
            host='postgres',  # Docker service name
            port=5432,        # Internal Docker port
            user='postgres',
            password='dev_password',
            database='dev_db'
        )
        logger.info("✅ Connected to database")
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return 1
    
    # Get instruments
    try:
        instruments = await conn.fetch("""
            SELECT id, symbol FROM dev_instruments 
            WHERE active = true AND symbol ~ '^[A-Z]{1,5}$'
            ORDER BY symbol LIMIT 10
        """)
        logger.info(f"📊 Retrieved {len(instruments)} instruments for testing")
    except Exception as e:
        logger.error(f"❌ Failed to get instruments: {e}")
        await conn.close()
        return 1
    
    # Process each instrument
    total_records = 0
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=365 * 30)  # 30 years
    
    for i, instrument in enumerate(instruments):
        symbol = instrument['symbol']
        instrument_id = instrument['id']
        
        logger.info(f"📈 Processing {symbol} ({i+1}/{len(instruments)})...")
        
        # Fetch from Tiingo API
        url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
        params = {
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'format': 'json',
            'token': api_key
        }
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                if data:
                    # Prepare records for insertion
                    records = []
                    for item in data:
                        records.append((
                            datetime.strptime(item['date'][:10], "%Y-%m-%d").date(),
                            symbol,
                            item.get('open'),
                            item.get('high'),
                            item.get('low'),
                            item.get('close'),
                            item.get('volume'),
                            instrument_id
                        ))
                    
                    # Insert with UPSERT
                    if records:
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
                        """, records)
                        
                        total_records += len(records)
                        logger.info(f"✅ Inserted {len(records)} records for {symbol}")
                    else:
                        logger.warning(f"⚠️ No records for {symbol}")
                else:
                    logger.warning(f"⚠️ Empty response for {symbol}")
            elif response.status_code == 429:
                logger.warning(f"⚠️ Rate limited for {symbol}, waiting...")
                time.sleep(60)
                continue
            else:
                logger.error(f"❌ API error for {symbol}: {response.status_code}")
            
        except Exception as e:
            logger.error(f"❌ Error processing {symbol}: {e}")
            continue
        
        # Rate limiting
        time.sleep(1.0)  # 1 second between requests
    
    await conn.close()
    logger.info(f"🎉 Complete! Processed {len(instruments)} instruments, {total_records} total records")
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)