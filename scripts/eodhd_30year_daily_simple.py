#!/usr/bin/env python3
"""
Simple EODHD 30-Year Daily Price Backfill

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
logger = logging.getLogger("eodhd_simple")

async def main():
    # Get API key
    api_key = os.getenv("EODHD_API_KEY")
    if not api_key:
        logger.error("EODHD_API_KEY environment variable not set")
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
    
    # Check if EODHD table exists, create if not
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS dev_daily_prices_eodhd (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                open NUMERIC(12,4),
                high NUMERIC(12,4),
                low NUMERIC(12,4),
                close NUMERIC(12,4),
                adjusted_close NUMERIC(12,4),
                volume BIGINT,
                instrument_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, instrument_id)
            )
        """)
        logger.info("✅ EODHD table ready")
    except Exception as e:
        logger.error(f"❌ Failed to create table: {e}")
        await conn.close()
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
        
        # Fetch from EODHD API
        url = f"https://eodhd.com/api/eod/{symbol}.US"
        params = {
            'from': start_date.strftime('%Y-%m-%d'),
            'to': end_date.strftime('%Y-%m-%d'),
            'period': 'd',
            'fmt': 'json',
            'api_token': api_key
        }
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                if data and isinstance(data, list):
                    # Prepare records for insertion
                    records = []
                    for item in data:
                        records.append((
                            datetime.strptime(item['date'], "%Y-%m-%d").date(),
                            symbol,
                            item.get('open'),
                            item.get('high'),
                            item.get('low'),
                            item.get('close'),
                            item.get('adjusted_close'),
                            item.get('volume'),
                            instrument_id
                        ))
                    
                    # Insert with UPSERT
                    if records:
                        await conn.executemany("""
                            INSERT INTO dev_daily_prices_eodhd 
                            (date, symbol, open, high, low, close, adjusted_close, volume, instrument_id)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            ON CONFLICT (date, instrument_id) DO UPDATE SET
                                symbol = EXCLUDED.symbol,
                                open = EXCLUDED.open,
                                high = EXCLUDED.high,
                                low = EXCLUDED.low,
                                close = EXCLUDED.close,
                                adjusted_close = EXCLUDED.adjusted_close,
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
        
        # Rate limiting (EODHD allows 20 requests/minute for free tier)
        time.sleep(3.0)  # 3 seconds between requests
    
    await conn.close()
    logger.info(f"🎉 Complete! Processed {len(instruments)} instruments, {total_records} total records")
    return 0

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)