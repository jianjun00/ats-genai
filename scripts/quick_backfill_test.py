#!/usr/bin/env python3
"""
Quick Backfill Test

Test backfill with just 1-2 symbols to verify the system works.
"""

import os
import sys
import asyncio
import asyncpg
import requests
import logging
from datetime import datetime, timedelta, date
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("quick_backfill_test")

async def main():
    """Quick test of backfill functionality."""
    
    # API keys
    tiingo_api_key = "5f40b4f36e171405746304ec0e5a6f3aa9ca77e5"
    
    # Database connection
    db_url = "postgresql://postgres:dev_password@ats-dev-postgres:5432/dev_db"
    
    try:
        logger.info("🔗 Connecting to database...")
        conn = await asyncpg.connect(db_url)
        
        # Find a symbol with no Tiingo data
        result = await conn.fetchrow("""
            SELECT i.id, i.symbol
            FROM dev_instruments i
            LEFT JOIN dev_daily_prices_tiingo t ON i.id = t.instrument_id
            WHERE i.active = true 
              AND i.symbol IS NOT NULL
              AND i.symbol ~ '^[A-Z]{1,4}$'
              AND t.instrument_id IS NULL
            ORDER BY i.symbol
            LIMIT 1
        """)
        
        if not result:
            logger.info("✅ No symbols found that need Tiingo data - all covered!")
            await conn.close()
            return
            
        symbol = result['symbol']
        instrument_id = result['id']
        
        logger.info(f"🎯 Testing backfill for {symbol} (ID: {instrument_id})")
        
        # Download recent data from Tiingo
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)  # Just last month for testing
        
        url = f"https://api.tiingo.com/tiingo/daily/{symbol}/prices"
        params = {
            'startDate': start_date.strftime('%Y-%m-%d'),
            'endDate': end_date.strftime('%Y-%m-%d'),
            'format': 'json',
            'token': tiingo_api_key
        }
        
        logger.info(f"📡 Downloading Tiingo data for {symbol} from {start_date} to {end_date}...")
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"✅ Downloaded {len(data)} records for {symbol}")
            
            if data:
                # Insert data
                rows = []
                for price in data:
                    try:
                        date_val = datetime.strptime(price['date'][:10], '%Y-%m-%d').date()
                        rows.append((
                            date_val, symbol, price.get('open'), price.get('high'),
                            price.get('low'), price.get('close'), price.get('volume', 0), instrument_id
                        ))
                    except:
                        continue
                
                if rows:
                    await conn.executemany("""
                        INSERT INTO dev_daily_prices_tiingo 
                        (date, symbol, open, high, low, close, volume, instrument_id)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ON CONFLICT (date, instrument_id) DO NOTHING
                    """, rows)
                    
                    logger.info(f"💾 Successfully inserted {len(rows)} records for {symbol}")
                    
                    # Verify insertion
                    count = await conn.fetchval(
                        "SELECT COUNT(*) FROM dev_daily_prices_tiingo WHERE instrument_id = $1",
                        instrument_id
                    )
                    logger.info(f"📊 Total Tiingo records for {symbol}: {count}")
                    
        else:
            logger.warning(f"⚠️ Tiingo API returned {response.status_code} for {symbol}")
            
        await conn.close()
        logger.info("✅ Quick backfill test completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Quick backfill test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())