#!/usr/bin/env python3
"""
Create sample minute-level data for testing migration

This script creates realistic minute-level OHLC data for testing
the file-based storage migration process.
"""

import asyncio
import asyncpg
import logging
from datetime import datetime, timedelta
import random
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def create_sample_data():
    """Create sample minute data for testing migration"""
    
    # Database connection
    db_url = f"postgresql://postgres:{os.getenv('DB_PASSWORD', 'dev_password')}@{os.getenv('DB_HOST', 'postgres-simple')}:5432/{os.getenv('DB_NAME', 'dev_db')}"
    
    try:
        pool = await asyncpg.create_pool(db_url, min_size=1, max_size=3)
        logger.info("✅ Connected to database")
        
        # Get some instrument IDs
        async with pool.acquire() as conn:
            instruments = await conn.fetch("SELECT id FROM dev_instruments LIMIT 5")
            if not instruments:
                logger.error("❌ No instruments found in database")
                return
            
            logger.info(f"📊 Found {len(instruments)} instruments to generate data for")
            
            # Create FMP minute prices table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS dev_minute_prices_fmp (
                    id SERIAL PRIMARY KEY,
                    instrument_id INTEGER NOT NULL REFERENCES dev_instruments(id),
                    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                    open_price NUMERIC(10, 4),
                    high_price NUMERIC(10, 4),
                    low_price NUMERIC(10, 4),
                    close_price NUMERIC(10, 4),
                    volume BIGINT,
                    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
                    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
                    UNIQUE(instrument_id, timestamp)
                )
            """)
            
            logger.info("✅ Created dev_minute_prices_fmp table")
            
            # Generate sample data for last 7 days
            end_time = datetime.now().replace(hour=16, minute=0, second=0, microsecond=0)  # Market close
            start_time = end_time - timedelta(days=7)
            
            total_records = 0
            
            for instrument in instruments:
                instrument_id = instrument['id']
                logger.info(f"📈 Generating data for instrument {instrument_id}")
                
                # Base price (random between 50-300)
                base_price = random.uniform(50, 300)
                current_price = base_price
                
                # Generate minute data
                current_time = start_time
                batch_data = []
                
                while current_time < end_time:
                    # Skip weekends
                    if current_time.weekday() >= 5:
                        current_time += timedelta(minutes=1)
                        continue
                    
                    # Only market hours (9:30 AM - 4:00 PM EST)
                    hour = current_time.hour
                    minute = current_time.minute
                    
                    if hour < 9 or (hour == 9 and minute < 30) or hour >= 16:
                        current_time += timedelta(minutes=1)
                        continue
                    
                    # Generate realistic OHLC data
                    volatility = random.uniform(0.002, 0.01)  # 0.2% to 1% volatility
                    
                    # Price movement
                    price_change = random.gauss(0, volatility) * current_price
                    open_price = current_price
                    close_price = current_price + price_change
                    
                    # High and low
                    high_volatility = random.uniform(0.001, 0.005)
                    low_volatility = random.uniform(0.001, 0.005)
                    
                    high_price = max(open_price, close_price) + random.uniform(0, high_volatility) * current_price
                    low_price = min(open_price, close_price) - random.uniform(0, low_volatility) * current_price
                    
                    # Volume (random between 1000-50000)
                    volume = random.randint(1000, 50000)
                    
                    batch_data.append((
                        instrument_id,
                        current_time,
                        round(open_price, 4),
                        round(high_price, 4),
                        round(low_price, 4),
                        round(close_price, 4),
                        volume
                    ))
                    
                    current_price = close_price
                    current_time += timedelta(minutes=1)
                    
                    # Insert in batches
                    if len(batch_data) >= 1000:
                        await conn.executemany("""
                            INSERT INTO dev_minute_prices_fmp
                            (instrument_id, timestamp, open_price, high_price, low_price, close_price, volume)
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                            ON CONFLICT (instrument_id, timestamp) DO NOTHING
                        """, batch_data)
                        
                        total_records += len(batch_data)
                        logger.info(f"✅ Inserted {len(batch_data)} records for instrument {instrument_id}")
                        batch_data = []
                
                # Insert remaining data
                if batch_data:
                    await conn.executemany("""
                        INSERT INTO dev_minute_prices_fmp
                        (instrument_id, timestamp, open_price, high_price, low_price, close_price, volume)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        ON CONFLICT (instrument_id, timestamp) DO NOTHING
                    """, batch_data)
                    
                    total_records += len(batch_data)
                    logger.info(f"✅ Final batch: {len(batch_data)} records for instrument {instrument_id}")
            
            logger.info(f"🎉 Generated {total_records} total minute records")
            
            # Verify data
            count = await conn.fetchval("SELECT COUNT(*) FROM dev_minute_prices_fmp")
            logger.info(f"📊 Total records in table: {count}")
            
        await pool.close()
        
    except Exception as e:
        logger.error(f"❌ Error creating sample data: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(create_sample_data())