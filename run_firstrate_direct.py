#!/usr/bin/env python3
"""
Direct FirstRate backfill execution without complex Docker setup
"""

import asyncio
import asyncpg
import logging
from datetime import datetime
from pathlib import Path

# Simple logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def simple_firstrate_backfill():
    """Simple FirstRate backfill for testing."""
    
    print("🚀 Direct FirstRate Backfill Test")
    print("=" * 50)
    
    try:
        # Database connection string for ATS dev environment
        db_url = "postgresql://postgres@localhost:5432/dev_db"
        
        print("📊 Connecting to database...")
        pool = await asyncpg.create_pool(
            db_url,
            min_size=1,
            max_size=3,
            command_timeout=30
        )
        
        print("📈 Testing FirstRate data access...")
        
        # Simple data parsing test using basic zipfile
        import zipfile
        import csv
        import io
        
        data_path = Path("/mnt/d/ats-data/firstrate-data/stock")
        zip_files = list(data_path.glob("stock_A_*.zip"))
        
        if not zip_files:
            print("❌ No FirstRate zip files found")
            return
            
        zip_file = zip_files[0]
        print(f"📂 Processing: {zip_file.name}")
        
        bars_to_insert = []
        
        with zipfile.ZipFile(zip_file, 'r') as zf:
            aapl_file = "AAPL_full_1min_adjsplitdiv.txt"
            
            if aapl_file in zf.namelist():
                print(f"📊 Found AAPL data file")
                
                with zf.open(aapl_file, 'r') as f:
                    text_data = io.TextIOWrapper(f, encoding='utf-8')
                    csv_reader = csv.reader(text_data)
                    
                    bars_processed = 0
                    
                    for row_num, row in enumerate(csv_reader):
                        if bars_processed >= 1000:  # Limit for test
                            break
                            
                        if len(row) == 6:
                            try:
                                timestamp_str, open_str, high_str, low_str, close_str, volume_str = row
                                
                                # Parse data
                                timestamp = datetime.strptime(timestamp_str.strip(), '%Y-%m-%d %H:%M:%S')
                                
                                # Filter to recent data only (2020+)
                                if timestamp.year < 2020:
                                    continue
                                
                                open_price = float(open_str.strip())
                                high_price = float(high_str.strip())
                                low_price = float(low_str.strip())
                                close_price = float(close_str.strip())
                                volume = int(float(volume_str.strip()))
                                
                                # Basic validation
                                if (low_price <= open_price <= high_price and 
                                    low_price <= close_price <= high_price and
                                    all(p > 0 for p in [open_price, high_price, low_price, close_price])):
                                    
                                    bars_to_insert.append((
                                        'AAPL',
                                        timestamp,
                                        open_price,
                                        high_price,
                                        low_price, 
                                        close_price,
                                        volume,
                                        None,  # vwap
                                        None,  # returns
                                        1.0,   # quality_score
                                        'firstrate',
                                        {}     # data_source_flags
                                    ))
                                    
                                    bars_processed += 1
                                    
                            except (ValueError, IndexError) as e:
                                continue
                
                print(f"✅ Parsed {len(bars_to_insert)} valid AAPL bars (2020+)")
                
                if bars_to_insert:
                    # Show sample data
                    sample = bars_to_insert[0]
                    print(f"📊 Sample: {sample[1]} | O:{sample[2]:.4f} H:{sample[3]:.4f} L:{sample[4]:.4f} C:{sample[5]:.4f} | V:{sample[6]:,}")
                    
                    # Insert into database
                    print("💾 Inserting into database...")
                    
                    insert_query = """
                    INSERT INTO minute_bars (
                        symbol, timestamp, open, high, low, close, volume,
                        vwap, returns, quality_score, vendor, data_source_flags
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (symbol, timestamp) DO NOTHING
                    """
                    
                    async with pool.acquire() as conn:
                        result = await conn.executemany(insert_query, bars_to_insert)
                    
                    # Verify insertion
                    async with pool.acquire() as conn:
                        count = await conn.fetchval(
                            "SELECT COUNT(*) FROM minute_bars WHERE symbol = 'AAPL' AND vendor = 'firstrate'"
                        )
                    
                    print(f"✅ Inserted {count:,} AAPL bars into database")
                    
                    # Show date range
                    async with pool.acquire() as conn:
                        date_range = await conn.fetchrow("""
                            SELECT MIN(timestamp) as first_bar, MAX(timestamp) as last_bar
                            FROM minute_bars 
                            WHERE symbol = 'AAPL' AND vendor = 'firstrate'
                        """)
                    
                    if date_range:
                        print(f"📅 Date range: {date_range['first_bar'].date()} to {date_range['last_bar'].date()}")
                    
                    print("🎯 SUCCESS: FirstRate backfill completed for AAPL")
                    
                else:
                    print("❌ No valid bars found for insertion")
            else:
                print("❌ AAPL file not found in zip")
        
        await pool.close()
        
    except Exception as e:
        print(f"❌ Backfill failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(simple_firstrate_backfill())