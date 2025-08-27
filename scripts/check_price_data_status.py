#!/usr/bin/env python3
"""
Check Price Data Status

Analyze current price data coverage and table structure for planning 30-year backfill.
"""

import sys
import asyncio
import logging
from datetime import datetime, date

# Add src to Python path
sys.path.insert(0, '/workspace/src')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def check_price_data_status():
    """Check current price data status and structure"""
    
    try:
        from config.database import Database
        from config.environment import Environment, EnvironmentType
        
        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=30.0)
        
        async with pool.acquire() as conn:
            logger.info("🔍 ANALYZING PRICE DATA INFRASTRUCTURE")
            logger.info("=" * 70)
            
            # 1. Check existing price tables
            price_tables = await conn.fetch("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_name LIKE '%price%' OR table_name LIKE '%daily%'
                ORDER BY table_name
            """)
            
            logger.info("📊 EXISTING PRICE TABLES:")
            for row in price_tables:
                table_name = row['table_name']
                
                try:
                    count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                    logger.info(f"  ✅ {table_name:40} {count:,} records")
                except Exception as e:
                    logger.info(f"  ❌ {table_name:40} Error: {str(e)[:50]}...")
            
            logger.info("")
            
            # 2. Check dev_daily_prices structure if it exists
            try:
                columns = await conn.fetch("""
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns 
                    WHERE table_name = 'dev_daily_prices'
                    ORDER BY ordinal_position
                """)
                
                if columns:
                    logger.info("🗂️ DEV_DAILY_PRICES TABLE STRUCTURE:")
                    for col in columns:
                        logger.info(f"  {col['column_name']:20} {col['data_type']:15} {'NULL' if col['is_nullable'] == 'YES' else 'NOT NULL':8}")
                    logger.info("")
                else:
                    logger.info("⚠️ dev_daily_prices table does not exist")
                    logger.info("")
            except Exception as e:
                logger.info(f"⚠️ Could not check dev_daily_prices structure: {e}")
                logger.info("")
            
            # 3. Check vendor-specific price tables
            vendor_tables = ['dev_daily_prices_polygon', 'dev_daily_prices_tiingo', 'dev_daily_prices_eodhd', 'dev_daily_prices_alphavantage']
            
            logger.info("📈 VENDOR PRICE DATA COVERAGE:")
            for table in vendor_tables:
                try:
                    count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                    
                    if count > 0:
                        date_range = await conn.fetchrow(f"""
                            SELECT MIN(date) as min_date, MAX(date) as max_date 
                            FROM {table}
                        """)
                        min_date = date_range['min_date'] if date_range else None
                        max_date = date_range['max_date'] if date_range else None
                        
                        unique_symbols = await conn.fetchval(f"""
                            SELECT COUNT(DISTINCT symbol) FROM {table}
                        """)
                        
                        logger.info(f"  ✅ {table:35} {count:8,} records, {unique_symbols:,} symbols")
                        if min_date and max_date:
                            logger.info(f"     {'':35} {min_date} to {max_date} ({(max_date - min_date).days} days)")
                    else:
                        logger.info(f"  ⚠️ {table:35} {count:8,} records (empty)")
                        
                except Exception as e:
                    logger.info(f"  ❌ {table:35} Does not exist or error: {str(e)[:40]}...")
            
            logger.info("")
            
            # 4. Check current instrument coverage vs price data
            total_instruments = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments")
            
            instruments_with_price_data = 0
            try:
                # Try different possible price tables
                for table in vendor_tables:
                    try:
                        count = await conn.fetchval(f"""
                            SELECT COUNT(DISTINCT i.symbol) 
                            FROM dev_instruments i 
                            JOIN {table} p ON i.symbol = p.symbol
                        """)
                        if count > instruments_with_price_data:
                            instruments_with_price_data = count
                            best_table = table
                    except:
                        continue
            except Exception as e:
                logger.info(f"⚠️ Could not check instrument-price relationship: {e}")
            
            logger.info("🎯 PRICE DATA COVERAGE SUMMARY:")
            logger.info(f"  Total instruments: {total_instruments:,}")
            logger.info(f"  Instruments with price data: {instruments_with_price_data:,}")
            if total_instruments > 0:
                coverage = (instruments_with_price_data / total_instruments) * 100
                logger.info(f"  Price data coverage: {coverage:.1f}%")
            logger.info("")
            
            # 5. Check for EODHD API key
            import os
            eodhd_key = os.getenv('EODHD_API_KEY')
            logger.info("🔑 API KEY STATUS:")
            if eodhd_key:
                logger.info(f"  ✅ EODHD_API_KEY present (length: {len(eodhd_key)})")
            else:
                logger.info("  ❌ EODHD_API_KEY not found in environment")
            
            polygon_key = os.getenv('POLYGON_API_KEY')
            if polygon_key:
                logger.info(f"  ✅ POLYGON_API_KEY present (length: {len(polygon_key)})")
            else:
                logger.info("  ⚠️ POLYGON_API_KEY not found")
            
            tiingo_key = os.getenv('TIINGO_API_KEY')
            if tiingo_key:
                logger.info(f"  ✅ TIINGO_API_KEY present (length: {len(tiingo_key)})")
            else:
                logger.info("  ⚠️ TIINGO_API_KEY not found")
                
            logger.info("")
            logger.info("=" * 70)
        
        await pool.close()
        
    except Exception as e:
        logger.error(f"❌ Price data status check failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(check_price_data_status())