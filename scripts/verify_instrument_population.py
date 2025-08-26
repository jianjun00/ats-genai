#!/usr/bin/env python3
"""
Verification script for instrument population from all providers
"""

import sys
sys.path.append('/workspace/src')

import asyncio
from config.environment import Environment, EnvironmentType
from config.database import Database
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("verify_instrument_population")

async def verify_populations():
    """Verify instrument population from all providers"""
    
    # Initialize environment
    env = Environment(env_type=EnvironmentType.DEV)
    
    # Create database connection
    pool = await Database.create_connection_pool(env=env)
    
    try:
        async with pool.acquire() as conn:
            logger.info("🔍 Verifying instrument population results...")
            
            # Check Polygon instruments
            polygon_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument_polygon")
            logger.info(f"📈 Polygon instruments: {polygon_count:,}")
            
            if polygon_count > 0:
                polygon_samples = await conn.fetch("""
                    SELECT symbol, name, exchange, type 
                    FROM dev_instrument_polygon 
                    ORDER BY symbol 
                    LIMIT 5
                """)
                logger.info("   Sample Polygon instruments:")
                for row in polygon_samples:
                    logger.info(f"     • {row['symbol']}: {row['name']} ({row['exchange']})")
            
            # Check Tiingo instruments  
            tiingo_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument_tiingo")
            logger.info(f"📊 Tiingo instruments: {tiingo_count:,}")
            
            if tiingo_count > 0:
                tiingo_samples = await conn.fetch("""
                    SELECT symbol, name, exchange, start_date 
                    FROM dev_instrument_tiingo 
                    ORDER BY symbol 
                    LIMIT 5
                """)
                logger.info("   Sample Tiingo instruments:")
                for row in tiingo_samples:
                    logger.info(f"     • {row['symbol']}: {row['name']} (since {row['start_date']})")
            
            # Check EODHD instruments
            eodhd_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument_eodhd")
            logger.info(f"🌍 EODHD instruments: {eodhd_count:,}")
            
            if eodhd_count > 0:
                eodhd_samples = await conn.fetch("""
                    SELECT symbol, name, exchange, asset_type 
                    FROM dev_instrument_eodhd 
                    ORDER BY symbol 
                    LIMIT 5
                """)
                logger.info("   Sample EODHD instruments:")
                for row in eodhd_samples:
                    logger.info(f"     • {row['symbol']}: {row['name']} ({row['asset_type']})")
            
            # Summary
            total_instruments = polygon_count + tiingo_count + eodhd_count
            logger.info(f"")
            logger.info(f"📋 SUMMARY:")
            logger.info(f"   Total instruments across all providers: {total_instruments:,}")
            logger.info(f"   ✅ Polygon: {polygon_count:,} instruments")
            logger.info(f"   ✅ Tiingo: {tiingo_count:,} instruments")  
            logger.info(f"   ✅ EODHD: {eodhd_count:,} instruments")
            
            # Check for overlapping symbols
            overlap_query = """
                SELECT p.symbol as symbol
                FROM dev_instrument_polygon p
                INNER JOIN dev_instrument_tiingo t ON p.symbol = t.symbol
                INNER JOIN dev_instrument_eodhd e ON p.symbol = e.symbol
                ORDER BY p.symbol
                LIMIT 10
            """
            overlapping = await conn.fetch(overlap_query)
            if overlapping:
                logger.info(f"")
                logger.info(f"🔗 Overlapping symbols (available in all 3 providers):")
                for row in overlapping:
                    logger.info(f"     • {row['symbol']}")
            
            # Check tables structure
            logger.info(f"")
            logger.info(f"🏗️  Database tables verified:")
            tables = ['dev_instrument_polygon', 'dev_instrument_tiingo', 'dev_instrument_eodhd']
            for table in tables:
                exists = await conn.fetchval(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table}'")
                logger.info(f"     • {table}: {'✅ exists' if exists else '❌ missing'}")
                
    finally:
        await pool.close()

async def main():
    logger.info("🚀 Starting instrument population verification...")
    try:
        await verify_populations()
        logger.info("✅ Verification completed successfully!")
    except Exception as e:
        logger.error(f"❌ Verification failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())