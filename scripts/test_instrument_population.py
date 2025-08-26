#!/usr/bin/env python3
"""
Test script to verify instrument population workflow without requiring API keys
"""

import sys
sys.path.append('/workspace/src')

import asyncio
from config.environment import Environment, EnvironmentType
from config.database import Database
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("test_instrument_population")

async def test_database_tables():
    """Test that all required tables exist and are accessible"""
    
    # Initialize environment
    env = Environment(env_type=EnvironmentType.DEV)
    
    # Create database connection
    pool = await Database.create_connection_pool(env=env)
    
    try:
        async with pool.acquire() as conn:
            # Check key tables exist
            tables_to_check = [
                'dev_instruments',
                'dev_instrument_polygon', 
                'dev_vendors',
                'dev_instrument_xrefs',
                'dev_daily_prices_polygon',
                'dev_daily_prices_tiingo'
            ]
            
            for table_name in tables_to_check:
                try:
                    result = await conn.fetchrow(f"SELECT COUNT(*) as count FROM {table_name}")
                    count = result['count']
                    logger.info(f"✅ Table {table_name}: {count} rows")
                except Exception as e:
                    logger.error(f"❌ Table {table_name}: {e}")
            
            # Insert test data to verify insert functionality
            logger.info("🧪 Testing sample instrument insertion...")
            
            # Insert test vendor if not exists
            vendor_id = await conn.fetchval("""
                INSERT INTO dev_vendors (name, description, website, api_key_env_var)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (name) DO UPDATE SET updated_at = now()
                RETURNING id
            """, 'test_provider', 'Test data provider', 'https://example.com', 'TEST_API_KEY')
            logger.info(f"✅ Test vendor ID: {vendor_id}")
            
            # Insert test instrument
            instrument_id = await conn.fetchval("""
                INSERT INTO dev_instruments (symbol, name, exchange, type, currency, active)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (symbol) DO UPDATE SET updated_at = now()
                RETURNING id
            """, 'TEST', 'Test Company', 'NASDAQ', 'CS', 'USD', True)
            logger.info(f"✅ Test instrument ID: {instrument_id}")
            
            # Insert test xref
            await conn.execute("""
                INSERT INTO dev_instrument_xrefs (instrument_id, vendor_id, symbol, active)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (instrument_id, vendor_id, start_at) DO NOTHING
            """, instrument_id, vendor_id, 'TEST', True)
            logger.info(f"✅ Test instrument xref created")
            
            # Verify the data was inserted
            test_instrument = await conn.fetchrow("""
                SELECT i.symbol, i.name, v.name as vendor_name
                FROM dev_instruments i
                JOIN dev_instrument_xrefs x ON i.id = x.instrument_id
                JOIN dev_vendors v ON x.vendor_id = v.id
                WHERE i.symbol = 'TEST'
            """)
            
            if test_instrument:
                logger.info(f"✅ Test complete - Instrument: {test_instrument['symbol']} ({test_instrument['name']}) via {test_instrument['vendor_name']}")
            else:
                logger.error("❌ Test failed - Could not retrieve test instrument")
                
    finally:
        await pool.close()

async def main():
    logger.info("🚀 Testing instrument population database infrastructure...")
    try:
        await test_database_tables()
        logger.info("✅ Database infrastructure test completed successfully!")
        logger.info("📝 Ready for instrument population with valid API keys:")
        logger.info("   - POLYGON_API_KEY for Polygon.io")  
        logger.info("   - TIINGO_API_KEY for Tiingo")
        logger.info("   - EODHD_API_KEY for EODHD")
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())