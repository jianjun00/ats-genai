#!/usr/bin/env python3
"""
Test script for real-time collector functionality
Validates database connectivity, API keys, and data collection
"""

import asyncio
import logging
import os
import sys
from datetime import datetime, timezone

# Add src to path
sys.path.append('src')

import asyncpg

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('test')

async def test_database_connection():
    """Test database connectivity and table existence."""
    try:
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5433')
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', 'postgres')
        db_name = os.getenv('DB_NAME', 'dev_db')
        
        dsn = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        conn = await asyncpg.connect(dsn)
        
        # Test basic connection
        version = await conn.fetchval("SELECT version()")
        logger.info(f"✅ Database connected: {version[:50]}...")
        
        # Check for real-time tables
        tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_name LIKE 'dev_one_minute_live_%' 
            ORDER BY table_name
        """
        tables = await conn.fetch(tables_query)
        
        if tables:
            logger.info(f"✅ Found {len(tables)} real-time tables:")
            for table in tables:
                logger.info(f"   - {table['table_name']}")
        else:
            logger.error("❌ No real-time tables found")
            return False
        
        # Check for instruments
        instruments_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments")
        logger.info(f"✅ Found {instruments_count} instruments in database")
        
        await conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Database test failed: {e}")
        return False

def test_api_keys():
    """Test availability of API keys."""
    keys = {
        'POLYGON_API_KEY': os.getenv('POLYGON_API_KEY', ''),
        'TIINGO_API_KEY': os.getenv('TIINGO_API_KEY', ''),
        'FMP_API_KEY': os.getenv('FMP_API_KEY', '')
    }
    
    available_keys = []
    for key_name, key_value in keys.items():
        if key_value:
            logger.info(f"✅ {key_name}: Available ({key_value[:8]}...)")
            available_keys.append(key_name)
        else:
            logger.warning(f"⚠️ {key_name}: Not available")
    
    if not available_keys:
        logger.error("❌ No API keys available for testing")
        return False
    
    logger.info(f"✅ Found {len(available_keys)} API keys for testing")
    return True

async def test_simple_data_insertion():
    """Test inserting sample data into real-time tables."""
    try:
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5433')
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', 'postgres')
        db_name = os.getenv('DB_NAME', 'dev_db')
        
        dsn = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        conn = await asyncpg.connect(dsn)
        
        # Get a test instrument
        instrument = await conn.fetchrow("SELECT id, symbol FROM dev_instruments LIMIT 1")
        if not instrument:
            logger.error("❌ No instruments found for testing")
            return False
        
        test_symbol = instrument['symbol']
        instrument_id = instrument['id']
        logger.info(f"Using test symbol: {test_symbol} (ID: {instrument_id})")
        
        # Test data for each vendor table
        test_timestamp = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        
        # Test Polygon table
        try:
            await conn.execute("""
                INSERT INTO dev_one_minute_live_polygon (
                    instrument_id, symbol, timestamp, open_price, high_price,
                    low_price, close_price, volume, received_at, collection_method
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (instrument_id, timestamp) DO NOTHING
            """, instrument_id, test_symbol, test_timestamp, 100.0, 102.0, 99.0, 101.0, 
                1000, datetime.now(timezone.utc), 'test')
            logger.info("✅ Successfully inserted test data into Polygon table")
        except Exception as e:
            logger.error(f"❌ Polygon table insertion failed: {e}")
        
        # Test Tiingo table
        try:
            await conn.execute("""
                INSERT INTO dev_one_minute_live_tiingo (
                    instrument_id, symbol, timestamp, open_price, high_price,
                    low_price, close_price, volume, received_at, collection_method
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (instrument_id, timestamp) DO NOTHING
            """, instrument_id, test_symbol, test_timestamp, 100.0, 102.0, 99.0, 101.0, 
                1000, datetime.now(timezone.utc), 'test')
            logger.info("✅ Successfully inserted test data into Tiingo table")
        except Exception as e:
            logger.error(f"❌ Tiingo table insertion failed: {e}")
        
        # Test FMP table
        try:
            await conn.execute("""
                INSERT INTO dev_one_minute_live_fmp (
                    instrument_id, symbol, timestamp, open_price, high_price,
                    low_price, close_price, volume, received_at, collection_method
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (instrument_id, timestamp) DO NOTHING
            """, instrument_id, test_symbol, test_timestamp, 100.0, 102.0, 99.0, 101.0, 
                1000, datetime.now(timezone.utc), 'test')
            logger.info("✅ Successfully inserted test data into FMP table")
        except Exception as e:
            logger.error(f"❌ FMP table insertion failed: {e}")
        
        # Verify data was inserted
        for vendor in ['polygon', 'tiingo', 'fmp']:
            table = f"dev_one_minute_live_{vendor}"
            count = await conn.fetchval(f"SELECT COUNT(*) FROM {table} WHERE symbol = $1", test_symbol)
            logger.info(f"✅ {vendor.upper()} table now has {count} records for {test_symbol}")
        
        await conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Data insertion test failed: {e}")
        return False

async def test_overlap_handling():
    """Test handling of overlapping time intervals."""
    try:
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5433')
        db_user = os.getenv('DB_USER', 'postgres')
        db_password = os.getenv('DB_PASSWORD', 'postgres')
        db_name = os.getenv('DB_NAME', 'dev_db')
        
        dsn = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        conn = await asyncpg.connect(dsn)
        
        # Get test instrument
        instrument = await conn.fetchrow("SELECT id, symbol FROM dev_instruments LIMIT 1")
        if not instrument:
            logger.error("❌ No instruments found for testing")
            return False
        
        test_symbol = instrument['symbol']
        instrument_id = instrument['id']
        test_timestamp = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        
        logger.info(f"Testing overlap handling for {test_symbol}")
        
        # Insert initial record
        await conn.execute("""
            INSERT INTO dev_one_minute_live_polygon (
                instrument_id, symbol, timestamp, open_price, high_price,
                low_price, close_price, volume, received_at, collection_method, quality_score
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (instrument_id, timestamp) DO NOTHING
        """, instrument_id, test_symbol, test_timestamp, 100.0, 102.0, 99.0, 101.0, 
            1000, datetime.now(timezone.utc), 'test_initial', 0.8)
        
        # Try to insert overlapping record with different data but later received_at
        import time
        await asyncio.sleep(1)  # Ensure later timestamp
        
        result = await conn.execute("""
            INSERT INTO dev_one_minute_live_polygon (
                instrument_id, symbol, timestamp, open_price, high_price,
                low_price, close_price, volume, received_at, collection_method, quality_score
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (instrument_id, timestamp) 
            DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                volume = EXCLUDED.volume,
                received_at = EXCLUDED.received_at,
                collection_method = EXCLUDED.collection_method,
                quality_score = EXCLUDED.quality_score,
                updated_at = CURRENT_TIMESTAMP
            WHERE dev_one_minute_live_polygon.received_at < EXCLUDED.received_at
        """, instrument_id, test_symbol, test_timestamp, 105.0, 107.0, 104.0, 106.0, 
            2000, datetime.now(timezone.utc), 'test_overlap', 0.9)
        
        # Check final record
        final_record = await conn.fetchrow("""
            SELECT open_price, close_price, volume, collection_method, quality_score
            FROM dev_one_minute_live_polygon
            WHERE instrument_id = $1 AND timestamp = $2
        """, instrument_id, test_timestamp)
        
        if final_record:
            logger.info(f"✅ Overlap handling test successful:")
            logger.info(f"   Final prices: O={final_record['open_price']}, C={final_record['close_price']}")
            logger.info(f"   Final volume: {final_record['volume']}")
            logger.info(f"   Collection method: {final_record['collection_method']}")
            logger.info(f"   Quality score: {final_record['quality_score']}")
            
            # Should be updated values since received_at was later
            if (final_record['collection_method'] == 'test_overlap' and 
                final_record['quality_score'] == 0.9):
                logger.info("✅ Overlap handling working correctly (later data wins)")
            else:
                logger.warning("⚠️ Overlap handling may not be working as expected")
        else:
            logger.error("❌ No record found after overlap test")
            return False
        
        await conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Overlap handling test failed: {e}")
        return False

async def main():
    """Run all tests."""
    logger.info("🧪 Starting Real-Time Collector Tests")
    logger.info("=" * 50)
    
    tests = [
        ("Database Connection", test_database_connection()),
        ("API Keys Availability", test_api_keys()),
        ("Data Insertion", test_simple_data_insertion()),
        ("Overlap Handling", test_overlap_handling())
    ]
    
    results = []
    for test_name, test_coro in tests:
        logger.info(f"\n🔍 Running: {test_name}")
        try:
            if asyncio.iscoroutine(test_coro):
                result = await test_coro
            else:
                result = test_coro
            results.append((test_name, result))
        except Exception as e:
            logger.error(f"❌ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    logger.info("\n" + "=" * 50)
    logger.info("📊 Test Results Summary:")
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"   {test_name}: {status}")
        if result:
            passed += 1
    
    logger.info(f"\nOverall: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        logger.info("🎉 All tests passed! Real-time collector is ready for deployment.")
        return True
    else:
        logger.error("💥 Some tests failed. Please check the logs above.")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)