#!/usr/bin/env python3
"""
Test Coverage Catalog Deployment in Kubernetes Environment

This script tests the deployed coverage catalog functionality to ensure 
everything is working correctly in the K8s environment.
"""

import asyncio
import asyncpg
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)

async def test_coverage_deployment():
    """Test the deployed coverage catalog functionality"""
    
    # Database connection from K8s environment
    db_url = "postgresql://postgres:dev_password@postgres-simple:5432/dev_db"
    
    try:
        # Connect to database
        conn = await asyncpg.connect(db_url)
        logger.info("✅ Connected to database")
        
        # Test 1: Verify tables exist
        tables = await conn.fetch("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public' AND tablename LIKE 'coverage%'
            ORDER BY tablename
        """)
        logger.info(f"✅ Found {len(tables)} coverage tables: {[t['tablename'] for t in tables]}")
        
        # Test 2: Insert sample coverage interval
        now = datetime.now()
        await conn.execute("""
            INSERT INTO coverage_intervals 
            (symbol, vendor, data_type, start_time, end_time, record_count, expected_count, completeness_ratio)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """, 'AAPL', 'polygon', 'minute', now, now + timedelta(hours=1), 58, 60, 0.9667)
        logger.info("✅ Inserted sample coverage interval")
        
        # Test 3: Query coverage data
        coverage_data = await conn.fetchrow("""
            SELECT symbol, vendor, data_type, record_count, expected_count, completeness_ratio
            FROM coverage_intervals 
            WHERE symbol = $1 AND vendor = $2
        """, 'AAPL', 'polygon')
        logger.info(f"✅ Retrieved coverage data: {dict(coverage_data)}")
        
        # Test 4: Test TimescaleDB hypertable
        hypertable_info = await conn.fetchrow("""
            SELECT * FROM _timescaledb_catalog.hypertable 
            WHERE table_name = 'coverage_intervals'
        """)
        logger.info(f"✅ TimescaleDB hypertable active: {hypertable_info['table_name']}")
        
        # Test 5: Insert sample summary data
        await conn.execute("""
            INSERT INTO coverage_summary 
            (symbol, vendor, data_type, current_status, coverage_24h, quality_24h, gaps_24h, records_24h)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (symbol, vendor, data_type) DO UPDATE SET
                coverage_24h = EXCLUDED.coverage_24h,
                last_updated = NOW()
        """, 'AAPL', 'polygon', 'minute', 'active', 96.67, 0.95, 1, 390)
        logger.info("✅ Inserted/updated coverage summary")
        
        # Test 6: Query summary data
        summary_data = await conn.fetchrow("""
            SELECT symbol, vendor, current_status, coverage_24h, quality_24h, gaps_24h
            FROM coverage_summary 
            WHERE symbol = $1 AND vendor = $2
        """, 'AAPL', 'polygon')
        logger.info(f"✅ Retrieved summary data: {dict(summary_data)}")
        
        # Test 7: Clean up test data
        await conn.execute("DELETE FROM coverage_intervals WHERE symbol = $1", 'AAPL')
        await conn.execute("DELETE FROM coverage_summary WHERE symbol = $1", 'AAPL')
        logger.info("✅ Cleaned up test data")
        
        await conn.close()
        logger.info("🎉 Coverage catalog deployment test completed successfully!")
        logger.info("📊 All core functionality verified in Kubernetes environment")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(test_coverage_deployment())