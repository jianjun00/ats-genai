#!/usr/bin/env python3
"""
Check Fundamental Data Status

Analyze current fundamental data coverage across all vendors and instruments.
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

async def check_fundamental_data_status():
    """Check current fundamental data status and coverage."""
    
    try:
        from config.database import Database
        from config.environment import Environment, EnvironmentType
        
        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=30.0)
        
        async with pool.acquire() as conn:
            logger.info("🔍 ANALYZING FUNDAMENTAL DATA INFRASTRUCTURE")
            logger.info("=" * 70)
            
            # 1. Check all fundamental-related tables
            fundamental_tables = await conn.fetch("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_name LIKE '%fundamental%' 
                   OR table_name LIKE '%financial%'
                   OR table_name LIKE '%earnings%'
                   OR table_name LIKE '%balance%'
                   OR table_name LIKE '%income%'
                   OR table_name LIKE '%cash%'
                   OR table_name LIKE '%ratios%'
                ORDER BY table_name
            """)
            
            logger.info("📊 FUNDAMENTAL DATA TABLES:")
            for row in fundamental_tables:
                table_name = row['table_name']
                
                try:
                    count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                    logger.info(f"  ✅ {table_name:40} {count:,} records")
                except Exception as e:
                    logger.info(f"  ❌ {table_name:40} Error: {str(e)[:50]}...")
            
            logger.info("")
            
            # 2. Check polygon fundamentals specifically
            logger.info("📈 POLYGON FUNDAMENTALS ANALYSIS:")
            try:
                polygon_fundamentals = await conn.fetch("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_name LIKE '%polygon%' 
                      AND (table_name LIKE '%fundamental%' OR table_name LIKE '%financial%')
                    ORDER BY table_name
                """)
                
                for row in polygon_fundamentals:
                    table_name = row['table_name']
                    try:
                        count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
                        
                        if count > 0:
                            # Get date range and symbol count
                            stats = await conn.fetchrow(f"""
                                SELECT 
                                    COUNT(DISTINCT symbol) as unique_symbols,
                                    MIN(date) as min_date,
                                    MAX(date) as max_date
                                FROM {table_name}
                                WHERE date IS NOT NULL
                            """)
                            
                            if stats and stats['unique_symbols']:
                                logger.info(f"  ✅ {table_name:35} {count:8,} records, {stats['unique_symbols']:,} symbols")
                                if stats['min_date'] and stats['max_date']:
                                    logger.info(f"     {'':35} {stats['min_date']} to {stats['max_date']}")
                            else:
                                logger.info(f"  ✅ {table_name:35} {count:8,} records")
                        else:
                            logger.info(f"  ⚠️ {table_name:35} {count:8,} records (empty)")
                            
                    except Exception as e:
                        logger.info(f"  ❌ {table_name:35} Error: {str(e)[:40]}...")
                        
            except Exception as e:
                logger.info(f"⚠️ Could not analyze Polygon fundamentals: {e}")
            
            logger.info("")
            
            # 3. Check instrument coverage vs fundamental data
            logger.info("🎯 FUNDAMENTAL DATA COVERAGE ANALYSIS:")
            
            total_instruments = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments WHERE active = true")
            logger.info(f"  Total active instruments: {total_instruments:,}")
            
            # Check coverage for different fundamental tables
            fundamental_coverage_tables = [
                'dev_polygon_fundamentals_annual',
                'dev_polygon_fundamentals_quarterly', 
                'dev_polygon_fundamentals',
                'dev_fundamentals',
                'polygon_fundamentals'
            ]
            
            for table in fundamental_coverage_tables:
                try:
                    coverage_stats = await conn.fetchrow(f"""
                        SELECT 
                            COUNT(DISTINCT i.symbol) as instruments_with_data,
                            COUNT(*) as total_records
                        FROM dev_instruments i 
                        JOIN {table} f ON i.symbol = f.symbol
                        WHERE i.active = true
                    """)
                    
                    if coverage_stats and coverage_stats['instruments_with_data']:
                        coverage_pct = (coverage_stats['instruments_with_data'] / total_instruments) * 100
                        logger.info(f"  ✅ {table:30} {coverage_stats['instruments_with_data']:,} instruments ({coverage_pct:.1f}%), {coverage_stats['total_records']:,} records")
                    
                except Exception as e:
                    logger.debug(f"  ❌ {table:30} Table not found or error")
            
            logger.info("")
            
            # 4. Check for recent fundamental data
            logger.info("📅 RECENT FUNDAMENTAL DATA:")
            recent_cutoff = "2024-01-01"
            
            for table in fundamental_coverage_tables:
                try:
                    recent_count = await conn.fetchval(f"""
                        SELECT COUNT(DISTINCT symbol) 
                        FROM {table} 
                        WHERE date >= '{recent_cutoff}'
                    """)
                    
                    if recent_count and recent_count > 0:
                        logger.info(f"  📈 {table:30} {recent_count:,} symbols with data since {recent_cutoff}")
                    
                except Exception as e:
                    continue
            
            logger.info("")
            
            # 5. Check for fundamental data collection scripts
            logger.info("🔧 FUNDAMENTAL DATA COLLECTION STATUS:")
            
            # Look for vendor-specific fundamental tables
            vendor_tables = await conn.fetch("""
                SELECT table_name FROM information_schema.tables 
                WHERE (table_name LIKE '%polygon%' OR table_name LIKE '%tiingo%' OR table_name LIKE '%eodhd%')
                  AND (table_name LIKE '%fundamental%' OR table_name LIKE '%financial%' OR table_name LIKE '%earnings%')
                ORDER BY table_name
            """)
            
            vendors_found = set()
            for row in vendor_tables:
                table_name = row['table_name']
                if 'polygon' in table_name:
                    vendors_found.add('polygon')
                elif 'tiingo' in table_name:
                    vendors_found.add('tiingo')
                elif 'eodhd' in table_name:
                    vendors_found.add('eodhd')
            
            logger.info(f"  Vendors with fundamental tables: {', '.join(sorted(vendors_found)) if vendors_found else 'None found'}")
            
            logger.info("")
            logger.info("=" * 70)
        
        await pool.close()
        
    except Exception as e:
        logger.error(f"❌ Fundamental data status check failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(check_fundamental_data_status())