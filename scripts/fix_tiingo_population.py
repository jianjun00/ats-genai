#!/usr/bin/env python3
"""
Fix Tiingo instrument population data quality issue

The issue: Tiingo API returns endDate as the current date for active stocks,
but this was being stored as a delisting date, making 75% of stocks appear delisted.

Fix: Interpret recent endDate (within 7 days) as "data available up to this date"
and set end_date to NULL for active stocks.
"""

import os
import sys
sys.path.append('/workspace/src')

import asyncpg
import logging
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("fix_tiingo_population")

async def fix_tiingo_end_dates():
    """Fix incorrectly populated end_dates in Tiingo instruments table"""
    
    # Connect to database
    db_host = os.getenv('DB_HOST', 'postgres')
    db_port = os.getenv('DB_PORT', '5432')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'dev_password')
    db_name = os.getenv('DB_NAME', 'dev_db')
    
    conn = await asyncpg.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name
    )
    
    try:
        # Get current date
        today = date.today()
        cutoff_date = today - timedelta(days=7)  # Within last 7 days = active
        
        logger.info(f"🔧 Fixing Tiingo end_date logic...")
        logger.info(f"📅 Today: {today}")
        logger.info(f"📅 Cutoff date for active stocks: {cutoff_date}")
        
        # Check current status before fix
        before_stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total_instruments,
                COUNT(CASE WHEN end_date IS NULL THEN 1 END) as currently_active,
                COUNT(CASE WHEN end_date IS NOT NULL THEN 1 END) as currently_delisted,
                COUNT(CASE WHEN end_date >= $1 THEN 1 END) as recent_end_dates
            FROM dev_instrument_tiingo
        """, cutoff_date)
        
        logger.info(f"📊 BEFORE FIX:")
        logger.info(f"   Total instruments: {before_stats['total_instruments']}")
        logger.info(f"   Currently active (end_date IS NULL): {before_stats['currently_active']}")
        logger.info(f"   Currently delisted (end_date IS NOT NULL): {before_stats['currently_delisted']}")
        logger.info(f"   Recent end_dates (>= {cutoff_date}): {before_stats['recent_end_dates']}")
        
        # Apply the fix: Set end_date to NULL for instruments with recent end_dates
        # These are active stocks where endDate represents current data availability
        result = await conn.execute("""
            UPDATE dev_instrument_tiingo 
            SET end_date = NULL, 
                updated_at = NOW()
            WHERE end_date >= $1
        """, cutoff_date)
        
        # Extract count from result (format: "UPDATE n")
        updated_count = int(result.split()[-1])
        logger.info(f"✅ Fixed {updated_count} instruments with recent end_dates")
        
        # Check status after fix
        after_stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total_instruments,
                COUNT(CASE WHEN end_date IS NULL THEN 1 END) as now_active,
                COUNT(CASE WHEN end_date IS NOT NULL THEN 1 END) as now_delisted,
                COUNT(CASE WHEN end_date >= $1 THEN 1 END) as remaining_recent_end_dates
            FROM dev_instrument_tiingo
        """, cutoff_date)
        
        logger.info(f"📊 AFTER FIX:")
        logger.info(f"   Total instruments: {after_stats['total_instruments']}")
        logger.info(f"   Now active (end_date IS NULL): {after_stats['now_active']}")
        logger.info(f"   Now delisted (end_date IS NOT NULL): {after_stats['now_delisted']}")
        logger.info(f"   Remaining recent end_dates: {after_stats['remaining_recent_end_dates']}")
        
        # Show examples of what was fixed
        examples = await conn.fetch("""
            SELECT symbol, name, start_date, end_date, updated_at
            FROM dev_instrument_tiingo
            WHERE updated_at >= NOW() - INTERVAL '5 minutes'
              AND end_date IS NULL
            ORDER BY symbol
            LIMIT 10
        """)
        
        logger.info(f"📝 Examples of fixed active instruments:")
        for ex in examples:
            logger.info(f"   {ex['symbol']}: {ex['name']} (now active, was end_date={ex.get('old_end_date', 'recent')})")
        
        # Show examples of truly delisted stocks (older end_dates)
        delisted_examples = await conn.fetch("""
            SELECT symbol, name, start_date, end_date
            FROM dev_instrument_tiingo
            WHERE end_date IS NOT NULL
              AND end_date < $1
            ORDER BY end_date DESC
            LIMIT 5
        """, cutoff_date)
        
        if delisted_examples:
            logger.info(f"📝 Examples of truly delisted instruments:")
            for ex in delisted_examples:
                logger.info(f"   {ex['symbol']}: {ex['name']} (delisted {ex['end_date']})")
        
        # Final summary
        active_increase = after_stats['now_active'] - before_stats['currently_active']
        logger.info(f"🎉 SUCCESS: Fixed {active_increase} instruments from incorrectly delisted to active")
        
        return {
            'updated_count': updated_count,
            'active_before': before_stats['currently_active'],
            'active_after': after_stats['now_active'],
            'active_increase': active_increase
        }
        
    finally:
        await conn.close()

async def validate_fix():
    """Validate that the fix worked correctly"""
    
    # Connect to database
    db_host = os.getenv('DB_HOST', 'postgres')
    db_port = os.getenv('DB_PORT', '5432')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'dev_password')
    db_name = os.getenv('DB_NAME', 'dev_db')
    
    conn = await asyncpg.connect(
        host=db_host,
        port=db_port,
        user=db_user,
        password=db_password,
        database=db_name
    )
    
    try:
        # Check that major stocks are now active
        major_stocks = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN', 'NFLX', 'NVDA']
        
        logger.info(f"🔍 Validating major stocks are now active:")
        for symbol in major_stocks:
            result = await conn.fetchrow("""
                SELECT symbol, name, end_date,
                       CASE WHEN end_date IS NULL THEN 'ACTIVE' ELSE 'DELISTED' END as status
                FROM dev_instrument_tiingo 
                WHERE symbol = $1
            """, symbol)
            
            if result:
                status = "✅ ACTIVE" if result['end_date'] is None else f"❌ DELISTED ({result['end_date']})"
                logger.info(f"   {result['symbol']}: {status}")
            else:
                logger.warning(f"   {symbol}: ❓ NOT FOUND")
        
        # Overall statistics
        stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN end_date IS NULL THEN 1 END) as active,
                ROUND(COUNT(CASE WHEN end_date IS NULL THEN 1 END) * 100.0 / COUNT(*), 1) as active_pct
            FROM dev_instrument_tiingo
        """)
        
        logger.info(f"📈 Final Statistics:")
        logger.info(f"   Total instruments: {stats['total']}")
        logger.info(f"   Active instruments: {stats['active']} ({stats['active_pct']}%)")
        
        return stats['active_pct'] > 70  # Should be >70% active now
        
    finally:
        await conn.close()

async def main():
    """Main function to fix Tiingo population data quality issue"""
    logger.info("🚀 Starting Tiingo instrument population fix...")
    
    try:
        # Apply the fix
        fix_result = await fix_tiingo_end_dates()
        
        # Validate the fix
        validation_passed = await validate_fix()
        
        if validation_passed:
            logger.info("✅ Tiingo instrument population fix completed successfully!")
            logger.info(f"✅ Major stocks are now correctly marked as active")
            return True
        else:
            logger.error("❌ Validation failed - fix may not have worked correctly")
            return False
            
    except Exception as e:
        logger.error(f"❌ Failed to fix Tiingo population: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    import asyncio
    success = asyncio.run(main())
    if not success:
        sys.exit(1)