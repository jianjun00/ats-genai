#!/usr/bin/env python3
"""
Rebuild Dev Instruments - Major US Exchanges Only

Instead of deleting records, this script rebuilds the dev_instruments table
with only major US exchange instruments. Much faster than deletion.

Environment Variables:
- DRY_RUN: Set to 'true' to preview what would be kept without executing
"""

import os
import sys
import asyncio
import asyncpg
import logging
from datetime import datetime
from typing import List, Set

# Add src to Python path
sys.path.insert(0, '/workspace/src')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DevInstrumentsRebuild:
    """Rebuild dev_instruments table with only major US exchanges"""
    
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.start_time = datetime.now()
        
        # Define major US exchanges to KEEP
        self.major_us_exchanges = {
            'NASDAQ', 'NYSE', 'NYSE ARCA', 'NYSE MKT', 'AMEX',
            'XNAS', 'XNYS',  # Alternative codes
            'BATS',  # Major US exchange
            'NYSE NAT'  # Include NYSE National
        }
        
        # Statistics tracking
        self.stats = {
            'original_count': 0,
            'major_us_count': 0,
            'removed_count': 0,
            'final_count': 0
        }

    async def analyze_rebuild_plan(self, pool: asyncpg.Pool) -> None:
        """Analyze what the rebuild will look like"""
        logger.info("📊 Analyzing rebuild plan...")
        
        async with pool.acquire() as conn:
            # Get original count
            self.stats['original_count'] = await conn.fetchval(
                "SELECT COUNT(*) FROM dev_instruments"
            )
            
            # Get major US exchanges count
            major_us_exchanges_sql = "', '".join(self.major_us_exchanges)
            self.stats['major_us_count'] = await conn.fetchval(f"""
                SELECT COUNT(*) FROM dev_instruments 
                WHERE exchange IN ('{major_us_exchanges_sql}')
            """)
            
            # Calculate what will be removed
            self.stats['removed_count'] = self.stats['original_count'] - self.stats['major_us_count']
            
            # Get breakdown of what will be kept
            major_us_breakdown = await conn.fetch(f"""
                SELECT exchange, COUNT(*) as count
                FROM dev_instruments 
                WHERE exchange IN ('{major_us_exchanges_sql}')
                GROUP BY exchange 
                ORDER BY count DESC
            """)
            
            logger.info("=" * 70)
            logger.info("📈 REBUILD PLAN")
            logger.info("=" * 70)
            logger.info(f"Original Instruments: {self.stats['original_count']:,}")
            logger.info(f"Major US Exchanges: {self.stats['major_us_count']:,}")
            logger.info(f"Will be removed: {self.stats['removed_count']:,}")
            logger.info("")
            
            logger.info("📊 MAJOR US EXCHANGES TO KEEP:")
            for row in major_us_breakdown:
                exchange = row['exchange']
                count = row['count']
                percentage = (count / self.stats['major_us_count'] * 100) if self.stats['major_us_count'] > 0 else 0
                logger.info(f"  ✅ {exchange:15} {count:8,} ({percentage:5.1f}%)")
            
            logger.info("=" * 70)

    async def rebuild_dev_instruments_table(self, pool: asyncpg.Pool) -> None:
        """Rebuild dev_instruments table with only major US exchanges"""
        logger.info("🔨 Rebuilding dev_instruments table...")
        
        if self.dry_run:
            logger.info("🏃 DRY RUN MODE - No actual rebuild will occur")
            self.stats['final_count'] = self.stats['major_us_count']
            return
        
        major_us_exchanges_sql = "', '".join(self.major_us_exchanges)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Step 1: Rename current table to backup
                backup_table = f"dev_instruments_backup_full_{timestamp}"
                logger.info(f"📦 Creating backup table: {backup_table}")
                
                await conn.execute(f"ALTER TABLE dev_instruments RENAME TO {backup_table}")
                
                # Step 2: Create new dev_instruments table with only major US exchanges
                logger.info("🏗️ Creating new dev_instruments table with major US exchanges only...")
                
                await conn.execute(f"""
                    CREATE TABLE dev_instruments AS
                    SELECT * FROM {backup_table}
                    WHERE exchange IN ('{major_us_exchanges_sql}')
                """)
                
                # Step 3: Recreate indexes and constraints
                logger.info("🔧 Recreating indexes and constraints...")
                
                # Check and add constraints carefully
                try:
                    # Check if primary key exists
                    pk_exists = await conn.fetchval("""
                        SELECT COUNT(*) FROM pg_constraint 
                        WHERE conname = 'dev_instruments_pkey'
                    """)
                    
                    if not pk_exists:
                        await conn.execute("ALTER TABLE dev_instruments ADD CONSTRAINT dev_instruments_pkey PRIMARY KEY (id);")
                        logger.info("Added primary key constraint")
                    else:
                        logger.info("Primary key constraint already exists")
                    
                    # Check if unique symbol constraint exists
                    symbol_unique_exists = await conn.fetchval("""
                        SELECT COUNT(*) FROM pg_constraint 
                        WHERE conname = 'dev_instruments_symbol_key'
                    """)
                    
                    if not symbol_unique_exists:
                        await conn.execute("ALTER TABLE dev_instruments ADD CONSTRAINT dev_instruments_symbol_key UNIQUE (symbol);")
                        logger.info("Added unique symbol constraint")
                    else:
                        logger.info("Unique symbol constraint already exists")
                        
                except Exception as e:
                    logger.warning(f"Constraint creation warning: {e}")
                    # Continue with indexes even if constraints fail
                
                await conn.execute("""
                    -- Recreate indexes
                    CREATE INDEX IF NOT EXISTS idx_dev_instruments_exchange ON dev_instruments(exchange);
                    CREATE INDEX IF NOT EXISTS idx_dev_instruments_active ON dev_instruments(active);
                    CREATE INDEX IF NOT EXISTS idx_dev_instruments_type ON dev_instruments(type);
                """)
                
                # Step 4: Update sequence to match new table
                await conn.execute("""
                    SELECT setval('dev_instruments_id_seq', 
                        COALESCE((SELECT MAX(id) FROM dev_instruments), 0) + 1
                    )
                """)
                
                # Get final count
                self.stats['final_count'] = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments")
                
                logger.info(f"✅ Rebuild completed: {self.stats['final_count']:,} instruments in new table")
                logger.info(f"💾 Full backup available at: {backup_table}")

    async def verify_rebuild_results(self, pool: asyncpg.Pool) -> None:
        """Verify the rebuild results"""
        logger.info("✅ Verifying rebuild results...")
        
        async with pool.acquire() as conn:
            # Verify all remaining instruments are on major US exchanges
            major_us_exchanges_sql = "', '".join(self.major_us_exchanges)
            major_us_final = await conn.fetchval(f"""
                SELECT COUNT(*) FROM dev_instruments 
                WHERE exchange IN ('{major_us_exchanges_sql}')
            """)
            
            # Check for any non-major US exchanges (should be 0)
            non_major_remaining = await conn.fetchval(f"""
                SELECT COUNT(*) FROM dev_instruments 
                WHERE exchange NOT IN ('{major_us_exchanges_sql}')
                   OR exchange IS NULL
            """)
            
            # Get final exchange breakdown
            final_exchanges = await conn.fetch("""
                SELECT exchange, COUNT(*) as count
                FROM dev_instruments 
                GROUP BY exchange 
                ORDER BY count DESC
            """)
            
            # Check table structure
            table_info = await conn.fetch("""
                SELECT 
                    schemaname, tablename, 
                    hasindexes, hasrules, hastriggers
                FROM pg_tables 
                WHERE tablename = 'dev_instruments'
            """)
            
            logger.info("=" * 70)
            logger.info("✅ REBUILD VERIFICATION RESULTS")
            logger.info("=" * 70)
            logger.info(f"Original instruments: {self.stats['original_count']:,}")
            logger.info(f"Final instruments: {self.stats['final_count']:,}")
            logger.info(f"Removed instruments: {self.stats['original_count'] - self.stats['final_count']:,}")
            logger.info(f"Major US exchanges: {major_us_final:,}")
            logger.info(f"Non-major remaining: {non_major_remaining:,}")
            logger.info("")
            
            logger.info("📊 FINAL EXCHANGE BREAKDOWN:")
            for row in final_exchanges:
                exchange = row['exchange'] or 'NULL'
                count = row['count']
                percentage = (count / self.stats['final_count'] * 100) if self.stats['final_count'] > 0 else 0
                logger.info(f"  ✅ {exchange:15} {count:8,} ({percentage:5.1f}%)")
            
            logger.info("")
            logger.info("🔧 TABLE STRUCTURE:")
            for row in table_info:
                logger.info(f"  Schema: {row['schemaname']}")
                logger.info(f"  Table: {row['tablename']}")
                logger.info(f"  Has Indexes: {row['hasindexes']}")
                logger.info(f"  Has Rules: {row['hasrules']}")
                logger.info(f"  Has Triggers: {row['hastriggers']}")
            
            # Validation checks
            if non_major_remaining > 0:
                logger.warning(f"⚠️ {non_major_remaining} non-major US instruments remain!")
            else:
                logger.info("✅ All remaining instruments are on major US exchanges")
            
            if self.stats['final_count'] != major_us_final:
                logger.warning("⚠️ Final count doesn't match major US count")
            else:
                logger.info("✅ Final count matches major US exchanges perfectly")
            
            logger.info("=" * 70)

    def log_summary(self):
        """Log final summary of rebuild operation"""
        elapsed = datetime.now() - self.start_time
        
        logger.info("=" * 70)
        logger.info("🎉 DEV_INSTRUMENTS REBUILD COMPLETE")
        logger.info("=" * 70)
        logger.info(f"⏱️  Total Time: {elapsed}")
        logger.info(f"🏃 Dry Run Mode: {self.dry_run}")
        logger.info("")
        logger.info("📊 OPERATION SUMMARY:")
        logger.info(f"  Original Count: {self.stats['original_count']:,}")
        logger.info(f"  Final Count: {self.stats['final_count']:,}")
        logger.info(f"  Removed Count: {self.stats['original_count'] - self.stats['final_count']:,}")
        logger.info(f"  Reduction: {((self.stats['original_count'] - self.stats['final_count']) / self.stats['original_count'] * 100):.1f}%")
        logger.info("")
        
        if self.dry_run:
            logger.info("🏃 DRY RUN COMPLETED - No actual changes made")
        else:
            logger.info("✅ REBUILD COMPLETED - Only major US exchanges remain")
            logger.info("💾 Full backup table created with original data")
            
        logger.info("=" * 70)

async def main():
    """Main execution function"""
    
    # Configuration
    dry_run = os.getenv('DRY_RUN', 'false').lower() == 'true'
    
    logger.info("🚀 Starting Dev Instruments Table Rebuild - Major US Exchanges Only")
    logger.info(f"🏃 Dry Run Mode: {dry_run}")
    
    if dry_run:
        logger.info("⚠️ DRY RUN MODE - No actual changes will be made")
    else:
        logger.info("⚠️ LIVE MODE - Table will be rebuilt (with backup)")
    
    try:
        # Database connection
        from config.database import Database
        from config.environment import Environment, EnvironmentType
        
        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=60.0)
        
        rebuild = DevInstrumentsRebuild(dry_run=dry_run)
        
        # Step 1: Analyze rebuild plan
        await rebuild.analyze_rebuild_plan(pool)
        
        if not dry_run:
            logger.info("")
            logger.warning("⚠️ THIS WILL REBUILD THE DEV_INSTRUMENTS TABLE!")
            logger.warning(f"⚠️ {rebuild.stats['removed_count']:,} instruments will be removed")
            logger.warning("⚠️ Original table will be backed up automatically")
            logger.info("")
        
        # Step 2: Rebuild the table
        await rebuild.rebuild_dev_instruments_table(pool)
        
        # Step 3: Verify results
        await rebuild.verify_rebuild_results(pool)
        
        # Step 4: Log final summary
        rebuild.log_summary()
        
        await pool.close()
        return 0
        
    except Exception as e:
        logger.error(f"❌ Rebuild failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)