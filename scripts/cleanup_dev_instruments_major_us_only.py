#!/usr/bin/env python3
"""
Cleanup Dev Instruments - Keep Major US Exchanges Only

Removes all instruments from dev_instruments table that are NOT on major US exchanges.
Keeps only: NASDAQ, NYSE, NYSE ARCA, NYSE MKT, AMEX, BATS, XNAS, XNYS

Environment Variables:
- DRY_RUN: Set to 'true' to preview what would be deleted without executing
- BACKUP_BEFORE_DELETE: Set to 'true' to create backup before deletion
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

class DevInstrumentsCleanup:
    """Clean up dev_instruments to keep only major US exchanges"""
    
    def __init__(self, dry_run: bool = False, backup_before_delete: bool = True):
        self.dry_run = dry_run
        self.backup_before_delete = backup_before_delete
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
            'initial_count': 0,
            'major_us_count': 0,
            'to_delete_count': 0,
            'deleted_count': 0,
            'backup_created': False,
            'price_data_orphans': 0
        }

    async def analyze_current_state(self, pool: asyncpg.Pool) -> None:
        """Analyze current dev_instruments state"""
        logger.info("📊 Analyzing current dev_instruments state...")
        
        async with pool.acquire() as conn:
            # Get total count
            self.stats['initial_count'] = await conn.fetchval(
                "SELECT COUNT(*) FROM dev_instruments"
            )
            
            # Get major US exchanges count
            major_us_exchanges_sql = "', '".join(self.major_us_exchanges)
            self.stats['major_us_count'] = await conn.fetchval(f"""
                SELECT COUNT(*) FROM dev_instruments 
                WHERE exchange IN ('{major_us_exchanges_sql}')
            """)
            
            # Calculate what would be deleted
            self.stats['to_delete_count'] = self.stats['initial_count'] - self.stats['major_us_count']
            
            # Check for price data that would become orphaned
            self.stats['price_data_orphans'] = await conn.fetchval(f"""
                SELECT COUNT(*) FROM dev_daily_prices_polygon p
                JOIN dev_instruments i ON i.id = p.instrument_id
                WHERE i.exchange NOT IN ('{major_us_exchanges_sql}')
            """)
            
            # Get breakdown by exchange
            exchange_breakdown = await conn.fetch("""
                SELECT 
                    exchange,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dev_instruments), 2) as percentage
                FROM dev_instruments 
                GROUP BY exchange 
                ORDER BY count DESC
            """)
            
            logger.info("=" * 70)
            logger.info("📈 CURRENT DEV_INSTRUMENTS BREAKDOWN")
            logger.info("=" * 70)
            logger.info(f"Total Instruments: {self.stats['initial_count']:,}")
            logger.info(f"Major US Exchanges: {self.stats['major_us_count']:,}")
            logger.info(f"To be deleted: {self.stats['to_delete_count']:,}")
            logger.info(f"Price data orphans: {self.stats['price_data_orphans']:,}")
            logger.info("")
            
            logger.info("📊 EXCHANGE BREAKDOWN (Top 15):")
            for i, row in enumerate(exchange_breakdown[:15]):
                exchange = row['exchange'] or 'NULL'
                count = row['count']
                percentage = row['percentage']
                
                # Mark major US exchanges
                marker = "✅" if exchange in self.major_us_exchanges else "❌"
                logger.info(f"  {marker} {exchange:15} {count:8,} ({percentage:5.1f}%)")
            
            logger.info("")
            logger.info(f"🎯 MAJOR US EXCHANGES TO KEEP:")
            for exchange in sorted(self.major_us_exchanges):
                logger.info(f"  ✅ {exchange}")
            
            logger.info("=" * 70)

    async def create_backup(self, pool: asyncpg.Pool) -> bool:
        """Create backup of instruments that will be deleted"""
        if not self.backup_before_delete:
            logger.info("⏭️ Skipping backup (BACKUP_BEFORE_DELETE=false)")
            return True
            
        logger.info("💾 Creating backup of instruments to be deleted...")
        
        try:
            async with pool.acquire() as conn:
                # Create backup table with timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_table = f"dev_instruments_backup_deleted_{timestamp}"
                
                major_us_exchanges_sql = "', '".join(self.major_us_exchanges)
                
                # Create backup table with instruments that will be deleted
                result = await conn.execute(f"""
                    CREATE TABLE {backup_table} AS
                    SELECT * FROM dev_instruments 
                    WHERE exchange NOT IN ('{major_us_exchanges_sql}')
                       OR exchange IS NULL
                """)
                
                # Get count of backed up records
                backup_count = await conn.fetchval(f"SELECT COUNT(*) FROM {backup_table}")
                
                logger.info(f"✅ Backup created: {backup_table}")
                logger.info(f"✅ Backed up {backup_count:,} instruments")
                
                # Add comment to backup table
                await conn.execute(f"""
                    COMMENT ON TABLE {backup_table} IS 
                    'Backup of dev_instruments records deleted on {timestamp} - non-major US exchanges only'
                """)
                
                self.stats['backup_created'] = True
                return True
                
        except Exception as e:
            logger.error(f"❌ Backup failed: {e}")
            return False

    async def delete_non_major_us_instruments(self, pool: asyncpg.Pool) -> None:
        """Delete all instruments not on major US exchanges"""
        logger.info("🗑️ Deleting non-major US exchange instruments...")
        
        if self.dry_run:
            logger.info("🏃 DRY RUN MODE - No actual deletion will occur")
            self.stats['deleted_count'] = self.stats['to_delete_count']
            return
            
        async with pool.acquire() as conn:
            async with conn.transaction():
                major_us_exchanges_sql = "', '".join(self.major_us_exchanges)
                
                # Delete non-major US instruments
                result = await conn.execute(f"""
                    DELETE FROM dev_instruments 
                    WHERE exchange NOT IN ('{major_us_exchanges_sql}')
                       OR exchange IS NULL
                """)
                
                # Parse result to get deleted count
                if result.startswith('DELETE'):
                    self.stats['deleted_count'] = int(result.split()[1])
                else:
                    # Fallback: count remaining to calculate deleted
                    remaining_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments")
                    self.stats['deleted_count'] = self.stats['initial_count'] - remaining_count
                
                logger.info(f"✅ Deleted {self.stats['deleted_count']:,} non-major US instruments")

    async def verify_cleanup_results(self, pool: asyncpg.Pool) -> None:
        """Verify the cleanup results"""
        logger.info("✅ Verifying cleanup results...")
        
        async with pool.acquire() as conn:
            # Get final counts
            final_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments")
            
            # Verify all remaining instruments are on major US exchanges
            major_us_exchanges_sql = "', '".join(self.major_us_exchanges)
            major_us_final = await conn.fetchval(f"""
                SELECT COUNT(*) FROM dev_instruments 
                WHERE exchange IN ('{major_us_exchanges_sql}')
            """)
            
            # Check for any remaining non-major US exchanges
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
            
            logger.info("=" * 70)
            logger.info("✅ CLEANUP VERIFICATION RESULTS")
            logger.info("=" * 70)
            logger.info(f"Initial instruments: {self.stats['initial_count']:,}")
            logger.info(f"Deleted instruments: {self.stats['deleted_count']:,}")
            logger.info(f"Final instruments: {final_count:,}")
            logger.info(f"Major US exchanges: {major_us_final:,}")
            logger.info(f"Non-major remaining: {non_major_remaining:,}")
            logger.info("")
            
            logger.info("📊 FINAL EXCHANGE BREAKDOWN:")
            for row in final_exchanges:
                exchange = row['exchange'] or 'NULL'
                count = row['count']
                percentage = (count / final_count * 100) if final_count > 0 else 0
                logger.info(f"  ✅ {exchange:15} {count:8,} ({percentage:5.1f}%)")
            
            # Validation checks
            if non_major_remaining > 0:
                logger.warning(f"⚠️ {non_major_remaining} non-major US instruments remain!")
            else:
                logger.info("✅ All remaining instruments are on major US exchanges")
            
            if abs(final_count - self.stats['major_us_count']) > 10:  # Allow small variance
                logger.warning("⚠️ Final count doesn't match expected major US count")
            else:
                logger.info("✅ Final count matches expected major US exchanges")
            
            logger.info("=" * 70)

    def log_summary(self):
        """Log final summary of cleanup operation"""
        elapsed = datetime.now() - self.start_time
        
        logger.info("=" * 70)
        logger.info("🎉 DEV_INSTRUMENTS CLEANUP COMPLETE")
        logger.info("=" * 70)
        logger.info(f"⏱️  Total Time: {elapsed}")
        logger.info(f"🏃 Dry Run Mode: {self.dry_run}")
        logger.info(f"💾 Backup Created: {self.stats['backup_created']}")
        logger.info("")
        logger.info("📊 OPERATION SUMMARY:")
        logger.info(f"  Initial Count: {self.stats['initial_count']:,}")
        logger.info(f"  Major US Count: {self.stats['major_us_count']:,}")
        logger.info(f"  Deleted Count: {self.stats['deleted_count']:,}")
        logger.info(f"  Price Data Orphans: {self.stats['price_data_orphans']:,}")
        logger.info("")
        
        if self.dry_run:
            logger.info("🏃 DRY RUN COMPLETED - No actual changes made")
        else:
            logger.info("✅ CLEANUP COMPLETED - Only major US exchanges remain")
            
        logger.info("=" * 70)

async def main():
    """Main execution function"""
    
    # Configuration
    dry_run = os.getenv('DRY_RUN', 'false').lower() == 'true'
    backup_before_delete = os.getenv('BACKUP_BEFORE_DELETE', 'true').lower() == 'true'
    
    logger.info("🚀 Starting Dev Instruments Cleanup - Major US Exchanges Only")
    logger.info(f"🏃 Dry Run Mode: {dry_run}")
    logger.info(f"💾 Backup Before Delete: {backup_before_delete}")
    
    if dry_run:
        logger.info("⚠️ DRY RUN MODE - No actual changes will be made")
    else:
        logger.info("⚠️ LIVE MODE - Instruments will be permanently deleted")
    
    try:
        # Database connection
        from config.database import Database
        from config.environment import Environment, EnvironmentType
        
        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=30.0)
        
        cleanup = DevInstrumentsCleanup(
            dry_run=dry_run, 
            backup_before_delete=backup_before_delete
        )
        
        # Step 1: Analyze current state
        await cleanup.analyze_current_state(pool)
        
        # Confirm if not dry run
        if not dry_run:
            logger.info("")
            logger.warning("⚠️ THIS WILL PERMANENTLY DELETE NON-MAJOR US EXCHANGE INSTRUMENTS!")
            logger.warning(f"⚠️ {cleanup.stats['to_delete_count']:,} instruments will be deleted")
            logger.warning(f"⚠️ {cleanup.stats['price_data_orphans']:,} price records will become orphaned")
            logger.info("")
            
        # Step 2: Create backup if requested
        if backup_before_delete and not dry_run:
            backup_success = await cleanup.create_backup(pool)
            if not backup_success:
                logger.error("❌ Backup failed - aborting cleanup")
                return 1
        
        # Step 3: Delete non-major US instruments
        await cleanup.delete_non_major_us_instruments(pool)
        
        # Step 4: Verify results
        await cleanup.verify_cleanup_results(pool)
        
        # Step 5: Log final summary
        cleanup.log_summary()
        
        await pool.close()
        return 0
        
    except Exception as e:
        logger.error(f"❌ Cleanup failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)