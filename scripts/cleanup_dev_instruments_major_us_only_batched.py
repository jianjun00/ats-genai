#!/usr/bin/env python3
"""
Cleanup Dev Instruments - Keep Major US Exchanges Only (Batched)

Removes all instruments from dev_instruments table that are NOT on major US exchanges.
Uses batched deletion for better performance with large datasets.

Environment Variables:
- DRY_RUN: Set to 'true' to preview what would be deleted without executing
- BATCH_SIZE: Number of records to delete per batch (default: 1000)
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

class BatchedDevInstrumentsCleanup:
    """Clean up dev_instruments to keep only major US exchanges using batched deletion"""
    
    def __init__(self, dry_run: bool = False, batch_size: int = 1000):
        self.dry_run = dry_run
        self.batch_size = batch_size
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
            'batches_processed': 0
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
            
            logger.info("=" * 60)
            logger.info("📈 DELETION PLAN")
            logger.info("=" * 60)
            logger.info(f"Total Instruments: {self.stats['initial_count']:,}")
            logger.info(f"Major US Exchanges: {self.stats['major_us_count']:,}")
            logger.info(f"To be deleted: {self.stats['to_delete_count']:,}")
            logger.info(f"Batch size: {self.batch_size:,}")
            logger.info(f"Estimated batches: {(self.stats['to_delete_count'] // self.batch_size) + 1}")
            logger.info("=" * 60)

    async def delete_non_major_us_instruments_batched(self, pool: asyncpg.Pool) -> None:
        """Delete all instruments not on major US exchanges using batched deletion"""
        logger.info("🗑️ Starting batched deletion of non-major US exchange instruments...")
        
        if self.dry_run:
            logger.info("🏃 DRY RUN MODE - No actual deletion will occur")
            self.stats['deleted_count'] = self.stats['to_delete_count']
            return
        
        major_us_exchanges_sql = "', '".join(self.major_us_exchanges)
        
        async with pool.acquire() as conn:
            while True:
                # Delete one batch
                result = await conn.execute(f"""
                    DELETE FROM dev_instruments 
                    WHERE id IN (
                        SELECT id FROM dev_instruments 
                        WHERE exchange NOT IN ('{major_us_exchanges_sql}')
                           OR exchange IS NULL
                        LIMIT {self.batch_size}
                    )
                """)
                
                # Parse deletion count from result
                if result.startswith('DELETE'):
                    batch_deleted = int(result.split()[1])
                else:
                    batch_deleted = 0
                
                if batch_deleted == 0:
                    break  # No more records to delete
                
                self.stats['deleted_count'] += batch_deleted
                self.stats['batches_processed'] += 1
                
                logger.info(f"📦 Batch {self.stats['batches_processed']}: Deleted {batch_deleted:,} instruments (Total: {self.stats['deleted_count']:,})")
                
                # Brief pause to avoid overwhelming the database
                await asyncio.sleep(0.1)
        
        logger.info(f"✅ Batched deletion completed: {self.stats['deleted_count']:,} instruments deleted in {self.stats['batches_processed']} batches")

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
            
            logger.info("=" * 70)

    def log_summary(self):
        """Log final summary of cleanup operation"""
        elapsed = datetime.now() - self.start_time
        
        logger.info("=" * 70)
        logger.info("🎉 BATCHED DEV_INSTRUMENTS CLEANUP COMPLETE")
        logger.info("=" * 70)
        logger.info(f"⏱️  Total Time: {elapsed}")
        logger.info(f"🏃 Dry Run Mode: {self.dry_run}")
        logger.info(f"📦 Batches Processed: {self.stats['batches_processed']}")
        logger.info("")
        logger.info("📊 OPERATION SUMMARY:")
        logger.info(f"  Initial Count: {self.stats['initial_count']:,}")
        logger.info(f"  Deleted Count: {self.stats['deleted_count']:,}")
        logger.info(f"  Batch Size: {self.batch_size:,}")
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
    batch_size = int(os.getenv('BATCH_SIZE', '1000'))
    
    logger.info("🚀 Starting Batched Dev Instruments Cleanup - Major US Exchanges Only")
    logger.info(f"🏃 Dry Run Mode: {dry_run}")
    logger.info(f"📦 Batch Size: {batch_size:,}")
    
    if dry_run:
        logger.info("⚠️ DRY RUN MODE - No actual changes will be made")
    else:
        logger.info("⚠️ LIVE MODE - Instruments will be permanently deleted")
    
    try:
        # Database connection
        from config.database import Database
        from config.environment import Environment, EnvironmentType
        
        env = Environment(EnvironmentType.DEV)
        pool = await Database.create_connection_pool(env=env, timeout=60.0)  # Increased timeout
        
        cleanup = BatchedDevInstrumentsCleanup(
            dry_run=dry_run, 
            batch_size=batch_size
        )
        
        # Step 1: Analyze current state
        await cleanup.analyze_current_state(pool)
        
        if not dry_run:
            logger.info("")
            logger.warning("⚠️ THIS WILL PERMANENTLY DELETE NON-MAJOR US EXCHANGE INSTRUMENTS!")
            logger.warning(f"⚠️ {cleanup.stats['to_delete_count']:,} instruments will be deleted")
            logger.info("")
        
        # Step 2: Delete non-major US instruments in batches
        await cleanup.delete_non_major_us_instruments_batched(pool)
        
        # Step 3: Verify results
        await cleanup.verify_cleanup_results(pool)
        
        # Step 4: Log final summary
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