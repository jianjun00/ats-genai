#!/usr/bin/env python3
"""
EODHD Completion Monitor and Unified Population Trigger

Monitors EODHD bulk population completion and automatically executes
unified instrument population when ready.

Features:
- Monitors EODHD completion status via log file and database counts
- Executes unified population automatically when EODHD completes
- Provides comprehensive progress reporting
- Handles error cases and restarts
"""

import asyncio
import asyncpg
import logging
import os
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class UnifiedPopulationMonitor:
    """Monitor EODHD and execute unified population when ready"""
    
    def __init__(self):
        self.eodhd_log_path = "/tmp/eodhd_bulk_population.log"
        self.check_interval = 30  # Check every 30 seconds
        self.target_batch = 254   # Expected final batch number
        self.target_count = 50746 # Expected final instrument count
        self.start_time = datetime.now()
        
    async def check_eodhd_completion(self) -> dict:
        """Check EODHD completion status"""
        try:
            # Check log file for completion
            if os.path.exists(self.eodhd_log_path):
                with open(self.eodhd_log_path, 'r') as f:
                    content = f.read()
                    
                # Look for completion indicators
                if "EODHD BULK POPULATION COMPLETE" in content:
                    return {'status': 'completed', 'method': 'log_completion_marker'}
                
                # Check latest batch number
                lines = content.split('\n')
                current_batch = 0
                for line in reversed(lines):
                    if 'Processing batch' in line and '/254' in line:
                        try:
                            batch_part = line.split('batch ')[1].split('/')[0]
                            current_batch = int(batch_part)
                            break
                        except:
                            continue
                
                if current_batch >= self.target_batch:
                    return {'status': 'completed', 'method': 'batch_count', 'current_batch': current_batch}
                else:
                    return {'status': 'running', 'current_batch': current_batch}
            
            # Database count check as fallback
            from config.database import Database
            from config.environment import Environment, EnvironmentType
            
            env = Environment(EnvironmentType.DEV)
            pool = await Database.create_connection_pool(env=env, timeout=10.0)
            
            async with pool.acquire() as conn:
                count = await conn.fetchval("SELECT COUNT(*) FROM dev_instrument_eodhd")
            
            await pool.close()
            
            if count >= self.target_count:
                return {'status': 'completed', 'method': 'database_count', 'count': count}
            else:
                return {'status': 'running', 'count': count}
                
        except Exception as e:
            logger.error(f"❌ Error checking EODHD status: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def execute_unified_population(self) -> bool:
        """Execute unified instrument population"""
        logger.info("🚀 EODHD COMPLETED! Starting Unified Instrument Population...")
        
        try:
            # Run unified population script
            cmd = [
                "python3", "scripts/run_dev.py", "run", 
                "--script", "scripts/unified_instrument_population.py"
            ]
            
            logger.info("📦 Executing: " + " ".join(cmd))
            
            # Execute with real-time logging
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            
            # Stream output in real-time
            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    logger.info(f"📋 {output.strip()}")
            
            return_code = process.poll()
            
            if return_code == 0:
                logger.info("✅ Unified Instrument Population completed successfully!")
                return True
            else:
                logger.error(f"❌ Unified population failed with return code: {return_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error executing unified population: {e}")
            return False
    
    async def verify_unified_results(self) -> dict:
        """Verify unified population results"""
        try:
            from config.database import Database
            from config.environment import Environment, EnvironmentType
            
            env = Environment(EnvironmentType.DEV)
            pool = await Database.create_connection_pool(env=env, timeout=10.0)
            
            async with pool.acquire() as conn:
                # Get final counts
                unified_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments")
                active_count = await conn.fetchval("SELECT COUNT(*) FROM dev_instruments WHERE active = true")
                
                # Check price data integrity
                price_records = await conn.fetchval("SELECT COUNT(*) FROM dev_daily_prices_polygon")
                price_with_instruments = await conn.fetchval("""
                    SELECT COUNT(*) FROM dev_daily_prices_polygon p
                    WHERE EXISTS (SELECT 1 FROM dev_instruments i WHERE i.id = p.instrument_id)
                """)
                
                integrity_pct = (price_with_instruments / price_records * 100) if price_records > 0 else 0
            
            await pool.close()
            
            return {
                'unified_instruments': unified_count,
                'active_instruments': active_count,
                'price_records': price_records,
                'price_integrity': price_with_instruments,
                'integrity_percentage': integrity_pct
            }
            
        except Exception as e:
            logger.error(f"❌ Error verifying results: {e}")
            return {'error': str(e)}
    
    async def run_monitoring_loop(self):
        """Main monitoring loop"""
        logger.info("🔍 Starting EODHD completion monitoring...")
        logger.info(f"📊 Target: {self.target_count:,} instruments in {self.target_batch} batches")
        logger.info(f"⏱️  Check interval: {self.check_interval} seconds")
        
        while True:
            try:
                status = await self.check_eodhd_completion()
                elapsed = datetime.now() - self.start_time
                
                if status['status'] == 'completed':
                    logger.info("=" * 80)
                    logger.info("🎉 EODHD BULK POPULATION COMPLETED!")
                    logger.info(f"✅ Method: {status.get('method', 'unknown')}")
                    if 'current_batch' in status:
                        logger.info(f"📦 Final Batch: {status['current_batch']}")
                    if 'count' in status:
                        logger.info(f"📊 Final Count: {status['count']:,}")
                    logger.info(f"⏱️  Total Time: {elapsed}")
                    logger.info("=" * 80)
                    
                    # Execute unified population
                    success = await self.execute_unified_population()
                    
                    if success:
                        # Verify results
                        results = await self.verify_unified_results()
                        
                        logger.info("=" * 80)
                        logger.info("🎉 UNIFIED INSTRUMENT POPULATION COMPLETE!")
                        logger.info("=" * 80)
                        if 'error' not in results:
                            logger.info(f"📊 Total Instruments: {results['unified_instruments']:,}")
                            logger.info(f"✅ Active Instruments: {results['active_instruments']:,}")
                            logger.info(f"🔗 Price Data Records: {results['price_records']:,}")
                            logger.info(f"🔗 Price Integrity: {results['integrity_percentage']:.1f}%")
                        logger.info("=" * 80)
                        
                        logger.info("🏁 All operations completed successfully!")
                        break
                    else:
                        logger.error("❌ Unified population failed. Monitoring stopped.")
                        break
                        
                elif status['status'] == 'running':
                    current_batch = status.get('current_batch', 'Unknown')
                    count = status.get('count', 'Unknown')
                    
                    if isinstance(current_batch, int) and current_batch > 0:
                        progress_pct = (current_batch / self.target_batch) * 100
                        eta_batches = self.target_batch - current_batch
                        eta_time = timedelta(seconds=eta_batches * 21)  # ~21 seconds per batch
                        
                        logger.info(f"⏳ EODHD Running: Batch {current_batch}/{self.target_batch} "
                                   f"({progress_pct:.1f}%) | ETA: {eta_time}")
                    else:
                        logger.info(f"⏳ EODHD Running: {count:,} instruments processed")
                
                elif status['status'] == 'error':
                    logger.warning(f"⚠️ Status check error: {status['error']}")
                
                # Wait before next check
                await asyncio.sleep(self.check_interval)
                
            except KeyboardInterrupt:
                logger.info("🛑 Monitoring stopped by user")
                break
            except Exception as e:
                logger.error(f"❌ Monitoring error: {e}")
                await asyncio.sleep(self.check_interval)

async def main():
    """Main execution"""
    logger.info("🚀 EODHD Completion Monitor Started")
    
    monitor = UnifiedPopulationMonitor()
    await monitor.run_monitoring_loop()
    
    logger.info("🏁 Monitor completed")

if __name__ == "__main__":
    asyncio.run(main())