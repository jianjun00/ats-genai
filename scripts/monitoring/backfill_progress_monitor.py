#!/usr/bin/env python3
"""
Comprehensive Backfill Progress Monitor

Real-time monitoring of Phase 1 (10k minute data) and Phase 3 (historical daily) 
backfill jobs with ETA calculations and performance metrics.
"""

import asyncio
import asyncpg
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("backfill_progress_monitor")

class BackfillProgressMonitor:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', 5432)),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'dev_password'),
            'database': os.getenv('DB_NAME', 'dev_db')
        }
        
        # Checkpoint file paths (if accessible)
        self.checkpoint_files = {
            'phase1': '/app/checkpoints/phase1_10k_checkpoint.json',
            'phase3': '/app/checkpoints/phase3_historical_checkpoint.json'
        }
        
        # Track progress metrics
        self.baseline_metrics = None
        self.last_measurement = None
    
    async def get_database_connection(self):
        return await asyncpg.connect(**self.db_config)
    
    async def get_current_database_metrics(self):
        """Get current database metrics for progress tracking."""
        try:
            conn = await self.get_database_connection()
            try:
                # Minute data metrics
                minute_data_metrics = await conn.fetchrow("""
                    SELECT 
                        COUNT(DISTINCT instrument_id) as unique_instruments,
                        COUNT(*) as total_minute_records,
                        MIN(timestamp) as earliest_minute,
                        MAX(timestamp) as latest_minute,
                        COUNT(CASE WHEN vendor = 'polygon' THEN 1 END) as polygon_records,
                        COUNT(CASE WHEN vendor = 'tiingo' THEN 1 END) as tiingo_records
                    FROM dev_minute_data
                """)
                
                # Daily data metrics  
                daily_metrics_tiingo = await conn.fetchrow("""
                    SELECT 
                        COUNT(DISTINCT instrument_id) as unique_instruments,
                        COUNT(*) as total_daily_records,
                        MIN(date) as earliest_date,
                        MAX(date) as latest_date
                    FROM dev_daily_prices_tiingo
                """)
                
                daily_metrics_polygon = await conn.fetchrow("""
                    SELECT 
                        COUNT(DISTINCT instrument_id) as unique_instruments,
                        COUNT(*) as total_daily_records,
                        MIN(date) as earliest_date,
                        MAX(date) as latest_date
                    FROM dev_daily_prices_polygon
                """)
                
                # Total instruments
                total_instruments = await conn.fetchval("""
                    SELECT COUNT(*) FROM dev_instruments WHERE is_active = true
                """)
                
                return {
                    'timestamp': datetime.now(),
                    'minute_data': dict(minute_data_metrics),
                    'daily_tiingo': dict(daily_metrics_tiingo),
                    'daily_polygon': dict(daily_metrics_polygon),
                    'total_instruments': total_instruments
                }
                
            finally:
                await conn.close()
        
        except Exception as e:
            logger.error(f"Failed to get database metrics: {e}")
            return None
    
    def calculate_progress_rates(self, current_metrics, baseline_metrics):
        """Calculate progress rates and ETAs."""
        if not baseline_metrics:
            return None
        
        time_diff = (current_metrics['timestamp'] - baseline_metrics['timestamp']).total_seconds()
        if time_diff <= 0:
            return None
        
        # Minute data progress
        minute_records_added = current_metrics['minute_data']['total_minute_records'] - baseline_metrics['minute_data']['total_minute_records']
        minute_rate_per_second = minute_records_added / time_diff
        minute_rate_per_hour = minute_rate_per_second * 3600
        
        # Daily data progress
        daily_tiingo_added = current_metrics['daily_tiingo']['total_daily_records'] - baseline_metrics['daily_tiingo']['total_daily_records']
        daily_polygon_added = current_metrics['daily_polygon']['total_daily_records'] - baseline_metrics['daily_polygon']['total_daily_records']
        
        # Phase 1 ETA calculation (targeting 5B records)
        target_minute_records = 5_000_000_000
        remaining_minute_records = target_minute_records - current_metrics['minute_data']['total_minute_records']
        phase1_eta_seconds = remaining_minute_records / minute_rate_per_second if minute_rate_per_second > 0 else None
        
        # Phase 3 ETA calculation (targeting ~60M daily records)
        target_daily_records = 60_000_000
        current_total_daily = current_metrics['daily_tiingo']['total_daily_records'] + current_metrics['daily_polygon']['total_daily_records']
        remaining_daily_records = target_daily_records - current_total_daily
        daily_rate_per_second = (daily_tiingo_added + daily_polygon_added) / time_diff
        phase3_eta_seconds = remaining_daily_records / daily_rate_per_second if daily_rate_per_second > 0 else None
        
        return {
            'minute_data': {
                'records_added': minute_records_added,
                'rate_per_hour': minute_rate_per_hour,
                'current_total': current_metrics['minute_data']['total_minute_records'],
                'target_total': target_minute_records,
                'completion_percentage': (current_metrics['minute_data']['total_minute_records'] / target_minute_records) * 100,
                'eta_seconds': phase1_eta_seconds,
                'eta_human': self._seconds_to_human(phase1_eta_seconds) if phase1_eta_seconds else 'Unknown'
            },
            'daily_data': {
                'tiingo_added': daily_tiingo_added,
                'polygon_added': daily_polygon_added,
                'current_total': current_total_daily,
                'target_total': target_daily_records,
                'completion_percentage': (current_total_daily / target_daily_records) * 100,
                'eta_seconds': phase3_eta_seconds,
                'eta_human': self._seconds_to_human(phase3_eta_seconds) if phase3_eta_seconds else 'Unknown'
            }
        }
    
    def _seconds_to_human(self, seconds):
        """Convert seconds to human-readable format."""
        if seconds is None or seconds <= 0:
            return "Unknown"
        
        days = int(seconds // 86400)
        hours = int((seconds % 86400) // 3600)
        minutes = int((seconds % 3600) // 60)
        
        if days > 0:
            return f"{days}d {hours}h {minutes}m"
        elif hours > 0:
            return f"{hours}h {minutes}m"
        else:
            return f"{minutes}m"
    
    async def generate_progress_report(self):
        """Generate comprehensive progress report."""
        current_metrics = await self.get_current_database_metrics()
        if not current_metrics:
            logger.error("Failed to get current metrics")
            return
        
        logger.info("🎯 COMPREHENSIVE BACKFILL PROGRESS REPORT")
        logger.info("=" * 80)
        
        # Current status
        logger.info(f"📊 CURRENT DATABASE STATUS (as of {current_metrics['timestamp'].strftime('%Y-%m-%d %H:%M:%S')})")
        logger.info(f"   Total active instruments: {current_metrics['total_instruments']:,}")
        logger.info(f"   Minute data records: {current_metrics['minute_data']['total_minute_records']:,}")
        logger.info(f"   Minute data instruments: {current_metrics['minute_data']['unique_instruments']:,}")
        logger.info(f"   Polygon minute records: {current_metrics['minute_data']['polygon_records']:,}")
        logger.info(f"   Tiingo minute records: {current_metrics['minute_data']['tiingo_records']:,}")
        logger.info(f"")
        logger.info(f"   Daily data (Tiingo): {current_metrics['daily_tiingo']['total_daily_records']:,} records")
        logger.info(f"   Daily data (Polygon): {current_metrics['daily_polygon']['total_daily_records']:,} records")
        logger.info(f"   Daily instruments (Tiingo): {current_metrics['daily_tiingo']['unique_instruments']:,}")
        logger.info(f"   Daily instruments (Polygon): {current_metrics['daily_polygon']['unique_instruments']:,}")
        
        # Progress rates if we have baseline
        if self.baseline_metrics:
            progress_rates = self.calculate_progress_rates(current_metrics, self.baseline_metrics)
            if progress_rates:
                logger.info(f"")
                logger.info(f"🚀 PHASE 1 PROGRESS (10K MINUTE DATA 2020-2025):")
                minute_progress = progress_rates['minute_data']
                logger.info(f"   Records added since last check: {minute_progress['records_added']:,}")
                logger.info(f"   Current rate: {minute_progress['rate_per_hour']:,.0f} records/hour")
                logger.info(f"   Progress: {minute_progress['current_total']:,} / {minute_progress['target_total']:,} ({minute_progress['completion_percentage']:.3f}%)")
                logger.info(f"   Estimated completion: {minute_progress['eta_human']}")
                
                logger.info(f"")
                logger.info(f"🏛️ PHASE 3 PROGRESS (HISTORICAL DAILY 1995-2018):")
                daily_progress = progress_rates['daily_data']
                logger.info(f"   Tiingo records added: {daily_progress['tiingo_added']:,}")
                logger.info(f"   Polygon records added: {daily_progress['polygon_added']:,}")
                logger.info(f"   Progress: {daily_progress['current_total']:,} / {daily_progress['target_total']:,} ({daily_progress['completion_percentage']:.3f}%)")
                logger.info(f"   Estimated completion: {daily_progress['eta_human']}")
        
        # Coverage analysis
        logger.info(f"")
        logger.info(f"📈 COVERAGE ANALYSIS:")
        minute_instrument_coverage = (current_metrics['minute_data']['unique_instruments'] / current_metrics['total_instruments']) * 100
        daily_tiingo_coverage = (current_metrics['daily_tiingo']['unique_instruments'] / current_metrics['total_instruments']) * 100
        daily_polygon_coverage = (current_metrics['daily_polygon']['unique_instruments'] / current_metrics['total_instruments']) * 100
        
        logger.info(f"   Minute data instrument coverage: {minute_instrument_coverage:.1f}%")
        logger.info(f"   Daily data (Tiingo) coverage: {daily_tiingo_coverage:.1f}%")
        logger.info(f"   Daily data (Polygon) coverage: {daily_polygon_coverage:.1f}%")
        
        # Update baseline for next measurement
        self.baseline_metrics = current_metrics
        logger.info("=" * 80)
    
    async def monitor_continuous(self, interval_minutes=5):
        """Continuously monitor backfill progress."""
        logger.info(f"🔄 Starting continuous monitoring (updates every {interval_minutes} minutes)")
        
        # Get initial baseline
        self.baseline_metrics = await self.get_current_database_metrics()
        
        while True:
            await self.generate_progress_report()
            logger.info(f"😴 Sleeping for {interval_minutes} minutes...")
            logger.info("")
            await asyncio.sleep(interval_minutes * 60)

async def main():
    """Main monitoring function."""
    import sys
    
    monitor = BackfillProgressMonitor()
    
    if len(sys.argv) > 1 and sys.argv[1] == "continuous":
        # Continuous monitoring mode
        interval = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        await monitor.monitor_continuous(interval)
    else:
        # Single report mode
        await monitor.generate_progress_report()

if __name__ == "__main__":
    asyncio.run(main())