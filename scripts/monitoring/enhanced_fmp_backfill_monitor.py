#!/usr/bin/env python3
"""
Enhanced FMP Backfill Progress Monitor

Monitor the progress of the enhanced FMP backfill job with exponential backoff.
Provides real-time statistics on:
- Progress percentage
- Records inserted
- Retry statistics
- Circuit breaker status
- ETA calculation
"""

import asyncio
import asyncpg
import os
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

class FMPBackfillMonitor:
    """Monitor FMP backfill progress"""
    
    def __init__(self):
        self.db_url = f"postgresql://postgres:{os.getenv('DB_PASSWORD', 'dev_password')}@{os.getenv('DB_HOST', 'postgres-simple')}:5432/dev_db"
        self.checkpoint_file = "/tmp/fmp_backfill_checkpoint.json"
        
    async def get_fmp_coverage_stats(self) -> dict:
        """Get current FMP data coverage statistics"""
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=2)
        
        try:
            async with pool.acquire() as conn:
                # Total instruments
                total_instruments = await conn.fetchval(
                    "SELECT COUNT(DISTINCT symbol) FROM dev_instruments WHERE symbol IS NOT NULL"
                )
                
                # FMP data coverage
                fmp_coverage = await conn.fetch("""
                    SELECT 
                        COUNT(DISTINCT dp.instrument_id) as instruments_with_data,
                        COUNT(*) as total_records,
                        MIN(dp.date) as earliest_date,
                        MAX(dp.date) as latest_date,
                        COUNT(CASE WHEN dp.created_at >= CURRENT_DATE THEN 1 END) as records_today
                    FROM dev_daily_prices_fmp dp
                    JOIN dev_instruments i ON dp.instrument_id = i.id
                """)
                
                stats = fmp_coverage[0]
                
                return {
                    'total_instruments': total_instruments,
                    'instruments_with_data': stats['instruments_with_data'],
                    'coverage_percentage': (stats['instruments_with_data'] / total_instruments * 100) if total_instruments > 0 else 0,
                    'total_records': stats['total_records'],
                    'records_today': stats['records_today'],
                    'earliest_date': stats['earliest_date'],
                    'latest_date': stats['latest_date'],
                    'date_range_years': (stats['latest_date'] - stats['earliest_date']).days / 365.25 if stats['latest_date'] and stats['earliest_date'] else 0
                }
        
        finally:
            await pool.close()
    
    def load_checkpoint_progress(self) -> dict:
        """Load progress from checkpoint file"""
        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                
                progress = data.get('progress', {})
                circuit_breaker = data.get('circuit_breaker_state', {})
                
                return {
                    'total_symbols': progress.get('total_symbols', 0),
                    'completed_symbols': progress.get('completed_symbols', 0),
                    'failed_symbols': len(progress.get('failed_symbols', [])),
                    'skipped_symbols': len(progress.get('skipped_symbols', [])),
                    'total_records_inserted': progress.get('total_records_inserted', 0),
                    'retry_stats': progress.get('retry_stats', {}),
                    'circuit_breaker_trips': progress.get('circuit_breaker_trips', 0),
                    'circuit_breaker_state': circuit_breaker.get('state', 'CLOSED'),
                    'daily_calls_made': data.get('daily_calls_made', 0),
                    'start_time': progress.get('start_time'),
                    'last_checkpoint': progress.get('last_checkpoint')
                }
        except Exception as e:
            print(f"Error loading checkpoint: {e}")
        
        return {}
    
    def calculate_eta(self, progress_data: dict) -> str:
        """Calculate estimated completion time"""
        if not progress_data.get('start_time') or progress_data.get('completed_symbols', 0) == 0:
            return "Unknown"
        
        try:
            start_time = datetime.fromisoformat(progress_data['start_time'])
            elapsed = datetime.now() - start_time
            
            completed = progress_data['completed_symbols']
            total = progress_data['total_symbols']
            remaining = total - completed
            
            if completed == 0:
                return "Unknown"
            
            rate = completed / elapsed.total_seconds()  # symbols per second
            estimated_seconds = remaining / rate
            
            eta = datetime.now() + timedelta(seconds=estimated_seconds)
            return eta.strftime("%Y-%m-%d %H:%M:%S")
        
        except Exception:
            return "Unknown"
    
    async def print_status_report(self):
        """Print comprehensive status report"""
        print("=" * 80)
        print("🚀 ENHANCED FMP BACKFILL PROGRESS MONITOR")
        print("=" * 80)
        
        # Database coverage stats
        print("\n📊 DATABASE COVERAGE STATISTICS")
        print("-" * 40)
        
        try:
            coverage_stats = await self.get_fmp_coverage_stats()
            print(f"Total Instruments:      {coverage_stats['total_instruments']:,}")
            print(f"Instruments with Data:  {coverage_stats['instruments_with_data']:,}")
            print(f"Coverage Percentage:    {coverage_stats['coverage_percentage']:.2f}%")
            print(f"Total FMP Records:      {coverage_stats['total_records']:,}")
            print(f"Records Added Today:    {coverage_stats['records_today']:,}")
            print(f"Date Range:             {coverage_stats['earliest_date']} to {coverage_stats['latest_date']}")
            print(f"Years of Data:          {coverage_stats['date_range_years']:.1f} years")
        
        except Exception as e:
            print(f"❌ Error getting coverage stats: {e}")
        
        # Checkpoint progress
        print("\n🔄 BACKFILL JOB PROGRESS")
        print("-" * 40)
        
        progress_data = self.load_checkpoint_progress()
        
        if progress_data:
            completed = progress_data['completed_symbols']
            total = progress_data['total_symbols']
            percentage = (completed / total * 100) if total > 0 else 0
            
            print(f"Progress:               {completed:,}/{total:,} symbols ({percentage:.2f}%)")
            print(f"Records Inserted:       {progress_data['total_records_inserted']:,}")
            print(f"Failed Symbols:         {progress_data['failed_symbols']:,}")
            print(f"Skipped Symbols:        {progress_data['skipped_symbols']:,}")
            print(f"Daily API Calls:        {progress_data['daily_calls_made']:,}")
            
            # Progress bar
            bar_width = 50
            filled = int(bar_width * percentage / 100)
            bar = "█" * filled + "░" * (bar_width - filled)
            print(f"Progress Bar:           [{bar}] {percentage:.1f}%")
            
            # ETA calculation
            eta = self.calculate_eta(progress_data)
            print(f"Estimated Completion:   {eta}")
            
            # Retry statistics
            retry_stats = progress_data.get('retry_stats', {})
            if retry_stats:
                print(f"\n🔁 RETRY STATISTICS")
                print("-" * 40)
                print(f"Total Retries:          {retry_stats.get('total_retries', 0):,}")
                print(f"HTTP 403 Retries:       {retry_stats.get('http_403_retries', 0):,}")
                print(f"HTTP 429 Retries:       {retry_stats.get('http_429_retries', 0):,}")
                print(f"HTTP 5xx Retries:       {retry_stats.get('http_5xx_retries', 0):,}")
                print(f"Successful Retries:     {retry_stats.get('successful_retries', 0):,}")
            
            # Circuit breaker status
            print(f"\n⚡ CIRCUIT BREAKER STATUS")
            print("-" * 40)
            print(f"Current State:          {progress_data['circuit_breaker_state']}")
            print(f"Circuit Trips:          {progress_data['circuit_breaker_trips']:,}")
            
            # Timestamps
            if progress_data.get('start_time'):
                start_time = datetime.fromisoformat(progress_data['start_time'])
                elapsed = datetime.now() - start_time
                print(f"\n⏱️  TIMING INFORMATION")
                print("-" * 40)
                print(f"Started:                {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"Running Time:           {elapsed}")
                
                if progress_data.get('last_checkpoint'):
                    last_checkpoint = datetime.fromisoformat(progress_data['last_checkpoint'])
                    print(f"Last Checkpoint:        {last_checkpoint.strftime('%Y-%m-%d %H:%M:%S')}")
        
        else:
            print("📂 No checkpoint file found - job may not have started yet")
        
        print("\n" + "=" * 80)

async def main():
    """Main execution"""
    parser = argparse.ArgumentParser(description='Enhanced FMP Backfill Progress Monitor')
    parser.add_argument('--watch', action='store_true', help='Continuously monitor progress')
    parser.add_argument('--interval', type=int, default=60, help='Update interval in seconds (default: 60)')
    
    args = parser.parse_args()
    
    monitor = FMPBackfillMonitor()
    
    if args.watch:
        print("🔍 Starting continuous monitoring (Ctrl+C to stop)...")
        try:
            while True:
                await monitor.print_status_report()
                print(f"\n⏳ Next update in {args.interval} seconds...\n")
                await asyncio.sleep(args.interval)
        except KeyboardInterrupt:
            print("\n👋 Monitoring stopped by user")
    else:
        await monitor.print_status_report()

if __name__ == "__main__":
    asyncio.run(main())