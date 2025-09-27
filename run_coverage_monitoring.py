#!/usr/bin/env python3
"""
Run Coverage Monitoring System
Test the new monitoring and gap detection system
"""

import asyncio
import sys
import os
from datetime import datetime

# Add src to path
sys.path.insert(0, 'src')

from infrastructure.monitoring.legacy.coverage_monitor import CoverageMonitor

async def run_monitoring_test():
    """Test the coverage monitoring system."""
    
    # Database configuration
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 4432)),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', 'intg_password'),
        'database': os.getenv('DB_NAME', 'intg_db')
    }
    
    print("🚀 COVERAGE MONITORING SYSTEM TEST")
    print("="*60)
    print(f"📊 Database: {db_config['host']}:{db_config['port']}/{db_config['database']}")
    print(f"⏰ Started: {datetime.now()}")
    print()
    
    monitor = CoverageMonitor(db_config)
    
    # Initialize
    await monitor.initialize()
    
    # Run daily monitoring for FirstRate
    print("🔍 Running daily monitoring for FirstRate...")
    await monitor.run_daily_monitoring(vendors=['firstrate'], lookback_days=30)
    
    # Query and display results
    async with monitor.db_pool.acquire() as conn:
        # Show coverage summary
        print("\n📊 COVERAGE SUMMARY:")
        coverage_summary = await conn.fetch("""
            SELECT vendor, data_type, total_symbols, symbols_complete, symbols_missing, coverage_percentage
            FROM v_current_coverage_summary
            ORDER BY vendor, data_type
        """)
        
        for row in coverage_summary:
            print(f"  {row['vendor']:10} {row['data_type']:12} | "
                 f"{row['symbols_complete']:,}/{row['total_symbols']:,} symbols | "
                 f"{row['coverage_percentage']:.1f}% coverage")
        
        # Show top priority gaps
        print("\n🚨 TOP PRIORITY GAPS:")
        priority_gaps = await conn.fetch("""
            SELECT vendor, data_type, symbol, gap_start_date, gap_end_date, 
                   gap_days, priority_score, adjusted_priority
            FROM v_active_backfill_queue
            LIMIT 10
        """)
        
        if priority_gaps:
            for gap in priority_gaps:
                print(f"  🔴 {gap['symbol']:6} | {gap['gap_start_date']} to {gap['gap_end_date']} | "
                     f"{gap['gap_days']:2} days | Priority: {gap['adjusted_priority']:.1f}")
        else:
            print("  ✅ No high-priority gaps found")
        
        # Show daily metrics trend
        print("\n📈 RECENT COVERAGE TREND:")
        trend_data = await conn.fetch("""
            SELECT metric_date, vendor, data_type, coverage_percentage, 
                   coverage_change
            FROM v_coverage_trending
            WHERE metric_date >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY vendor, data_type, metric_date DESC
            LIMIT 10
        """)
        
        for row in trend_data:
            change_str = ""
            if row['coverage_change'] is not None:
                if row['coverage_change'] > 0:
                    change_str = f" (+{row['coverage_change']:.1f}%)"
                elif row['coverage_change'] < 0:
                    change_str = f" ({row['coverage_change']:.1f}%)"
            
            print(f"  {row['metric_date']} {row['vendor']:10} {row['data_type']:12} | "
                 f"{row['coverage_percentage']:.1f}%{change_str}")
        
        # Show recent backfill operations
        print("\n🔧 RECENT BACKFILL OPERATIONS:")
        recent_ops = await conn.fetch("""
            SELECT operation_type, vendor, data_type, 
                   array_length(symbols_requested, 1) as symbols_count,
                   status, duration_seconds, created_at
            FROM dev_backfill_operations
            WHERE created_at >= NOW() - INTERVAL '7 days'
            ORDER BY created_at DESC
            LIMIT 5
        """)
        
        if recent_ops:
            for op in recent_ops:
                duration_str = f"{op['duration_seconds']}s" if op['duration_seconds'] else "N/A"
                print(f"  {op['created_at'].strftime('%m-%d %H:%M')} | {op['operation_type']:10} | "
                     f"{op['vendor']:10} {op['data_type']:12} | "
                     f"{op['symbols_count']} symbols | {op['status']:10} | {duration_str}")
        else:
            print("  ℹ️  No recent backfill operations")
    
    print("\n" + "="*60)
    print("✅ MONITORING TEST COMPLETE")
    print("="*60)
    
if __name__ == "__main__":
    asyncio.run(run_monitoring_test())