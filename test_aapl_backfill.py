#!/usr/bin/env python3
"""
Test AAPL FirstRate backfill
"""

import asyncio
import sys
import os
from datetime import datetime

# Add src directory to Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

async def main():
    """Test AAPL backfill."""
    
    print("🚀 FirstRate AAPL Backfill Test")
    print("=" * 40)
    
    try:
        from market_data.agent.firstrate_minute_adapter import FirstRateMinuteAdapter
        
        # Initialize adapter
        data_path = "/mnt/d/ats-data/firstrate-data/stock"
        
        async with FirstRateMinuteAdapter(data_path) as adapter:
            print("📊 Testing AAPL data parsing...")
            
            # Test data fetching for AAPL with recent date filter
            start_date = datetime(2020, 1, 1)
            bars_count = 0
            sample_bars = []
            
            async for bar in adapter.fetch_minute_bars_async(
                ['AAPL'], 
                start_date=start_date,
                max_bars_per_symbol=100  # Limit for test
            ):
                bars_count += 1
                if len(sample_bars) < 5:
                    sample_bars.append(bar)
            
            print(f"✅ Found {bars_count} AAPL bars since 2020")
            
            if sample_bars:
                print("\n📈 Sample bars:")
                for i, bar in enumerate(sample_bars):
                    print(f"  {i+1}. {bar.timestamp} | O:{bar.open} H:{bar.high} L:{bar.low} C:{bar.close} | V:{bar.volume:,}")
                
                # Show date range
                print(f"\n📅 First bar: {sample_bars[0].timestamp}")
                print(f"📅 Quality scores: {[float(bar.quality_score) for bar in sample_bars[:3]]}")
            
            # Get parsing stats
            stats = adapter.get_parsing_stats()
            print(f"\n📊 Parsing stats:")
            print(f"  Files processed: {stats.total_files_processed}")
            print(f"  Symbols processed: {stats.total_symbols_processed}")
            print(f"  Total bars parsed: {stats.total_bars_parsed:,}")
            print(f"  Processing time: {stats.processing_time_seconds:.2f}s")
            
            if bars_count > 0:
                print(f"\n✅ SUCCESS: AAPL data ready for backfill ({bars_count:,} bars)")
                print("Next: Run actual database insert")
            else:
                print("\n❌ No AAPL data found - check FirstRate files")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(main())