#!/usr/bin/env python3
"""
Test to reproduce the cache duplication issue causing identical OHLCV values.

This test shows that UnifiedMarketDataManager cache returns the same dataset
for overlapping time range requests, causing training data duplication.
"""

import asyncio
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add src to path
sys.path.append('src')

async def test_cache_duplication():
    """Test that demonstrates cache returning same data for overlapping ranges."""
    print("🧪 Testing Cache Duplication Issue")
    print("=" * 60)
    
    try:
        from core.market_data.unified_manager import UnifiedMarketDataManager, UnifiedMarketDataConfig
        from core.market_data.constants import Timeframe, Vendor
        
        # Create config with cache enabled (default behavior)
        config = UnifiedMarketDataConfig(
            enable_cache=True,  # This is the problem
            file_storage_path="/mnt/d/ats-data"
        )
        
        manager = UnifiedMarketDataManager(config)
        await manager.initialize()
        
        # Test overlapping time range requests that should return different data
        base_time = datetime(2025, 7, 1, 6, 0, 0)
        symbol = "TSLA"
        
        print(f"🔍 Testing overlapping requests for {symbol}")
        
        # Request 1: 6:00-7:00 
        start1 = base_time
        end1 = base_time + timedelta(hours=1)
        print(f"\n📅 Request 1: {start1} to {end1}")
        
        result1 = await manager.get_minute_ohlc_batch(
            symbols=[symbol],
            start_datetime=start1,
            end_datetime=end1,
            timeframe=Timeframe.FIVE_MINUTE,
            vendor=Vendor.FIRSTRATE
        )
        
        # Request 2: 6:30-7:30 (overlapping)
        start2 = base_time + timedelta(minutes=30)
        end2 = base_time + timedelta(hours=1, minutes=30)
        print(f"📅 Request 2: {start2} to {end2}")
        
        result2 = await manager.get_minute_ohlc_batch(
            symbols=[symbol],
            start_datetime=start2,
            end_datetime=end2,
            timeframe=Timeframe.FIVE_MINUTE,
            vendor=Vendor.FIRSTRATE
        )
        
        # Analyze results
        print(f"\n📊 Analysis:")
        
        if symbol in result1 and symbol in result2:
            data1 = result1[symbol]
            data2 = result2[symbol]
            
            if data1 is not None and data2 is not None:
                print(f"   Request 1: {len(data1)} records")
                print(f"   Request 2: {len(data2)} records")
                
                # Check if they're identical (cache hit issue)
                if data1 is data2:  # Same object reference
                    print("❌ CACHE ISSUE: Both requests returned the SAME OBJECT")
                    print("   This causes training data duplication!")
                elif len(data1) > 0 and len(data2) > 0:
                    # Check first record OHLCV values
                    first1 = data1.iloc[0] if hasattr(data1, 'iloc') else data1[0]
                    first2 = data2.iloc[0] if hasattr(data2, 'iloc') else data2[0]
                    
                    print(f"   First record 1: O:{first1.get('open', 'N/A')} H:{first1.get('high', 'N/A')}")
                    print(f"   First record 2: O:{first2.get('open', 'N/A')} H:{first2.get('high', 'N/A')}")
                    
                    if (first1.get('open') == first2.get('open') and 
                        first1.get('high') == first2.get('high')):
                        print("❌ OHLCV VALUES ARE IDENTICAL - Cache duplication confirmed")
                    else:
                        print("✅ OHLCV values are different - No cache issue")
                else:
                    print("⚠️  No data returned")
            else:
                print("⚠️  One or both requests returned None")
        else:
            print(f"⚠️  Symbol {symbol} not found in results")
            
        print(f"\n💡 Solution: Disable cache during training data generation")
        print(f"   config = UnifiedMarketDataConfig(enable_cache=False)")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_cache_duplication())