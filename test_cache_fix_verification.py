#!/usr/bin/env python3
"""
Test to verify that disabling cache fixes the duplicate OHLCV values issue.

This test will:
1. Simulate training data generation with cache enabled (reproduces issue)
2. Simulate training data generation with cache disabled (verifies fix)
3. Compare results to confirm different OHLCV values are generated
"""

import sys
import asyncio
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path
sys.path.append('src')

async def test_cache_fix():
    """Test that disabling cache fixes duplicate OHLCV values."""
    print("🧪 Testing Cache Fix for Duplicate OHLCV Values")
    print("=" * 70)
    
    try:
        from core.market_data.unified_manager import UnifiedMarketDataManager, MarketDataConfig, VendorType, StorageBackend
        from core.market_data.constants import Timeframe, Vendor
        from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
        
        # Test parameters
        symbol = "TSLA"
        base_time = datetime(2025, 7, 1, 6, 30, 0)  # Use different time to avoid existing cache
        intervals = [
            base_time,
            base_time + timedelta(minutes=5),
            base_time + timedelta(minutes=10),
            base_time + timedelta(minutes=15)
        ]
        
        print(f"🔍 Testing with symbol: {symbol}")
        print(f"📅 Time intervals: {[t.strftime('%H:%M') for t in intervals]}")
        
        # Test 1: WITH CACHE (should show duplicates)
        print(f"\n📋 Test 1: WITH CACHE ENABLED")
        print("-" * 40)
        
        config_with_cache = MarketDataConfig(
            vendors=[VendorType.FIRSTRATE],
            storage_backend=StorageBackend.FILE,
            file_storage_path="/data/minute-bars/firstrate",
            enable_cache=True  # CACHE ENABLED - should cause duplicates
        )
        
        manager_with_cache = UnifiedMarketDataManager(config_with_cache)
        await manager_with_cache.initialize()
        
        ohlcv_values_with_cache = []
        for i, interval_time in enumerate(intervals):
            start_time = interval_time
            end_time = interval_time + timedelta(minutes=5)
            
            result = await manager_with_cache.get_minute_ohlc_batch(
                symbols=[symbol],
                start_datetime=start_time,
                end_datetime=end_time,
                timeframe=Timeframe.FIVE_MINUTE,
                vendor=Vendor.FIRSTRATE
            )
            
            if symbol in result and result[symbol] is not None and len(result[symbol]) > 0:
                first_bar = result[symbol].iloc[0] if hasattr(result[symbol], 'iloc') else result[symbol][0]
                ohlc = {
                    'open': first_bar.get('open', 0),
                    'high': first_bar.get('high', 0),
                    'low': first_bar.get('low', 0),
                    'close': first_bar.get('close', 0)
                }
                ohlcv_values_with_cache.append(ohlc)
                print(f"   Interval {i+1} ({interval_time.strftime('%H:%M')}): O:{ohlc['open']:.2f} H:{ohlc['high']:.2f} L:{ohlc['low']:.2f} C:{ohlc['close']:.2f}")
            else:
                print(f"   Interval {i+1} ({interval_time.strftime('%H:%M')}): No data")
                ohlcv_values_with_cache.append(None)
        
        # Test 2: WITHOUT CACHE (should show unique values)
        print(f"\n📋 Test 2: WITH CACHE DISABLED")
        print("-" * 40)
        
        config_without_cache = MarketDataConfig(
            vendors=[VendorType.FIRSTRATE],
            storage_backend=StorageBackend.FILE,
            file_storage_path="/data/minute-bars/firstrate",
            enable_cache=False  # CACHE DISABLED - should fix duplicates
        )
        
        manager_without_cache = UnifiedMarketDataManager(config_without_cache)
        await manager_without_cache.initialize()
        
        ohlcv_values_without_cache = []
        for i, interval_time in enumerate(intervals):
            start_time = interval_time
            end_time = interval_time + timedelta(minutes=5)
            
            result = await manager_without_cache.get_minute_ohlc_batch(
                symbols=[symbol],
                start_datetime=start_time,
                end_datetime=end_time,
                timeframe=Timeframe.FIVE_MINUTE,
                vendor=Vendor.FIRSTRATE
            )
            
            if symbol in result and result[symbol] is not None and len(result[symbol]) > 0:
                first_bar = result[symbol].iloc[0] if hasattr(result[symbol], 'iloc') else result[symbol][0]
                ohlc = {
                    'open': first_bar.get('open', 0),
                    'high': first_bar.get('high', 0),
                    'low': first_bar.get('low', 0),
                    'close': first_bar.get('close', 0)
                }
                ohlcv_values_without_cache.append(ohlc)
                print(f"   Interval {i+1} ({interval_time.strftime('%H:%M')}): O:{ohlc['open']:.2f} H:{ohlc['high']:.2f} L:{ohlc['low']:.2f} C:{ohlc['close']:.2f}")
            else:
                print(f"   Interval {i+1} ({interval_time.strftime('%H:%M')}): No data")
                ohlcv_values_without_cache.append(None)
        
        # Analysis
        print(f"\n📊 ANALYSIS")
        print("=" * 70)
        
        # Check for duplicates in cached version
        valid_cached_values = [v for v in ohlcv_values_with_cache if v is not None]
        valid_uncached_values = [v for v in ohlcv_values_without_cache if v is not None]
        
        if len(valid_cached_values) >= 2:
            cached_duplicates = all(
                v['open'] == valid_cached_values[0]['open'] and
                v['high'] == valid_cached_values[0]['high'] and
                v['low'] == valid_cached_values[0]['low'] and
                v['close'] == valid_cached_values[0]['close']
                for v in valid_cached_values
            )
            print(f"🔍 Cache enabled results:  {'❌ ALL IDENTICAL (cache issue)' if cached_duplicates else '✅ Values vary'}")
        else:
            print(f"🔍 Cache enabled results:  ⚠️  Insufficient data ({len(valid_cached_values)} records)")
        
        if len(valid_uncached_values) >= 2:
            uncached_duplicates = all(
                v['open'] == valid_uncached_values[0]['open'] and
                v['high'] == valid_uncached_values[0]['high'] and
                v['low'] == valid_uncached_values[0]['low'] and
                v['close'] == valid_uncached_values[0]['close']
                for v in valid_uncached_values
            )
            print(f"🔍 Cache disabled results: {'❌ Still identical (fix failed)' if uncached_duplicates else '✅ Values vary (fix works!)'}")
        else:
            print(f"🔍 Cache disabled results: ⚠️  Insufficient data ({len(valid_uncached_values)} records)")
        
        # Conclusion
        print(f"\n🎯 CONCLUSION")
        print("-" * 40)
        if len(valid_cached_values) >= 2 and len(valid_uncached_values) >= 2:
            if cached_duplicates and not uncached_duplicates:
                print("✅ FIX VERIFIED: Disabling cache resolves duplicate OHLCV issue")
                return True
            elif not cached_duplicates and not uncached_duplicates:
                print("ℹ️  Both configurations show varying values (cache may not be the issue)")
                return False
            elif cached_duplicates and uncached_duplicates:
                print("❌ FIX FAILED: Both configurations still show duplicates")
                return False
            else:
                print("⚠️  Unexpected result pattern")
                return False
        else:
            print("⚠️  Insufficient data to verify fix")
            return False
            
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(test_cache_fix())
    print(f"\n{'🟢 TEST PASSED' if success else '🔴 TEST FAILED'}")
    sys.exit(0 if success else 1)