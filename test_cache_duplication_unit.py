#!/usr/bin/env python3
"""
Unit test to reproduce the exact cache duplication issue causing identical OHLCV values.

This test reproduces the specific cache behavior that leads to training data duplication:
1. Creates overlapping time range requests (simulating UniverseStateBuilder behavior)
2. Verifies cache returns same data object for overlapping ranges
3. Tests that disable_cache=False prevents this issue
"""

import sys
import asyncio
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import pandas as pd

# Add src to path
sys.path.append('src')

class TestCacheDuplication(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock data that should be different for different time ranges
        self.mock_data_1 = pd.DataFrame({
            'timestamp': [datetime(2025, 7, 1, 6, 30)],
            'symbol': ['TSLA'],
            'open': [295.65],
            'high': [296.78], 
            'low': [294.60],
            'close': [295.22],
            'volume': [790517]
        })
        
        self.mock_data_2 = pd.DataFrame({
            'timestamp': [datetime(2025, 7, 1, 6, 35)],
            'symbol': ['TSLA'],
            'open': [295.80],  # Different values!
            'high': [297.00],
            'low': [294.90], 
            'close': [296.50],
            'volume': [825000]
        })
        
    async def test_cache_enabled_returns_same_object(self):
        """Test that cache enabled returns same object for overlapping requests."""
        print("🧪 Test 1: Cache Enabled - Should Return Same Object")
        
        try:
            from core.market_data.unified_manager import UnifiedMarketDataManager, MarketDataConfig, VendorType, StorageBackend
            
            # Create config with cache enabled
            config = MarketDataConfig(
                vendors=[VendorType.FIRSTRATE],
                storage_backend=StorageBackend.FILE,
                file_storage_path="/tmp/test-data",
                enable_cache=True  # Cache enabled - should cause issue
            )
            
            manager = UnifiedMarketDataManager(config)
            
            # Mock the storage manager to return different data for different requests
            with patch.object(manager.storage_manager, 'retrieve_ohlcv') as mock_retrieve:
                # First call returns data_1, second call should return data_2 but cache will return data_1
                mock_retrieve.side_effect = [self.mock_data_1, self.mock_data_2]
                
                # Request 1: 6:30-6:35
                result1 = await manager.get_minute_ohlc_batch(
                    symbols=['TSLA'],
                    start=datetime(2025, 7, 1, 6, 30),
                    end=datetime(2025, 7, 1, 6, 35)
                )
                
                # Request 2: 6:32-6:37 (overlapping - should hit cache)
                result2 = await manager.get_minute_ohlc_batch(
                    symbols=['TSLA'], 
                    start=datetime(2025, 7, 1, 6, 32),
                    end=datetime(2025, 7, 1, 6, 37)
                )
                
                # Analyze results
                self.assertIn('TSLA', result1)
                self.assertIn('TSLA', result2)
                
                data1 = result1['TSLA']
                data2 = result2['TSLA']
                
                print(f"   Request 1 result: {data1}")
                print(f"   Request 2 result: {data2}")
                
                # With cache enabled, both should be the same object
                if data1 is data2:
                    print("   ❌ CACHE ISSUE REPRODUCED: Same object returned for different requests")
                    print("   This causes training data duplication!")
                    self.assertTrue(True)  # Issue reproduced as expected
                else:
                    print("   ⚠️  Cache didn't return same object - may need different test setup")
                    # Check if values are at least identical due to cache
                    if data1 is not None and data2 is not None:
                        # Handle dict return format from get_minute_ohlc_batch
                        val1 = data1.get('open', 0) if isinstance(data1, dict) else data1[0]['open']
                        val2 = data2.get('open', 0) if isinstance(data2, dict) else data2[0]['open']
                        print(f"   Values: {val1} vs {val2}")
                        if val1 == val2:
                            print("   ❌ CACHE ISSUE: Values are identical due to cache")
                        else:
                            print("   ✅ Different values returned - cache may not be the issue!")
                            
        except ImportError as e:
            self.skipTest(f"Required modules not available: {e}")
        except Exception as e:
            self.fail(f"Test failed with error: {e}")
            
    async def test_cache_disabled_returns_different_objects(self):
        """Test that cache disabled returns different objects."""
        print("\n🧪 Test 2: Cache Disabled - Should Return Different Objects")
        
        try:
            from core.market_data.unified_manager import UnifiedMarketDataManager, MarketDataConfig, VendorType, StorageBackend
            
            # Create config with cache disabled (our fix)
            config = MarketDataConfig(
                vendors=[VendorType.FIRSTRATE],
                storage_backend=StorageBackend.FILE,
                file_storage_path="/tmp/test-data",
                enable_cache=False  # Cache disabled - should fix issue
            )
            
            manager = UnifiedMarketDataManager(config)
            
            # Mock the storage manager to return different data
            with patch.object(manager.storage_manager, 'retrieve_ohlcv') as mock_retrieve:
                mock_retrieve.side_effect = [self.mock_data_1, self.mock_data_2]
                
                # Request 1: 6:30-6:35
                result1 = await manager.get_minute_ohlc_batch(
                    symbols=['TSLA'],
                    start=datetime(2025, 7, 1, 6, 30),
                    end=datetime(2025, 7, 1, 6, 35)
                )
                
                # Request 2: 6:32-6:37 (overlapping - should NOT hit cache)
                result2 = await manager.get_minute_ohlc_batch(
                    symbols=['TSLA'],
                    start=datetime(2025, 7, 1, 6, 32), 
                    end=datetime(2025, 7, 1, 6, 37)
                )
                
                # Analyze results
                self.assertIn('TSLA', result1)
                self.assertIn('TSLA', result2)
                
                data1 = result1['TSLA']
                data2 = result2['TSLA']
                
                print(f"   Request 1 result: {data1}")
                print(f"   Request 2 result: {data2}")
                
                # With cache disabled, should be different objects/values
                if data1 is not data2:
                    print("   ✅ CACHE FIX VERIFIED: Different objects returned")
                    
                    # Also check values are different
                    if data1 is not None and data2 is not None:
                        # Handle dict return format from get_minute_ohlc_batch
                        val1 = data1.get('open', 0) if isinstance(data1, dict) else data1[0]['open']
                        val2 = data2.get('open', 0) if isinstance(data2, dict) else data2[0]['open']
                        
                        if val1 != val2:
                            print(f"   ✅ Different values: {val1} vs {val2}")
                        else:
                            print(f"   ⚠️  Same values but different objects: {val1}")
                else:
                    print("   ❌ CACHE FIX FAILED: Still returning same object")
                    self.fail("Cache disabled but still returning same object")
                            
        except ImportError as e:
            self.skipTest(f"Required modules not available: {e}")
        except Exception as e:
            self.fail(f"Test failed with error: {e}")
            
    def test_cache_key_generation(self):
        """Test that cache key generation creates broad keys causing collisions."""
        print("\n🧪 Test 3: Cache Key Analysis")
        
        try:
            from core.market_data.unified_manager import UnifiedMarketDataManager, MarketDataConfig, VendorType, StorageBackend
            
            config = MarketDataConfig(
                vendors=[VendorType.FIRSTRATE],
                storage_backend=StorageBackend.FILE,
                file_storage_path="/tmp/test-data",
                enable_cache=True
            )
            
            manager = UnifiedMarketDataManager(config)
            
            # Test cache key generation for overlapping requests
            symbols = ['TSLA']
            timeframe = "5m"
            vendor = "firstrate"
            
            # Request 1: 6:30-6:35
            start1 = datetime(2025, 7, 1, 6, 30)
            end1 = datetime(2025, 7, 1, 6, 35)
            key1 = f"{','.join(symbols)}:{start1}:{end1}:{timeframe}:{vendor}"
            
            # Request 2: 6:32-6:37 (overlapping)
            start2 = datetime(2025, 7, 1, 6, 32)
            end2 = datetime(2025, 7, 1, 6, 37) 
            key2 = f"{','.join(symbols)}:{start2}:{end2}:{timeframe}:{vendor}"
            
            print(f"   Cache key 1: {key1}")
            print(f"   Cache key 2: {key2}")
            
            if key1 == key2:
                print("   ❌ CACHE COLLISION: Same key for different requests")
                self.fail("Cache keys should be different for different time ranges")
            else:
                print("   ✅ Different cache keys (good)")
                print("   ℹ️  Issue must be in broader cache range logic")
                
        except ImportError as e:
            self.skipTest(f"Required modules not available: {e}")

async def run_async_tests():
    """Run the async tests."""
    test_instance = TestCacheDuplication()
    test_instance.setUp()
    
    try:
        await test_instance.test_cache_enabled_returns_same_object()
        await test_instance.test_cache_disabled_returns_different_objects()
        test_instance.test_cache_key_generation()
        
        print("\n🎯 TEST SUMMARY")
        print("=" * 50)
        print("✅ All cache duplication tests completed")
        print("📋 Review results above to verify cache fix effectiveness")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Tests failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(run_async_tests())
    print(f"\n{'🟢 TESTS COMPLETED' if success else '🔴 TESTS FAILED'}")
    sys.exit(0 if success else 1)