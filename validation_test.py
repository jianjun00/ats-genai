#!/usr/bin/env python3
"""
Quick validation test for service architecture components
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

def test_imports():
    """Test that all key components can be imported."""
    print("🔍 Testing service architecture component imports...")

    try:
        # Test caching components
        from src.infrastructure.caching import MemoryCache, CacheConfig
        print("✅ Caching components imported successfully")

        # Test optimization components
        from src.infrastructure.optimization import PerformanceProfiler
        print("✅ Optimization components imported successfully")

        # Test migration components
        from src.infrastructure.migration import MigrationOrchestrator
        print("✅ Migration components imported successfully")

        # Test service discovery components
        from src.infrastructure.service_discovery import ServiceRegistry
        print("✅ Service discovery components imported successfully")

        return True

    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False

def test_caching_functionality():
    """Test basic caching functionality."""
    print("\n🚀 Testing caching functionality...")

    try:
        import asyncio
        from src.infrastructure.caching import MemoryCache, CacheConfig, EvictionPolicy

        async def test_cache():
            config = CacheConfig(
                ttl_seconds=300,
                max_size=100,
                eviction_policy=EvictionPolicy.LRU,
                namespace="test"
            )

            cache = MemoryCache(config)

            # Test basic operations
            await cache.set("test_key", "test_value")
            value = await cache.get("test_key")

            assert value == "test_value", f"Expected 'test_value', got {value}"

            # Test metrics
            metrics = await cache.get_metrics()
            assert metrics.total_requests > 0, "Expected some requests recorded"

            print("✅ Basic caching operations work correctly")
            return True

        # Run async test
        asyncio.run(test_cache())
        return True

    except Exception as e:
        print(f"❌ Caching test failed: {e}")
        return False

def test_performance_profiler():
    """Test performance profiler functionality."""
    print("\n📊 Testing performance profiler...")

    try:
        import asyncio
        from src.infrastructure.optimization import PerformanceProfiler

        async def test_profiler():
            profiler = PerformanceProfiler()

            # Test profiling context manager
            async with profiler.profile_operation("test_operation"):
                await asyncio.sleep(0.01)  # Small delay for testing

            # Check that metrics were recorded
            assert len(profiler.metrics_history) > 0, "Expected metrics to be recorded"

            # Get performance summary
            summary = profiler.get_performance_summary("test_operation")
            assert summary is not None, "Expected performance summary"

            print("✅ Performance profiler works correctly")
            return True

        # Run async test
        asyncio.run(test_profiler())
        return True

    except Exception as e:
        print(f"❌ Performance profiler test failed: {e}")
        return False

def run_all_validations():
    """Run all validation tests."""
    print("🧪 SERVICE ARCHITECTURE VALIDATION")
    print("=" * 50)

    tests = [
        ("Component Imports", test_imports),
        ("Caching Functionality", test_caching_functionality),
        ("Performance Profiler", test_performance_profiler)
    ]

    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} failed with exception: {e}")
            results.append((test_name, False))

    # Summary
    print("\n📊 VALIDATION SUMMARY")
    print("=" * 30)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")

    print(f"\nOverall: {passed}/{total} tests passed ({passed/total*100:.0f}%)")

    if passed == total:
        print("🎉 All validation tests passed! System is ready.")
        return True
    else:
        print("⚠️  Some validation tests failed. Check logs above.")
        return False

if __name__ == "__main__":
    success = run_all_validations()
    sys.exit(0 if success else 1)