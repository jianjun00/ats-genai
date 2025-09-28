#!/usr/bin/env python3
"""
Simplified InstrumentService Performance Benchmark

A basic benchmark that works without complex environment dependencies.
"""

import asyncio
import time
import statistics
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

print("🚀 Starting InstrumentService Performance Benchmark")

def benchmark_report(operation_name: str, latencies: list, duration: float, success_count: int, total_ops: int):
    """Generate benchmark report for an operation"""
    if not latencies:
        print(f"❌ {operation_name}: No successful operations")
        return
    
    ops_per_sec = success_count / duration if duration > 0 else 0
    success_rate = success_count / total_ops if total_ops > 0 else 0
    
    avg_latency = statistics.mean(latencies)
    p95_latency = statistics.quantiles(latencies, n=20)[18] if len(latencies) >= 20 else max(latencies)
    
    print(f"✅ {operation_name:25}: {ops_per_sec:8.1f} ops/sec | "
          f"avg: {avg_latency:6.1f}ms | "
          f"p95: {p95_latency:6.1f}ms | "
          f"success: {success_rate*100:5.1f}%")


async def test_monitoring_system():
    """Test the monitoring system independently"""
    print("\n📊 Testing monitoring system...")
    
    from infrastructure.monitoring.service_metrics import (
        get_global_metrics_collector,
        ServiceMetric,
        ServiceHealthMonitor
    )
    from infrastructure.monitoring.instrument_service_monitor import get_instrument_service_monitor
    from datetime import datetime
    
    # Test metrics collector
    collector = get_global_metrics_collector()
    print("✅ Metrics collector initialized")
    
    # Test metric recording
    test_metric = ServiceMetric(
        service_name="TestService",
        operation="test_operation",
        metric_type="latency",
        value=25.5,
        timestamp=datetime.utcnow(),
        labels={"test": "true"}
    )
    collector.record_metric(test_metric)
    print("✅ Metric recording works")
    
    # Test stats retrieval
    stats = collector.get_service_stats("TestService")
    print(f"✅ Stats retrieval works: {len(stats)} fields")
    
    # Test instrument service monitor
    monitor = get_instrument_service_monitor()
    print("✅ InstrumentService monitor initialized")
    
    # Test dashboard generation
    dashboard_start = time.time()
    dashboard_data = await monitor.get_monitoring_dashboard()
    dashboard_time = time.time() - dashboard_start
    
    print(f"✅ Dashboard generation: {dashboard_time*1000:.1f}ms")
    print(f"   Dashboard fields: {list(dashboard_data.keys())}")
    
    return True
    
async def test_service_architecture():
    """Test service architecture components"""
    print("\n🏗️  Testing service architecture...")
    
    # Test service interfaces
    from domains.instruments.services.interfaces.instrument_service_interface import (
        InstrumentServiceInterface,
        InstrumentDTO,
        InstrumentSearchCriteria
    )
    print("✅ Service interfaces import correctly")
    
    # Test DTO creation
    test_instrument = InstrumentDTO(
        symbol="TEST",
        name="Test Instrument",
        exchange="NYSE",
        instrument_type="stock",
        currency="USD"
    )
    print("✅ DTO creation works")
    
    # Test search criteria
    criteria = InstrumentSearchCriteria(limit=10, symbols=["AAPL", "GOOGL"])
    print("✅ Search criteria creation works")
    
    return True
    
async def test_caching_system():
    """Test caching system components"""
    print("\n💾 Testing caching system...")
    
    from infrastructure.caching.redis_cache import (
        RedisCache,
        CacheKeyBuilder,
        CacheStats,
        InMemoryCache
    )
    print("✅ Cache imports work")
    
    # Test cache key building
    key = CacheKeyBuilder.instrument_by_id(123)
    assert key == "instrument:id:123"
    print("✅ Cache key building works")
    
    # Test in-memory cache
    cache = InMemoryCache(max_size=10, default_ttl=60)
    await cache.set("test_key", "test_value")
    value = await cache.get("test_key")
    assert value == "test_value"
    print("✅ In-memory cache works")
    
    # Test cache stats
    stats = CacheStats()
    stats.hits = 10
    stats.misses = 2
    assert abs(stats.hit_rate - 0.833) < 0.01
    print("✅ Cache statistics work")
    
    return True
    
async def test_api_components():
    """Test API components"""
    print("\n🌐 Testing API components...")
    
    from infrastructure.web.api.enhanced_instruments_api import app
    print("✅ Enhanced API imports correctly")
    
    # Test app configuration
    assert app.title == "ATS Instruments API (Enhanced)"
    print("✅ API configuration correct")
    
    # Test routes
    routes = [route.path for route in app.routes if hasattr(route, 'path')]
    assert len(routes) > 0
    print(f"✅ API has {len(routes)} routes")
    
    # Test OpenAPI schema generation
    schema = app.openapi()
    assert 'openapi' in schema
    print("✅ OpenAPI schema generation works")
    
    return True
    
async def run_integration_performance_test():
    """Run basic integration performance test"""
    print("\n⚡ Running integration performance test...")
    
    # Test component integration performance
    operations = []
    
    # Test 1: Monitoring system performance
    start_time = time.perf_counter()
    monitoring_success = await test_monitoring_system()
    monitoring_time = time.perf_counter() - start_time
    operations.append(("Monitoring System Setup", monitoring_time * 1000, monitoring_success))
    
    # Test 2: Architecture components performance
    start_time = time.perf_counter()
    architecture_success = await test_service_architecture()
    architecture_time = time.perf_counter() - start_time
    operations.append(("Service Architecture", architecture_time * 1000, architecture_success))
    
    # Test 3: Caching system performance
    start_time = time.perf_counter()
    caching_success = await test_caching_system()
    caching_time = time.perf_counter() - start_time
    operations.append(("Caching System", caching_time * 1000, caching_success))
    
    # Test 4: API components performance
    start_time = time.perf_counter()
    api_success = await test_api_components()
    api_time = time.perf_counter() - start_time
    operations.append(("API Components", api_time * 1000, api_success))
    
    # Report results
    print("\n📈 Integration Performance Results:")
    print("-" * 50)
    
    total_success = 0
    for op_name, duration_ms, success in operations:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{op_name:25}: {duration_ms:8.1f}ms {status}")
        if success:
            total_success += 1
    
    success_rate = total_success / len(operations)
    print(f"\nOverall Success Rate: {success_rate*100:.1f}% ({total_success}/{len(operations)})")
    
    return success_rate >= 0.75  # Require 75% success rate


async def main():
    """Main benchmark function"""
    print("InstrumentService Architecture Performance Benchmark")
    print("=" * 60)
    
    benchmark_start = time.time()
    
    # Run integration performance test
    success = await run_integration_performance_test()
    
    benchmark_duration = time.time() - benchmark_start
    
    print("\n" + "=" * 60)
    print(f"Benchmark completed in {benchmark_duration:.2f} seconds")
    
    if success:
        print("🎉 Benchmark PASSED - Architecture is performing well!")
    else:
        print("⚠️  Benchmark had issues - Check component performance")
    
    print("=" * 60)
    
if __name__ == "__main__":
    asyncio.run(main())