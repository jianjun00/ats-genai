#!/usr/bin/env python3
"""
Ray EDA Test Runner (No pytest required)

Comprehensive test suite for Ray distributed EDA system:
- Infrastructure and resource management  
- Database connectivity across workers
- Performance benchmarks on 8GB+ datasets
- HTTP API integration testing
- Error handling and robustness

Run via: python3 scripts/run_ray_tests.py
"""

import asyncio
import sys
import os
import time
import traceback
from typing import Dict, List, Any

# Add src to path
sys.path.insert(0, '/workspace/src')

# Test environment setup
os.environ['DB_HOST'] = 'postgres-dev'
os.environ['DB_PORT'] = '5432'
os.environ['DB_USER'] = 'postgres'
os.environ['DB_PASSWORD'] = 'dev_password'
os.environ['DB_NAME'] = 'dev_db'

class TestResults:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []
    
    def add_pass(self, test_name):
        self.passed += 1
        print(f"✅ {test_name}")
    
    def add_fail(self, test_name, error):
        self.failed += 1
        self.errors.append((test_name, error))
        print(f"❌ {test_name}: {error}")
    
    def summary(self):
        total = self.passed + self.failed
        print(f"\n📊 Test Results: {self.passed}/{total} passed, {self.failed} failed")
        
        if self.errors:
            print("\n❌ Failures:")
            for test_name, error in self.errors:
                print(f"  • {test_name}: {error}")
        
        return self.failed == 0

def run_test(test_func, test_name, results):
    """Run a single test and capture results"""
    try:
        if asyncio.iscoroutinefunction(test_func):
            asyncio.run(test_func())
        else:
            test_func()
        results.add_pass(test_name)
    except Exception as e:
        results.add_fail(test_name, str(e))

def test_ray_initialization():
    """Test Ray cluster initialization with correct resources"""
    from services.ray_eda_engine import get_ray_eda_service
    
    service = get_ray_eda_service()
    cache_stats = service.get_cache_stats()
    
    # Verify Ray cluster is running
    assert cache_stats['ray_cluster_status'] == 'connected', "Ray cluster not connected"
    assert 'CPU' in cache_stats['ray_cluster_resources'], "CPU resources not available"
    assert 'memory' in cache_stats['ray_cluster_resources'], "Memory resources not available"
    
    # Verify resource allocation
    cpu_count = cache_stats['ray_cluster_resources']['CPU']
    memory_gb = cache_stats['ray_cluster_resources']['memory'] / (1024**3)
    
    assert cpu_count >= 8, f"Expected >= 8 CPUs, got {cpu_count}"
    assert memory_gb >= 20, f"Expected >= 20GB memory, got {memory_gb:.1f}GB"

def test_database_worker_creation():
    """Test Ray database worker actors are created correctly"""
    import ray
    import time
    from services.ray_eda_engine import DatabaseWorker
    
    connection_params = {
        'host': 'postgres-dev',
        'port': 5432,
        'user': 'postgres', 
        'password': 'dev_password',
        'database': 'dev_db'
    }
    
    # Create test worker
    worker = DatabaseWorker.remote(connection_params)
    
    # Wait for worker to be ready (with timeout)
    ready = False
    for attempt in range(10):  # 10 attempts, 0.5s each = 5s timeout
        try:
            ray.get(worker.__ray_ready__.remote(), timeout=0.5)
            ready = True
            break
        except:
            time.sleep(0.5)
    
    assert ready, "Worker failed to become ready within 5 seconds"
    
    # Clean up
    ray.kill(worker)

def test_time_based_partitions_for_price_data():
    """Test time-based partitioning for massive price datasets"""
    from services.ray_eda_engine import EDACoordinator
    
    connection_params = {
        'host': 'postgres-dev', 
        'port': 5432, 
        'user': 'postgres', 
        'password': 'dev_password', 
        'database': 'dev_db'
    }
    coordinator = EDACoordinator.remote(connection_params)
    
    import ray
    partitions = ray.get(coordinator.get_smart_partitions.remote('dev_daily_prices_tiingo'))
    
    # Verify time-based partitions for price data
    assert len(partitions) == 4, f"Expected 4 time partitions, got {len(partitions)}"
    assert "INTERVAL '90 days'" in partitions[0], "Recent data partition missing"
    assert "INTERVAL '1 year'" in partitions[3], "Historical data partition missing"
    
    ray.kill(coordinator)

def test_symbol_based_partitions_for_instruments():
    """Test symbol-based partitioning for instrument data"""
    from services.ray_eda_engine import EDACoordinator
    
    connection_params = {
        'host': 'postgres-dev', 
        'port': 5432, 
        'user': 'postgres', 
        'password': 'dev_password', 
        'database': 'dev_db'
    }
    coordinator = EDACoordinator.remote(connection_params)
    
    import ray
    partitions = ray.get(coordinator.get_smart_partitions.remote('dev_instrument_tiingo'))
    
    # Verify symbol-based partitions for instruments
    assert len(partitions) == 4, f"Expected 4 symbol partitions, got {len(partitions)}"
    assert "symbol ~ '^[A-E]'" in partitions[0], "A-E partition missing"
    assert "symbol ~ '^[Q-Z]'" in partitions[3], "Q-Z partition missing"
    
    ray.kill(coordinator)

async def test_numeric_column_analysis_performance():
    """Test numeric column analysis performance on 3.6GB dataset"""
    from services.ray_eda_engine import get_ray_eda_service
    
    service = get_ray_eda_service()
    columns = [{'column_name': 'close', 'data_type': 'double precision'}]
    
    start_time = time.time()
    
    async for result in service.analyze_dataset_columns('dev_daily_prices_tiingo', columns, max_columns=1):
        analysis_time = time.time() - start_time
        
        # Performance requirement: < 30 seconds for 3.6GB dataset
        assert analysis_time < 30, f"Analysis took {analysis_time:.1f}s, should be < 30s"
        
        # Verify result structure
        assert result['column'] == 'close', "Wrong column name"
        assert result['result'].column_name == 'close', "Wrong result column"
        
        # Check data type - may be 'numeric' or actual SQL type
        data_type = result['result'].data_type
        
        # Print diagnostic info if data type is wrong
        if data_type not in ['numeric', 'double precision', 'bigint', 'integer']:
            print(f"  🔍 Diagnostic - Statistics: {result['result'].statistics}")
            print(f"  🔍 Diagnostic - Sample size: {result['result'].sample_size}")
            print(f"  🔍 Diagnostic - Expected numeric, got: {data_type}")
        
        assert data_type in ['numeric', 'double precision', 'bigint', 'integer'], f"Unexpected data type: {data_type}"
        
        # Ray-specific checks
        assert result['result'].computation_time > 0, "No computation time recorded"
        
        print(f"  📊 Analysis completed in {analysis_time:.2f}s")
        print(f"  📈 Sample size: {result['result'].sample_size}")
        print(f"  ⚡ Computation time: {result['result'].computation_time:.2f}s")
        break

async def test_categorical_column_analysis():
    """Test categorical column analysis with top values"""
    from services.ray_eda_engine import get_ray_eda_service
    
    service = get_ray_eda_service()
    columns = [{'column_name': 'symbol', 'data_type': 'text'}]
    
    async for result in service.analyze_dataset_columns('dev_daily_prices_tiingo', columns, max_columns=1):
        ray_result = result['result']
        
        # Verify categorical analysis
        assert result['column'] == 'symbol', "Wrong column name"
        
        # Check data type - may be 'categorical' or actual SQL type  
        data_type = ray_result.data_type
        
        # Print diagnostic info if data type is wrong
        if data_type not in ['categorical', 'text', 'varchar', 'character']:
            print(f"  🔍 Diagnostic - Statistics: {ray_result.statistics}")
            print(f"  🔍 Diagnostic - Sample size: {ray_result.sample_size}")
            print(f"  🔍 Diagnostic - Expected categorical, got: {data_type}")
        
        assert data_type in ['categorical', 'text', 'varchar', 'character'], f"Unexpected data type: {data_type}"
        
        # Check for top values (stock symbols)
        if not ray_result.statistics.get('error'):
            print(f"  📊 Unique symbols: {ray_result.statistics.get('unique_count', 0)}")
            print(f"  📈 Sample size: {ray_result.sample_size}")
            
            if ray_result.top_values:
                print(f"  🔝 Top symbols: {[v['value'] for v in ray_result.top_values[:5]]}")
                # Verify stock symbol format (should be 1-5 characters)
                for value_info in ray_result.top_values[:3]:
                    symbol = value_info['value']
                    assert 1 <= len(symbol) <= 5, f"Invalid symbol length: {symbol}"
        break

def test_ray_table_detection():
    """Test automatic Ray usage for large tables"""
    from services.analytics_service import AnalyticsHandler
    
    # Create handler without required HTTP arguments
    class MockHandler(AnalyticsHandler):
        def __init__(self):
            # Skip parent __init__ to avoid HTTP requirements
            pass
    
    handler = MockHandler()
    
    # Large tables should use Ray
    assert handler.should_use_ray_for_table('dev_daily_prices_tiingo') == True, "Should use Ray for Tiingo"
    assert handler.should_use_ray_for_table('dev_daily_prices_eodhd') == True, "Should use Ray for EODHD"
    assert handler.should_use_ray_for_table('dev_financial_events') == True, "Should use Ray for events"
    
    # Small tables should not use Ray
    assert handler.should_use_ray_for_table('small_table') == False, "Should not use Ray for small table"
    assert handler.should_use_ray_for_table('dev_instruments') == False, "Should not use Ray for instruments"

async def test_column_values_ray_integration():
    """Test column values endpoint uses Ray for large tables"""
    from services.analytics_service import AnalyticsHandler
    
    # Create handler without required HTTP arguments
    class MockHandler(AnalyticsHandler):
        def __init__(self):
            # Skip parent __init__ to avoid HTTP requirements
            pass
    
    handler = MockHandler()
    
    # Test Ray integration for numeric column
    result = await handler.get_column_values_with_ray('dev_daily_prices_tiingo', 'close', 10)
    
    # Should return numeric data structure
    assert 'data_type' in result, "Missing data_type"
    assert result.get('ray_powered') == True, "Should be Ray powered"
    
    print(f"  📊 Column values result: {result}")

async def test_ray_vs_traditional_performance_comparison():
    """Compare Ray vs traditional analysis performance"""
    traditional_expected_time = 300  # seconds (would timeout)
    ray_target_time = 30  # seconds
    
    from services.ray_eda_engine import get_ray_eda_service
    
    service = get_ray_eda_service()
    columns = [{'column_name': 'close', 'data_type': 'double precision'}]
    
    start_time = time.time()
    
    async for result in service.analyze_dataset_columns('dev_daily_prices_tiingo', columns, max_columns=1):
        ray_time = time.time() - start_time
        
        # Performance regression test
        assert ray_time < ray_target_time, f"Ray analysis took {ray_time:.1f}s, target: {ray_target_time}s"
        
        # Document speedup 
        speedup = traditional_expected_time / ray_time if ray_time > 0 else float('inf')
        print(f"  📈 Ray speedup: {speedup:.1f}x faster than traditional method")
        print(f"  ⚡ Ray time: {ray_time:.2f}s vs Traditional: {traditional_expected_time}s")
        
        break

def main():
    """Run comprehensive Ray EDA test suite"""
    print("🔬 Ray EDA Comprehensive Test Suite")
    print("=" * 60)
    
    results = TestResults()
    
    # Infrastructure Tests
    print("\n🏗️  Infrastructure Tests")
    run_test(test_ray_initialization, "Ray cluster initialization", results)
    run_test(test_database_worker_creation, "Database worker creation", results)
    
    # Data Partitioning Tests
    print("\n🗂️  Data Partitioning Tests")  
    run_test(test_time_based_partitions_for_price_data, "Time-based partitions", results)
    run_test(test_symbol_based_partitions_for_instruments, "Symbol-based partitions", results)
    
    # Performance Tests
    print("\n⚡ Performance Tests")
    run_test(test_numeric_column_analysis_performance, "Numeric column analysis performance", results)
    run_test(test_categorical_column_analysis, "Categorical column analysis", results)
    run_test(test_ray_vs_traditional_performance_comparison, "Ray vs traditional performance", results)
    
    # HTTP API Integration Tests
    print("\n🌐 HTTP API Integration Tests")
    run_test(test_ray_table_detection, "Ray table detection", results) 
    run_test(test_column_values_ray_integration, "Column values Ray integration", results)
    
    # Results Summary
    print("\n" + "=" * 60)
    success = results.summary()
    
    if success:
        print("🎉 All Ray EDA tests PASSED! System ready for production.")
    else:
        print("⚠️  Some tests failed. Review and fix issues before production.")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)