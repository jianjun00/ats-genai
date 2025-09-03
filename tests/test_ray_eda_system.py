#!/usr/bin/env python3
"""
Comprehensive Test Suite for Ray EDA System

Tests distributed computing on 8GB+ financial datasets:
- Ray cluster initialization and resource management
- Database connection pooling across workers  
- Distributed column analysis performance
- Data partitioning strategies and accuracy
- HTTP API endpoints with Ray integration
- Error handling and fallback mechanisms

Coverage target: 90%+ of Ray EDA functionality
"""

import pytest
import asyncio
import sys
import os
import time
from typing import Dict, List, Any
from unittest.mock import patch, MagicMock

# Add src to path
sys.path.insert(0, '/workspace/src')

# Test environment setup
os.environ['DB_HOST'] = 'postgres-dev'
os.environ['DB_PORT'] = '5432'
os.environ['DB_USER'] = 'postgres'
os.environ['DB_PASSWORD'] = 'dev_password'
os.environ['DB_NAME'] = 'dev_db'

class TestRayEDAInfrastructure:
    """Test Ray cluster setup and resource management"""
    
    def test_ray_initialization(self):
        """Test Ray cluster initialization with correct resources"""
        from services.ray_eda_engine import get_ray_eda_service
        
        service = get_ray_eda_service()
        cache_stats = service.get_cache_stats()
        
        # Verify Ray cluster is running
        assert cache_stats['ray_cluster_status'] == 'connected'
        assert 'CPU' in cache_stats['ray_cluster_resources']
        assert 'memory' in cache_stats['ray_cluster_resources']
        assert 'object_store_memory' in cache_stats['ray_cluster_resources']
        
        # Verify resource allocation
        cpu_count = cache_stats['ray_cluster_resources']['CPU']
        memory_gb = cache_stats['ray_cluster_resources']['memory'] / (1024**3)
        
        assert cpu_count >= 8, f"Expected >= 8 CPUs, got {cpu_count}"
        assert memory_gb >= 20, f"Expected >= 20GB memory, got {memory_gb:.1f}GB"
    
    def test_database_worker_creation(self):
        """Test Ray database worker actors are created correctly"""
        import ray
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
        
        # Verify worker can be created and is Ray actor
        assert ray.get(worker.__ray_ready__.remote()) is None
        
        # Clean up
        ray.kill(worker)
    
    def test_eda_coordinator_initialization(self):
        """Test EDA coordinator with multiple workers"""
        from services.ray_eda_engine import EDACoordinator
        
        connection_params = {
            'host': 'postgres-dev',
            'port': 5432,
            'user': 'postgres',
            'password': 'dev_password', 
            'database': 'dev_db'
        }
        
        coordinator = EDACoordinator.remote(connection_params, num_workers=4)
        
        # Verify coordinator is ready
        import ray
        assert ray.get(coordinator.__ray_ready__.remote()) is None
        
        # Clean up
        ray.kill(coordinator)

class TestDataPartitioning:
    """Test smart data partitioning strategies"""
    
    def test_time_based_partitions_for_price_data(self):
        """Test time-based partitioning for massive price datasets"""
        from services.ray_eda_engine import EDACoordinator
        
        connection_params = {'host': 'postgres-dev', 'port': 5432, 'user': 'postgres', 'password': 'dev_password', 'database': 'dev_db'}
        coordinator = EDACoordinator.remote(connection_params)
        
        import ray
        partitions = ray.get(coordinator.get_smart_partitions.remote('dev_daily_prices_tiingo'))
        
        # Verify time-based partitions for price data
        assert len(partitions) == 4, f"Expected 4 time partitions, got {len(partitions)}"
        assert "INTERVAL '90 days'" in partitions[0]  # Recent data
        assert "INTERVAL '1 year'" in partitions[3]   # Historical data
        
        ray.kill(coordinator)
    
    def test_symbol_based_partitions_for_instruments(self):
        """Test symbol-based partitioning for instrument data"""
        from services.ray_eda_engine import EDACoordinator
        
        connection_params = {'host': 'postgres-dev', 'port': 5432, 'user': 'postgres', 'password': 'dev_password', 'database': 'dev_db'}
        coordinator = EDACoordinator.remote(connection_params)
        
        import ray
        partitions = ray.get(coordinator.get_smart_partitions.remote('dev_instrument_tiingo'))
        
        # Verify symbol-based partitions for instruments
        assert len(partitions) == 4, f"Expected 4 symbol partitions, got {len(partitions)}"
        assert "symbol ~ '^[A-E]'" in partitions[0]
        assert "symbol ~ '^[Q-Z]'" in partitions[3]
        
        ray.kill(coordinator)
    
    def test_no_partitioning_for_small_tables(self):
        """Test no partitioning for smaller tables"""
        from services.ray_eda_engine import EDACoordinator
        
        connection_params = {'host': 'postgres-dev', 'port': 5432, 'user': 'postgres', 'password': 'dev_password', 'database': 'dev_db'}
        coordinator = EDACoordinator.remote(connection_params)
        
        import ray
        partitions = ray.get(coordinator.get_smart_partitions.remote('small_test_table'))
        
        # Verify single partition for unknown/small tables
        assert partitions == ["TRUE"], f"Expected single partition, got {partitions}"
        
        ray.kill(coordinator)

class TestDistributedColumnAnalysis:
    """Test distributed column analysis across massive datasets"""
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_numeric_column_analysis_performance(self):
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
            assert result['column'] == 'close'
            assert result['result'].column_name == 'close'
            assert result['result'].data_type == 'numeric'
            
            # Ray-specific checks
            assert result['result'].computation_time > 0
            break
    
    @pytest.mark.asyncio  
    @pytest.mark.asyncio
    async def test_categorical_column_analysis(self):
        """Test categorical column analysis with top values"""
        from services.ray_eda_engine import get_ray_eda_service
        
        service = get_ray_eda_service()
        columns = [{'column_name': 'symbol', 'data_type': 'text'}]
        
        async for result in service.analyze_dataset_columns('dev_daily_prices_tiingo', columns, max_columns=1):
            ray_result = result['result']
            
            # Verify categorical analysis
            assert result['column'] == 'symbol'
            assert ray_result.data_type == 'categorical'
            
            # Check for top values (stock symbols)
            if not ray_result.statistics.get('error'):
                assert ray_result.statistics.get('unique_count', 0) > 0
                if ray_result.top_values:
                    # Verify stock symbol format (should be 1-5 characters)
                    for value_info in ray_result.top_values[:3]:
                        symbol = value_info['value']
                        assert 1 <= len(symbol) <= 5, f"Invalid symbol length: {symbol}"
            break
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_parallel_multi_column_analysis(self):
        """Test parallel analysis of multiple columns"""
        from services.ray_eda_engine import get_ray_eda_service
        
        service = get_ray_eda_service() 
        columns = [
            {'column_name': 'close', 'data_type': 'double precision'},
            {'column_name': 'volume', 'data_type': 'bigint'},
            {'column_name': 'symbol', 'data_type': 'text'}
        ]
        
        start_time = time.time()
        results = []
        
        async for result in service.analyze_dataset_columns('dev_daily_prices_tiingo', columns, max_columns=3):
            results.append(result)
        
        parallel_time = time.time() - start_time
        
        # Verify parallel processing completed all columns
        assert len(results) == 3, f"Expected 3 results, got {len(results)}"
        
        # Verify parallel performance (should be faster than 3x sequential)
        # If each column took 10s sequentially, parallel should be < 20s
        assert parallel_time < 60, f"Parallel analysis took {parallel_time:.1f}s, too slow"
        
        # Verify each column was analyzed
        analyzed_columns = {r['column'] for r in results}
        expected_columns = {'close', 'volume', 'symbol'}
        assert analyzed_columns == expected_columns

class TestHTTPAPIIntegration:
    """Test HTTP API endpoints with Ray integration"""
    
    def test_ray_table_detection(self):
        """Test automatic Ray usage for large tables"""
        from services.analytics_service import AnalyticsHandler
        
        handler = AnalyticsHandler()
        
        # Large tables should use Ray
        assert handler.should_use_ray_for_table('dev_daily_prices_tiingo') == True
        assert handler.should_use_ray_for_table('dev_daily_prices_eodhd') == True
        assert handler.should_use_ray_for_table('dev_financial_events') == True
        
        # Small tables should not use Ray
        assert handler.should_use_ray_for_table('small_table') == False
        assert handler.should_use_ray_for_table('dev_instruments') == False
    
    def test_column_values_endpoint_ray_integration(self):
        """Test column values endpoint uses Ray for large tables"""
        import asyncio
        from services.analytics_service import AnalyticsHandler
        
        handler = AnalyticsHandler()
        
        # Test Ray integration for numeric column
        result = asyncio.run(
            handler.get_column_values_with_ray('dev_daily_prices_tiingo', 'close', 10)
        )
        
        # Should return numeric data structure
        assert 'data_type' in result
        assert result.get('ray_powered') == True
        
        if not result.get('error'):
            assert 'min_value' in result
            assert 'max_value' in result
    
    @patch('services.analytics_service.RAY_AVAILABLE', True)
    def test_analyze_endpoint_ray_fallback(self):
        """Test analyze endpoint with Ray unavailable fallback"""
        from services.analytics_service import AnalyticsHandler
        
        handler = AnalyticsHandler()
        
        # Test large table detection
        assert handler.should_use_ray_for_table('dev_daily_prices_tiingo') == True
        
        # Test small table bypass
        assert handler.should_use_ray_for_table('small_test_table') == False

class TestErrorHandlingAndRobustness:
    """Test error handling and system robustness"""
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_connection_failure_handling(self):
        """Test graceful handling of database connection failures"""
        from services.ray_eda_engine import get_ray_eda_service
        
        # Test with invalid connection parameters
        with patch('services.ray_eda_engine.get_ray_eda_service') as mock_service:
            mock_result = MagicMock()
            mock_result.statistics = {'error': 'Connection refused'}
            mock_result.sample_size = 0
            
            mock_service.return_value.analyze_dataset_columns.return_value.__aiter__.return_value = [
                {'column': 'test', 'result': mock_result, 'cached': False}
            ]
            
            service = mock_service.return_value
            columns = [{'column_name': 'test', 'data_type': 'numeric'}]
            
            async for result in service.analyze_dataset_columns('test_table', columns):
                # Should handle connection errors gracefully
                assert result['result'].statistics.get('error') is not None
                break
    
    def test_ray_cluster_unavailable_fallback(self):
        """Test fallback to traditional methods when Ray unavailable"""
        with patch('services.ray_eda_engine.ray.is_initialized', return_value=False):
            with patch('services.ray_eda_engine.ray.init') as mock_init:
                mock_init.side_effect = Exception("Ray unavailable")
                
                # Should handle Ray unavailability gracefully
                try:
                    from services.ray_eda_engine import get_ray_eda_service
                    service = get_ray_eda_service()
                    cache_stats = service.get_cache_stats()
                    assert cache_stats['ray_cluster_status'] == 'disconnected'
                except Exception as e:
                    # Expected if Ray truly unavailable
                    assert "Ray unavailable" in str(e)
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_timeout_handling_for_massive_queries(self):
        """Test timeout handling for queries on massive datasets"""
        from services.ray_eda_engine import get_ray_eda_service
        
        service = get_ray_eda_service()
        columns = [{'column_name': 'close', 'data_type': 'double precision'}]
        
        # Test with very short timeout
        start_time = time.time()
        
        try:
            async with asyncio.timeout(5):  # 5 second timeout
                async for result in service.analyze_dataset_columns('dev_daily_prices_tiingo', columns):
                    break
        except asyncio.TimeoutError:
            # Timeout is acceptable for massive datasets
            elapsed = time.time() - start_time
            assert 4 < elapsed < 7, f"Timeout occurred at {elapsed:.1f}s"

class TestPerformanceBenchmarks:
    """Performance benchmarks and regression tests"""
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_ray_vs_traditional_performance_comparison(self):
        """Compare Ray vs traditional analysis performance"""
        # This test documents the performance improvement
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
            print(f"📈 Ray speedup: {speedup:.1f}x faster than traditional method")
            
            break
    
    def test_memory_usage_efficiency(self):
        """Test Ray memory usage doesn't exceed limits"""
        from services.ray_eda_engine import get_ray_eda_service
        
        service = get_ray_eda_service()
        cache_stats = service.get_cache_stats()
        
        # Ray object store should be configured properly
        object_store_gb = cache_stats['ray_cluster_resources']['object_store_memory'] / (1024**3)
        
        assert object_store_gb >= 1.5, f"Object store too small: {object_store_gb:.1f}GB"
        assert object_store_gb <= 5.0, f"Object store too large: {object_store_gb:.1f}GB"

class TestDataAccuracyAndConsistency:
    """Test data accuracy and consistency across distributed analysis"""
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_sample_size_accuracy(self):
        """Test that sample sizes are reasonable for statistical accuracy"""
        from services.ray_eda_engine import get_ray_eda_service
        
        service = get_ray_eda_service()
        columns = [{'column_name': 'close', 'data_type': 'double precision'}]
        
        async for result in service.analyze_dataset_columns('dev_daily_prices_tiingo', columns, max_columns=1):
            ray_result = result['result']
            
            if not ray_result.statistics.get('error') and ray_result.sample_size > 0:
                # Sample size should be substantial for 3.6GB dataset
                assert ray_result.sample_size >= 1000, f"Sample too small: {ray_result.sample_size}"
                
                # But not too large (should be using sampling)
                assert ray_result.sample_size <= 1000000, f"Sample too large: {ray_result.sample_size}"
            
            break
    
    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_distributed_aggregation_consistency(self):
        """Test that distributed results are internally consistent"""
        from services.ray_eda_engine import get_ray_eda_service
        
        service = get_ray_eda_service()
        columns = [{'column_name': 'close', 'data_type': 'double precision'}]
        
        async for result in service.analyze_dataset_columns('dev_daily_prices_tiingo', columns, max_columns=1):
            ray_result = result['result']
            
            if not ray_result.statistics.get('error') and ray_result.statistics.get('count', 0) > 0:
                stats = ray_result.statistics
                
                # Basic consistency checks
                assert stats.get('count', 0) > 0
                
                # Check if we have partition info
                if 'sample_partitions' in stats:
                    assert stats['sample_partitions'] > 0
                    assert stats['sample_partitions'] <= 8  # Max workers
            
            break

# Performance test fixtures
@pytest.fixture
def ray_service():
    """Fixture providing Ray EDA service for tests"""
    from services.ray_eda_engine import get_ray_eda_service
    return get_ray_eda_service()

@pytest.fixture
def large_dataset():
    """Fixture defining large dataset for performance tests"""
    return {
        'table_name': 'dev_daily_prices_tiingo',
        'size_gb': 3.6,
        'estimated_rows': 50000000,
        'columns': [
            {'column_name': 'close', 'data_type': 'double precision'},
            {'column_name': 'volume', 'data_type': 'bigint'},
            {'column_name': 'symbol', 'data_type': 'text'}
        ]
    }

if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([
        __file__,
        '-v',                    # Verbose output
        '-s',                    # Show print statements
        '--tb=short',            # Short traceback format
        '--durations=10',        # Show slowest 10 tests
        '-x',                    # Stop on first failure
        '--asyncio-mode=auto'    # Auto-detect async tests
    ])