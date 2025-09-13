"""
Performance benchmarks and load testing for InstrumentService.

Tests performance characteristics, scalability, and resource utilization
of the InstrumentService under various load conditions.
"""

import pytest
import asyncio
import time
import statistics
import psutil
import concurrent.futures
from typing import List, Dict, Any
from datetime import datetime

# Service imports
from domains.instruments.services.interfaces.instrument_service_interface import (
    InstrumentDTO,
    InstrumentSearchCriteria,
    InstrumentOperationResult
)
from domains.instruments.services.config.service_container import get_instrument_service
from core.platform.config.environment import Environment, EnvironmentType

# Monitoring imports
from infrastructure.monitoring.instrument_service_monitor import get_instrument_service_monitor


class PerformanceBenchmark:
    """Performance benchmark result"""
    
    def __init__(self, operation: str, iterations: int, duration: float, success_count: int):
        self.operation = operation
        self.iterations = iterations
        self.duration = duration
        self.success_count = success_count
        self.failure_count = iterations - success_count
        self.success_rate = success_count / iterations if iterations > 0 else 0
        self.operations_per_second = success_count / duration if duration > 0 else 0
        self.average_latency_ms = (duration * 1000) / success_count if success_count > 0 else 0


class ResourceMonitor:
    """Monitor resource usage during performance tests"""
    
    def __init__(self):
        self.cpu_samples = []
        self.memory_samples = []
        self.start_time = None
        self.monitoring = False
        self._monitor_task = None
    
    async def start_monitoring(self):
        """Start resource monitoring"""
        self.monitoring = True
        self.start_time = time.time()
        self._monitor_task = asyncio.create_task(self._monitor_loop())
    
    async def stop_monitoring(self):
        """Stop resource monitoring"""
        self.monitoring = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
    
    async def _monitor_loop(self):
        """Resource monitoring loop"""
        while self.monitoring:
            try:
                # Sample CPU and memory usage
                cpu_percent = psutil.cpu_percent(interval=None)
                memory_info = psutil.virtual_memory()
                memory_mb = memory_info.used / (1024 * 1024)
                
                self.cpu_samples.append(cpu_percent)
                self.memory_samples.append(memory_mb)
                
                await asyncio.sleep(0.1)  # Sample every 100ms
                
            except asyncio.CancelledError:
                break
            except Exception:
                pass  # Ignore monitoring errors
    
    def get_stats(self) -> Dict[str, Any]:
        """Get resource usage statistics"""
        if not self.cpu_samples or not self.memory_samples:
            return {}
        
        return {
            'duration_seconds': time.time() - self.start_time if self.start_time else 0,
            'cpu_usage': {
                'avg': statistics.mean(self.cpu_samples),
                'max': max(self.cpu_samples),
                'min': min(self.cpu_samples),
                'samples': len(self.cpu_samples)
            },
            'memory_usage_mb': {
                'avg': statistics.mean(self.memory_samples),
                'max': max(self.memory_samples),
                'min': min(self.memory_samples),
                'samples': len(self.memory_samples)
            }
        }


class TestInstrumentServicePerformance:
    """Performance benchmarks for InstrumentService operations"""
    
    @pytest.fixture
    async def test_environment(self):
        """Create test environment"""
        return Environment(None, EnvironmentType.DEV)
    
    @pytest.fixture
    async def instrument_service(self, test_environment):
        """Get instrument service for testing"""
        return await get_instrument_service(test_environment)
    
    @pytest.fixture
    def resource_monitor(self):
        """Get resource monitor"""
        return ResourceMonitor()
    
    @pytest.fixture
    def sample_instruments(self):
        """Sample instruments for testing"""
        return [
            InstrumentDTO(
                symbol=f"PERF{i:03d}",
                name=f"Performance Test Instrument {i}",
                exchange="NYSE",
                instrument_type="stock",
                currency="USD"
            )
            for i in range(100)
        ]
    
    @pytest.mark.asyncio
    async def test_single_operation_performance(self, instrument_service):
        """Test performance of individual operations"""
        operations = [
            ('validate_symbol', lambda: instrument_service.validate_symbol("AAPL")),
            ('get_instrument_count', lambda: instrument_service.get_instrument_count()),
            ('list_instruments', lambda: instrument_service.list_instruments(InstrumentSearchCriteria(limit=10)))
        ]
        
        results = {}
        
        for operation_name, operation_func in operations:
            # Warmup
            for _ in range(5):
                try:
                    await operation_func()
                except:
                    pass
            
            # Benchmark
            iterations = 50
            latencies = []
            success_count = 0
            
            start_time = time.time()
            
            for _ in range(iterations):
                op_start = time.time()
                try:
                    await operation_func()
                    op_end = time.time()
                    latencies.append((op_end - op_start) * 1000)  # Convert to ms
                    success_count += 1
                except Exception:
                    pass
            
            total_time = time.time() - start_time
            
            if latencies:
                results[operation_name] = {
                    'iterations': iterations,
                    'success_count': success_count,
                    'success_rate': success_count / iterations,
                    'total_time_s': total_time,
                    'avg_latency_ms': statistics.mean(latencies),
                    'p50_latency_ms': statistics.median(latencies),
                    'p95_latency_ms': statistics.quantiles(latencies, n=20)[18] if len(latencies) >= 20 else max(latencies),
                    'p99_latency_ms': statistics.quantiles(latencies, n=100)[98] if len(latencies) >= 100 else max(latencies),
                    'min_latency_ms': min(latencies),
                    'max_latency_ms': max(latencies),
                    'operations_per_second': success_count / total_time if total_time > 0 else 0
                }
        
        # Verify performance requirements
        for operation_name, stats in results.items():
            print(f"\n{operation_name} Performance:")
            print(f"  Operations/sec: {stats['operations_per_second']:.2f}")
            print(f"  Avg latency: {stats['avg_latency_ms']:.2f}ms")
            print(f"  P95 latency: {stats['p95_latency_ms']:.2f}ms")
            print(f"  Success rate: {stats['success_rate']*100:.1f}%")
            
            # Basic performance assertions
            assert stats['success_rate'] >= 0.9, f"{operation_name} success rate too low: {stats['success_rate']}"
            assert stats['avg_latency_ms'] < 1000, f"{operation_name} avg latency too high: {stats['avg_latency_ms']}ms"
    
    @pytest.mark.asyncio
    async def test_concurrent_operations_performance(self, instrument_service, resource_monitor):
        """Test performance under concurrent load"""
        await resource_monitor.start_monitoring()
        
        try:
            # Test concurrent validate_symbol operations
            async def validate_operation():
                try:
                    return await instrument_service.validate_symbol("AAPL")
                except:
                    return False
            
            # Test with increasing concurrency levels
            concurrency_levels = [5, 10, 20]
            results = {}
            
            for concurrency in concurrency_levels:
                print(f"\nTesting with concurrency level: {concurrency}")
                
                start_time = time.time()
                
                # Create concurrent tasks
                tasks = [validate_operation() for _ in range(concurrency * 10)]  # 10 ops per concurrent thread
                
                # Run tasks concurrently
                task_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                total_time = time.time() - start_time
                success_count = sum(1 for r in task_results if r is True)
                total_ops = len(tasks)
                
                results[concurrency] = {
                    'concurrency': concurrency,
                    'total_operations': total_ops,
                    'success_count': success_count,
                    'success_rate': success_count / total_ops if total_ops > 0 else 0,
                    'total_time_s': total_time,
                    'operations_per_second': success_count / total_time if total_time > 0 else 0
                }
                
                print(f"  Success rate: {results[concurrency]['success_rate']*100:.1f}%")
                print(f"  Ops/sec: {results[concurrency]['operations_per_second']:.2f}")
            
            # Verify concurrent performance
            for concurrency, stats in results.items():
                assert stats['success_rate'] >= 0.8, f"Concurrent operations success rate too low at {concurrency}: {stats['success_rate']}"
                assert stats['operations_per_second'] > 5, f"Throughput too low at concurrency {concurrency}: {stats['operations_per_second']}"
                
        finally:
            await resource_monitor.stop_monitoring()
            
            # Print resource usage
            resource_stats = resource_monitor.get_stats()
            if resource_stats:
                print(f"\nResource Usage:")
                print(f"  Avg CPU: {resource_stats['cpu_usage']['avg']:.1f}%")
                print(f"  Max CPU: {resource_stats['cpu_usage']['max']:.1f}%")
                print(f"  Avg Memory: {resource_stats['memory_usage_mb']['avg']:.1f}MB")
    
    @pytest.mark.asyncio
    async def test_batch_operations_performance(self, instrument_service, sample_instruments):
        """Test performance of batch operations"""
        # Test batch creation performance
        batch_sizes = [10, 50, 100]
        results = {}
        
        for batch_size in batch_sizes:
            batch_instruments = sample_instruments[:batch_size]
            
            start_time = time.time()
            
            try:
                result = await instrument_service.create_instruments_batch(batch_instruments)
                total_time = time.time() - start_time
                
                results[batch_size] = {
                    'batch_size': batch_size,
                    'success': result.success,
                    'created_count': result.created_count,
                    'total_time_s': total_time,
                    'instruments_per_second': result.created_count / total_time if total_time > 0 else 0
                }
                
                print(f"\nBatch size {batch_size}:")
                print(f"  Created: {result.created_count}")
                print(f"  Time: {total_time:.3f}s")
                print(f"  Rate: {results[batch_size]['instruments_per_second']:.2f} instruments/sec")
                
            except Exception as e:
                print(f"Batch size {batch_size} failed: {e}")
                results[batch_size] = {
                    'batch_size': batch_size,
                    'success': False,
                    'error': str(e)
                }
        
        # Verify batch performance scales reasonably
        successful_results = {k: v for k, v in results.items() if v.get('success', False)}
        if len(successful_results) >= 2:
            # Check that larger batches have better throughput per item
            sorted_results = sorted(successful_results.items(), key=lambda x: x[0])
            for i in range(len(sorted_results) - 1):
                current_rate = sorted_results[i][1]['instruments_per_second']
                next_rate = sorted_results[i + 1][1]['instruments_per_second']
                # Allow some variance, but expect general scaling improvement
                assert next_rate >= current_rate * 0.5, f"Batch performance doesn't scale: {current_rate} -> {next_rate}"
    
    @pytest.mark.asyncio 
    async def test_memory_usage_under_load(self, instrument_service, resource_monitor):
        """Test memory usage patterns under sustained load"""
        await resource_monitor.start_monitoring()
        
        try:
            # Perform sustained operations for memory leak detection
            operations_count = 200
            
            for i in range(operations_count):
                # Mix of different operations
                if i % 4 == 0:
                    await instrument_service.validate_symbol("AAPL")
                elif i % 4 == 1:
                    await instrument_service.get_instrument_count()
                elif i % 4 == 2:
                    criteria = InstrumentSearchCriteria(limit=5)
                    await instrument_service.list_instruments(criteria)
                else:
                    # Light operation
                    await instrument_service.validate_symbol("GOOGL")
                
                # Small delay to allow memory patterns to emerge
                if i % 50 == 0:
                    await asyncio.sleep(0.1)
            
        finally:
            await resource_monitor.stop_monitoring()
            
            resource_stats = resource_monitor.get_stats()
            if resource_stats and 'memory_usage_mb' in resource_stats:
                memory_stats = resource_stats['memory_usage_mb']
                memory_growth = memory_stats['max'] - memory_stats['min']
                
                print(f"\nMemory Usage Analysis:")
                print(f"  Min memory: {memory_stats['min']:.1f}MB")
                print(f"  Max memory: {memory_stats['max']:.1f}MB")
                print(f"  Avg memory: {memory_stats['avg']:.1f}MB")
                print(f"  Memory growth: {memory_growth:.1f}MB")
                
                # Check for reasonable memory usage patterns
                assert memory_growth < 100, f"Excessive memory growth: {memory_growth}MB"
    
    @pytest.mark.asyncio
    async def test_service_monitoring_integration_performance(self, instrument_service):
        """Test performance impact of monitoring system"""
        monitor = get_instrument_service_monitor()
        
        # Test operations with monitoring
        operations_with_monitoring = []
        start_time = time.time()
        
        for _ in range(50):
            op_start = time.time()
            await instrument_service.validate_symbol("AAPL") 
            op_end = time.time()
            operations_with_monitoring.append((op_end - op_start) * 1000)
        
        with_monitoring_time = time.time() - start_time
        
        # Generate monitoring dashboard (this exercises the monitoring system)
        dashboard_start = time.time()
        dashboard_data = await monitor.get_monitoring_dashboard()
        dashboard_time = time.time() - dashboard_start
        
        # Analyze results
        avg_op_time = statistics.mean(operations_with_monitoring)
        monitoring_overhead = dashboard_time
        
        print(f"\nMonitoring Performance Analysis:")
        print(f"  Avg operation time (with monitoring): {avg_op_time:.2f}ms")
        print(f"  Dashboard generation time: {dashboard_time*1000:.2f}ms")
        print(f"  Total test time: {with_monitoring_time:.3f}s")
        
        # Verify monitoring doesn't add excessive overhead
        assert avg_op_time < 100, f"Operations too slow with monitoring: {avg_op_time}ms"
        assert dashboard_time < 2.0, f"Dashboard generation too slow: {dashboard_time}s"
        assert dashboard_data is not None, "Dashboard should return data"


class TestInstrumentServiceLoadTesting:
    """Load testing scenarios for InstrumentService"""
    
    @pytest.fixture
    async def instrument_service(self):
        """Get instrument service for load testing"""
        env = Environment(None, EnvironmentType.DEV)
        return await get_instrument_service(env)
    
    @pytest.mark.asyncio
    async def test_sustained_load_scenario(self, instrument_service):
        """Test service under sustained load"""
        duration_seconds = 30
        target_ops_per_second = 10
        total_target_operations = duration_seconds * target_ops_per_second
        
        print(f"\nSustained Load Test:")
        print(f"  Duration: {duration_seconds}s")
        print(f"  Target: {target_ops_per_second} ops/sec")
        print(f"  Total operations: {total_target_operations}")
        
        successful_operations = 0
        failed_operations = 0
        start_time = time.time()
        
        # Create operation tasks with controlled timing
        async def rate_limited_operation():
            try:
                await instrument_service.validate_symbol("AAPL")
                return True
            except:
                return False
        
        # Run operations at target rate
        tasks = []
        for i in range(total_target_operations):
            task = asyncio.create_task(rate_limited_operation())
            tasks.append(task)
            
            # Rate limiting - wait to maintain target ops/sec
            if i % target_ops_per_second == 0 and i > 0:
                await asyncio.sleep(1)
        
        # Wait for all operations to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        total_time = time.time() - start_time
        
        # Analyze results
        successful_operations = sum(1 for r in results if r is True)
        failed_operations = len(results) - successful_operations
        actual_ops_per_second = successful_operations / total_time if total_time > 0 else 0
        
        print(f"\nSustained Load Results:")
        print(f"  Successful operations: {successful_operations}")
        print(f"  Failed operations: {failed_operations}")
        print(f"  Actual ops/sec: {actual_ops_per_second:.2f}")
        print(f"  Success rate: {(successful_operations/len(results))*100:.1f}%")
        
        # Verify load handling
        assert successful_operations >= total_target_operations * 0.8, f"Too many failed operations: {failed_operations}"
        assert actual_ops_per_second >= target_ops_per_second * 0.5, f"Throughput too low: {actual_ops_per_second}"
    
    @pytest.mark.asyncio
    async def test_spike_load_scenario(self, instrument_service):
        """Test service handling of spike load"""
        # Simulate traffic spike
        normal_load = 10  # operations
        spike_load = 50   # operations
        
        print(f"\nSpike Load Test:")
        print(f"  Normal load: {normal_load} operations")
        print(f"  Spike load: {spike_load} operations")
        
        # Normal load phase
        normal_tasks = [instrument_service.validate_symbol("AAPL") for _ in range(normal_load)]
        normal_start = time.time()
        normal_results = await asyncio.gather(*normal_tasks, return_exceptions=True)
        normal_time = time.time() - normal_start
        
        normal_success = sum(1 for r in normal_results if not isinstance(r, Exception))
        
        # Spike load phase
        spike_tasks = [instrument_service.validate_symbol("GOOGL") for _ in range(spike_load)]
        spike_start = time.time()
        spike_results = await asyncio.gather(*spike_tasks, return_exceptions=True)
        spike_time = time.time() - spike_start
        
        spike_success = sum(1 for r in spike_results if not isinstance(r, Exception))
        
        print(f"\nSpike Load Results:")
        print(f"  Normal phase: {normal_success}/{normal_load} success in {normal_time:.2f}s")
        print(f"  Spike phase: {spike_success}/{spike_load} success in {spike_time:.2f}s")
        print(f"  Normal ops/sec: {normal_success/normal_time:.2f}")
        print(f"  Spike ops/sec: {spike_success/spike_time:.2f}")
        
        # Verify spike handling
        normal_success_rate = normal_success / normal_load
        spike_success_rate = spike_success / spike_load
        
        assert normal_success_rate >= 0.9, f"Normal load success rate too low: {normal_success_rate}"
        assert spike_success_rate >= 0.7, f"Spike load success rate too low: {spike_success_rate}"


if __name__ == "__main__":
    # Run with: pytest tests/performance/test_instrument_service_benchmarks.py -v -s
    pytest.main([__file__, "-v", "-s", "--tb=short"])