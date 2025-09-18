#!/usr/bin/env python3
"""
Performance and Load Tests for Real-time Collection System

Tests system performance under various load conditions and scenarios:
- High-frequency data collection
- Large volume processing
- Memory usage optimization
- Database connection pooling
- Concurrent collection scenarios
"""

import pytest
import asyncio
import asyncpg
import logging
import psutil
import time
import statistics
import os
import sys
import gc

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from domains.market_data.services.realtime.aapl_tsla_synthetic_collector import AAPLTSLASyntheticCollector

logger = logging.getLogger(__name__)

@pytest.fixture
async def performance_db_pool():
    """High-performance database pool for load testing"""
    dsn = "postgresql://postgres:intg_password@localhost:4432/intg_db"
    try:
        pool = await asyncpg.create_pool(
            dsn,
            min_size=5,
            max_size=20,  # Higher connection limit for performance tests
            command_timeout=60,
            server_settings={'jit': 'off'}  # Disable JIT for consistent timing
        )

        # Ensure test tables exist
        async with pool.acquire() as conn:
            await conn.execute("CREATE TABLE IF NOT EXISTS perf_test_tiingo AS SELECT * FROM intg_one_minute_live_tiingo WHERE 1=0")
            await conn.execute("CREATE TABLE IF NOT EXISTS perf_test_polygon AS SELECT * FROM intg_one_minute_live_polygon WHERE 1=0")

        yield pool

        # Cleanup
        async with pool.acquire() as conn:
            await conn.execute("DROP TABLE IF EXISTS perf_test_tiingo")
            await conn.execute("DROP TABLE IF EXISTS perf_test_polygon")

        await pool.close()

    except Exception as e:
        logger.warning(f"Cannot connect to performance database: {e}")
        pytest.skip("Performance database not available")


class PerformanceMonitor:
    """Monitor system performance during tests"""

    def __init__(self):
        self.start_time = None
        self.start_memory = None
        self.peak_memory = 0
        self.measurements = []

    def start(self):
        """Start monitoring"""
        self.start_time = time.time()
        self.start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        self.peak_memory = self.start_memory

    def measure(self, label: str = None):
        """Take a measurement"""
        current_time = time.time()
        current_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        cpu_percent = psutil.Process().cpu_percent()

        self.peak_memory = max(self.peak_memory, current_memory)

        measurement = {
            'label': label or f'measurement_{len(self.measurements)}',
            'elapsed_time': current_time - self.start_time,
            'memory_mb': current_memory,
            'memory_delta': current_memory - self.start_memory,
            'cpu_percent': cpu_percent,
            'timestamp': current_time
        }

        self.measurements.append(measurement)
        return measurement

    def report(self):
        """Generate performance report"""
        if not self.measurements:
            return "No measurements taken"

        total_time = self.measurements[-1]['elapsed_time']
        memory_growth = self.peak_memory - self.start_memory
        avg_cpu = statistics.mean(m['cpu_percent'] for m in self.measurements if m['cpu_percent'] > 0)

        return {
            'total_time': total_time,
            'memory_growth_mb': memory_growth,
            'peak_memory_mb': self.peak_memory,
            'avg_cpu_percent': avg_cpu,
            'measurements': len(self.measurements)
        }


class TestHighFrequencyCollection:
    """Test high-frequency data collection scenarios"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_rapid_successive_collections(self, performance_db_pool):
        """Test rapid successive data collections"""
        collector = AAPLTSLASyntheticCollector()
        collector.pool = performance_db_pool

        monitor = PerformanceMonitor()
        monitor.start()

        # Perform 100 rapid collections
        results = []
        for i in range(100):
            start_time = time.time()
            result = await collector.generate_and_store_data()
            collection_time = time.time() - start_time

            results.append({
                'iteration': i,
                'records_stored': result,
                'collection_time': collection_time
            })

            if i % 20 == 0:  # Monitor every 20 iterations
                monitor.measure(f'iteration_{i}')

        final_report = monitor.report()

        # Analyze results
        collection_times = [r['collection_time'] for r in results]
        avg_time = statistics.mean(collection_times)
        max_time = max(collection_times)
        min_time = min(collection_times)

        logger.info(f"Rapid collection: {len(results)} operations")
        logger.info(f"Collection times: avg={avg_time:.3f}s, max={max_time:.3f}s, min={min_time:.3f}s")
        logger.info(f"Memory growth: {final_report['memory_growth_mb']:.1f}MB")
        logger.info(f"Total time: {final_report['total_time']:.1f}s")

        # Performance assertions
        assert avg_time < 0.1  # Average collection should be under 100ms
        assert max_time < 0.5  # No single collection should exceed 500ms
        assert final_report['memory_growth_mb'] < 100  # Memory growth should be reasonable

        # Check consistency
        total_records = sum(r['records_stored'] for r in results)
        assert total_records > 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_concurrent_high_frequency_collectors(self, performance_db_pool):
        """Test multiple collectors running concurrently at high frequency"""
        num_collectors = 10
        collections_per_collector = 20

        collectors = [AAPLTSLASyntheticCollector() for _ in range(num_collectors)]
        for collector in collectors:
            collector.pool = performance_db_pool

        monitor = PerformanceMonitor()
        monitor.start()

        async def collector_task(collector_id, collector):
            """Task for a single collector"""
            results = []
            for i in range(collections_per_collector):
                start_time = time.time()
                result = await collector.generate_and_store_data()
                collection_time = time.time() - start_time

                results.append({
                    'collector_id': collector_id,
                    'iteration': i,
                    'records_stored': result,
                    'collection_time': collection_time
                })

                # Small delay to prevent overwhelming the database
                await asyncio.sleep(0.01)

            return results

        # Run all collectors concurrently
        tasks = [collector_task(i, collectors[i]) for i in range(num_collectors)]
        all_results = await asyncio.gather(*tasks)

        monitor.measure('concurrent_complete')
        final_report = monitor.report()

        # Flatten results
        flat_results = [item for sublist in all_results for item in sublist]

        # Analyze performance
        collection_times = [r['collection_time'] for r in flat_results]
        avg_time = statistics.mean(collection_times)
        percentile_95 = sorted(collection_times)[int(len(collection_times) * 0.95)]

        logger.info(f"Concurrent collection: {num_collectors} collectors, {len(flat_results)} total operations")
        logger.info(f"Performance: avg={avg_time:.3f}s, 95th percentile={percentile_95:.3f}s")
        logger.info(f"Throughput: {len(flat_results)/final_report['total_time']:.1f} ops/sec")

        # Performance assertions
        assert avg_time < 0.2  # Should maintain reasonable performance under concurrency
        assert percentile_95 < 1.0  # 95% of operations should complete within 1 second
        assert final_report['memory_growth_mb'] < 200  # Memory usage should be controlled


class TestLargeVolumeProcessing:
    """Test processing of large data volumes"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_extended_collection_session(self, performance_db_pool):
        """Test extended data collection session"""
        collector = AAPLTSLASyntheticCollector()
        collector.pool = performance_db_pool

        monitor = PerformanceMonitor()
        monitor.start()

        # Simulate 1 hour of minute-by-minute collection (60 collections)
        total_records = 0
        collection_times = []

        for minute in range(60):
            start_time = time.time()
            result = await collector.generate_and_store_data()
            collection_time = time.time() - start_time

            total_records += result
            collection_times.append(collection_time)

            if minute % 15 == 0:  # Monitor every 15 minutes
                monitor.measure(f'minute_{minute}')

            # Small delay to simulate real-time collection
            await asyncio.sleep(0.1)

        final_report = monitor.report()

        # Analyze long-term performance stability
        first_quarter = collection_times[:15]
        last_quarter = collection_times[-15:]

        avg_first = statistics.mean(first_quarter)
        avg_last = statistics.mean(last_quarter)
        performance_degradation = (avg_last - avg_first) / avg_first

        logger.info(f"Extended session: {total_records} total records over {final_report['total_time']:.1f}s")
        logger.info(f"Performance stability: {performance_degradation:.2%} degradation")
        logger.info(f"Memory usage: {final_report['peak_memory_mb']:.1f}MB peak")

        # Stability assertions
        assert performance_degradation < 0.5  # Performance shouldn't degrade more than 50%
        assert final_report['memory_growth_mb'] < 50  # Memory leaks should be minimal
        assert total_records > 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_bulk_historical_backfill(self, performance_db_pool):
        """Test bulk historical data backfill performance"""
        collector = AAPLTSLASyntheticCollector()
        collector.pool = performance_db_pool

        monitor = PerformanceMonitor()
        monitor.start()

        # Simulate backfilling 24 hours of minute data (1440 data points)
        batch_size = 100
        total_batches = 1440 // batch_size

        total_records = 0
        batch_times = []

        for batch_num in range(total_batches):
            batch_start = time.time()

            # Generate batch of data
            batch_records = 0
            for _ in range(batch_size):
                result = await collector.generate_and_store_data()
                batch_records += result

                # Very short delay to avoid overwhelming
                await asyncio.sleep(0.001)

            batch_time = time.time() - batch_start
            batch_times.append(batch_time)
            total_records += batch_records

            if batch_num % 5 == 0:
                monitor.measure(f'batch_{batch_num}')

        final_report = monitor.report()

        # Analyze batch processing performance
        avg_batch_time = statistics.mean(batch_times)
        throughput = total_records / final_report['total_time']

        logger.info(f"Bulk backfill: {total_records} records in {total_batches} batches")
        logger.info(f"Throughput: {throughput:.1f} records/second")
        logger.info(f"Batch performance: {avg_batch_time:.3f}s average")

        # Performance assertions
        assert throughput > 50  # Should process at least 50 records per second
        assert avg_batch_time < 5.0  # Batches should complete in reasonable time
        assert final_report['memory_growth_mb'] < 100


class TestMemoryOptimization:
    """Test memory usage optimization"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_memory_efficiency(self, performance_db_pool):
        """Test memory efficiency during sustained operations"""
        collector = AAPLTSLASyntheticCollector()
        collector.pool = performance_db_pool

        # Measure baseline memory
        baseline_memory = psutil.Process().memory_info().rss / 1024 / 1024

        # Run sustained operations
        memory_samples = []
        for i in range(200):
            await collector.generate_and_store_data()

            if i % 10 == 0:
                current_memory = psutil.Process().memory_info().rss / 1024 / 1024
                memory_samples.append(current_memory - baseline_memory)

                # Force garbage collection periodically
                if i % 50 == 0:
                    gc.collect()

        # Analyze memory usage pattern
        max_memory_growth = max(memory_samples)
        final_memory_growth = memory_samples[-1]
        memory_stability = statistics.stdev(memory_samples[-20:])  # Last 20 samples

        logger.info(f"Memory efficiency: max growth={max_memory_growth:.1f}MB")
        logger.info(f"Final memory growth: {final_memory_growth:.1f}MB")
        logger.info(f"Memory stability (stdev): {memory_stability:.1f}MB")

        # Memory efficiency assertions
        assert max_memory_growth < 100  # Should not exceed 100MB growth
        assert final_memory_growth < 50  # Final growth should be reasonable
        assert memory_stability < 10  # Memory usage should be stable

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_connection_pool_efficiency(self, performance_db_pool):
        """Test database connection pool efficiency"""
        collectors = [AAPLTSLASyntheticCollector() for _ in range(25)]  # More than pool size

        for collector in collectors:
            collector.pool = performance_db_pool

        monitor = PerformanceMonitor()
        monitor.start()

        # Test connection pool under pressure
        async def connection_stress_test():
            tasks = []
            for collector in collectors:
                tasks.append(collector.generate_and_store_data())

            results = await asyncio.gather(*tasks, return_exceptions=True)
            return results

        # Run multiple rounds to test pool reuse
        all_results = []
        for round_num in range(5):
            round_results = await connection_stress_test()
            all_results.extend(round_results)
            monitor.measure(f'round_{round_num}')
            await asyncio.sleep(0.1)  # Brief pause between rounds

        final_report = monitor.report()

        # Analyze connection handling
        exceptions = [r for r in all_results if isinstance(r, Exception)]
        successes = [r for r in all_results if isinstance(r, int)]

        success_rate = len(successes) / len(all_results)

        logger.info(f"Connection pool test: {len(all_results)} operations")
        logger.info(f"Success rate: {success_rate:.2%}")
        logger.info(f"Exceptions: {len(exceptions)}")

        # Connection pool assertions
        assert success_rate > 0.95  # Should handle connection pressure well
        assert len(exceptions) < 5  # Minimal exceptions expected


class TestScalabilityLimits:
    """Test system scalability limits"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_maximum_concurrent_operations(self, performance_db_pool):
        """Test maximum sustainable concurrent operations"""
        base_collectors = 50
        max_collectors = 100

        # Test increasing levels of concurrency
        for num_collectors in range(base_collectors, max_collectors + 1, 10):
            collectors = [AAPLTSLASyntheticCollector() for _ in range(num_collectors)]

            for collector in collectors:
                collector.pool = performance_db_pool

            start_time = time.time()

            # Run concurrent operations
            tasks = [collector.generate_and_store_data() for collector in collectors]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            elapsed = time.time() - start_time

            # Analyze results
            exceptions = [r for r in results if isinstance(r, Exception)]
            successes = [r for r in results if isinstance(r, int)]
            success_rate = len(successes) / len(results)
            throughput = len(successes) / elapsed

            logger.info(f"Concurrency {num_collectors}: success_rate={success_rate:.2%}, throughput={throughput:.1f}/s")

            # Check if we've hit scalability limits
            if success_rate < 0.8:
                logger.warning(f"Scalability limit reached at {num_collectors} concurrent operations")
                break

            # Brief pause between tests
            await asyncio.sleep(1.0)

        # Should handle at least base level of concurrency
        assert success_rate > 0.8

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_data_volume_limits(self, performance_db_pool):
        """Test data volume processing limits"""
        collector = AAPLTSLASyntheticCollector()
        collector.pool = performance_db_pool

        # Test increasingly large data volumes
        target_records = 10000
        batch_size = 500

        monitor = PerformanceMonitor()
        monitor.start()

        total_processed = 0
        processing_times = []

        while total_processed < target_records:
            batch_start = time.time()

            batch_records = 0
            for _ in range(batch_size):
                result = await collector.generate_and_store_data()
                batch_records += result

                if batch_records >= batch_size * 4:  # 4 records per operation (2 symbols × 2 vendors)
                    break

            batch_time = time.time() - batch_start
            processing_times.append(batch_time)
            total_processed += batch_records

            # Monitor memory usage
            if total_processed % 2000 == 0:
                monitor.measure(f'records_{total_processed}')

        final_report = monitor.report()

        # Analyze volume processing performance
        avg_batch_time = statistics.mean(processing_times)
        final_throughput = total_processed / final_report['total_time']

        logger.info(f"Volume processing: {total_processed} records")
        logger.info(f"Final throughput: {final_throughput:.1f} records/second")
        logger.info(f"Memory usage: {final_report['peak_memory_mb']:.1f}MB peak")

        # Volume processing assertions
        assert total_processed >= target_records
        assert final_throughput > 20  # Should maintain reasonable throughput
        assert final_report['memory_growth_mb'] < 200


class TestPerformanceRegression:
    """Test for performance regressions"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_baseline_performance_benchmark(self, performance_db_pool):
        """Establish baseline performance benchmark"""
        collector = AAPLTSLASyntheticCollector()
        collector.pool = performance_db_pool

        # Standard benchmark test
        num_operations = 100

        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024

        results = []
        for i in range(num_operations):
            op_start = time.time()
            result = await collector.generate_and_store_data()
            op_time = time.time() - op_start

            results.append({
                'operation': i,
                'records': result,
                'time': op_time
            })

        total_time = time.time() - start_time
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024

        # Calculate benchmark metrics
        operation_times = [r['time'] for r in results]
        avg_operation_time = statistics.mean(operation_times)
        p95_operation_time = sorted(operation_times)[int(len(operation_times) * 0.95)]
        total_records = sum(r['records'] for r in results)
        throughput = total_records / total_time
        memory_usage = end_memory - start_memory

        benchmark_results = {
            'avg_operation_time': avg_operation_time,
            'p95_operation_time': p95_operation_time,
            'throughput': throughput,
            'memory_usage_mb': memory_usage,
            'total_time': total_time,
            'operations': num_operations,
            'records': total_records
        }

        logger.info("Performance Benchmark Results:")
        for key, value in benchmark_results.items():
            logger.info(f"  {key}: {value}")

        # Baseline performance expectations
        assert avg_operation_time < 0.1  # < 100ms per operation
        assert p95_operation_time < 0.2   # < 200ms for 95th percentile
        assert throughput > 100           # > 100 records/second
        assert memory_usage < 50          # < 50MB memory growth

        return benchmark_results


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])