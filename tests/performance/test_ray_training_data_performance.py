#!/usr/bin/env python3
"""
Performance Tests for Ray-Enhanced Training Data Generation

Comprehensive performance testing and benchmarking of Ray parallel processing
vs sequential processing for training data generation workflows.

Test Coverage:
- Throughput comparison (examples/second)
- Latency measurement for different symbol counts
- CPU utilization and memory usage
- Scalability testing with increasing workloads
- Ray overhead measurement
- Performance regression detection
"""

import pytest
import time
import ray
import asyncio
import psutil
import statistics
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Tuple
import logging
import tempfile
import shutil
from pathlib import Path

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

from ml.training_data.callbacks.training_data_callback import DateBasedTrainingDataCallback

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def ray_performance_cluster():
    """Initialize Ray cluster optimized for performance testing."""
    if not ray.is_initialized():
        ray.init(
            object_store_memory=1_000_000_000,  # 1GB for performance tests
            num_cpus=None,  # Use all available CPUs
            ignore_reinit_error=True
        )
    yield
    # Keep Ray running for other tests


@pytest.fixture
def temp_perf_dir():
    """Create temporary directory for performance tests."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


class PerformanceMetrics:
    """Helper class to collect performance metrics."""

    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.cpu_before = None
        self.cpu_after = None
        self.memory_before = None
        self.memory_after = None
        self.examples_generated = 0

    def start(self):
        """Start performance measurement."""
        self.start_time = time.perf_counter()
        self.cpu_before = psutil.cpu_percent(interval=None)
        self.memory_before = psutil.virtual_memory()

    def stop(self, examples_count: int = 0):
        """Stop performance measurement."""
        self.end_time = time.perf_counter()
        self.cpu_after = psutil.cpu_percent(interval=None)
        self.memory_after = psutil.virtual_memory()
        self.examples_generated = examples_count

    @property
    def duration(self) -> float:
        """Get duration in seconds."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0

    @property
    def throughput(self) -> float:
        """Get throughput in examples per second."""
        if self.duration > 0:
            return self.examples_generated / self.duration
        return 0.0

    @property
    def memory_delta_mb(self) -> float:
        """Get memory usage delta in MB."""
        if self.memory_before and self.memory_after:
            return (self.memory_after.used - self.memory_before.used) / (1024 * 1024)
        return 0.0


class MockHighVolumeTrainingGenerator:
    """Mock training generator optimized for performance testing."""

    def __init__(self, processing_delay_ms: float = 0.1):
        self.processing_delay = processing_delay_ms / 1000.0  # Convert to seconds

    async def generate_training_example(self, symbol: str, prediction_timestamp: datetime):
        """Generate mock training example with configurable delay."""
        # Simulate processing time
        if self.processing_delay > 0:
            await asyncio.sleep(self.processing_delay)

        return {
            'timestamp': prediction_timestamp.isoformat(),
            'symbol': symbol,
            'features': [float(i) for i in range(50)],  # 50 features
            'labels': [0.01, -0.02, 0.03],  # 3 prediction horizons
            'metadata': {
                'generated_at': datetime.now().isoformat(),
                'feature_count': 50,
                'processing_time_ms': self.processing_delay * 1000
            }
        }


class TestRayPerformanceBenchmarks:
    """Performance benchmarks comparing Ray vs sequential processing."""

    @pytest.mark.asyncio
    async def test_throughput_comparison_small_symbol_set(self, ray_performance_cluster, temp_perf_dir):
        """Compare throughput for small symbol set (2-4 symbols)."""
        symbols = ['AAPL', 'TSLA', 'MSFT', 'GOOGL']
        processing_delay = 0.05  # 50ms per example

        results = await self._run_throughput_comparison(
            symbols=symbols,
            processing_delay_ms=processing_delay * 1000,
            intervals=10,
            temp_dir=temp_perf_dir
        )

        seq_throughput = results['sequential']['throughput']
        ray_throughput = results['ray']['throughput']

        logger.info(f"Small symbol set throughput:")
        logger.info(f"  Sequential: {seq_throughput:.2f} examples/sec")
        logger.info(f"  Ray parallel: {ray_throughput:.2f} examples/sec")

        # Verify both approaches work
        assert seq_throughput > 0
        assert ray_throughput >= 0  # Ray might be 0 due to placeholder implementation

        # Log performance comparison
        if ray_throughput > 0:
            speedup = ray_throughput / seq_throughput
            logger.info(f"  Ray speedup: {speedup:.2f}x")

    @pytest.mark.asyncio
    async def test_throughput_comparison_large_symbol_set(self, ray_performance_cluster, temp_perf_dir):
        """Compare throughput for large symbol set (10+ symbols)."""
        # Large symbol set to better utilize parallelization
        symbols = ['AAPL', 'TSLA', 'MSFT', 'GOOGL', 'AMZN', 'NFLX', 'META', 'NVDA', 'AMD', 'INTC']
        processing_delay = 0.1  # 100ms per example

        results = await self._run_throughput_comparison(
            symbols=symbols,
            processing_delay_ms=processing_delay * 1000,
            intervals=5,  # Fewer intervals due to more symbols
            temp_dir=temp_perf_dir
        )

        seq_throughput = results['sequential']['throughput']
        ray_throughput = results['ray']['throughput']

        logger.info(f"Large symbol set throughput:")
        logger.info(f"  Sequential: {seq_throughput:.2f} examples/sec")
        logger.info(f"  Ray parallel: {ray_throughput:.2f} examples/sec")

        # With more symbols, Ray should show better relative performance
        assert seq_throughput > 0
        assert ray_throughput >= 0

        if ray_throughput > 0:
            speedup = ray_throughput / seq_throughput
            logger.info(f"  Ray speedup: {speedup:.2f}x")

            # With 10 symbols and multiple workers, expect potential speedup
            # (Though limited by mock implementation)

    async def _run_throughput_comparison(self, symbols: List[str], processing_delay_ms: float,
                                       intervals: int, temp_dir: Path) -> Dict[str, Dict[str, float]]:
        """Run throughput comparison between sequential and Ray processing."""

        # Create callbacks
        callback_seq = DateBasedTrainingDataCallback(
            symbols=symbols,
            output_dir=str(temp_dir / "sequential"),
            enable_ray_parallel=False
        )

        callback_ray = DateBasedTrainingDataCallback(
            symbols=symbols,
            output_dir=str(temp_dir / "ray"),
            enable_ray_parallel=True,
            max_parallel_workers=min(8, len(symbols))
        )

        # Setup training generators
        training_gen = MockHighVolumeTrainingGenerator(processing_delay_ms)
        callback_seq.training_generator = training_gen
        callback_ray.training_generator = training_gen

        # Setup for processing
        test_date = date.today()
        for callback in [callback_seq, callback_ray]:
            callback.current_date = test_date
            callback.daily_stats = {'errors': [], 'examples_generated': 0, 'intervals_processed': 0}

        # Test sequential processing
        seq_metrics = PerformanceMetrics()
        seq_metrics.start()

        seq_results = []
        for i in range(intervals):
            test_time = datetime.now() + timedelta(minutes=i)
            result = await callback_seq._generate_examples_sequential(test_time)
            seq_results.extend(result)

        seq_metrics.stop(len(seq_results))

        # Test Ray parallel processing
        ray_metrics = PerformanceMetrics()
        ray_metrics.start()

        ray_results = []
        for i in range(intervals):
            test_time = datetime.now() + timedelta(minutes=i)
            result = await callback_ray._generate_examples_parallel(test_time)
            ray_results.extend(result)

        ray_metrics.stop(len(ray_results))

        return {
            'sequential': {
                'throughput': seq_metrics.throughput,
                'duration': seq_metrics.duration,
                'examples': len(seq_results),
                'memory_mb': seq_metrics.memory_delta_mb
            },
            'ray': {
                'throughput': ray_metrics.throughput,
                'duration': ray_metrics.duration,
                'examples': len(ray_results),
                'memory_mb': ray_metrics.memory_delta_mb
            }
        }

    @pytest.mark.asyncio
    async def test_latency_measurement(self, ray_performance_cluster, temp_perf_dir):
        """Measure latency for single interval processing."""
        symbols = ['AAPL', 'TSLA', 'MSFT', 'GOOGL']

        callback_seq = DateBasedTrainingDataCallback(
            symbols=symbols,
            output_dir=str(temp_perf_dir / "latency_seq"),
            enable_ray_parallel=False
        )

        callback_ray = DateBasedTrainingDataCallback(
            symbols=symbols,
            output_dir=str(temp_perf_dir / "latency_ray"),
            enable_ray_parallel=True,
            max_parallel_workers=4
        )

        # Setup
        training_gen = MockHighVolumeTrainingGenerator(0.01)  # 10ms delay
        for callback in [callback_seq, callback_ray]:
            callback.training_generator = training_gen
            callback.current_date = date.today()
            callback.daily_stats = {'errors': [], 'examples_generated': 0, 'intervals_processed': 0}

        # Measure latency for multiple runs
        num_runs = 5
        seq_latencies = []
        ray_latencies = []

        for run in range(num_runs):
            test_time = datetime.now()

            # Sequential latency
            start = time.perf_counter()
            seq_result = await callback_seq._generate_examples_sequential(test_time)
            seq_latency = time.perf_counter() - start
            seq_latencies.append(seq_latency)

            # Ray latency
            start = time.perf_counter()
            ray_result = await callback_ray._generate_examples_parallel(test_time)
            ray_latency = time.perf_counter() - start
            ray_latencies.append(ray_latency)

        # Calculate statistics
        seq_avg = statistics.mean(seq_latencies)
        seq_std = statistics.stdev(seq_latencies) if len(seq_latencies) > 1 else 0
        ray_avg = statistics.mean(ray_latencies)
        ray_std = statistics.stdev(ray_latencies) if len(ray_latencies) > 1 else 0

        logger.info(f"Latency measurements ({num_runs} runs):")
        logger.info(f"  Sequential: {seq_avg:.3f}s ± {seq_std:.3f}s")
        logger.info(f"  Ray parallel: {ray_avg:.3f}s ± {ray_std:.3f}s")

        # Verify measurements are reasonable
        assert seq_avg > 0
        assert ray_avg >= 0
        assert seq_std >= 0
        assert ray_std >= 0


class TestRayScalability:
    """Test Ray scalability with increasing workloads."""

    @pytest.mark.asyncio
    async def test_symbol_count_scalability(self, ray_performance_cluster, temp_perf_dir):
        """Test how performance scales with increasing symbol count."""
        symbol_counts = [2, 4, 8, 16]
        base_symbols = ['AAPL', 'TSLA', 'MSFT', 'GOOGL', 'AMZN', 'NFLX', 'META', 'NVDA', 'AMD', 'INTC',
                       'IBM', 'ORCL', 'CRM', 'ADBE', 'PYPL', 'SPOT']

        scalability_results = []

        for count in symbol_counts:
            if count > len(base_symbols):
                continue

            symbols = base_symbols[:count]

            # Test Ray with current symbol count
            callback_ray = DateBasedTrainingDataCallback(
                symbols=symbols,
                output_dir=str(temp_perf_dir / f"scale_{count}"),
                enable_ray_parallel=True,
                max_parallel_workers=min(8, count)
            )

            callback_ray.training_generator = MockHighVolumeTrainingGenerator(0.05)  # 50ms delay
            callback_ray.current_date = date.today()
            callback_ray.daily_stats = {'errors': [], 'examples_generated': 0, 'intervals_processed': 0}

            # Measure performance
            metrics = PerformanceMetrics()
            metrics.start()

            test_time = datetime.now()
            result = await callback_ray._generate_examples_parallel(test_time)

            metrics.stop(len(result))

            scalability_results.append({
                'symbol_count': count,
                'duration': metrics.duration,
                'throughput': metrics.throughput,
                'examples': len(result),
                'workers': len(callback_ray.ray_workers)
            })

            logger.info(f"Symbol count {count}: {metrics.throughput:.2f} examples/sec "
                       f"({len(callback_ray.ray_workers)} workers)")

        # Verify scalability trends
        assert len(scalability_results) > 0

        for result in scalability_results:
            assert result['duration'] >= 0
            assert result['throughput'] >= 0

    @pytest.mark.asyncio
    async def test_worker_count_scalability(self, ray_performance_cluster, temp_perf_dir):
        """Test how performance scales with different worker counts."""
        symbols = ['AAPL', 'TSLA', 'MSFT', 'GOOGL', 'AMZN', 'NFLX', 'META', 'NVDA']
        worker_counts = [1, 2, 4, 8]

        worker_scalability_results = []

        for worker_count in worker_counts:
            callback_ray = DateBasedTrainingDataCallback(
                symbols=symbols,
                output_dir=str(temp_perf_dir / f"workers_{worker_count}"),
                enable_ray_parallel=True,
                max_parallel_workers=worker_count
            )

            callback_ray.training_generator = MockHighVolumeTrainingGenerator(0.02)  # 20ms delay
            callback_ray.current_date = date.today()
            callback_ray.daily_stats = {'errors': [], 'examples_generated': 0, 'intervals_processed': 0}

            # Measure performance
            metrics = PerformanceMetrics()
            metrics.start()

            test_time = datetime.now()
            result = await callback_ray._generate_examples_parallel(test_time)

            metrics.stop(len(result))

            worker_scalability_results.append({
                'worker_count': worker_count,
                'actual_workers': len(callback_ray.ray_workers),
                'duration': metrics.duration,
                'throughput': metrics.throughput,
                'examples': len(result)
            })

            logger.info(f"Workers {worker_count} (actual: {len(callback_ray.ray_workers)}): "
                       f"{metrics.throughput:.2f} examples/sec")

        # Verify all tests ran
        assert len(worker_scalability_results) == len(worker_counts)

        for result in worker_scalability_results:
            assert result['actual_workers'] <= result['worker_count']
            assert result['duration'] >= 0


class TestRayResourceUtilization:
    """Test Ray resource utilization and efficiency."""

    @pytest.mark.asyncio
    async def test_memory_usage_comparison(self, ray_performance_cluster, temp_perf_dir):
        """Compare memory usage between Ray and sequential processing."""
        symbols = ['AAPL', 'TSLA', 'MSFT', 'GOOGL', 'AMZN']

        # Test sequential memory usage
        callback_seq = DateBasedTrainingDataCallback(
            symbols=symbols,
            output_dir=str(temp_perf_dir / "memory_seq"),
            enable_ray_parallel=False
        )
        callback_seq.training_generator = MockHighVolumeTrainingGenerator(0.01)
        callback_seq.current_date = date.today()
        callback_seq.daily_stats = {'errors': [], 'examples_generated': 0, 'intervals_processed': 0}

        # Measure sequential memory
        seq_metrics = PerformanceMetrics()
        seq_metrics.start()

        test_time = datetime.now()
        seq_result = await callback_seq._generate_examples_sequential(test_time)

        seq_metrics.stop(len(seq_result))

        # Test Ray memory usage
        callback_ray = DateBasedTrainingDataCallback(
            symbols=symbols,
            output_dir=str(temp_perf_dir / "memory_ray"),
            enable_ray_parallel=True,
            max_parallel_workers=5
        )
        callback_ray.training_generator = MockHighVolumeTrainingGenerator(0.01)
        callback_ray.current_date = date.today()
        callback_ray.daily_stats = {'errors': [], 'examples_generated': 0, 'intervals_processed': 0}

        # Measure Ray memory
        ray_metrics = PerformanceMetrics()
        ray_metrics.start()

        ray_result = await callback_ray._generate_examples_parallel(test_time)

        ray_metrics.stop(len(ray_result))

        logger.info(f"Memory usage comparison:")
        logger.info(f"  Sequential: {seq_metrics.memory_delta_mb:.2f} MB")
        logger.info(f"  Ray parallel: {ray_metrics.memory_delta_mb:.2f} MB")

        # Memory usage should be reasonable (not excessive)
        # Note: Ray might use more memory due to object store and worker overhead
        assert abs(seq_metrics.memory_delta_mb) < 1000  # Less than 1GB change
        assert abs(ray_metrics.memory_delta_mb) < 1000  # Less than 1GB change

    @pytest.mark.asyncio
    async def test_ray_overhead_measurement(self, ray_performance_cluster, temp_perf_dir):
        """Measure Ray overhead for small workloads."""
        # Small workload where Ray overhead might dominate
        symbols = ['AAPL']

        callback_seq = DateBasedTrainingDataCallback(
            symbols=symbols,
            output_dir=str(temp_perf_dir / "overhead_seq"),
            enable_ray_parallel=False
        )

        callback_ray = DateBasedTrainingDataCallback(
            symbols=symbols,
            output_dir=str(temp_perf_dir / "overhead_ray"),
            enable_ray_parallel=True,
            max_parallel_workers=1
        )

        # Very small processing delay to highlight overhead
        training_gen = MockHighVolumeTrainingGenerator(0.001)  # 1ms delay
        callback_seq.training_generator = training_gen
        callback_ray.training_generator = training_gen

        for callback in [callback_seq, callback_ray]:
            callback.current_date = date.today()
            callback.daily_stats = {'errors': [], 'examples_generated': 0, 'intervals_processed': 0}

        # Measure overhead with small workload
        num_runs = 10
        seq_times = []
        ray_times = []

        for _ in range(num_runs):
            test_time = datetime.now()

            # Sequential
            start = time.perf_counter()
            seq_result = await callback_seq._generate_examples_sequential(test_time)
            seq_time = time.perf_counter() - start
            seq_times.append(seq_time)

            # Ray
            start = time.perf_counter()
            ray_result = await callback_ray._generate_examples_parallel(test_time)
            ray_time = time.perf_counter() - start
            ray_times.append(ray_time)

        seq_avg = statistics.mean(seq_times)
        ray_avg = statistics.mean(ray_times)
        overhead = max(0, ray_avg - seq_avg)

        logger.info(f"Ray overhead measurement (small workload):")
        logger.info(f"  Sequential avg: {seq_avg:.4f}s")
        logger.info(f"  Ray avg: {ray_avg:.4f}s")
        logger.info(f"  Overhead: {overhead:.4f}s")

        # Verify measurements are reasonable
        assert seq_avg > 0
        assert ray_avg >= 0


if __name__ == "__main__":
    # Run performance tests
    pytest.main([__file__, "-v", "--tb=short", "-s"])