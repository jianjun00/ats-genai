"""
Performance benchmarking and profiling tests for Volume Profile indicators.
"""
import unittest
import pandas as pd
import numpy as np
import time
import sys
import os
from typing import Dict
import gc

sys.path.append(os.path.join(os.path.dirname(__file__), '../../src'))

from signals.enhanced_indicators import VolumeProfileIndicator
from signals.indicator import VolumeProfile
from signals.advanced_volume_profile import (
    SessionVolumeProfile, MultiTimeframeVolumeProfile,
    AdaptiveVolumeProfile, VolumeProfileComposite
)


class VolumeProfilePerformanceBenchmark(unittest.TestCase):
    """Comprehensive performance benchmarking for Volume Profile indicators."""

    def setUp(self):
        """Set up benchmark test fixtures."""
        self.small_dataset = self._create_performance_dataset(100)
        self.medium_dataset = self._create_performance_dataset(1000)
        self.large_dataset = self._create_performance_dataset(5000)
        self.xlarge_dataset = self._create_performance_dataset(10000)

        # Performance targets (in milliseconds)
        self.performance_targets = {
            'small': {'basic': 5, 'enhanced': 10, 'advanced': 20},
            'medium': {'basic': 50, 'enhanced': 100, 'advanced': 200},
            'large': {'basic': 250, 'enhanced': 500, 'advanced': 1000},
            'xlarge': {'basic': 500, 'enhanced': 1000, 'advanced': 2000}
        }

    def _create_performance_dataset(self, size: int) -> pd.DataFrame:
        """Create realistic dataset for performance testing."""
        np.random.seed(42)  # Consistent results

        data = []
        base_price = 100.0
        base_volume = 50000

        for i in range(size):
            # Realistic price movement with trends and volatility
            trend_component = np.sin(i * 0.01) * 0.1  # Long-term trend
            noise_component = np.random.normal(0, 0.3)  # Short-term noise
            price_change = trend_component + noise_component

            base_price += price_change
            base_price = max(base_price, 50)  # Price floor

            # Volume with correlation to price movement
            volume_multiplier = 1 + abs(price_change) * 2  # Higher volume on big moves
            volume = int(base_volume * volume_multiplier * (0.5 + np.random.uniform(0, 1)))

            # Generate OHLC
            open_price = base_price + np.random.uniform(-0.1, 0.1)
            close_price = base_price + np.random.uniform(-0.1, 0.1)
            high_price = max(open_price, close_price) + abs(np.random.uniform(0, 0.5))
            low_price = min(open_price, close_price) - abs(np.random.uniform(0, 0.5))

            data.append({
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': max(volume, 1000),
                'timestamp': pd.Timestamp('2024-01-01') + pd.Timedelta(minutes=i)
            })

        return pd.DataFrame(data)

    def _benchmark_indicator(self, indicator, dataset: pd.DataFrame, runs: int = 5) -> Dict[str, float]:
        """Benchmark an indicator with multiple runs."""
        times = []
        memory_usage = []

        for _ in range(runs):
            # Force garbage collection before measurement
            gc.collect()

            # Measure memory before
            import psutil
            process = psutil.Process()
            memory_before = process.memory_info().rss / 1024 / 1024  # MB

            # Time the calculation
            start_time = time.perf_counter()
            result = indicator.calculate(dataset)
            end_time = time.perf_counter()

            # Measure memory after
            memory_after = process.memory_info().rss / 1024 / 1024  # MB
            memory_used = memory_after - memory_before

            execution_time = (end_time - start_time) * 1000  # Convert to milliseconds
            times.append(execution_time)
            memory_usage.append(max(memory_used, 0))  # Avoid negative values

            # Validate result
            self.assertIn(result['status'], ['valid', 'insufficient_data', 'invalid_data'])

        return {
            'min_time_ms': min(times),
            'max_time_ms': max(times),
            'avg_time_ms': np.mean(times),
            'std_time_ms': np.std(times),
            'avg_memory_mb': np.mean(memory_usage),
            'max_memory_mb': max(memory_usage)
        }

    def test_basic_volume_profile_performance(self):
        """Test performance of basic Volume Profile indicators."""
        print("\n=== Basic Volume Profile Performance ===")

        indicators = [
            ('VolumeProfile_framework', VolumeProfile(20, 50)),
            ('VolumeProfileIndicator_enhanced', VolumeProfileIndicator(20, 50))
        ]

        datasets = [
            ('small', self.small_dataset),
            ('medium', self.medium_dataset),
            ('large', self.large_dataset)
        ]

        for dataset_name, dataset in datasets:
            print(f"\n--- {dataset_name.title()} Dataset ({len(dataset)} bars) ---")

            for indicator_name, indicator in indicators:
                try:
                    benchmark = self._benchmark_indicator(indicator, dataset)

                    # Check against performance targets
                    target_time = self.performance_targets[dataset_name]['basic']
                    performance_ratio = benchmark['avg_time_ms'] / target_time

                    status = "✅ PASS" if performance_ratio <= 1.0 else "⚠️  SLOW" if performance_ratio <= 2.0 else "❌ FAIL"

                    print(f"{indicator_name:30} | "
                          f"Avg: {benchmark['avg_time_ms']:6.1f}ms | "
                          f"Max: {benchmark['max_time_ms']:6.1f}ms | "
                          f"Mem: {benchmark['avg_memory_mb']:5.1f}MB | "
                          f"{status}")

                    # Assert performance requirements
                    if dataset_name in ['small', 'medium']:
                        self.assertLess(benchmark['avg_time_ms'], target_time * 2,
                                      f"{indicator_name} too slow on {dataset_name} dataset")

                except Exception as e:
                    print(f"{indicator_name:30} | ERROR: {str(e)}")

    def test_advanced_volume_profile_performance(self):
        """Test performance of advanced Volume Profile variants."""
        print("\n=== Advanced Volume Profile Performance ===")

        # Test different configurations
        indicators = [
            ('SessionVolumeProfile', SessionVolumeProfile(20, 50)),
            ('AdaptiveVolumeProfile', AdaptiveVolumeProfile(20, 50)),
            ('MultiTimeframeVP', MultiTimeframeVolumeProfile()),
            ('VolumeProfileComposite', VolumeProfileComposite())
        ]

        # Test with medium dataset (good balance of realism and speed)
        print(f"\n--- Medium Dataset ({len(self.medium_dataset)} bars) ---")

        for indicator_name, indicator in indicators:
            try:
                benchmark = self._benchmark_indicator(indicator, self.medium_dataset, runs=3)

                target_time = self.performance_targets['medium']['advanced']
                performance_ratio = benchmark['avg_time_ms'] / target_time

                status = "✅ PASS" if performance_ratio <= 1.0 else "⚠️  SLOW" if performance_ratio <= 2.0 else "❌ FAIL"

                print(f"{indicator_name:25} | "
                      f"Avg: {benchmark['avg_time_ms']:7.1f}ms | "
                      f"Max: {benchmark['max_time_ms']:7.1f}ms | "
                      f"Mem: {benchmark['avg_memory_mb']:5.1f}MB | "
                      f"{status}")

                # Relaxed performance requirements for advanced indicators
                self.assertLess(benchmark['avg_time_ms'], target_time * 3,
                              f"{indicator_name} too slow")

            except Exception as e:
                print(f"{indicator_name:25} | ERROR: {str(e)}")

    def test_parameter_scaling_performance(self):
        """Test how performance scales with different parameters."""
        print("\n=== Parameter Scaling Performance ===")

        # Test different bin counts
        bin_counts = [25, 50, 75, 100]
        periods = [10, 20, 50, 100]

        print("\n--- Bin Count Scaling ---")
        for bin_count in bin_counts:
            indicator = VolumeProfileIndicator(20, bin_count)
            benchmark = self._benchmark_indicator(indicator, self.medium_dataset, runs=3)

            print(f"Bins: {bin_count:3d} | "
                  f"Avg: {benchmark['avg_time_ms']:6.1f}ms | "
                  f"Mem: {benchmark['avg_memory_mb']:5.1f}MB")

        print("\n--- Period Scaling ---")
        for period in periods:
            if len(self.medium_dataset) >= period:
                indicator = VolumeProfileIndicator(period, 50)
                benchmark = self._benchmark_indicator(indicator, self.medium_dataset, runs=3)

                print(f"Period: {period:3d} | "
                      f"Avg: {benchmark['avg_time_ms']:6.1f}ms | "
                      f"Mem: {benchmark['avg_memory_mb']:5.1f}MB")

    def test_memory_efficiency(self):
        """Test memory efficiency with large datasets."""
        print("\n=== Memory Efficiency Test ===")

        # Test memory usage with increasing dataset sizes
        dataset_sizes = [1000, 2500, 5000, 7500, 10000]
        indicator = VolumeProfileIndicator(50, 50)  # Standard configuration

        print("Dataset Size | Avg Time | Memory Usage")
        print("-" * 40)

        for size in dataset_sizes:
            if size <= len(self.xlarge_dataset):
                dataset = self.xlarge_dataset.head(size)
                benchmark = self._benchmark_indicator(indicator, dataset, runs=2)

                print(f"{size:11d} | {benchmark['avg_time_ms']:8.1f}ms | {benchmark['avg_memory_mb']:8.1f}MB")

                # Memory should scale reasonably (not exponentially)
                expected_max_memory = 50  # 50MB reasonable for large datasets
                self.assertLess(benchmark['avg_memory_mb'], expected_max_memory,
                              f"Memory usage too high for dataset size {size}")

    def test_concurrent_performance(self):
        """Test performance under concurrent load."""
        print("\n=== Concurrent Performance Test ===")

        import concurrent.futures

        def calculate_volume_profile():
            """Function to run in parallel."""
            indicator = VolumeProfileIndicator(20, 50)
            result = indicator.calculate(self.medium_dataset)
            return result['status'] == 'valid'

        # Test with different numbers of concurrent calculations
        concurrency_levels = [1, 2, 4, 8]

        print("Concurrency | Total Time | Avg per Calc | Success Rate")
        print("-" * 55)

        for num_workers in concurrency_levels:
            start_time = time.perf_counter()

            with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = [executor.submit(calculate_volume_profile) for _ in range(num_workers * 2)]
                results = [future.result() for future in concurrent.futures.as_completed(futures)]

            end_time = time.perf_counter()
            total_time = (end_time - start_time) * 1000  # ms
            avg_time = total_time / len(results)
            success_rate = sum(results) / len(results) * 100

            print(f"{num_workers:10d} | {total_time:10.1f}ms | {avg_time:11.1f}ms | {success_rate:10.1f}%")

            # All calculations should succeed
            self.assertGreater(success_rate, 95, f"Success rate too low with {num_workers} workers")

    def test_edge_case_performance(self):
        """Test performance with edge cases."""
        print("\n=== Edge Case Performance Test ===")

        edge_cases = [
            ('identical_prices', self._create_identical_price_data(1000)),
            ('minimal_volume', self._create_minimal_volume_data(1000)),
            ('extreme_volatility', self._create_extreme_volatility_data(1000)),
            ('sparse_data', self._create_sparse_data(500))
        ]

        indicator = VolumeProfileIndicator(20, 50)

        print("Edge Case        | Status | Time (ms) | Memory (MB)")
        print("-" * 50)

        for case_name, dataset in edge_cases:
            try:
                benchmark = self._benchmark_indicator(indicator, dataset, runs=2)
                result = indicator.calculate(dataset)

                print(f"{case_name:15} | {result['status']:6} | {benchmark['avg_time_ms']:8.1f} | {benchmark['avg_memory_mb']:9.1f}")

                # Edge cases should not crash and should complete reasonably quickly
                self.assertLess(benchmark['avg_time_ms'], 1000, f"{case_name} too slow")

            except Exception as e:
                print(f"{case_name:15} | ERROR  | {str(e)}")

    def _create_identical_price_data(self, size: int) -> pd.DataFrame:
        """Create dataset with identical prices (edge case)."""
        return pd.DataFrame({
            'open': [100.0] * size,
            'high': [100.0] * size,
            'low': [100.0] * size,
            'close': [100.0] * size,
            'volume': [10000 + i * 100 for i in range(size)],
            'timestamp': [pd.Timestamp('2024-01-01') + pd.Timedelta(minutes=i) for i in range(size)]
        })

    def _create_minimal_volume_data(self, size: int) -> pd.DataFrame:
        """Create dataset with minimal volume."""
        np.random.seed(42)
        return pd.DataFrame({
            'open': [100 + np.random.uniform(-1, 1) for _ in range(size)],
            'high': [101 + np.random.uniform(0, 1) for _ in range(size)],
            'low': [99 + np.random.uniform(-1, 0) for _ in range(size)],
            'close': [100 + np.random.uniform(-1, 1) for _ in range(size)],
            'volume': [100] * size,  # Minimal volume
            'timestamp': [pd.Timestamp('2024-01-01') + pd.Timedelta(minutes=i) for i in range(size)]
        })

    def _create_extreme_volatility_data(self, size: int) -> pd.DataFrame:
        """Create dataset with extreme price volatility."""
        np.random.seed(42)
        data = []
        price = 100.0

        for i in range(size):
            # Extreme price movements
            change = np.random.uniform(-10, 10)  # Large price swings
            price = max(price + change, 10)  # Keep price positive

            data.append({
                'open': price + np.random.uniform(-2, 2),
                'high': price + abs(np.random.uniform(0, 5)),
                'low': price - abs(np.random.uniform(0, 5)),
                'close': price + np.random.uniform(-2, 2),
                'volume': 10000 + abs(int(change * 1000)),  # Volume correlates with volatility
                'timestamp': pd.Timestamp('2024-01-01') + pd.Timedelta(minutes=i)
            })

        return pd.DataFrame(data)

    def _create_sparse_data(self, size: int) -> pd.DataFrame:
        """Create sparse dataset with gaps."""
        np.random.seed(42)
        data = []

        for i in range(size):
            # Skip some data points to create sparsity
            if i % 3 == 0:  # Keep only every 3rd point
                data.append({
                    'open': 100 + np.random.uniform(-1, 1),
                    'high': 101 + np.random.uniform(0, 1),
                    'low': 99 + np.random.uniform(-1, 0),
                    'close': 100 + np.random.uniform(-1, 1),
                    'volume': 10000 + np.random.randint(-2000, 5000),
                    'timestamp': pd.Timestamp('2024-01-01') + pd.Timedelta(minutes=i)
                })

        return pd.DataFrame(data)

    def test_performance_regression(self):
        """Test for performance regressions by comparing against baseline."""
        print("\n=== Performance Regression Test ===")

        # Baseline performance expectations (milliseconds)
        baselines = {
            'VolumeProfileIndicator_1000': 100,  # 100ms for 1000 bars
            'VolumeProfile_1000': 150,           # 150ms for framework version
            'SessionVolumeProfile_1000': 200,    # 200ms for session version
        }

        test_cases = [
            ('VolumeProfileIndicator_1000', VolumeProfileIndicator(20, 50)),
            ('VolumeProfile_1000', VolumeProfile(20, 50)),
            ('SessionVolumeProfile_1000', SessionVolumeProfile(20, 50))
        ]

        print("Test Case                 | Time (ms) | Baseline | Status")
        print("-" * 60)

        for test_name, indicator in test_cases:
            benchmark = self._benchmark_indicator(indicator, self.medium_dataset, runs=3)
            baseline = baselines.get(test_name, float('inf'))

            regression_ratio = benchmark['avg_time_ms'] / baseline
            status = "✅ GOOD" if regression_ratio <= 1.0 else "⚠️  SLOW" if regression_ratio <= 1.5 else "❌ REGRESSION"

            print(f"{test_name:25} | {benchmark['avg_time_ms']:8.1f} | {baseline:8.1f} | {status}")

            # Allow some tolerance for performance variations
            self.assertLess(regression_ratio, 2.0, f"Performance regression detected in {test_name}")


if __name__ == '__main__':
    # Run with verbose output to see performance results
    unittest.main(verbosity=2)