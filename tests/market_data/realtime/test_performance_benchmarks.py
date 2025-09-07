#!/usr/bin/env python3
"""
Performance Benchmark Tests for Real-Time Market Data Collection System

Tests cover:
- Data throughput benchmarks
- Latency measurements
- Memory usage profiling
- Concurrent processing limits
- Database performance
- API response time benchmarks
"""

import pytest
import asyncio
import time
import psutil
import tracemalloc
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta, timezone
import json
import os
import statistics
from concurrent.futures import ThreadPoolExecutor

# Import the modules under test
import sys
sys.path.append('src')

from domains.market_data.services.realtime.streaming_collector import RealtimeStreamingCollector, MinuteBar
from domains.market_data.services.realtime.gap_detector import GapDetectionEngine
from domains.market_data.services.realtime.weekly_backfill import WeeklyBackfillEngine
from domains.market_data.services.realtime.metrics_exporter import MetricsCollector

@pytest.mark.benchmark
class TestDataThroughputBenchmarks:
    """Benchmark data processing throughput"""

    @pytest.fixture
    async def benchmark_collector(self):
        """Set up collector for benchmarking"""
        with patch('market_data.realtime.streaming_collector.Environment'):
            mock_pool = AsyncMock()
            mock_conn = AsyncMock()
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

            with patch('market_data.realtime.streaming_collector.asyncpg.create_pool', return_value=mock_pool):
                collector = RealtimeStreamingCollector()
                await collector.initialize()

                # Set up test universe
                collector.universe_symbols = {f'SYM{i:04d}' for i in range(100)}
                collector.instrument_mapping = {f'SYM{i:04d}': i for i in range(100)}

                yield collector
                await collector.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_minute_bar_processing_throughput(self, benchmark_collector):
        """Benchmark minute bar processing throughput"""

        # Generate test data
        test_data = []
        base_timestamp = datetime.now(timezone.utc)

        for i in range(1000):  # 1000 minute bars
            data = {
                'ev': 'AM',
                'sym': f'SYM{i % 100:04d}',
                't': int((base_timestamp + timedelta(minutes=i)).timestamp() * 1000),
                'o': 150.0 + (i % 50) * 0.1,
                'h': 152.0 + (i % 50) * 0.1,
                'l': 149.0 + (i % 50) * 0.1,
                'c': 151.0 + (i % 50) * 0.1,
                'v': 1000000 + i * 1000,
                'vw': 150.5 + (i % 50) * 0.1,
                'n': 500 + i
            }
            test_data.append(data)

        # Benchmark processing
        start_time = time.time()

        tasks = []
        for data in test_data:
            task = asyncio.create_task(
                benchmark_collector._process_polygon_minute_bar(data)
            )
            tasks.append(task)

        await asyncio.gather(*tasks)

        end_time = time.time()
        processing_time = end_time - start_time

        # Calculate throughput metrics
        bars_per_second = len(test_data) / processing_time

        print(f"\n📊 Minute Bar Processing Benchmark:")
        print(f"   Total bars processed: {len(test_data)}")
        print(f"   Processing time: {processing_time:.3f} seconds")
        print(f"   Throughput: {bars_per_second:.1f} bars/second")

        # Performance assertions
        assert bars_per_second > 100, f"Throughput too low: {bars_per_second:.1f} bars/second"
        assert processing_time < 20.0, f"Processing took too long: {processing_time:.3f} seconds"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_concurrent_symbol_processing(self, benchmark_collector):
        """Benchmark concurrent processing of multiple symbols"""

        # Generate data for multiple symbols simultaneously
        symbols = [f'SYM{i:04d}' for i in range(50)]

        async def process_symbol_data(symbol, num_bars=100):
            """Process data for a single symbol"""
            start_time = time.time()

            for i in range(num_bars):
                data = {
                    'ev': 'AM',
                    'sym': symbol,
                    't': int((datetime.now(timezone.utc) + timedelta(minutes=i)).timestamp() * 1000),
                    'o': 150.0, 'h': 152.0, 'l': 149.0, 'c': 151.0,
                    'v': 1000000, 'vw': 150.5, 'n': 500
                }

                await benchmark_collector._process_polygon_minute_bar(data)

            return time.time() - start_time

        # Process all symbols concurrently
        start_time = time.time()

        tasks = [process_symbol_data(symbol) for symbol in symbols]
        symbol_times = await asyncio.gather(*tasks)

        total_time = time.time() - start_time

        # Calculate metrics
        total_bars = len(symbols) * 100
        overall_throughput = total_bars / total_time
        avg_symbol_time = statistics.mean(symbol_times)

        print(f"\n📊 Concurrent Symbol Processing Benchmark:")
        print(f"   Symbols processed: {len(symbols)}")
        print(f"   Total bars: {total_bars}")
        print(f"   Total time: {total_time:.3f} seconds")
        print(f"   Overall throughput: {overall_throughput:.1f} bars/second")
        print(f"   Average time per symbol: {avg_symbol_time:.3f} seconds")

        # Performance assertions
        assert overall_throughput > 200, f"Concurrent throughput too low: {overall_throughput:.1f}"
        assert total_time < 30.0, f"Total processing time too long: {total_time:.3f} seconds"

@pytest.mark.benchmark
class TestLatencyBenchmarks:
    """Benchmark data processing latency"""

    @pytest.fixture
    async def latency_collector(self):
        """Set up collector for latency testing"""
        with patch('market_data.realtime.streaming_collector.Environment'):
            mock_pool = AsyncMock()
            mock_conn = AsyncMock()
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

            with patch('market_data.realtime.streaming_collector.asyncpg.create_pool', return_value=mock_pool):
                collector = RealtimeStreamingCollector()
                await collector.initialize()

                collector.universe_symbols = {'AAPL'}
                collector.instrument_mapping = {'AAPL': 1}

                yield collector
                await collector.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_single_bar_processing_latency(self, latency_collector):
        """Benchmark latency for processing a single minute bar"""

        latencies = []

        # Process 100 individual bars and measure latency
        for i in range(100):
            data = {
                'ev': 'AM',
                'sym': 'AAPL',
                't': int((datetime.now(timezone.utc) + timedelta(minutes=i)).timestamp() * 1000),
                'o': 150.0, 'h': 152.0, 'l': 149.0, 'c': 151.0,
                'v': 1000000, 'vw': 150.5, 'n': 500
            }

            start_time = time.perf_counter()
            await latency_collector._process_polygon_minute_bar(data)
            end_time = time.perf_counter()

            latency_ms = (end_time - start_time) * 1000
            latencies.append(latency_ms)

        # Calculate latency statistics
        avg_latency = statistics.mean(latencies)
        median_latency = statistics.median(latencies)
        p95_latency = sorted(latencies)[int(0.95 * len(latencies))]
        p99_latency = sorted(latencies)[int(0.99 * len(latencies))]

        print(f"\n⏱️ Single Bar Processing Latency Benchmark:")
        print(f"   Samples: {len(latencies)}")
        print(f"   Average latency: {avg_latency:.2f} ms")
        print(f"   Median latency: {median_latency:.2f} ms")
        print(f"   95th percentile: {p95_latency:.2f} ms")
        print(f"   99th percentile: {p99_latency:.2f} ms")

        # Performance assertions
        assert avg_latency < 50.0, f"Average latency too high: {avg_latency:.2f} ms"
        assert p95_latency < 100.0, f"95th percentile latency too high: {p95_latency:.2f} ms"
        assert p99_latency < 200.0, f"99th percentile latency too high: {p99_latency:.2f} ms"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_operation_latency(self, latency_collector):
        """Benchmark database operation latency"""

        # Mock database with timing
        original_execute = latency_collector.pool.acquire.return_value.__aenter__.return_value.execute

        db_latencies = []

        async def timed_execute(*args, **kwargs):
            start_time = time.perf_counter()
            result = await original_execute(*args, **kwargs)
            end_time = time.perf_counter()

            latency_ms = (end_time - start_time) * 1000
            db_latencies.append(latency_ms)
            return result

        latency_collector.pool.acquire.return_value.__aenter__.return_value.execute = timed_execute

        # Create test minute bars
        for i in range(50):
            bar = MinuteBar(
                vendor='polygon',
                symbol='AAPL',
                instrument_id=1,
                timestamp=datetime.now(timezone.utc) + timedelta(minutes=i),
                open_price=150.0,
                high_price=152.0,
                low_price=149.0,
                close_price=151.0,
                volume=1000000
            )

            await latency_collector._store_minute_bar(bar)

        if db_latencies:
            avg_db_latency = statistics.mean(db_latencies)
            p95_db_latency = sorted(db_latencies)[int(0.95 * len(db_latencies))]

            print(f"\n💾 Database Operation Latency Benchmark:")
            print(f"   Operations: {len(db_latencies)}")
            print(f"   Average latency: {avg_db_latency:.2f} ms")
            print(f"   95th percentile: {p95_db_latency:.2f} ms")

            # Performance assertions (mock operations should be fast)
            assert avg_db_latency < 10.0, f"DB latency too high: {avg_db_latency:.2f} ms"

@pytest.mark.benchmark
class TestMemoryUsageBenchmarks:
    """Benchmark memory usage patterns"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_memory_usage_during_high_throughput(self):
        """Benchmark memory usage during high-throughput processing"""

        tracemalloc.start()

        with patch('market_data.realtime.streaming_collector.Environment'):
            mock_pool = AsyncMock()
            mock_conn = AsyncMock()
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

            with patch('market_data.realtime.streaming_collector.asyncpg.create_pool', return_value=mock_pool):
                collector = RealtimeStreamingCollector()
                await collector.initialize()

                collector.universe_symbols = {f'SYM{i:04d}' for i in range(100)}
                collector.instrument_mapping = {f'SYM{i:04d}': i for i in range(100)}

                # Measure initial memory
                initial_memory = tracemalloc.get_traced_memory()[0]

                # Process large amount of data
                tasks = []
                for i in range(5000):  # 5000 minute bars
                    data = {
                        'ev': 'AM',
                        'sym': f'SYM{i % 100:04d}',
                        't': int((datetime.now(timezone.utc) + timedelta(minutes=i)).timestamp() * 1000),
                        'o': 150.0, 'h': 152.0, 'l': 149.0, 'c': 151.0,
                        'v': 1000000, 'vw': 150.5, 'n': 500
                    }

                    task = asyncio.create_task(collector._process_polygon_minute_bar(data))
                    tasks.append(task)

                await asyncio.gather(*tasks)

                # Measure peak memory
                current_memory, peak_memory = tracemalloc.get_traced_memory()

                await collector.shutdown()

        tracemalloc.stop()

        # Calculate memory metrics
        memory_increase = peak_memory - initial_memory
        memory_per_bar = memory_increase / 5000

        print(f"\n🧠 Memory Usage Benchmark:")
        print(f"   Initial memory: {initial_memory / 1024 / 1024:.2f} MB")
        print(f"   Peak memory: {peak_memory / 1024 / 1024:.2f} MB")
        print(f"   Memory increase: {memory_increase / 1024 / 1024:.2f} MB")
        print(f"   Memory per bar: {memory_per_bar:.2f} bytes")

        # Performance assertions
        assert memory_increase < 100 * 1024 * 1024, f"Memory increase too high: {memory_increase / 1024 / 1024:.2f} MB"
        assert memory_per_bar < 1000, f"Memory per bar too high: {memory_per_bar:.2f} bytes"

    def test_gap_detection_memory_efficiency(self):
        """Test memory efficiency of gap detection algorithms"""

        tracemalloc.start()

        with patch('market_data.realtime.gap_detector.Environment'):
            gap_detector = GapDetectionEngine()

            # Create large number of gaps for testing
            gaps = []
            base_time = datetime.now(timezone.utc)

            for i in range(10000):  # 10,000 gaps
                from domains.market_data.services.realtime.gap_detector import DataGap
                gap = DataGap(
                    vendor='polygon',
                    symbol=f'SYM{i % 1000:04d}',
                    gap_start=base_time + timedelta(minutes=i),
                    gap_end=base_time + timedelta(minutes=i+5),
                    gap_duration_minutes=5,
                    missing_bars_count=5,
                    gap_type='temporary_delay',
                    severity='low'
                )
                gaps.append(gap)

            # Test gap prioritization memory usage
            initial_memory = tracemalloc.get_traced_memory()[0]

            prioritized_gaps = gap_detector._prioritize_gaps(gaps)

            current_memory, peak_memory = tracemalloc.get_traced_memory()

        tracemalloc.stop()

        memory_increase = peak_memory - initial_memory
        memory_per_gap = memory_increase / len(gaps)

        print(f"\n🔍 Gap Detection Memory Benchmark:")
        print(f"   Gaps processed: {len(gaps)}")
        print(f"   Memory increase: {memory_increase / 1024:.2f} KB")
        print(f"   Memory per gap: {memory_per_gap:.2f} bytes")

        # Verify functionality
        assert len(prioritized_gaps) == len(gaps)

        # Performance assertions
        assert memory_increase < 50 * 1024 * 1024, f"Memory usage too high: {memory_increase / 1024 / 1024:.2f} MB"
        assert memory_per_gap < 500, f"Memory per gap too high: {memory_per_gap:.2f} bytes"

@pytest.mark.benchmark
class TestConcurrencyBenchmarks:
    """Benchmark concurrent processing capabilities"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_backfill_concurrency_limits(self):
        """Test concurrency limits for backfill operations"""

        with patch('market_data.realtime.weekly_backfill.Environment'):
            backfill_engine = WeeklyBackfillEngine()

            # Create test jobs
            jobs = []
            for i in range(100):
                job = Mock()
                job.job_id = f'job-{i}'
                job.vendor = 'polygon'
                job.symbol = f'SYM{i:03d}'
                job.status = 'pending'
                jobs.append(job)

            # Mock job execution with varying delays
            async def mock_execute_job(job):
                # Simulate API call with random delay
                delay = 0.1 + (hash(job.job_id) % 100) / 1000  # 0.1-0.2 seconds
                await asyncio.sleep(delay)
                return True

            backfill_engine._execute_backfill_job = mock_execute_job
            backfill_engine._mark_job_completed = AsyncMock()
            backfill_engine._mark_job_failed = AsyncMock()

            # Test different concurrency levels
            concurrency_levels = [1, 5, 10, 20, 50]
            results = {}

            for concurrency in concurrency_levels:
                start_time = time.time()

                await backfill_engine._process_jobs_concurrently(
                    jobs[:50],  # Process first 50 jobs
                    max_concurrent=concurrency
                )

                processing_time = time.time() - start_time
                throughput = 50 / processing_time

                results[concurrency] = {
                    'time': processing_time,
                    'throughput': throughput
                }

                # Reset for next test
                backfill_engine._mark_job_completed.reset_mock()

            print(f"\n🔄 Concurrency Benchmark Results:")
            for concurrency, metrics in results.items():
                print(f"   Concurrency {concurrency:2d}: {metrics['time']:.2f}s, {metrics['throughput']:.1f} jobs/sec")

            # Verify optimal concurrency provides better performance
            assert results[10]['throughput'] > results[1]['throughput'], "Higher concurrency should improve throughput"
            assert results[5]['time'] < results[1]['time'], "Higher concurrency should reduce total time"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_concurrent_gap_detection(self):
        """Benchmark concurrent gap detection across multiple vendors"""

        with patch('market_data.realtime.gap_detector.Environment'):
            gap_detector = GapDetectionEngine()

            # Mock database connection
            mock_pool = AsyncMock()
            mock_conn = AsyncMock()
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
            gap_detector.pool = mock_pool

            # Mock gap detection for multiple symbols
            symbols = [f'SYM{i:03d}' for i in range(100)]
            vendors = ['polygon', 'tiingo', 'fmp']

            # Mock gap detection results
            mock_conn.fetch.return_value = [
                {
                    'prev_timestamp': datetime.now(timezone.utc) - timedelta(minutes=10),
                    'timestamp': datetime.now(timezone.utc),
                    'gap_minutes': 10.0
                }
            ]

            async def detect_gaps_for_vendor(vendor):
                """Detect gaps for all symbols for one vendor"""
                tasks = []
                for symbol in symbols:
                    task = asyncio.create_task(
                        gap_detector._detect_symbol_gaps(vendor, symbol)
                    )
                    tasks.append(task)

                gap_lists = await asyncio.gather(*tasks)
                return [gap for gap_list in gap_lists for gap in gap_list]

            # Run gap detection for all vendors concurrently
            start_time = time.time()

            vendor_tasks = [detect_gaps_for_vendor(vendor) for vendor in vendors]
            vendor_gaps = await asyncio.gather(*vendor_tasks)

            processing_time = time.time() - start_time

            # Calculate metrics
            total_symbols = len(symbols) * len(vendors)
            symbols_per_second = total_symbols / processing_time

            print(f"\n🔍 Concurrent Gap Detection Benchmark:")
            print(f"   Vendors: {len(vendors)}")
            print(f"   Symbols per vendor: {len(symbols)}")
            print(f"   Total symbol-vendor combinations: {total_symbols}")
            print(f"   Processing time: {processing_time:.3f} seconds")
            print(f"   Throughput: {symbols_per_second:.1f} symbols/second")

            # Performance assertions
            assert symbols_per_second > 50, f"Gap detection throughput too low: {symbols_per_second:.1f}"
            assert processing_time < 10.0, f"Gap detection took too long: {processing_time:.3f} seconds"

@pytest.mark.benchmark
class TestDatabasePerformanceBenchmarks:
    """Benchmark database operation performance"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_batch_insert_performance(self):
        """Benchmark batch insert performance"""

        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        # Track execution times
        execution_times = []

        async def timed_execute(*args, **kwargs):
            start_time = time.perf_counter()
            await asyncio.sleep(0.001)  # Simulate DB operation
            end_time = time.perf_counter()
            execution_times.append((end_time - start_time) * 1000)
            return None

        mock_conn.execute = timed_execute

        with patch('market_data.realtime.streaming_collector.Environment'):
            with patch('market_data.realtime.streaming_collector.asyncpg.create_pool', return_value=mock_pool):
                collector = RealtimeStreamingCollector()
                await collector.initialize()

                collector.universe_symbols = {'AAPL'}
                collector.instrument_mapping = {'AAPL': 1}

                # Test different batch sizes
                batch_sizes = [1, 10, 50, 100, 500]
                batch_results = {}

                for batch_size in batch_sizes:
                    execution_times.clear()

                    # Create batch of minute bars
                    start_time = time.time()

                    tasks = []
                    for i in range(batch_size):
                        bar = MinuteBar(
                            vendor='polygon',
                            symbol='AAPL',
                            instrument_id=1,
                            timestamp=datetime.now(timezone.utc) + timedelta(minutes=i),
                            open_price=150.0,
                            high_price=152.0,
                            low_price=149.0,
                            close_price=151.0,
                            volume=1000000
                        )

                        task = asyncio.create_task(collector._store_minute_bar(bar))
                        tasks.append(task)

                    await asyncio.gather(*tasks)

                    total_time = time.time() - start_time
                    throughput = batch_size / total_time

                    batch_results[batch_size] = {
                        'total_time': total_time,
                        'throughput': throughput,
                        'avg_execution_time': statistics.mean(execution_times) if execution_times else 0
                    }

                await collector.shutdown()

        print(f"\n💾 Database Batch Insert Benchmark:")
        for batch_size, results in batch_results.items():
            print(f"   Batch size {batch_size:3d}: {results['total_time']:.3f}s, "
                  f"{results['throughput']:.1f} inserts/sec, "
                  f"{results['avg_execution_time']:.2f}ms avg")

        # Verify larger batches are more efficient
        assert batch_results[100]['throughput'] > batch_results[1]['throughput'], \
            "Batch processing should be more efficient"

@pytest.mark.benchmark
class TestAPIResponseTimeBenchmarks:
    """Benchmark API response time handling"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_api_response_time_simulation(self):
        """Test performance with varying API response times"""

        with patch('market_data.realtime.gap_detector.Environment'):
            gap_detector = GapDetectionEngine()

            # Test different response times
            response_times = [0.1, 0.5, 1.0, 2.0, 5.0]  # seconds
            results = {}

            for response_time in response_times:
                # Mock API with specific response time
                mock_response_data = {
                    'results': [
                        {
                            't': int(datetime.now(timezone.utc).timestamp() * 1000),
                            'o': 150.0, 'h': 152.0, 'l': 149.0, 'c': 151.0, 'v': 1000000
                        }
                    ]
                }

                mock_response = AsyncMock()
                mock_response.status = 200
                mock_response.json.return_value = mock_response_data

                mock_session = AsyncMock()

                async def delayed_response(*args, **kwargs):
                    await asyncio.sleep(response_time)
                    return mock_response

                mock_session.get.return_value.__aenter__ = delayed_response

                # Create test gap
                gap = Mock()
                gap.vendor = 'polygon'
                gap.symbol = 'AAPL'
                gap.gap_start = datetime.now(timezone.utc) - timedelta(minutes=5)
                gap.gap_end = datetime.now(timezone.utc)

                gap_detector._store_backfilled_data = AsyncMock()

                # Measure backfill performance
                start_time = time.time()

                with patch('aiohttp.ClientSession', return_value=mock_session):
                    success = await gap_detector._backfill_polygon_gap(gap)

                processing_time = time.time() - start_time

                results[response_time] = {
                    'processing_time': processing_time,
                    'success': success,
                    'overhead': processing_time - response_time
                }

            print(f"\n🌐 API Response Time Benchmark:")
            for response_time, metrics in results.items():
                print(f"   API latency {response_time:.1f}s: "
                      f"total {metrics['processing_time']:.3f}s, "
                      f"overhead {metrics['overhead']:.3f}s, "
                      f"success: {metrics['success']}")

            # Verify system handles various response times
            assert all(result['success'] for result in results.values()), \
                "System should handle all response time scenarios"

            # Verify overhead is reasonable
            for response_time, metrics in results.items():
                assert metrics['overhead'] < 0.1, \
                    f"Processing overhead too high for {response_time}s API latency"

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short', '-m', 'benchmark'])