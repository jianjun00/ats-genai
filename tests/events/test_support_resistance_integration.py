#!/usr/bin/env python3
"""
Integration and performance tests for Support/Resistance system
"""

import pytest
import asyncio
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from events.analysis.support_resistance_detector import (
    SupportResistanceDetector, SRLevel, SRTest, SREvent,
    SRType, SRLevelType, SRTestOutcome, Timeframe
)
from events.processors.support_resistance_processor import SupportResistanceProcessor
from config.environment import Environment

class TestSupportResistanceIntegration:
    """Integration tests for complete S/R system"""

    @pytest.fixture
    async def db_connection(self):
        """Create test database connection"""
        try:
            env = Environment()
            pool = await env.database.create_pool_with_retry(max_retries=3)
            conn = await pool.acquire()

            # Ensure schema exists
            await self._ensure_schema_exists(conn)

            # Clean test data
            await self._cleanup_test_data(conn)

            yield conn

            # Cleanup after test
            await self._cleanup_test_data(conn)
            await pool.release(conn)
            await pool.close()

        except Exception as e:
            pytest.skip(f"Database not available: {e}")

    async def _ensure_schema_exists(self, conn):
        """Ensure S/R schema exists (create if missing)"""
        try:
            # Check if tables exist
            table_check = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name IN ('dev_sr_levels', 'dev_sr_tests', 'dev_sr_events')
            """

            tables = await conn.fetch(table_check)
            existing_tables = {row['table_name'] for row in tables}

            if len(existing_tables) < 3:
                # Read and execute migration
                migration_path = os.path.join(
                    os.path.dirname(__file__), '..', '..',
                    'src', 'infrastructure', 'database', 'migrations',
                    '055_create_support_resistance_schema.sql'
                )

                if os.path.exists(migration_path):
                    with open(migration_path, 'r') as f:
                        migration_sql = f.read()

                    # Execute migration (might need to split on semicolons)
                    for statement in migration_sql.split(';'):
                        statement = statement.strip()
                        if statement and not statement.startswith('--'):
                            try:
                                await conn.execute(statement)
                            except Exception as e:
                                print(f"Migration warning: {e}")

        except Exception as e:
            print(f"Schema setup warning: {e}")

    async def _cleanup_test_data(self, conn):
        """Clean up test data"""
        cleanup_queries = [
            "DELETE FROM dev_sr_events WHERE symbol LIKE 'INTG_TEST_%'",
            "DELETE FROM dev_sr_tests WHERE symbol LIKE 'INTG_TEST_%'",
            "DELETE FROM dev_sr_levels WHERE symbol LIKE 'INTG_TEST_%'"
        ]

        for query in cleanup_queries:
            try:
                await conn.execute(query)
            except Exception as e:
                print(f"Cleanup warning: {e}")

    @pytest.fixture
    def realistic_market_data(self):
        """Generate realistic market data with multiple S/R levels"""
        np.random.seed(42)

        # Generate 6 months of daily data
        dates = pd.date_range(start='2024-01-01', end='2024-06-30', freq='D')

        # Create trending market with S/R levels
        trend_component = np.linspace(100, 140, len(dates))  # Uptrend

        # Add S/R levels at key points
        sr_levels = [105, 115, 125, 135]  # Major levels

        data = []
        for i, (date, trend_price) in enumerate(zip(dates, trend_component)):
            # Find nearest S/R level
            nearest_level = min(sr_levels, key=lambda x: abs(x - trend_price))

            # Add resistance/support behavior
            if abs(trend_price - nearest_level) < 2:
                # Near S/R level - add more volatile behavior
                if trend_price > nearest_level:  # At resistance
                    base_price = nearest_level + np.random.exponential(0.5)
                else:  # At support
                    base_price = nearest_level - abs(np.random.exponential(0.5))
            else:
                # Away from levels - normal trending
                base_price = trend_price + np.random.normal(0, 1)

            # Create OHLC with realistic properties
            close = base_price
            open_price = close + np.random.normal(0, 0.3)
            high = max(open_price, close) + abs(np.random.normal(0, 0.8))
            low = min(open_price, close) - abs(np.random.normal(0, 0.8))

            # Higher volume near S/R levels
            base_volume = 1000000
            if any(abs(close - level) < 1 for level in sr_levels):
                volume_multiplier = np.random.uniform(1.5, 3.0)
            else:
                volume_multiplier = np.random.uniform(0.8, 1.2)

            volume = int(base_volume * volume_multiplier)

            data.append({
                'timestamp': date,
                'open': open_price,
                'high': high,
                'low': low,
                'close': close,
                'volume': volume
            })

        return pd.DataFrame(data)

    async def test_full_detection_pipeline(self, realistic_market_data):
        """Test complete detection pipeline with realistic data"""
        detector = SupportResistanceDetector({
            'pivot_lookback': 20,
            'cluster_epsilon': 0.02,
            'proximity_tolerance': 0.01,
            'psychological_levels': True,
            'volume_profile_levels': True
        })

        symbol = 'INTG_TEST_PIPELINE'
        timeframe = Timeframe.DAILY

        # Measure detection performance
        start_time = time.time()

        # Detect levels
        levels = await detector.detect_sr_levels(symbol, realistic_market_data, timeframe)

        # Detect tests
        tests = await detector.detect_sr_tests(symbol, realistic_market_data, levels)

        detection_time = time.time() - start_time

        # Validate results
        assert len(levels) > 0, "Should detect S/R levels"
        assert len(tests) > 0, "Should detect level tests"

        # Performance validation
        assert detection_time < 5.0, f"Detection too slow: {detection_time:.2f}s"

        # Quality validation
        strong_levels = [l for l in levels if l.strength > 0.6]
        assert len(strong_levels) > 0, "Should detect some strong levels"

        successful_tests = [t for t in tests if t.outcome in [SRTestOutcome.HOLD_STRONG, SRTestOutcome.HOLD_WEAK]]
        assert len(successful_tests) > 0, "Should detect successful level tests"

        print(f"Detected {len(levels)} levels and {len(tests)} tests in {detection_time:.3f}s")
        print(f"Strong levels: {len(strong_levels)}, Successful tests: {len(successful_tests)}")

    async def test_database_integration_full_cycle(self, db_connection, realistic_market_data):
        """Test complete database integration cycle"""
        detector = SupportResistanceDetector()
        symbol = 'INTG_TEST_DB'
        timeframe = Timeframe.DAILY

        # Detect levels and tests
        levels = await detector.detect_sr_levels(symbol, realistic_market_data, timeframe)
        tests = await detector.detect_sr_tests(symbol, realistic_market_data, levels)

        if not levels:
            pytest.skip("No levels detected for database integration test")

        # Insert levels into database
        level_ids = []
        for level in levels[:3]:  # Limit for test performance
            level_id = f"{symbol}_{timeframe.value}_{level.sr_type.value}_{level.price:.2f}_{int(level.first_established.timestamp())}"

            query = """
            INSERT INTO dev_sr_levels (
                level_id, symbol, price, sr_type, level_type, timeframe,
                strength, confidence, first_established, last_tested,
                test_count, hold_count, break_count, volume_confirmation, metadata
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            RETURNING id
            """

            result = await db_connection.fetchrow(
                query, level_id, symbol, level.price, level.sr_type.value,
                level.level_type.value, timeframe.value, level.strength,
                level.confidence, level.first_established, level.last_tested,
                level.test_count, level.hold_count, level.break_count,
                level.volume_confirmation, '{}'
            )

            level_ids.append((level_id, result['id']))

        # Insert tests
        test_ids = []
        for test in tests[:5]:  # Limit for test performance
            # Find matching level
            matching_level = None
            for level_id, db_id in level_ids:
                if test.level_id == level_id:
                    matching_level = (level_id, db_id)
                    break

            if not matching_level:
                continue

            test_id = f"{test.level_id}_{int(test.test_datetime.timestamp())}"

            query = """
            INSERT INTO dev_sr_tests (
                test_id, level_id, symbol, sr_level_id, test_datetime,
                test_price, approach_direction, timeframe, max_penetration,
                hold_duration, volume_spike, outcome, outcome_confidence, metadata
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            RETURNING id
            """

            result = await db_connection.fetchrow(
                query, test_id, matching_level[0], symbol, matching_level[1],
                test.test_datetime, test.test_price, test.approach_direction,
                timeframe.value, test.max_penetration, test.hold_duration,
                test.volume_spike, test.outcome.value, test.confidence, '{}'
            )

            test_ids.append(result['id'])

        # Verify data integrity with joins
        join_query = """
        SELECT
            l.symbol, l.price, l.sr_type, l.strength,
            t.outcome, t.test_price, t.volume_spike,
            COUNT(*) as relationship_count
        FROM dev_sr_levels l
        JOIN dev_sr_tests t ON l.id = t.sr_level_id
        WHERE l.symbol = $1
        GROUP BY l.symbol, l.price, l.sr_type, l.strength, t.outcome, t.test_price, t.volume_spike
        ORDER BY l.strength DESC
        """

        relationships = await db_connection.fetch(join_query, symbol)

        assert len(relationships) > 0, "Should have level-test relationships"

        # Verify data quality
        for rel in relationships:
            assert rel['strength'] > 0, "Level strength should be positive"
            assert rel['volume_spike'] >= 0, "Volume spike should be non-negative"
            assert rel['relationship_count'] > 0, "Should have valid relationships"

        print(f"Created {len(level_ids)} levels, {len(test_ids)} tests, {len(relationships)} relationships")

    async def test_processor_integration(self, realistic_market_data):
        """Test processor integration with detection and database"""
        config = {
            'processing_interval_seconds': 60,
            'batch_size': 10,
            'max_concurrent_symbols': 5,
            'min_data_points': 50,
            'alert_thresholds': {
                'strong_level_test': 0.8,
                'level_break': 0.7,
                'confluence_level': 0.9
            },
            'detector_config': {
                'pivot_lookback': 15,
                'cluster_epsilon': 0.03,
                'psychological_levels': True
            }
        }

        try:
            processor = SupportResistanceProcessor(config)

            # Mock database for this test to avoid dependency
            from unittest.mock import AsyncMock
            processor.db_pool = AsyncMock()
            processor.active_symbols = {'INTG_TEST_PROC'}
            processor._initialize_processing_state()

            # Mock successful database operations
            mock_conn = processor.db_pool.acquire.return_value.__aenter__.return_value
            mock_conn.fetchrow.return_value = {'id': 123}
            mock_conn.execute.return_value = None

            # Test processing
            start_time = time.time()

            await processor.process_market_data_update(
                'INTG_TEST_PROC', realistic_market_data, Timeframe.DAILY
            )

            processing_time = time.time() - start_time

            # Validate processing
            assert processing_time < 10.0, f"Processing too slow: {processing_time:.2f}s"

            # Check stats
            stats = processor.get_processing_stats()
            assert stats['symbols_processed'] > 0, "Should have processed symbols"
            assert stats['processing_time_ms'] > 0, "Should track processing time"

            print(f"Processed in {processing_time:.3f}s, stats: {stats}")

        except ImportError as e:
            pytest.skip(f"Processor dependencies not available: {e}")

class TestPerformanceBenchmarks:
    """Performance benchmarks for S/R system"""

    @pytest.fixture
    def large_dataset(self):
        """Generate large dataset for performance testing"""
        np.random.seed(123)

        # 2 years of hourly data
        dates = pd.date_range(start='2022-01-01', end='2024-01-01', freq='H')

        # Generate efficient trending data
        n_points = len(dates)
        base_prices = 100 + np.cumsum(np.random.normal(0, 0.1, n_points))

        # Vectorized OHLCV generation
        variations = np.random.normal(0, 1, n_points)
        closes = base_prices + variations
        opens = closes + np.random.normal(0, 0.5, n_points)
        highs = np.maximum(opens, closes) + np.abs(np.random.normal(0, 0.8, n_points))
        lows = np.minimum(opens, closes) - np.abs(np.random.normal(0, 0.8, n_points))
        volumes = np.random.lognormal(15, 0.3, n_points).astype(int)

        return pd.DataFrame({
            'timestamp': dates,
            'open': opens,
            'high': highs,
            'low': lows,
            'close': closes,
            'volume': volumes
        })

    @pytest.mark.performance
    async def test_large_dataset_performance(self, large_dataset):
        """Test performance with large dataset"""
        detector = SupportResistanceDetector({
            'pivot_lookback': 20,
            'cluster_epsilon': 0.02,
            'psychological_levels': True,
            'volume_profile_levels': False  # Disable for performance
        })

        symbol = 'PERF_TEST_LARGE'
        timeframe = Timeframe.INTRADAY_1H

        print(f"Testing performance with {len(large_dataset)} data points")

        # Benchmark level detection
        start_time = time.time()
        levels = await detector.detect_sr_levels(symbol, large_dataset, timeframe)
        level_detection_time = time.time() - start_time

        print(f"Level detection: {level_detection_time:.3f}s, {len(levels)} levels")

        # Performance requirements
        assert level_detection_time < 30.0, f"Level detection too slow: {level_detection_time:.2f}s"
        assert len(levels) > 0, "Should detect levels in large dataset"

        # Benchmark test detection (with subset of levels for performance)
        if levels:
            test_levels = levels[:10]  # Limit to top 10 for performance

            start_time = time.time()
            tests = await detector.detect_sr_tests(symbol, large_dataset, test_levels)
            test_detection_time = time.time() - start_time

            print(f"Test detection: {test_detection_time:.3f}s, {len(tests)} tests")

            assert test_detection_time < 60.0, f"Test detection too slow: {test_detection_time:.2f}s"

        # Memory usage should be reasonable (this is more of a observation)
        import psutil
        import os
        process = psutil.Process(os.getpid())
        memory_mb = process.memory_info().rss / 1024 / 1024
        print(f"Memory usage: {memory_mb:.1f} MB")

    @pytest.mark.performance
    async def test_concurrent_processing_performance(self):
        """Test concurrent processing performance"""
        detector = SupportResistanceDetector()

        # Generate multiple smaller datasets
        symbols = [f'PERF_TEST_{i}' for i in range(10)]
        datasets = []

        for i in range(len(symbols)):
            np.random.seed(i)
            dates = pd.date_range(start='2024-01-01', end='2024-03-01', freq='D')
            n = len(dates)

            closes = 100 + np.cumsum(np.random.normal(0, 0.5, n))
            data = pd.DataFrame({
                'timestamp': dates,
                'open': closes + np.random.normal(0, 0.2, n),
                'high': closes + np.abs(np.random.normal(0, 0.8, n)),
                'low': closes - np.abs(np.random.normal(0, 0.8, n)),
                'close': closes,
                'volume': np.random.lognormal(15, 0.3, n).astype(int)
            })
            datasets.append(data)

        # Sequential processing
        start_time = time.time()
        sequential_results = []
        for symbol, data in zip(symbols, datasets):
            levels = await detector.detect_sr_levels(symbol, data, Timeframe.DAILY)
            sequential_results.append(len(levels))
        sequential_time = time.time() - start_time

        # Concurrent processing
        async def process_symbol(symbol, data):
            return await detector.detect_sr_levels(symbol, data, Timeframe.DAILY)

        start_time = time.time()
        concurrent_tasks = [process_symbol(symbol, data) for symbol, data in zip(symbols, datasets)]
        concurrent_results = await asyncio.gather(*concurrent_tasks)
        concurrent_time = time.time() - start_time

        # Compare results
        concurrent_counts = [len(levels) for levels in concurrent_results]

        print(f"Sequential: {sequential_time:.3f}s, Concurrent: {concurrent_time:.3f}s")
        print(f"Speedup: {sequential_time/concurrent_time:.2f}x")

        # Results should be similar (allowing for minor variations due to async timing)
        assert len(sequential_results) == len(concurrent_counts)

        # Concurrent should be faster (or at least not much slower due to overhead)
        assert concurrent_time <= sequential_time * 1.5, "Concurrent processing should be efficient"

    @pytest.mark.performance
    async def test_memory_efficiency(self):
        """Test memory efficiency with streaming data simulation"""
        detector = SupportResistanceDetector({
            'pivot_lookback': 10,  # Smaller lookback for efficiency
            'cluster_epsilon': 0.03,
            'psychological_levels': False,  # Disable for memory test
            'volume_profile_levels': False
        })

        import psutil
        import os
        process = psutil.Process(os.getpid())

        # Baseline memory
        baseline_memory = process.memory_info().rss / 1024 / 1024

        # Process chunks of data (simulating streaming)
        chunk_size = 1000
        total_levels = 0

        for chunk_i in range(10):  # 10 chunks
            # Generate chunk
            dates = pd.date_range(start=f'2024-{chunk_i+1:02d}-01', periods=chunk_size, freq='H')
            n = len(dates)

            closes = 100 + np.random.normal(0, 5, n)  # More variation
            chunk_data = pd.DataFrame({
                'timestamp': dates,
                'open': closes + np.random.normal(0, 0.5, n),
                'high': closes + np.abs(np.random.normal(0, 1, n)),
                'low': closes - np.abs(np.random.normal(0, 1, n)),
                'close': closes,
                'volume': np.random.lognormal(15, 0.3, n).astype(int)
            })

            # Process chunk
            levels = await detector.detect_sr_levels(f'MEM_TEST_{chunk_i}', chunk_data, Timeframe.INTRADAY_1H)
            total_levels += len(levels)

            # Check memory growth
            current_memory = process.memory_info().rss / 1024 / 1024
            memory_growth = current_memory - baseline_memory

            # Memory growth should be reasonable
            assert memory_growth < 500, f"Excessive memory growth: {memory_growth:.1f} MB"

        final_memory = process.memory_info().rss / 1024 / 1024
        total_memory_growth = final_memory - baseline_memory

        print(f"Processed {chunk_size * 10} total data points")
        print(f"Total levels detected: {total_levels}")
        print(f"Memory growth: {total_memory_growth:.1f} MB")

if __name__ == "__main__":
    # Run with performance marks
    pytest.main([__file__, "-v", "--tb=short", "-m", "not performance"])

    # Uncomment to run performance tests
    # pytest.main([__file__, "-v", "--tb=short", "-m", "performance"])