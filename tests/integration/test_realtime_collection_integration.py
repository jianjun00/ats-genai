#!/usr/bin/env python3
"""
Integration Tests for Real-time Collection System

Tests the complete real-time data collection pipeline including:
- Database schema compatibility
- Multi-vendor data collection
- Data persistence and retrieval
- System performance under load
- Error recovery and resilience
"""

import pytest
import asyncio
import asyncpg
import logging
from datetime import datetime, timedelta
from decimal import Decimal
import os
import sys
import time

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from domains.market_data.services.realtime.aapl_tsla_synthetic_collector import AAPLTSLASyntheticCollector

logger = logging.getLogger(__name__)

@pytest.fixture
async def integration_db_pool():
    """Database pool for integration testing"""
    dsn = "postgresql://postgres:intg_password@localhost:4432/intg_db"
    try:
        pool = await asyncpg.create_pool(dsn, min_size=2, max_size=8)

        # Ensure tables exist
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS intg_one_minute_live_tiingo (
                    id BIGSERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    open_price DECIMAL(20,6),
                    high_price DECIMAL(20,6),
                    low_price DECIMAL(20,6),
                    close_price DECIMAL(20,6),
                    volume BIGINT,
                    vendor VARCHAR(20) DEFAULT 'tiingo',
                    data_latency_ms INTEGER,
                    quality_score DECIMAL(5,3),
                    received_at TIMESTAMPTZ DEFAULT NOW(),
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(symbol, timestamp)
                );
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS intg_one_minute_live_polygon (
                    id BIGSERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    open_price DECIMAL(20,6),
                    high_price DECIMAL(20,6),
                    low_price DECIMAL(20,6),
                    close_price DECIMAL(20,6),
                    volume BIGINT,
                    vwap DECIMAL(20,6),
                    trade_count INTEGER,
                    vendor VARCHAR(20) DEFAULT 'polygon',
                    data_latency_ms INTEGER,
                    quality_score DECIMAL(5,3),
                    received_at TIMESTAMPTZ DEFAULT NOW(),
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(symbol, timestamp)
                );
            """)

            # Create indexes
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_intg_tiingo_live_symbol_timestamp
                ON intg_one_minute_live_tiingo(symbol, timestamp);
            """)

            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_intg_polygon_live_symbol_timestamp
                ON intg_one_minute_live_polygon(symbol, timestamp);
            """)

        yield pool
        await pool.close()

    except Exception as e:
        logger.warning(f"Cannot connect to integration database: {e}")
        pytest.skip("Integration database not available")


class TestDatabaseIntegration:
    """Integration tests for database operations"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_schema_validation(self, integration_db_pool):
        """Test database schema matches collector expectations"""
        async with integration_db_pool.acquire() as conn:
            # Check Tiingo table schema
            tiingo_columns = await conn.fetch("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'intg_one_minute_live_tiingo'
                ORDER BY column_name;
            """)

            required_columns = {
                'symbol', 'timestamp', 'open_price', 'high_price',
                'low_price', 'close_price', 'volume', 'vendor',
                'data_latency_ms', 'quality_score', 'received_at'
            }

            existing_columns = {row['column_name'] for row in tiingo_columns}
            assert required_columns.issubset(existing_columns)

            # Check Polygon table schema
            polygon_columns = await conn.fetch("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'intg_one_minute_live_polygon'
                ORDER BY column_name;
            """)

            polygon_required = required_columns | {'vwap', 'trade_count'}
            polygon_existing = {row['column_name'] for row in polygon_columns}
            assert polygon_required.issubset(polygon_existing)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_data_persistence_and_retrieval(self, integration_db_pool):
        """Test complete data persistence and retrieval workflow"""
        collector = AAPLTSLASyntheticCollector()
        collector.pool = integration_db_pool

        # Clear existing test data
        async with integration_db_pool.acquire() as conn:
            await conn.execute("DELETE FROM intg_one_minute_live_tiingo WHERE symbol IN ('AAPL', 'TSLA')")
            await conn.execute("DELETE FROM intg_one_minute_live_polygon WHERE symbol IN ('AAPL', 'TSLA')")

        # Generate and store data
        initial_count = await collector.generate_and_store_data()
        assert initial_count > 0

        # Verify data was stored
        async with integration_db_pool.acquire() as conn:
            tiingo_count = await conn.fetchval("SELECT COUNT(*) FROM intg_one_minute_live_tiingo")
            polygon_count = await conn.fetchval("SELECT COUNT(*) FROM intg_one_minute_live_polygon")

            assert tiingo_count > 0
            assert polygon_count > 0

            # Verify data quality
            tiingo_data = await conn.fetch("""
                SELECT * FROM intg_one_minute_live_tiingo
                WHERE symbol IN ('AAPL', 'TSLA')
                ORDER BY symbol, timestamp
            """)

            for row in tiingo_data:
                assert row['symbol'] in ['AAPL', 'TSLA']
                assert row['open_price'] > 0
                assert row['high_price'] >= row['low_price']
                assert row['volume'] > 0
                assert 0 <= row['quality_score'] <= 1
                assert row['vendor'] == 'tiingo'

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_duplicate_handling(self, integration_db_pool):
        """Test UPSERT behavior with duplicate timestamps"""
        collector = AAPLTSLASyntheticCollector()
        collector.pool = integration_db_pool

        # Generate data with same timestamp
        fixed_timestamp = datetime.now().replace(second=0, microsecond=0)

        bar1 = collector.generate_minute_bar('AAPL', fixed_timestamp, 'tiingo')
        bar2 = collector.generate_minute_bar('AAPL', fixed_timestamp, 'tiingo')
        bar2['close_price'] = bar1['close_price'] + 1.0  # Make it different

        # Store first bar
        await collector.store_tiingo_data([bar1])

        # Store second bar (should update, not create duplicate)
        await collector.store_tiingo_data([bar2])

        # Verify only one record exists
        async with integration_db_pool.acquire() as conn:
            count = await conn.fetchval("""
                SELECT COUNT(*) FROM intg_one_minute_live_tiingo
                WHERE symbol = 'AAPL' AND timestamp = $1
            """, fixed_timestamp)

            assert count == 1

            # Verify it has the updated price
            price = await conn.fetchval("""
                SELECT close_price FROM intg_one_minute_live_tiingo
                WHERE symbol = 'AAPL' AND timestamp = $1
            """, fixed_timestamp)

            assert float(price) == bar2['close_price']

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_concurrent_writes(self, integration_db_pool):
        """Test concurrent writes to database"""
        collectors = [AAPLTSLASyntheticCollector() for _ in range(5)]

        for collector in collectors:
            collector.pool = integration_db_pool

        # Run concurrent collection
        tasks = [collector.generate_and_store_data() for collector in collectors]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # All should succeed
        for result in results:
            assert not isinstance(result, Exception)
            assert result >= 0

        # Verify data integrity
        async with integration_db_pool.acquire() as conn:
            # Check for any orphaned or corrupted records
            orphaned = await conn.fetchval("""
                SELECT COUNT(*) FROM intg_one_minute_live_tiingo
                WHERE open_price IS NULL OR close_price IS NULL
            """)
            assert orphaned == 0


class TestDataQualityIntegration:
    """Integration tests for data quality validation"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_cross_vendor_consistency(self, integration_db_pool):
        """Test data consistency between Tiingo and Polygon"""
        collector = AAPLTSLASyntheticCollector()
        collector.pool = integration_db_pool

        # Generate data for same timestamp
        timestamp = datetime.now().replace(second=0, microsecond=0)

        # Store data
        await collector.generate_and_store_data()

        # Compare vendor data
        async with integration_db_pool.acquire() as conn:
            comparison = await conn.fetch("""
                SELECT
                    t.symbol,
                    t.close_price as tiingo_price,
                    p.close_price as polygon_price,
                    ABS(t.close_price - p.close_price) / t.close_price as price_diff
                FROM intg_one_minute_live_tiingo t
                JOIN intg_one_minute_live_polygon p
                    ON t.symbol = p.symbol AND t.timestamp = p.timestamp
                WHERE t.timestamp >= NOW() - INTERVAL '5 minutes'
            """)

            for row in comparison:
                # Prices should be reasonably close (within 5% since they're synthetic)
                assert row['price_diff'] < 0.05, f"Price difference too large for {row['symbol']}"

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_data_completeness(self, integration_db_pool):
        """Test data completeness across collection cycles"""
        collector = AAPLTSLASyntheticCollector()
        collector.pool = integration_db_pool

        # Run multiple collection cycles
        for i in range(5):
            await collector.generate_and_store_data()
            await asyncio.sleep(0.1)  # Small delay

        # Verify completeness
        async with integration_db_pool.acquire() as conn:
            # Should have data for both symbols in both tables
            symbols_tiingo = await conn.fetch("""
                SELECT DISTINCT symbol FROM intg_one_minute_live_tiingo
                WHERE timestamp >= NOW() - INTERVAL '5 minutes'
            """)

            symbols_polygon = await conn.fetch("""
                SELECT DISTINCT symbol FROM intg_one_minute_live_polygon
                WHERE timestamp >= NOW() - INTERVAL '5 minutes'
            """)

            tiingo_symbols = {row['symbol'] for row in symbols_tiingo}
            polygon_symbols = {row['symbol'] for row in symbols_polygon}

            expected_symbols = {'AAPL', 'TSLA'}
            assert tiingo_symbols == expected_symbols
            assert polygon_symbols == expected_symbols

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_quality_score_distribution(self, integration_db_pool):
        """Test quality score distribution"""
        collector = AAPLTSLASyntheticCollector()
        collector.pool = integration_db_pool

        # Generate substantial amount of data
        for _ in range(10):
            await collector.generate_and_store_data()

        # Analyze quality scores
        async with integration_db_pool.acquire() as conn:
            stats = await conn.fetchrow("""
                SELECT
                    COUNT(*) as total_records,
                    AVG(quality_score) as avg_quality,
                    MIN(quality_score) as min_quality,
                    MAX(quality_score) as max_quality,
                    STDDEV(quality_score) as stddev_quality
                FROM (
                    SELECT quality_score FROM intg_one_minute_live_tiingo
                    UNION ALL
                    SELECT quality_score FROM intg_one_minute_live_polygon
                ) combined
            """)

            assert stats['total_records'] > 0
            assert 0.8 <= stats['avg_quality'] <= 1.0  # Should be high quality
            assert 0.5 <= stats['min_quality'] <= 1.0  # Should be reasonable minimum
            assert stats['max_quality'] <= 1.0  # Cannot exceed 1.0
            assert stats['stddev_quality'] > 0  # Should have some variation


class TestPerformanceIntegration:
    """Performance integration tests"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_bulk_data_insertion_performance(self, integration_db_pool):
        """Test performance of bulk data insertion"""
        collector = AAPLTSLASyntheticCollector()
        collector.pool = integration_db_pool

        start_time = time.time()

        # Insert substantial amount of data
        tasks = []
        for _ in range(20):  # 20 concurrent operations
            tasks.append(collector.generate_and_store_data())

        results = await asyncio.gather(*tasks)

        elapsed = time.time() - start_time
        total_records = sum(results)

        logger.info(f"Performance: Inserted {total_records} records in {elapsed:.2f} seconds")
        logger.info(f"Performance: {total_records/elapsed:.1f} records/second")

        # Should maintain reasonable performance
        assert elapsed < 30.0  # Should complete within 30 seconds
        assert total_records > 0  # Should actually insert data
        assert total_records / elapsed > 1.0  # At least 1 record per second

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_query_performance(self, integration_db_pool):
        """Test query performance on collected data"""
        collector = AAPLTSLASyntheticCollector()
        collector.pool = integration_db_pool

        # Ensure we have some data
        for _ in range(10):
            await collector.generate_and_store_data()

        # Test various query patterns
        async with integration_db_pool.acquire() as conn:
            start_time = time.time()

            # Recent data query (most common)
            recent_data = await conn.fetch("""
                SELECT * FROM intg_one_minute_live_tiingo
                WHERE timestamp >= NOW() - INTERVAL '1 hour'
                ORDER BY timestamp DESC, symbol
            """)

            query1_time = time.time() - start_time

            # Symbol-specific query
            start_time = time.time()

            aapl_data = await conn.fetch("""
                SELECT * FROM intg_one_minute_live_polygon
                WHERE symbol = 'AAPL' AND timestamp >= NOW() - INTERVAL '1 day'
                ORDER BY timestamp DESC
                LIMIT 100
            """)

            query2_time = time.time() - start_time

            # Aggregation query
            start_time = time.time()

            stats = await conn.fetchrow("""
                SELECT
                    symbol,
                    COUNT(*) as bar_count,
                    AVG(close_price) as avg_price,
                    AVG(volume) as avg_volume,
                    AVG(quality_score) as avg_quality
                FROM intg_one_minute_live_tiingo
                WHERE timestamp >= NOW() - INTERVAL '1 hour'
                GROUP BY symbol
            """)

            query3_time = time.time() - start_time

            logger.info(f"Query performance: Recent={query1_time:.3f}s, Symbol={query2_time:.3f}s, Aggregation={query3_time:.3f}s")

            # All queries should be fast
            assert query1_time < 1.0  # Recent data should be very fast
            assert query2_time < 1.0  # Indexed queries should be fast
            assert query3_time < 2.0  # Aggregations should be reasonable


class TestSystemResilience:
    """Test system resilience and error recovery"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_reconnection(self, integration_db_pool):
        """Test handling of database connection issues"""
        collector = AAPLTSLASyntheticCollector()
        collector.pool = integration_db_pool

        # Normal operation
        result1 = await collector.generate_and_store_data()
        assert result1 > 0

        # Simulate connection pool exhaustion
        original_pool = collector.pool
        collector.pool = None

        # Should handle gracefully
        result2 = await collector.generate_and_store_data()
        assert result2 == 0  # Should return 0 on failure, not crash

        # Restore connection
        collector.pool = original_pool

        # Should work again
        result3 = await collector.generate_and_store_data()
        assert result3 > 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_data_validation_under_stress(self, integration_db_pool):
        """Test data validation under concurrent load"""
        collectors = [AAPLTSLASyntheticCollector() for _ in range(10)]

        for collector in collectors:
            collector.pool = integration_db_pool

        # Run concurrent stress test
        tasks = []
        for collector in collectors:
            for _ in range(5):  # 5 operations per collector
                tasks.append(collector.generate_and_store_data())

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Count successes and failures
        successes = [r for r in results if isinstance(r, int) and r >= 0]
        failures = [r for r in results if isinstance(r, Exception)]

        logger.info(f"Stress test: {len(successes)} successes, {len(failures)} failures")

        # Should have high success rate
        success_rate = len(successes) / len(results)
        assert success_rate > 0.8  # At least 80% success rate

        # Verify data integrity after stress test
        async with integration_db_pool.acquire() as conn:
            total_records = await conn.fetchval("""
                SELECT COUNT(*) FROM (
                    SELECT id FROM intg_one_minute_live_tiingo
                    UNION ALL
                    SELECT id FROM intg_one_minute_live_polygon
                ) combined
            """)

            assert total_records > 0

            # Check for data corruption
            corrupted = await conn.fetchval("""
                SELECT COUNT(*) FROM intg_one_minute_live_tiingo
                WHERE open_price <= 0 OR close_price <= 0 OR volume < 0
            """)

            assert corrupted == 0  # No corrupted data


class TestMonitoringAndMetrics:
    """Test monitoring and metrics collection"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_collection_metrics_generation(self, integration_db_pool):
        """Test generation of collection metrics"""
        collector = AAPLTSLASyntheticCollector()
        collector.pool = integration_db_pool

        # Clear existing data for clean metrics
        async with integration_db_pool.acquire() as conn:
            await conn.execute("DELETE FROM intg_one_minute_live_tiingo WHERE symbol IN ('AAPL', 'TSLA')")
            await conn.execute("DELETE FROM intg_one_minute_live_polygon WHERE symbol IN ('AAPL', 'TSLA')")

        # Generate data and collect metrics
        start_time = time.time()

        for i in range(5):
            await collector.generate_and_store_data()
            await asyncio.sleep(0.1)

        elapsed = time.time() - start_time

        # Query metrics
        async with integration_db_pool.acquire() as conn:
            metrics = await conn.fetchrow("""
                SELECT
                    COUNT(*) as total_bars,
                    COUNT(DISTINCT symbol) as unique_symbols,
                    COUNT(DISTINCT DATE_TRUNC('minute', timestamp)) as unique_minutes,
                    AVG(quality_score) as avg_quality,
                    AVG(data_latency_ms) as avg_latency,
                    MIN(received_at) as first_received,
                    MAX(received_at) as last_received
                FROM (
                    SELECT symbol, timestamp, quality_score, data_latency_ms, received_at
                    FROM intg_one_minute_live_tiingo
                    WHERE symbol IN ('AAPL', 'TSLA')
                    UNION ALL
                    SELECT symbol, timestamp, quality_score, data_latency_ms, received_at
                    FROM intg_one_minute_live_polygon
                    WHERE symbol IN ('AAPL', 'TSLA')
                ) combined
            """)

            assert metrics['total_bars'] > 0
            assert metrics['unique_symbols'] == 2  # AAPL and TSLA
            assert metrics['avg_quality'] > 0.5
            assert metrics['avg_latency'] > 0

            collection_duration = metrics['last_received'] - metrics['first_received']
            assert collection_duration.total_seconds() > 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_data_freshness_monitoring(self, integration_db_pool):
        """Test data freshness monitoring"""
        collector = AAPLTSLASyntheticCollector()
        collector.pool = integration_db_pool

        # Generate current data
        await collector.generate_and_store_data()

        # Check data freshness
        async with integration_db_pool.acquire() as conn:
            freshness = await conn.fetchrow("""
                SELECT
                    symbol,
                    MAX(timestamp) as latest_data,
                    MAX(received_at) as latest_received,
                    EXTRACT(EPOCH FROM (NOW() - MAX(received_at))) as seconds_old
                FROM intg_one_minute_live_tiingo
                WHERE symbol IN ('AAPL', 'TSLA')
                GROUP BY symbol
                ORDER BY symbol
            """)

            # Data should be very fresh (within last few seconds)
            assert freshness['seconds_old'] < 30  # Less than 30 seconds old


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-x"])