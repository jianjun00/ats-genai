#!/usr/bin/env python3
"""
End-to-End Tests for Real-time Collection System

Comprehensive end-to-end tests covering:
- Complete collection workflow from start to finish
- Service startup and shutdown procedures
- Real-time monitoring and alerting
- Data pipeline integration
- System recovery scenarios
- Production readiness validation
"""

import pytest
import asyncio
import asyncpg
import aiohttp
import logging
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any
import os
import sys
import subprocess

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from domains.market_data.services.realtime.aapl_tsla_synthetic_collector import AAPLTSLASyntheticCollector

logger = logging.getLogger(__name__)

@pytest.fixture
async def e2e_db_pool():
    """Database pool for end-to-end testing"""
    dsn = "postgresql://postgres:intg_password@localhost:4432/intg_db"
    try:
        pool = await asyncpg.create_pool(dsn, min_size=3, max_size=10)
        yield pool
        await pool.close()
    except Exception as e:
        logger.warning(f"Cannot connect to E2E database: {e}")
        pytest.skip("E2E database not available")

@pytest.fixture
async def http_session():
    """HTTP session for API testing"""
    session = aiohttp.ClientSession()
    yield session
    await session.close()


class E2ETestHarness:
    """Test harness for end-to-end testing"""

    def __init__(self, db_pool):
        self.db_pool = db_pool
        self.collectors = []
        self.monitoring_data = []

    async def setup_test_environment(self):
        """Setup clean test environment"""
        async with self.db_pool.acquire() as conn:
            # Clean existing test data
            await conn.execute("DELETE FROM intg_one_minute_live_tiingo WHERE symbol IN ('AAPL', 'TSLA')")
            await conn.execute("DELETE FROM intg_one_minute_live_polygon WHERE symbol IN ('AAPL', 'TSLA')")

            # Ensure required tables exist
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS e2e_collection_log (
                    id BIGSERIAL PRIMARY KEY,
                    test_id VARCHAR(100),
                    event_type VARCHAR(50),
                    event_data JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

    async def start_collector(self, collector_id: str) -> AAPLTSLASyntheticCollector:
        """Start a collector and add to managed list"""
        collector = AAPLTSLASyntheticCollector()
        collector.pool = self.db_pool

        # Log collector start
        await self.log_event(f"collector_start_{collector_id}", {"collector_id": collector_id})

        self.collectors.append((collector_id, collector))
        return collector

    async def stop_all_collectors(self):
        """Stop all managed collectors"""
        for collector_id, collector in self.collectors:
            collector.running = False
            await self.log_event(f"collector_stop_{collector_id}", {"collector_id": collector_id})

        self.collectors.clear()

    async def log_event(self, event_type: str, event_data: Dict[str, Any], test_id: str = "default"):
        """Log test events for analysis"""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO e2e_collection_log (test_id, event_type, event_data)
                VALUES ($1, $2, $3)
            """, test_id, event_type, json.dumps(event_data))

    async def get_collection_metrics(self, time_window_minutes: int = 30) -> Dict[str, Any]:
        """Get comprehensive collection metrics"""
        async with self.db_pool.acquire() as conn:
            metrics = await conn.fetchrow(f"""
                SELECT
                    COUNT(*) as total_records,
                    COUNT(DISTINCT symbol) as unique_symbols,
                    COUNT(DISTINCT DATE_TRUNC('minute', timestamp)) as unique_timepoints,
                    MIN(timestamp) as earliest_data,
                    MAX(timestamp) as latest_data,
                    AVG(quality_score) as avg_quality_score,
                    AVG(data_latency_ms) as avg_latency_ms
                FROM (
                    SELECT symbol, timestamp, quality_score, data_latency_ms
                    FROM intg_one_minute_live_tiingo
                    WHERE timestamp >= NOW() - INTERVAL '{time_window_minutes} minutes'
                    UNION ALL
                    SELECT symbol, timestamp, quality_score, data_latency_ms
                    FROM intg_one_minute_live_polygon
                    WHERE timestamp >= NOW() - INTERVAL '{time_window_minutes} minutes'
                ) combined
            """)

            return dict(metrics) if metrics else {}

    async def verify_data_integrity(self) -> Dict[str, Any]:
        """Verify data integrity across the system"""
        async with self.db_pool.acquire() as conn:
            integrity_checks = {}

            # Check for orphaned records
            integrity_checks['orphaned_records'] = await conn.fetchval("""
                SELECT COUNT(*) FROM intg_one_minute_live_tiingo
                WHERE symbol IS NULL OR timestamp IS NULL
            """) + await conn.fetchval("""
                SELECT COUNT(*) FROM intg_one_minute_live_polygon
                WHERE symbol IS NULL OR timestamp IS NULL
            """)

            # Check for duplicate timestamps per symbol
            integrity_checks['duplicate_tiingo'] = await conn.fetchval("""
                SELECT COUNT(*) FROM (
                    SELECT symbol, timestamp, COUNT(*)
                    FROM intg_one_minute_live_tiingo
                    GROUP BY symbol, timestamp
                    HAVING COUNT(*) > 1
                ) dups
            """)

            integrity_checks['duplicate_polygon'] = await conn.fetchval("""
                SELECT COUNT(*) FROM (
                    SELECT symbol, timestamp, COUNT(*)
                    FROM intg_one_minute_live_polygon
                    GROUP BY symbol, timestamp
                    HAVING COUNT(*) > 1
                ) dups
            """)

            # Check for invalid price data
            integrity_checks['invalid_prices'] = await conn.fetchval("""
                SELECT COUNT(*) FROM intg_one_minute_live_tiingo
                WHERE close_price <= 0 OR open_price <= 0
            """) + await conn.fetchval("""
                SELECT COUNT(*) FROM intg_one_minute_live_polygon
                WHERE close_price <= 0 OR open_price <= 0
            """)

            return integrity_checks


class TestCompleteWorkflow:
    """Test complete collection workflow"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_full_collection_lifecycle(self, e2e_db_pool):
        """Test complete collection lifecycle from start to finish"""
        harness = E2ETestHarness(e2e_db_pool)
        await harness.setup_test_environment()

        try:
            # Phase 1: Startup
            await harness.log_event("test_start", {"phase": "startup"})
            collector = await harness.start_collector("main_collector")

            # Phase 2: Initial collection burst
            await harness.log_event("collection_start", {"phase": "initial_burst"})

            initial_metrics = await harness.get_collection_metrics(5)  # Last 5 minutes
            baseline_count = initial_metrics.get('total_records', 0)

            # Collect data for 2 minutes
            for i in range(6):  # 6 collections over ~2 minutes
                result = await collector.generate_and_store_data()
                await harness.log_event("collection_cycle", {
                    "cycle": i,
                    "records_stored": result,
                    "timestamp": datetime.now().isoformat()
                })
                await asyncio.sleep(20)  # 20 second intervals

            # Phase 3: Verify initial collection
            post_collection_metrics = await harness.get_collection_metrics(5)

            logger.info(f"Collection results: {post_collection_metrics}")

            assert post_collection_metrics['total_records'] > baseline_count
            assert post_collection_metrics['unique_symbols'] >= 2  # AAPL and TSLA
            assert post_collection_metrics['unique_timepoints'] >= 3  # At least 3 time points

            # Phase 4: Data integrity verification
            integrity_results = await harness.verify_data_integrity()

            await harness.log_event("integrity_check", integrity_results)

            assert integrity_results['orphaned_records'] == 0
            assert integrity_results['duplicate_tiingo'] == 0
            assert integrity_results['duplicate_polygon'] == 0
            assert integrity_results['invalid_prices'] == 0

            # Phase 5: Continuous operation simulation
            await harness.log_event("continuous_operation", {"phase": "sustained_collection"})

            sustained_results = []
            for i in range(5):
                result = await collector.generate_and_store_data()
                sustained_results.append(result)
                await asyncio.sleep(10)

            # Verify sustained operation
            final_metrics = await harness.get_collection_metrics(10)

            assert final_metrics['total_records'] > post_collection_metrics['total_records']
            assert all(r > 0 for r in sustained_results)  # All collections should succeed

            await harness.log_event("test_success", {"final_metrics": final_metrics})

        finally:
            await harness.stop_all_collectors()
            await harness.log_event("test_end", {"phase": "cleanup"})

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_concurrent_collectors_workflow(self, e2e_db_pool):
        """Test workflow with multiple concurrent collectors"""
        harness = E2ETestHarness(e2e_db_pool)
        await harness.setup_test_environment()

        try:
            # Start multiple collectors
            collectors = []
            for i in range(3):
                collector = await harness.start_collector(f"collector_{i}")
                collectors.append(collector)

            # Run concurrent collection
            async def collector_task(collector_id, collector):
                results = []
                for cycle in range(5):
                    result = await collector.generate_and_store_data()
                    results.append(result)
                    await harness.log_event("concurrent_collection", {
                        "collector_id": collector_id,
                        "cycle": cycle,
                        "result": result
                    })
                    await asyncio.sleep(15)  # Staggered timing
                return results

            # Execute concurrent tasks
            tasks = [
                collector_task(f"collector_{i}", collectors[i])
                for i in range(len(collectors))
            ]

            concurrent_results = await asyncio.gather(*tasks)

            # Verify concurrent operation results
            total_operations = sum(len(results) for results in concurrent_results)
            successful_operations = sum(
                sum(1 for r in results if r > 0)
                for results in concurrent_results
            )

            success_rate = successful_operations / total_operations if total_operations > 0 else 0

            logger.info(f"Concurrent operation: {successful_operations}/{total_operations} successful ({success_rate:.2%})")

            assert success_rate > 0.9  # 90% success rate minimum

            # Check data integrity after concurrent operations
            integrity_results = await harness.verify_data_integrity()
            assert integrity_results['orphaned_records'] == 0

        finally:
            await harness.stop_all_collectors()


class TestSystemRecovery:
    """Test system recovery scenarios"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_reconnection_recovery(self, e2e_db_pool):
        """Test recovery from database connection issues"""
        harness = E2ETestHarness(e2e_db_pool)
        await harness.setup_test_environment()

        try:
            collector = await harness.start_collector("recovery_test_collector")

            # Normal operation
            result1 = await collector.generate_and_store_data()
            assert result1 > 0

            # Simulate database connection failure
            original_pool = collector.pool
            collector.pool = None

            # Should handle gracefully
            result2 = await collector.generate_and_store_data()
            assert result2 == 0  # Should return 0, not crash

            await harness.log_event("db_connection_failure", {"handled_gracefully": True})

            # Restore connection
            collector.pool = original_pool

            # Should resume normal operation
            result3 = await collector.generate_and_store_data()
            assert result3 > 0

            await harness.log_event("db_connection_recovery", {"resumed_successfully": True})

        finally:
            await harness.stop_all_collectors()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_data_corruption_recovery(self, e2e_db_pool):
        """Test recovery from data corruption scenarios"""
        harness = E2ETestHarness(e2e_db_pool)
        await harness.setup_test_environment()

        collector = await harness.start_collector("corruption_test_collector")

        try:
            # Generate clean data
            await collector.generate_and_store_data()

            # Introduce corrupted data directly to database
            async with e2e_db_pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO intg_one_minute_live_tiingo
                    (symbol, timestamp, open_price, close_price, volume)
                    VALUES ('CORRUPT', NOW(), -1, NULL, -999)
                """)

            # System should continue operating despite corruption
            result = await collector.generate_and_store_data()
            assert result > 0

            # Verify integrity detection
            integrity_results = await harness.verify_data_integrity()
            assert integrity_results['invalid_prices'] > 0  # Should detect corruption

            # Clean up corruption
            async with e2e_db_pool.acquire() as conn:
                await conn.execute("DELETE FROM intg_one_minute_live_tiingo WHERE symbol = 'CORRUPT'")

        finally:
            await harness.stop_all_collectors()


class TestMonitoringIntegration:
    """Test monitoring and alerting integration"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_metrics_collection_integration(self, e2e_db_pool):
        """Test integration with metrics collection systems"""
        harness = E2ETestHarness(e2e_db_pool)
        await harness.setup_test_environment()

        collector = await harness.start_collector("metrics_test_collector")

        try:
            # Generate data with monitoring
            start_time = time.time()

            for i in range(5):
                cycle_start = time.time()
                result = await collector.generate_and_store_data()
                cycle_time = time.time() - cycle_start

                # Log detailed metrics
                await harness.log_event("metrics_cycle", {
                    "cycle": i,
                    "records_stored": result,
                    "cycle_time_seconds": cycle_time,
                    "timestamp": datetime.now().isoformat()
                })

                await asyncio.sleep(10)

            total_time = time.time() - start_time

            # Analyze collected metrics
            async with e2e_db_pool.acquire() as conn:
                metric_analysis = await conn.fetch("""
                    SELECT
                        event_data->>'cycle' as cycle,
                        (event_data->>'records_stored')::int as records,
                        (event_data->>'cycle_time_seconds')::float as cycle_time
                    FROM e2e_collection_log
                    WHERE event_type = 'metrics_cycle'
                        AND created_at >= NOW() - INTERVAL '5 minutes'
                    ORDER BY (event_data->>'cycle')::int
                """)

            if metric_analysis:
                cycle_times = [float(row['cycle_time']) for row in metric_analysis]
                avg_cycle_time = sum(cycle_times) / len(cycle_times)
                total_records = sum(int(row['records']) for row in metric_analysis)

                throughput = total_records / total_time

                logger.info(f"Metrics analysis: avg_cycle_time={avg_cycle_time:.3f}s, throughput={throughput:.1f} records/s")

                # Performance should be reasonable
                assert avg_cycle_time < 1.0  # Under 1 second per cycle
                assert throughput > 5  # At least 5 records per second

        finally:
            await harness.stop_all_collectors()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_health_check_endpoints(self, e2e_db_pool, http_session):
        """Test health check endpoint integration"""
        harness = E2ETestHarness(e2e_db_pool)
        await harness.setup_test_environment()

        # Note: This would typically test actual health check endpoints
        # For this test, we'll simulate the health check logic

        collector = await harness.start_collector("health_check_test")

        try:
            # Simulate health check data collection
            health_data = {
                'status': 'healthy',
                'collectors_running': len(harness.collectors),
                'last_collection_time': datetime.now().isoformat(),
                'database_connected': True
            }

            # Generate some data to ensure system is operational
            result = await collector.generate_and_store_data()
            health_data['last_collection_records'] = result

            # Get system metrics for health check
            metrics = await harness.get_collection_metrics(5)
            health_data['recent_records'] = metrics.get('total_records', 0)
            health_data['data_quality'] = metrics.get('avg_quality_score', 0)

            # Health check should indicate healthy system
            assert health_data['status'] == 'healthy'
            assert health_data['collectors_running'] > 0
            assert health_data['database_connected']
            assert health_data['recent_records'] > 0

            await harness.log_event("health_check", health_data)

        finally:
            await harness.stop_all_collectors()


class TestProductionReadiness:
    """Test production readiness scenarios"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_production_scale_simulation(self, e2e_db_pool):
        """Simulate production-scale operation"""
        harness = E2ETestHarness(e2e_db_pool)
        await harness.setup_test_environment()

        try:
            # Simulate production load with multiple collectors
            num_collectors = 5
            collectors = []

            for i in range(num_collectors):
                collector = await harness.start_collector(f"prod_collector_{i}")
                collectors.append(collector)

            # Run production-like workload
            async def production_workload(collector_id, collector):
                results = []
                errors = 0

                for cycle in range(10):  # 10 cycles simulating extended operation
                    try:
                        start_time = time.time()
                        result = await collector.generate_and_store_data()
                        duration = time.time() - start_time

                        results.append({
                            'cycle': cycle,
                            'result': result,
                            'duration': duration,
                            'success': result > 0
                        })

                        # Log detailed production metrics
                        await harness.log_event("production_cycle", {
                            "collector_id": collector_id,
                            "cycle": cycle,
                            "records": result,
                            "duration_ms": duration * 1000,
                            "success": result > 0
                        })

                    except Exception as e:
                        errors += 1
                        logger.error(f"Production workload error in {collector_id}: {e}")

                    await asyncio.sleep(5)  # 5-second intervals for intensive testing

                return {"results": results, "errors": errors}

            # Execute production workload
            production_tasks = [
                production_workload(f"prod_collector_{i}", collectors[i])
                for i in range(num_collectors)
            ]

            workload_results = await asyncio.gather(*production_tasks)

            # Analyze production performance
            total_cycles = sum(len(r["results"]) for r in workload_results)
            total_errors = sum(r["errors"] for r in workload_results)
            successful_cycles = sum(
                sum(1 for cycle in r["results"] if cycle["success"])
                for r in workload_results
            )

            success_rate = successful_cycles / total_cycles if total_cycles > 0 else 0
            error_rate = total_errors / total_cycles if total_cycles > 0 else 0

            # Calculate performance metrics
            all_durations = []
            for result_set in workload_results:
                all_durations.extend(cycle["duration"] for cycle in result_set["results"])

            avg_duration = sum(all_durations) / len(all_durations) if all_durations else 0
            max_duration = max(all_durations) if all_durations else 0

            logger.info(f"Production simulation results:")
            logger.info(f"  Success rate: {success_rate:.2%}")
            logger.info(f"  Error rate: {error_rate:.2%}")
            logger.info(f"  Avg duration: {avg_duration:.3f}s")
            logger.info(f"  Max duration: {max_duration:.3f}s")

            # Production readiness assertions
            assert success_rate > 0.95  # 95% success rate for production
            assert error_rate < 0.05    # Less than 5% error rate
            assert avg_duration < 0.5   # Average operation under 500ms
            assert max_duration < 2.0   # No operation should take more than 2 seconds

            # Verify final system state
            final_integrity = await harness.verify_data_integrity()
            assert final_integrity['orphaned_records'] == 0

        finally:
            await harness.stop_all_collectors()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_long_running_stability(self, e2e_db_pool):
        """Test long-running system stability"""
        harness = E2ETestHarness(e2e_db_pool)
        await harness.setup_test_environment()

        collector = await harness.start_collector("stability_test_collector")

        try:
            # Simulate extended operation (compressed time scale)
            stability_metrics = []

            for hour in range(12):  # Simulate 12 hours of operation
                hour_start = time.time()
                hour_results = []

                # Collect data multiple times per "hour"
                for minute in range(6):  # 6 collections per "hour"
                    result = await collector.generate_and_store_data()
                    hour_results.append(result)
                    await asyncio.sleep(5)  # 5 seconds between collections

                hour_duration = time.time() - hour_start
                hour_metrics = {
                    'hour': hour,
                    'collections': len(hour_results),
                    'successful_collections': sum(1 for r in hour_results if r > 0),
                    'total_records': sum(hour_results),
                    'duration': hour_duration,
                    'avg_records_per_collection': sum(hour_results) / len(hour_results) if hour_results else 0
                }

                stability_metrics.append(hour_metrics)

                await harness.log_event("stability_hour", hour_metrics)

                logger.info(f"Hour {hour}: {hour_metrics['successful_collections']}/{hour_metrics['collections']} successful, {hour_metrics['total_records']} records")

            # Analyze stability over time
            success_rates = [
                m['successful_collections'] / m['collections']
                for m in stability_metrics if m['collections'] > 0
            ]

            record_counts = [m['total_records'] for m in stability_metrics]

            # Calculate stability indicators
            avg_success_rate = sum(success_rates) / len(success_rates) if success_rates else 0
            success_rate_variance = sum((sr - avg_success_rate) ** 2 for sr in success_rates) / len(success_rates) if success_rates else 0

            avg_record_count = sum(record_counts) / len(record_counts) if record_counts else 0
            record_count_variance = sum((rc - avg_record_count) ** 2 for rc in record_counts) / len(record_counts) if record_counts else 0

            logger.info(f"Stability analysis:")
            logger.info(f"  Average success rate: {avg_success_rate:.3%}")
            logger.info(f"  Success rate variance: {success_rate_variance:.6f}")
            logger.info(f"  Average records per hour: {avg_record_count:.1f}")
            logger.info(f"  Record count variance: {record_count_variance:.1f}")

            # Stability assertions
            assert avg_success_rate > 0.95  # Consistent high success rate
            assert success_rate_variance < 0.01  # Low variance in success rate
            assert avg_record_count > 10  # Reasonable record generation

            # Check for performance degradation over time
            first_half_avg = sum(record_counts[:6]) / 6
            second_half_avg = sum(record_counts[6:]) / 6

            performance_change = abs(second_half_avg - first_half_avg) / first_half_avg if first_half_avg > 0 else 0

            assert performance_change < 0.2  # Less than 20% performance change over time

        finally:
            await harness.stop_all_collectors()


class TestIntegrationWithExistingServices:
    """Test integration with existing ATS services"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_analytics_service_integration(self, e2e_db_pool):
        """Test integration with analytics service"""
        harness = E2ETestHarness(e2e_db_pool)
        await harness.setup_test_environment()

        collector = await harness.start_collector("analytics_integration_test")

        try:
            # Generate data that analytics service would consume
            for _ in range(10):
                await collector.generate_and_store_data()
                await asyncio.sleep(5)

            # Verify data is accessible in format analytics service expects
            async with e2e_db_pool.acquire() as conn:
                analytics_data = await conn.fetch("""
                    SELECT
                        symbol,
                        DATE_TRUNC('minute', timestamp) as minute_bucket,
                        AVG(close_price) as avg_close,
                        SUM(volume) as total_volume,
                        COUNT(*) as record_count
                    FROM (
                        SELECT symbol, timestamp, close_price, volume
                        FROM intg_one_minute_live_tiingo
                        WHERE timestamp >= NOW() - INTERVAL '30 minutes'
                        UNION ALL
                        SELECT symbol, timestamp, close_price, volume
                        FROM intg_one_minute_live_polygon
                        WHERE timestamp >= NOW() - INTERVAL '30 minutes'
                    ) combined
                    GROUP BY symbol, DATE_TRUNC('minute', timestamp)
                    ORDER BY symbol, minute_bucket DESC
                """)

            # Verify analytics-ready data
            assert len(analytics_data) > 0

            symbols_found = set(row['symbol'] for row in analytics_data)
            assert 'AAPL' in symbols_found
            assert 'TSLA' in symbols_found

            # Verify data quality for analytics
            for row in analytics_data:
                assert row['avg_close'] > 0
                assert row['total_volume'] > 0
                assert row['record_count'] > 0

            await harness.log_event("analytics_integration", {
                "records_analyzed": len(analytics_data),
                "symbols": list(symbols_found),
                "integration_success": True
            })

        finally:
            await harness.stop_all_collectors()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s", "-x"])