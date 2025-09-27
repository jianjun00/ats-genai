#!/usr/bin/env python3
"""
Comprehensive tests for the Prometheus Metrics Exporter

Tests cover:
- Metrics collection and export
- Custom metric definitions
- Performance monitoring
- Health check endpoints
- Error metric tracking
- Resource utilization metrics
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta, timezone
import os
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily, HistogramMetricFamily

# Import the module under test
import sys
sys.path.append('src')

from domains.market_data.services.data_collection.realtime.metrics_exporter import (
    RealtimeMetricsExporter,
    MetricsCollector,
    HealthcheckHandler
)

class TestMetricsCollector:
    """Test the MetricsCollector class"""

    @pytest.fixture
    def mock_env(self):
        """Mock environment configuration"""
        with patch('market_data.realtime.metrics_exporter.Environment') as mock_env_class:
            mock_env = Mock()
            mock_env.get_database_url.return_value = "postgresql://test:test@localhost:5432/test"
            mock_env_class.return_value = mock_env
            yield mock_env

    @pytest.fixture
    def metrics_collector(self, mock_env):
        """Create a metrics collector instance with mocked dependencies"""
        with patch.dict(os.environ, {
            'METRICS_PORT': '9090',
            'METRICS_UPDATE_INTERVAL': '30',
            'ENABLE_DETAILED_METRICS': 'true'
        }):
            collector = MetricsCollector()
            return collector

    def test_collector_initialization(self, metrics_collector):
        """Test metrics collector initialization"""
        assert metrics_collector.metrics_port == 9090
        assert metrics_collector.update_interval == 30
        assert metrics_collector.enable_detailed_metrics is True

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_initialize_database_connection(self, metrics_collector, mock_env):
        """Test database initialization"""
        mock_pool = AsyncMock()

        with patch('market_data.realtime.metrics_exporter.asyncpg.create_pool', return_value=mock_pool):
            await metrics_collector.initialize()
            assert metrics_collector.pool == mock_pool
            mock_env.get_database_url.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_collect_realtime_streaming_metrics(self, metrics_collector):
        """Test collecting real-time streaming metrics"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        metrics_collector.pool = mock_pool

        # Mock database response for streaming metrics
        mock_conn.fetch.return_value = [
            {
                'vendor': 'polygon',
                'symbol': 'AAPL',
                'bars_received_1h': 60,
                'bars_stored_1h': 58,
                'avg_latency_ms': 1500,
                'max_latency_ms': 3000,
                'collection_health_score': 0.95,
                'consecutive_missing_bars': 2,
                'last_received_timestamp': datetime.now(timezone.utc) - timedelta(minutes=1)
            },
            {
                'vendor': 'tiingo',
                'symbol': 'MSFT',
                'bars_received_1h': 60,
                'bars_stored_1h': 60,
                'avg_latency_ms': 2000,
                'max_latency_ms': 4000,
                'collection_health_score': 1.0,
                'consecutive_missing_bars': 0,
                'last_received_timestamp': datetime.now(timezone.utc) - timedelta(seconds=30)
            }
        ]

        metrics = await metrics_collector._collect_streaming_metrics()

        assert len(metrics) == 2
        assert metrics[0]['vendor'] == 'polygon'
        assert metrics[0]['symbol'] == 'AAPL'
        assert metrics[0]['bars_received_1h'] == 60
        assert metrics[1]['collection_health_score'] == 1.0
        mock_conn.fetch.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_collect_gap_detection_metrics(self, metrics_collector):
        """Test collecting gap detection metrics"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        metrics_collector.pool = mock_pool

        # Mock database response for gap metrics
        mock_conn.fetch.return_value = [
            {
                'vendor': 'polygon',
                'gaps_detected_24h': 5,
                'gaps_backfilled_24h': 4,
                'avg_gap_duration_minutes': 15.5,
                'max_gap_duration_minutes': 45.0,
                'backfill_success_rate': 0.8
            },
            {
                'vendor': 'tiingo',
                'gaps_detected_24h': 2,
                'gaps_backfilled_24h': 2,
                'avg_gap_duration_minutes': 8.0,
                'max_gap_duration_minutes': 12.0,
                'backfill_success_rate': 1.0
            }
        ]

        metrics = await metrics_collector._collect_gap_metrics()

        assert len(metrics) == 2
        assert metrics[0]['gaps_detected_24h'] == 5
        assert metrics[0]['backfill_success_rate'] == 0.8
        assert metrics[1]['gaps_detected_24h'] == 2
        assert metrics[1]['backfill_success_rate'] == 1.0
        mock_conn.fetch.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_collect_validation_metrics(self, metrics_collector):
        """Test collecting validation metrics"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        metrics_collector.pool = mock_pool

        # Mock database response for validation metrics
        mock_conn.fetch.return_value = [
            {
                'vendor': 'polygon',
                'validations_passed_7d': 45,
                'validations_failed_7d': 5,
                'avg_accuracy_score': 0.95,
                'avg_price_difference': 0.001,
                'max_price_difference': 0.01,
                'avg_latency_minutes': 1.5
            },
            {
                'vendor': 'tiingo',
                'validations_passed_7d': 48,
                'validations_failed_7d': 2,
                'avg_accuracy_score': 0.98,
                'avg_price_difference': 0.0005,
                'max_price_difference': 0.005,
                'avg_latency_minutes': 2.0
            }
        ]

        metrics = await metrics_collector._collect_validation_metrics()

        assert len(metrics) == 2
        assert metrics[0]['validations_passed_7d'] == 45
        assert metrics[0]['avg_accuracy_score'] == 0.95
        assert metrics[1]['validations_passed_7d'] == 48
        assert metrics[1]['avg_accuracy_score'] == 0.98
        mock_conn.fetch.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_collect_system_metrics(self, metrics_collector):
        """Test collecting system performance metrics"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        metrics_collector.pool = mock_pool

        # Mock database response for system metrics
        mock_conn.fetchrow.return_value = {
            'active_connections': 15,
            'database_size_mb': 5120,
            'active_realtime_collectors': 2,
            'pending_backfill_jobs': 25,
            'running_backfill_jobs': 3,
            'failed_jobs_24h': 2
        }

        metrics = await metrics_collector._collect_system_metrics()

        assert metrics['active_connections'] == 15
        assert metrics['database_size_mb'] == 5120
        assert metrics['active_realtime_collectors'] == 2
        assert metrics['pending_backfill_jobs'] == 25
        mock_conn.fetchrow.assert_called_once()

    def test_generate_prometheus_metrics(self, metrics_collector):
        """Test generating Prometheus metrics from collected data"""
        # Sample collected metrics
        streaming_metrics = [
            {
                'vendor': 'polygon', 'symbol': 'AAPL',
                'bars_received_1h': 60, 'bars_stored_1h': 58,
                'avg_latency_ms': 1500, 'collection_health_score': 0.95
            }
        ]

        gap_metrics = [
            {
                'vendor': 'polygon',
                'gaps_detected_24h': 5, 'gaps_backfilled_24h': 4,
                'backfill_success_rate': 0.8
            }
        ]

        validation_metrics = [
            {
                'vendor': 'polygon',
                'validations_passed_7d': 45, 'validations_failed_7d': 5,
                'avg_accuracy_score': 0.95
            }
        ]

        system_metrics = {
            'active_connections': 15,
            'database_size_mb': 5120,
            'pending_backfill_jobs': 25
        }

        prometheus_metrics = metrics_collector._generate_prometheus_metrics(
            streaming_metrics, gap_metrics, validation_metrics, system_metrics
        )

        # Verify correct metric families are created
        metric_names = [metric.name for metric in prometheus_metrics]

        expected_metrics = [
            'realtime_bars_received_total',
            'realtime_bars_stored_total',
            'realtime_data_latency_seconds',
            'realtime_collection_health_score',
            'realtime_gaps_detected_total',
            'realtime_gaps_backfilled_total',
            'realtime_backfill_success_rate',
            'realtime_validation_accuracy_score',
            'realtime_system_connections',
            'realtime_database_size_bytes',
            'realtime_pending_jobs'
        ]

        for expected in expected_metrics:
            assert expected in metric_names

    def test_create_counter_metric(self, metrics_collector):
        """Test creating counter metrics"""
        metric = metrics_collector._create_counter_metric(
            'test_counter_total',
            'Test counter description',
            [('vendor', 'polygon'), ('symbol', 'AAPL')],
            100
        )

        assert isinstance(metric, CounterMetricFamily)
        assert metric.name == 'test_counter_total'
        assert metric.documentation == 'Test counter description'
        # Check that samples are added correctly
        assert len(metric.samples) > 0

    def test_create_gauge_metric(self, metrics_collector):
        """Test creating gauge metrics"""
        metric = metrics_collector._create_gauge_metric(
            'test_gauge',
            'Test gauge description',
            [('vendor', 'polygon'), ('symbol', 'AAPL')],
            0.95
        )

        assert isinstance(metric, GaugeMetricFamily)
        assert metric.name == 'test_gauge'
        assert metric.documentation == 'Test gauge description'
        # Check that samples are added correctly
        assert len(metric.samples) > 0

    def test_create_histogram_metric(self, metrics_collector):
        """Test creating histogram metrics"""
        # Sample latency data
        latency_values = [100, 200, 150, 300, 250, 180, 220]

        metric = metrics_collector._create_histogram_metric(
            'test_histogram_seconds',
            'Test histogram description',
            [('vendor', 'polygon')],
            latency_values
        )

        assert isinstance(metric, HistogramMetricFamily)
        assert metric.name == 'test_histogram_seconds'
        assert metric.documentation == 'Test histogram description'
        # Check that histogram buckets are created
        assert len(metric.samples) > 0

class TestRealtimeMetricsExporter:
    """Test the main RealtimeMetricsExporter class"""

    @pytest.fixture
    def mock_env(self):
        """Mock environment configuration"""
        with patch('market_data.realtime.metrics_exporter.Environment') as mock_env_class:
            mock_env = Mock()
            mock_env.get_database_url.return_value = "postgresql://test:test@localhost:5432/test"
            mock_env_class.return_value = mock_env
            yield mock_env

    @pytest.fixture
    def metrics_exporter(self, mock_env):
        """Create a metrics exporter instance with mocked dependencies"""
        with patch.dict(os.environ, {
            'METRICS_PORT': '9090',
            'METRICS_UPDATE_INTERVAL': '30',
            'ENABLE_HEALTH_CHECKS': 'true'
        }):
            exporter = RealtimeMetricsExporter()
            return exporter

    def test_exporter_initialization(self, metrics_exporter):
        """Test metrics exporter initialization"""
        assert metrics_exporter.metrics_port == 9090
        assert metrics_exporter.update_interval == 30
        assert metrics_exporter.enable_health_checks is True

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_initialize_exporter(self, metrics_exporter, mock_env):
        """Test exporter initialization"""
        with patch('market_data.realtime.metrics_exporter.start_http_server') as mock_start_server:
            mock_collector = AsyncMock()
            metrics_exporter.collector = mock_collector

            await metrics_exporter.initialize()

            mock_collector.initialize.assert_called_once()
            mock_start_server.assert_called_once_with(9090)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_metrics_update_loop(self, metrics_exporter):
        """Test metrics update loop"""
        mock_collector = AsyncMock()
        mock_collector.collect_all_metrics = AsyncMock()
        metrics_exporter.collector = mock_collector
        metrics_exporter.running = True

        # Mock update_interval to be very short for testing
        metrics_exporter.update_interval = 0.1

        # Start update loop for short time
        update_task = asyncio.create_task(metrics_exporter._metrics_update_loop())
        await asyncio.sleep(0.2)  # Let it run briefly

        metrics_exporter.running = False
        await update_task

        # Should have called collect_all_metrics at least once
        assert mock_collector.collect_all_metrics.call_count >= 1

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_start_exporter(self, metrics_exporter):
        """Test starting the metrics exporter"""
        metrics_exporter.initialize = AsyncMock()
        metrics_exporter._metrics_update_loop = AsyncMock()

        # Mock the update loop to return quickly
        async def mock_update_loop():
            await asyncio.sleep(0.1)

        metrics_exporter._metrics_update_loop = mock_update_loop

        # Start and quickly stop
        start_task = asyncio.create_task(metrics_exporter.start())
        await asyncio.sleep(0.05)

        metrics_exporter.running = False
        await start_task

        metrics_exporter.initialize.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_shutdown_exporter(self, metrics_exporter):
        """Test graceful shutdown"""
        mock_collector = AsyncMock()
        metrics_exporter.collector = mock_collector
        metrics_exporter.running = True

        await metrics_exporter.shutdown()

        assert metrics_exporter.running is False
        mock_collector.shutdown.assert_called_once()

class TestHealthcheckHandler:
    """Test the health check handler"""

    @pytest.fixture
    def health_handler(self):
        """Create a health check handler"""
        return HealthcheckHandler()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_health_check_endpoint(self, health_handler):
        """Test health check endpoint response"""
        mock_request = Mock()
        mock_response = Mock()

        # Mock aiohttp web response
        with patch('market_data.realtime.metrics_exporter.web.json_response') as mock_json_response:
            mock_json_response.return_value = mock_response

            response = await health_handler.health_check(mock_request)

            # Verify response structure
            mock_json_response.assert_called_once()
            call_args = mock_json_response.call_args[0][0]

            assert 'status' in call_args
            assert 'timestamp' in call_args
            assert 'checks' in call_args

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_ready_check_endpoint(self, health_handler):
        """Test readiness check endpoint"""
        mock_request = Mock()

        # Mock database check to pass
        health_handler._check_database_connectivity = AsyncMock(return_value=True)
        health_handler._check_metrics_collection = AsyncMock(return_value=True)

        with patch('market_data.realtime.metrics_exporter.web.json_response') as mock_json_response:
            await health_handler.ready_check(mock_request)

            call_args = mock_json_response.call_args[0][0]
            assert call_args['status'] == 'ready'

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_live_check_endpoint(self, health_handler):
        """Test liveness check endpoint"""
        mock_request = Mock()

        with patch('market_data.realtime.metrics_exporter.web.json_response') as mock_json_response:
            await health_handler.live_check(mock_request)

            call_args = mock_json_response.call_args[0][0]
            assert call_args['status'] == 'alive'

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_connectivity_check(self, health_handler):
        """Test database connectivity check"""
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.fetchval.return_value = 1

        health_handler.pool = mock_pool

        result = await health_handler._check_database_connectivity()

        assert result is True
        mock_conn.fetchval.assert_called_once_with("SELECT 1")

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_connectivity_check_failure(self, health_handler):
        """Test database connectivity check failure"""
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.fetchval.side_effect = Exception("Connection failed")

        health_handler.pool = mock_pool

        result = await health_handler._check_database_connectivity()

        assert result is False

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_metrics_collection_check(self, health_handler):
        """Test metrics collection health check"""
        # Mock recent metrics collection
        health_handler.last_collection_time = datetime.now(timezone.utc) - timedelta(seconds=30)

        result = await health_handler._check_metrics_collection()

        assert result is True

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_metrics_collection_check_stale(self, health_handler):
        """Test metrics collection check when stale"""
        # Mock stale metrics collection
        health_handler.last_collection_time = datetime.now(timezone.utc) - timedelta(minutes=10)

        result = await health_handler._check_metrics_collection()

        assert result is False

class TestIntegrationScenarios:
    """Test integration scenarios and end-to-end flows"""

    @pytest.fixture
    def metrics_exporter(self):
        with patch('market_data.realtime.metrics_exporter.Environment'):
            with patch.dict(os.environ, {
                'METRICS_PORT': '9090',
                'METRICS_UPDATE_INTERVAL': '30'
            }):
                return RealtimeMetricsExporter()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_full_metrics_collection_cycle(self, metrics_exporter):
        """Test complete metrics collection cycle"""
        mock_collector = AsyncMock()

        # Mock collected data
        mock_streaming_metrics = [
            {'vendor': 'polygon', 'symbol': 'AAPL', 'bars_received_1h': 60}
        ]
        mock_gap_metrics = [
            {'vendor': 'polygon', 'gaps_detected_24h': 5}
        ]
        mock_validation_metrics = [
            {'vendor': 'polygon', 'validations_passed_7d': 45}
        ]
        mock_system_metrics = {'active_connections': 15}

        mock_collector._collect_streaming_metrics.return_value = mock_streaming_metrics
        mock_collector._collect_gap_metrics.return_value = mock_gap_metrics
        mock_collector._collect_validation_metrics.return_value = mock_validation_metrics
        mock_collector._collect_system_metrics.return_value = mock_system_metrics
        mock_collector._generate_prometheus_metrics.return_value = []

        metrics_exporter.collector = mock_collector

        await mock_collector.collect_all_metrics()

        # Verify all metric types were collected
        mock_collector._collect_streaming_metrics.assert_called_once()
        mock_collector._collect_gap_metrics.assert_called_once()
        mock_collector._collect_validation_metrics.assert_called_once()
        mock_collector._collect_system_metrics.assert_called_once()
        mock_collector._generate_prometheus_metrics.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_error_handling_during_collection(self, metrics_exporter):
        """Test error handling during metrics collection"""
        mock_collector = AsyncMock()

        # Mock one collection method to fail
        mock_collector._collect_streaming_metrics.side_effect = Exception("Database error")
        mock_collector._collect_gap_metrics.return_value = []
        mock_collector._collect_validation_metrics.return_value = []
        mock_collector._collect_system_metrics.return_value = {}
        mock_collector._generate_prometheus_metrics.return_value = []

        metrics_exporter.collector = mock_collector

        # Should not raise exception, just log error and continue
        await mock_collector.collect_all_metrics()

        # Other collections should still be attempted
        mock_collector._collect_gap_metrics.assert_called_once()
        mock_collector._collect_validation_metrics.assert_called_once()
        mock_collector._collect_system_metrics.assert_called_once()

class TestPerformanceMetrics:
    """Test performance-related metrics and monitoring"""

    @pytest.fixture
    def metrics_collector(self):
        with patch('market_data.realtime.metrics_exporter.Environment'):
            return MetricsCollector()

    def test_metrics_collection_performance(self, metrics_collector):
        """Test metrics collection performance"""
        # Simulate large dataset
        large_streaming_metrics = [
            {
                'vendor': f'vendor_{i%3}',
                'symbol': f'SYM{i:04d}',
                'bars_received_1h': 60,
                'bars_stored_1h': 58,
                'avg_latency_ms': 1500,
                'collection_health_score': 0.95
            } for i in range(1000)
        ]

        import time
        start_time = time.time()

        # Generate metrics for large dataset
        prometheus_metrics = metrics_collector._generate_prometheus_metrics(
            large_streaming_metrics, [], [], {}
        )

        processing_time = time.time() - start_time

        # Should process large dataset efficiently
        assert processing_time < 1.0  # Should complete within 1 second
        assert len(prometheus_metrics) > 0

    def test_memory_usage_during_collection(self, metrics_collector):
        """Test memory usage during metrics collection"""
        import tracemalloc

        tracemalloc.start()

        # Simulate memory-intensive collection
        large_dataset = [
            {f'metric_{i}': f'value_{i}' for i in range(100)}
            for _ in range(100)
        ]

        # Process the dataset
        processed_data = metrics_collector._process_raw_metrics(large_dataset)

        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # Memory usage should be reasonable
        assert peak < 50 * 1024 * 1024  # Less than 50MB
        assert processed_data is not None

if __name__ == '__main__':
    pytest.main([__file__, '-v'])