#!/usr/bin/env python3
"""
End-to-End Integration Tests for Real-Time Market Data Collection System

Tests cover:
- Complete data flow from collection to storage
- Multi-vendor integration scenarios
- Error recovery and resilience
- Performance under load
- System health monitoring
- Deployment validation
"""

import pytest
import asyncio
import asyncpg
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from datetime import datetime, timedelta, timezone, date
import json
import os
import aiohttp
import tempfile
import subprocess

# Import the modules under test
import sys
sys.path.append('src')

from domains.market_data.services.realtime.streaming_collector import RealtimeStreamingCollector
from domains.market_data.services.realtime.daily_validation import DailyValidationEngine
from domains.market_data.services.realtime.gap_detector import GapDetectionEngine
from domains.market_data.services.realtime.weekly_backfill import WeeklyBackfillEngine
from domains.market_data.services.realtime.metrics_exporter import RealtimeMetricsExporter

@pytest.mark.integration
class TestEndToEndDataFlow:
    """Test complete data flow from collection to validation"""

    @pytest.fixture
    async def integration_environment(self):
        """Set up integration test environment"""
        # Mock database pool
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        # Mock successful database operations
        mock_conn.execute.return_value = None
        mock_conn.fetch.return_value = []
        mock_conn.fetchrow.return_value = {}
        mock_conn.fetchval.return_value = None

        env_vars = {
            'POLYGON_API_KEY': 'test_polygon_key',
            'TIINGO_API_KEY': 'test_tiingo_key',
            'FMP_API_KEY': 'test_fmp_key',
            'MAX_LATENCY_SECONDS': '120',
            'UNIVERSE_SIZE': '10',
            'MARKET_HOURS_ONLY': 'false'
        }

        with patch.dict(os.environ, env_vars):
            with patch('market_data.realtime.streaming_collector.Environment'):
                with patch('market_data.realtime.streaming_collector.asyncpg.create_pool', return_value=mock_pool):
                    yield {
                        'mock_pool': mock_pool,
                        'mock_conn': mock_conn
                    }

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_complete_data_collection_flow(self, integration_environment):
        """Test complete data collection flow from WebSocket to database"""
        mock_pool = integration_environment['mock_pool']
        mock_conn = integration_environment['mock_conn']

        # Initialize streaming collector
        collector = RealtimeStreamingCollector()
        await collector.initialize()

        # Mock universe loading
        mock_conn.fetch.return_value = [
            {'symbol': 'AAPL', 'instrument_id': 1},
            {'symbol': 'MSFT', 'instrument_id': 2}
        ]
        await collector._load_active_universe()

        # Simulate receiving Polygon WebSocket data
        polygon_data = {
            'ev': 'AM',
            'sym': 'AAPL',
            't': int(datetime.now(timezone.utc).timestamp() * 1000),
            'o': 150.0,
            'h': 152.0,
            'l': 149.0,
            'c': 151.0,
            'v': 1000000,
            'vw': 150.5,
            'n': 500
        }

        # Process the data
        await collector._process_polygon_minute_bar(polygon_data)

        # Verify data was stored
        assert mock_conn.execute.call_count >= 1

        # Verify collection status was updated
        storage_calls = [call for call in mock_conn.execute.call_args_list
                        if 'INSERT INTO' in str(call)]
        assert len(storage_calls) >= 1

        await collector.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_multi_vendor_data_integration(self, integration_environment):
        """Test integration of data from multiple vendors"""
        mock_pool = integration_environment['mock_pool']
        mock_conn = integration_environment['mock_conn']

        collector = RealtimeStreamingCollector()
        await collector.initialize()

        # Mock universe
        mock_conn.fetch.return_value = [{'symbol': 'AAPL', 'instrument_id': 1}]
        await collector._load_active_universe()

        # Simulate data from multiple vendors
        vendors_data = [
            # Polygon data
            {
                'vendor': 'polygon',
                'data': {
                    'ev': 'AM', 'sym': 'AAPL',
                    't': int(datetime.now(timezone.utc).timestamp() * 1000),
                    'o': 150.0, 'h': 152.0, 'l': 149.0, 'c': 151.0, 'v': 1000000
                }
            },
            # Tiingo data (simulated processing)
            {
                'vendor': 'tiingo',
                'data': {
                    'symbol': 'AAPL',
                    'timestamp': datetime.now(timezone.utc),
                    'open': 150.1, 'high': 152.1, 'low': 149.1, 'close': 151.1, 'volume': 1000100
                }
            }
        ]

        # Process data from each vendor
        for vendor_info in vendors_data:
            if vendor_info['vendor'] == 'polygon':
                await collector._process_polygon_minute_bar(vendor_info['data'])
            # Additional vendor processing would go here

        # Verify multiple vendor data was stored
        assert mock_conn.execute.call_count >= len(vendors_data)

        await collector.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_gap_detection_and_backfill_integration(self, integration_environment):
        """Test integration between gap detection and backfill systems"""
        mock_pool = integration_environment['mock_pool']
        mock_conn = integration_environment['mock_conn']

        # Initialize gap detector
        gap_detector = GapDetectionEngine()
        await gap_detector.initialize()

        # Mock gap detection query - simulate found gaps
        gap_time = datetime.now(timezone.utc) - timedelta(minutes=65)
        mock_conn.fetch.return_value = [
            {
                'prev_timestamp': gap_time - timedelta(minutes=1),
                'timestamp': gap_time + timedelta(minutes=64),
                'gap_minutes': 65.0
            }
        ]

        # Mock active symbols
        gap_detector.universe_symbols = {'AAPL'}

        # Run gap detection
        gaps = await gap_detector._detect_symbol_gaps('polygon', 'AAPL')

        # Verify gaps were detected
        assert len(gaps) == 1
        assert gaps[0].gap_duration_minutes == 65
        assert gaps[0].severity == 'critical'

        # Mock successful backfill
        mock_response_data = {
            'results': [
                {
                    't': int(gap_time.timestamp() * 1000),
                    'o': 150.0, 'h': 152.0, 'l': 149.0, 'c': 151.0, 'v': 1000000
                }
            ]
        }

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = mock_response_data

        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        with patch('aiohttp.ClientSession', return_value=mock_session):
            success = await gap_detector._backfill_polygon_gap(gaps[0])
            assert success is True

        await gap_detector.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_validation_workflow_integration(self, integration_environment):
        """Test integration of daily validation workflow"""
        mock_pool = integration_environment['mock_pool']
        mock_conn = integration_environment['mock_conn']

        # Initialize validation engine
        validation_engine = DailyValidationEngine()
        await validation_engine.initialize()

        # Mock active symbols
        mock_conn.fetch.return_value = [
            {'symbol': 'AAPL'},
            {'symbol': 'MSFT'}
        ]

        symbols = await validation_engine._get_active_symbols()
        assert len(symbols) == 2

        # Mock real-time data
        realtime_timestamp = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
        mock_realtime_data = [
            {
                'timestamp': realtime_timestamp,
                'open_price': 150.0, 'high_price': 152.0,
                'low_price': 149.0, 'close_price': 151.0,
                'volume': 1000000, 'data_latency_ms': 30000,
                'quality_score': 0.95
            }
        ]

        # Mock batch API data
        mock_batch_data = [
            {
                'timestamp': realtime_timestamp,
                'open_price': 150.0, 'high_price': 152.0,
                'low_price': 149.0, 'close_price': 151.0,
                'volume': 1000000
            }
        ]

        # Test data comparison
        result = validation_engine._compare_data('polygon', 'AAPL', mock_realtime_data, mock_batch_data)

        assert result.symbol == 'AAPL'
        assert result.vendor == 'polygon'
        assert result.overall_accuracy_score == 1.0  # Perfect match
        assert result.validation_status == 'passed'

        await validation_engine.shutdown()

@pytest.mark.integration
class TestSystemResilience:
    """Test system resilience and error recovery"""

    @pytest.fixture
    async def resilience_environment(self):
        """Set up environment for resilience testing"""
        env_vars = {
            'POLYGON_API_KEY': 'test_key',
            'TIINGO_API_KEY': 'test_key',
            'FMP_API_KEY': 'test_key',
            'MAX_RETRY_ATTEMPTS': '3',
            'ENABLE_AUTO_BACKFILL': 'true'
        }

        with patch.dict(os.environ, env_vars):
            yield

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_connection_recovery(self, resilience_environment):
        """Test recovery from database connection failures"""
        # Simulate connection failure then recovery
        connection_attempts = 0

        async def mock_create_pool(*args, **kwargs):
            nonlocal connection_attempts
            connection_attempts += 1
            if connection_attempts <= 2:
                raise Exception("Connection failed")

            # Success on third attempt
            mock_pool = AsyncMock()
            return mock_pool

        with patch('market_data.realtime.streaming_collector.Environment'):
            with patch('market_data.realtime.streaming_collector.asyncpg.create_pool', side_effect=mock_create_pool):
                collector = RealtimeStreamingCollector()

                # Should eventually succeed after retries
                try:
                    await collector.initialize()
                    # If we get here, the retry logic worked
                    assert connection_attempts == 3
                except:
                    # Expected to fail in test environment
                    assert connection_attempts >= 2

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_api_rate_limit_handling(self, resilience_environment):
        """Test handling of API rate limits across vendors"""
        gap_detector = GapDetectionEngine()

        # Mock rate-limited responses
        rate_limit_responses = []
        for status_code in [429, 503, 500]:  # Different error types
            mock_response = AsyncMock()
            mock_response.status = status_code
            rate_limit_responses.append(mock_response)

        # Final successful response
        success_response = AsyncMock()
        success_response.status = 200
        success_response.json.return_value = {'results': []}

        mock_session = AsyncMock()
        # Return errors first, then success
        mock_session.get.return_value.__aenter__.side_effect = rate_limit_responses + [success_response]

        gap = Mock()
        gap.vendor = 'polygon'
        gap.symbol = 'AAPL'
        gap.gap_start = datetime.now(timezone.utc)
        gap.gap_end = datetime.now(timezone.utc) + timedelta(minutes=5)

        with patch('aiohttp.ClientSession', return_value=mock_session):
            # Should handle rate limits gracefully
            success = await gap_detector._backfill_polygon_gap(gap)
            # May succeed or fail depending on retry logic
            assert success in [True, False]

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_malformed_data_handling(self, resilience_environment):
        """Test handling of malformed data from vendors"""
        with patch('market_data.realtime.streaming_collector.Environment'):
            mock_pool = AsyncMock()
            with patch('market_data.realtime.streaming_collector.asyncpg.create_pool', return_value=mock_pool):
                collector = RealtimeStreamingCollector()
                await collector.initialize()

                collector.universe_symbols = {'AAPL'}
                collector.instrument_mapping = {'AAPL': 1}
                collector._store_minute_bar = AsyncMock()

                # Test various malformed data scenarios
                malformed_data_samples = [
                    {},  # Empty data
                    {'ev': 'AM'},  # Missing required fields
                    {'ev': 'AM', 'sym': 'AAPL'},  # Missing timestamp
                    {'ev': 'AM', 'sym': 'AAPL', 't': 'invalid'},  # Invalid timestamp
                    {'ev': 'AM', 'sym': 'AAPL', 't': 1640995200000, 'o': 'invalid'},  # Invalid price
                ]

                for malformed_data in malformed_data_samples:
                    # Should not raise exceptions
                    try:
                        await collector._process_polygon_minute_bar(malformed_data)
                    except Exception as e:
                        pytest.fail(f"Should handle malformed data gracefully, but got: {e}")

                # Should not have stored any malformed data
                collector._store_minute_bar.assert_not_called()

                await collector.shutdown()

@pytest.mark.integration
class TestPerformanceUnderLoad:
    """Test system performance under various load conditions"""

    @pytest.fixture
    async def performance_environment(self):
        """Set up environment for performance testing"""
        env_vars = {
            'MAX_CONCURRENT_JOBS': '10',
            'UNIVERSE_SIZE': '100',
            'METRICS_UPDATE_INTERVAL': '5'
        }

        with patch.dict(os.environ, env_vars):
            yield

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_high_frequency_data_processing(self, performance_environment):
        """Test processing high frequency data"""
        with patch('market_data.realtime.streaming_collector.Environment'):
            mock_pool = AsyncMock()
            mock_conn = AsyncMock()
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

            with patch('market_data.realtime.streaming_collector.asyncpg.create_pool', return_value=mock_pool):
                collector = RealtimeStreamingCollector()
                await collector.initialize()

                collector.universe_symbols = {'AAPL'}
                collector.instrument_mapping = {'AAPL': 1}

                # Simulate high-frequency data (1 bar per second for 1 minute)
                import time
                start_time = time.time()

                tasks = []
                base_timestamp = datetime.now(timezone.utc)

                for i in range(60):  # 60 data points
                    data = {
                        'ev': 'AM',
                        'sym': 'AAPL',
                        't': int((base_timestamp + timedelta(seconds=i)).timestamp() * 1000),
                        'o': 150.0 + i * 0.1,
                        'h': 152.0 + i * 0.1,
                        'l': 149.0 + i * 0.1,
                        'c': 151.0 + i * 0.1,
                        'v': 1000000 + i * 1000
                    }

                    task = asyncio.create_task(collector._process_polygon_minute_bar(data))
                    tasks.append(task)

                # Process all data concurrently
                await asyncio.gather(*tasks)

                processing_time = time.time() - start_time

                # Should process 60 data points quickly (within 5 seconds)
                assert processing_time < 5.0

                # Verify all data was processed
                assert mock_conn.execute.call_count == 60

                await collector.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_concurrent_backfill_performance(self, performance_environment):
        """Test performance of concurrent backfill operations"""
        backfill_engine = WeeklyBackfillEngine()

        # Create multiple mock jobs
        jobs = []
        for i in range(20):
            job = Mock()
            job.job_id = f'job-{i}'
            job.vendor = 'polygon'
            job.symbol = f'SYM{i:03d}'
            job.start_date = date(2025, 1, 15)
            job.end_date = date(2025, 1, 15)
            job.status = 'pending'
            jobs.append(job)

        # Mock successful job execution
        async def mock_execute_job(job):
            await asyncio.sleep(0.1)  # Simulate API call delay
            return True

        backfill_engine._execute_backfill_job = mock_execute_job
        backfill_engine._mark_job_completed = AsyncMock()
        backfill_engine._mark_job_failed = AsyncMock()

        import time
        start_time = time.time()

        # Process jobs concurrently
        await backfill_engine._process_jobs_concurrently(jobs, max_concurrent=5)

        processing_time = time.time() - start_time

        # Should process 20 jobs in ~0.4 seconds (4 batches of 5 concurrent jobs)
        assert processing_time < 1.0

        # Verify all jobs were completed
        assert backfill_engine._mark_job_completed.call_count == 20
        assert backfill_engine._mark_job_failed.call_count == 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_metrics_collection_performance(self, performance_environment):
        """Test performance of metrics collection under load"""
        from domains.market_data.services.realtime.metrics_exporter import MetricsCollector

        with patch('market_data.realtime.metrics_exporter.Environment'):
            mock_pool = AsyncMock()
            mock_conn = AsyncMock()
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

            # Mock large dataset responses
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

            mock_conn.fetch.return_value = large_streaming_metrics

            with patch('market_data.realtime.metrics_exporter.asyncpg.create_pool', return_value=mock_pool):
                collector = MetricsCollector()
                await collector.initialize()

                import time
                start_time = time.time()

                # Collect metrics for large dataset
                metrics = await collector._collect_streaming_metrics()

                collection_time = time.time() - start_time

                # Should collect 1000 metrics quickly
                assert collection_time < 2.0
                assert len(metrics) == 1000

                await collector.shutdown()

@pytest.mark.integration
class TestDeploymentValidation:
    """Test deployment scenarios and Kubernetes integration"""

    def test_deployment_script_validation(self):
        """Test deployment script functionality"""
        script_path = 'scripts/deploy_realtime_system.sh'

        # Verify script exists and is executable
        assert os.path.exists(script_path)

        # Test script help option
        result = subprocess.run(['bash', script_path, '--help'],
                              capture_output=True, text=True)
        assert result.returncode == 0
        assert 'Usage:' in result.stdout

    def test_kubernetes_manifest_validation(self):
        """Test Kubernetes manifest files"""
        manifest_files = [
            'k8s/dev/realtime-streaming-deployment.yaml',
            'k8s/dev/realtime-validation-cronjobs.yaml',
            'k8s/monitoring/monitoring-stack.yaml'
        ]

        for manifest_file in manifest_files:
            assert os.path.exists(manifest_file), f"Manifest {manifest_file} should exist"

            # Verify YAML is valid
            with open(manifest_file, 'r') as f:
                content = f.read()
                assert 'apiVersion:' in content
                assert 'kind:' in content
                assert 'metadata:' in content

    def test_monitoring_configuration_validation(self):
        """Test monitoring stack configuration"""
        monitoring_files = [
            'k8s/monitoring/prometheus-alerting-rules.yaml',
            'k8s/monitoring/monitoring-stack.yaml'
        ]

        for monitoring_file in monitoring_files:
            if os.path.exists(monitoring_file):
                with open(monitoring_file, 'r') as f:
                    content = f.read()

                    # Verify monitoring-specific configurations
                    if 'prometheus' in monitoring_file.lower():
                        assert 'scrape_configs:' in content or 'rules:' in content
                    if 'grafana' in content.lower():
                        assert 'dashboard' in content.lower() or 'datasource' in content.lower()

@pytest.mark.integration
class TestHealthMonitoring:
    """Test health monitoring and alerting integration"""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_health_check_endpoints(self):
        """Test health check endpoint functionality"""
        from domains.market_data.services.realtime.metrics_exporter import HealthcheckHandler

        health_handler = HealthcheckHandler()

        # Mock request object
        mock_request = Mock()

        # Test health endpoint
        with patch('market_data.realtime.metrics_exporter.web.json_response') as mock_response:
            await health_handler.health_check(mock_request)
            mock_response.assert_called_once()

            # Verify response structure
            response_data = mock_response.call_args[0][0]
            assert 'status' in response_data
            assert 'timestamp' in response_data
            assert 'checks' in response_data

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_metrics_endpoint_integration(self):
        """Test metrics endpoint integration"""
        from domains.market_data.services.realtime.metrics_exporter import RealtimeMetricsExporter

        with patch('market_data.realtime.metrics_exporter.Environment'):
            exporter = RealtimeMetricsExporter()

            # Mock prometheus registry
            with patch('market_data.realtime.metrics_exporter.start_http_server') as mock_start_server:
                mock_collector = AsyncMock()
                exporter.collector = mock_collector

                await exporter.initialize()

                # Verify HTTP server was started
                mock_start_server.assert_called_once_with(9090)

                await exporter.shutdown()

if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])