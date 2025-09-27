#!/usr/bin/env python3
"""
Comprehensive tests for the Weekly Backfill Engine

Tests cover:
- Weekly backfill workflow
- Multi-vendor data reconciliation
- Large-scale data processing
- Progress tracking and resumption
- Error handling and retry logic
- Performance optimization
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, date, timezone
import os

# Import the module under test
import sys
sys.path.append('src')

from domains.market_data.services.data_collection.realtime.weekly_backfill import (
    WeeklyBackfillEngine,
    BackfillJob,
    BackfillStatus
)

class TestBackfillJob:
    """Test the BackfillJob data structure"""

    def test_backfill_job_creation(self):
        """Test creating BackfillJob with all fields"""
        start_date = date(2025, 1, 1)
        end_date = date(2025, 1, 7)

        job = BackfillJob(
            job_id='backfill-2025-01-01-01-07',
            vendor='polygon',
            symbol='AAPL',
            start_date=start_date,
            end_date=end_date,
            priority=1,
            status='pending',
            progress_percentage=0.0,
            bars_processed=0,
            bars_total=0,
            created_at=datetime.now(timezone.utc),
            started_at=None,
            completed_at=None,
            error_message=None,
            retry_count=0
        )

        assert job.job_id == 'backfill-2025-01-01-01-07'
        assert job.vendor == 'polygon'
        assert job.symbol == 'AAPL'
        assert job.start_date == start_date
        assert job.end_date == end_date
        assert job.priority == 1
        assert job.status == 'pending'
        assert job.progress_percentage == 0.0
        assert job.retry_count == 0

    def test_backfill_status_enum(self):
        """Test BackfillStatus enumeration"""
        assert BackfillStatus.PENDING == 'pending'
        assert BackfillStatus.RUNNING == 'running'
        assert BackfillStatus.COMPLETED == 'completed'
        assert BackfillStatus.FAILED == 'failed'
        assert BackfillStatus.CANCELLED == 'cancelled'

class TestWeeklyBackfillEngine:
    """Test the main WeeklyBackfillEngine class"""

    @pytest.fixture
    def mock_env(self):
        """Mock environment configuration"""
        with patch('market_data.realtime.weekly_backfill.Environment') as mock_env_class:
            mock_env = Mock()
            mock_env.get_database_url.return_value = "postgresql://test:test@localhost:5432/test"
            mock_env_class.return_value = mock_env
            yield mock_env

    @pytest.fixture
    def backfill_engine(self, mock_env):
        """Create a backfill engine instance with mocked dependencies"""
        with patch.dict(os.environ, {
            'BACKFILL_START_DATE': '2025-01-01',
            'BACKFILL_END_DATE': '2025-01-07',
            'MAX_CONCURRENT_JOBS': '5',
            'CHUNK_SIZE_DAYS': '1',
            'MAX_RETRY_ATTEMPTS': '3',
            'ENABLE_PROGRESS_TRACKING': 'true',
            'POLYGON_API_KEY': 'test_polygon_key',
            'TIINGO_API_KEY': 'test_tiingo_key',
            'FMP_API_KEY': 'test_fmp_key'
        }):
            engine = WeeklyBackfillEngine()
            return engine

    def test_engine_initialization(self, backfill_engine):
        """Test backfill engine initialization"""
        assert backfill_engine.start_date == date(2025, 1, 1)
        assert backfill_engine.end_date == date(2025, 1, 7)
        assert backfill_engine.max_concurrent_jobs == 5
        assert backfill_engine.chunk_size_days == 1
        assert backfill_engine.max_retry_attempts == 3
        assert backfill_engine.enable_progress_tracking is True
        assert backfill_engine.polygon_api_key == 'test_polygon_key'
        assert backfill_engine.tiingo_api_key == 'test_tiingo_key'
        assert backfill_engine.fmp_api_key == 'test_fmp_key'

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_initialize_database_connection(self, backfill_engine, mock_env):
        """Test database initialization"""
        mock_pool = AsyncMock()

        with patch('market_data.realtime.weekly_backfill.asyncpg.create_pool', return_value=mock_pool):
            await backfill_engine.initialize()
            assert backfill_engine.pool == mock_pool
            mock_env.get_database_url.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_get_backfill_universe(self, backfill_engine):
        """Test getting universe for backfill"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        backfill_engine.pool = mock_pool

        # Mock database response
        mock_conn.fetch.return_value = [
            {'symbol': 'AAPL', 'instrument_id': 1},
            {'symbol': 'MSFT', 'instrument_id': 2},
            {'symbol': 'GOOGL', 'instrument_id': 3}
        ]

        universe = await backfill_engine._get_backfill_universe()

        assert len(universe) == 3
        assert universe[0]['symbol'] == 'AAPL'
        assert universe[1]['symbol'] == 'MSFT'
        assert universe[2]['symbol'] == 'GOOGL'
        mock_conn.fetch.assert_called_once()

    def test_generate_backfill_jobs(self, backfill_engine):
        """Test generating backfill jobs from universe"""
        universe = [
            {'symbol': 'AAPL', 'instrument_id': 1},
            {'symbol': 'MSFT', 'instrument_id': 2}
        ]

        vendors = ['polygon', 'tiingo']
        jobs = backfill_engine._generate_backfill_jobs(universe, vendors)

        # Should create jobs for each symbol-vendor-date combination
        # 2 symbols × 2 vendors × 7 days = 28 jobs
        assert len(jobs) == 28

        # Check first job structure
        first_job = jobs[0]
        assert first_job.symbol in ['AAPL', 'MSFT']
        assert first_job.vendor in ['polygon', 'tiingo']
        assert first_job.status == BackfillStatus.PENDING
        assert first_job.retry_count == 0

    def test_prioritize_backfill_jobs(self, backfill_engine):
        """Test job prioritization logic"""
        jobs = [
            BackfillJob(
                job_id='job1', vendor='polygon', symbol='AAPL',
                start_date=date(2025, 1, 5), end_date=date(2025, 1, 5),
                priority=1, status='pending'
            ),
            BackfillJob(
                job_id='job2', vendor='tiingo', symbol='MSFT',
                start_date=date(2025, 1, 1), end_date=date(2025, 1, 1),
                priority=3, status='pending'
            ),
            BackfillJob(
                job_id='job3', vendor='polygon', symbol='GOOGL',
                start_date=date(2025, 1, 3), end_date=date(2025, 1, 3),
                priority=2, status='pending'
            )
        ]

        prioritized = backfill_engine._prioritize_jobs(jobs)

        # Should be sorted by priority (higher first), then date (newer first)
        assert prioritized[0].priority == 3
        assert prioritized[1].priority == 2
        assert prioritized[2].priority == 1

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_execute_backfill_job_polygon(self, backfill_engine):
        """Test executing backfill job for Polygon"""
        job = BackfillJob(
            job_id='test-job', vendor='polygon', symbol='AAPL',
            start_date=date(2025, 1, 15), end_date=date(2025, 1, 15),
            priority=1, status='pending'
        )

        # Mock successful API response
        mock_response_data = {
            'results': [
                {
                    't': int(datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc).timestamp() * 1000),
                    'o': 150.0, 'h': 152.0, 'l': 149.0, 'c': 151.0, 'v': 1000000,
                    'vw': 150.5, 'n': 500
                },
                {
                    't': int(datetime(2025, 1, 15, 14, 31, 0, tzinfo=timezone.utc).timestamp() * 1000),
                    'o': 151.0, 'h': 153.0, 'l': 150.0, 'c': 152.0, 'v': 1100000,
                    'vw': 151.5, 'n': 550
                }
            ]
        }

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = mock_response_data

        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        # Mock database operations
        backfill_engine._store_backfill_data = AsyncMock()
        backfill_engine._update_job_progress = AsyncMock()

        with patch('aiohttp.ClientSession', return_value=mock_session):
            result = await backfill_engine._execute_backfill_job(job)

            assert result is True
            backfill_engine._store_backfill_data.assert_called_once()
            backfill_engine._update_job_progress.assert_called()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_execute_backfill_job_tiingo(self, backfill_engine):
        """Test executing backfill job for Tiingo"""
        job = BackfillJob(
            job_id='test-job', vendor='tiingo', symbol='AAPL',
            start_date=date(2025, 1, 15), end_date=date(2025, 1, 15),
            priority=1, status='pending'
        )

        # Mock successful API response
        mock_response_data = [
            {
                'date': '2025-01-15T14:30:00Z',
                'open': 150.0, 'high': 152.0, 'low': 149.0, 'close': 151.0, 'volume': 1000000
            },
            {
                'date': '2025-01-15T14:31:00Z',
                'open': 151.0, 'high': 153.0, 'low': 150.0, 'close': 152.0, 'volume': 1100000
            }
        ]

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = mock_response_data

        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        # Mock database operations
        backfill_engine._store_backfill_data = AsyncMock()
        backfill_engine._update_job_progress = AsyncMock()

        with patch('aiohttp.ClientSession', return_value=mock_session):
            result = await backfill_engine._execute_backfill_job(job)

            assert result is True
            backfill_engine._store_backfill_data.assert_called_once()
            backfill_engine._update_job_progress.assert_called()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_execute_backfill_job_fmp(self, backfill_engine):
        """Test executing backfill job for FMP"""
        job = BackfillJob(
            job_id='test-job', vendor='fmp', symbol='AAPL',
            start_date=date(2025, 1, 15), end_date=date(2025, 1, 15),
            priority=1, status='pending'
        )

        # Mock successful API response
        mock_response_data = [
            {
                'date': '2025-01-15T14:30:00Z',
                'open': 150.0, 'high': 152.0, 'low': 149.0, 'close': 151.0, 'volume': 1000000
            },
            {
                'date': '2025-01-15T14:31:00Z',
                'open': 151.0, 'high': 153.0, 'low': 150.0, 'close': 152.0, 'volume': 1100000
            }
        ]

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = mock_response_data

        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        # Mock database operations
        backfill_engine._store_backfill_data = AsyncMock()
        backfill_engine._update_job_progress = AsyncMock()

        with patch('aiohttp.ClientSession', return_value=mock_session):
            result = await backfill_engine._execute_backfill_job(job)

            assert result is True
            backfill_engine._store_backfill_data.assert_called_once()
            backfill_engine._update_job_progress.assert_called()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_store_backfill_data(self, backfill_engine):
        """Test storing backfill data in database"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        backfill_engine.pool = mock_pool

        # Mock _get_instrument_id
        backfill_engine._get_instrument_id = AsyncMock(return_value=123)

        job = BackfillJob(
            job_id='test-job', vendor='polygon', symbol='AAPL',
            start_date=date(2025, 1, 15), end_date=date(2025, 1, 15),
            priority=1, status='running'
        )

        data = [
            {
                't': int(datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc).timestamp() * 1000),
                'o': 150.0, 'h': 152.0, 'l': 149.0, 'c': 151.0, 'v': 1000000,
                'vw': 150.5, 'n': 500
            }
        ]

        await backfill_engine._store_backfill_data(job, data)

        # Should call execute for each data point
        mock_conn.execute.assert_called()
        backfill_engine._get_instrument_id.assert_called_once_with('AAPL')

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_update_job_progress(self, backfill_engine):
        """Test updating job progress in database"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        backfill_engine.pool = mock_pool

        job = BackfillJob(
            job_id='test-job', vendor='polygon', symbol='AAPL',
            start_date=date(2025, 1, 15), end_date=date(2025, 1, 15),
            priority=1, status='running', progress_percentage=50.0,
            bars_processed=195, bars_total=390
        )

        await backfill_engine._update_job_progress(job, 'running')

        mock_conn.execute.assert_called_once()

        # Check that the SQL contains expected updates
        sql_call = mock_conn.execute.call_args[0][0]
        assert 'progress_percentage' in sql_call
        assert 'bars_processed' in sql_call
        assert 'status' in sql_call

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_mark_job_completed(self, backfill_engine):
        """Test marking job as completed"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        backfill_engine.pool = mock_pool

        job = BackfillJob(
            job_id='test-job', vendor='polygon', symbol='AAPL',
            start_date=date(2025, 1, 15), end_date=date(2025, 1, 15),
            priority=1, status='running'
        )

        await backfill_engine._mark_job_completed(job)

        mock_conn.execute.assert_called_once()

        # Check that the SQL contains expected status update
        sql_call = mock_conn.execute.call_args[0][0]
        assert 'status' in sql_call
        assert 'completed' in sql_call
        assert 'completed_at' in sql_call

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_mark_job_failed(self, backfill_engine):
        """Test marking job as failed"""
        mock_conn = AsyncMock()
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        backfill_engine.pool = mock_pool

        job = BackfillJob(
            job_id='test-job', vendor='polygon', symbol='AAPL',
            start_date=date(2025, 1, 15), end_date=date(2025, 1, 15),
            priority=1, status='running'
        )

        error_message = "API rate limit exceeded"
        await backfill_engine._mark_job_failed(job, error_message)

        mock_conn.execute.assert_called_once()

        # Check that the SQL contains expected status update
        sql_call = mock_conn.execute.call_args[0][0]
        assert 'status' in sql_call
        assert 'failed' in sql_call
        assert 'error_message' in sql_call

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_process_jobs_concurrently(self, backfill_engine):
        """Test concurrent job processing"""
        jobs = [
            BackfillJob(
                job_id=f'job-{i}', vendor='polygon', symbol='AAPL',
                start_date=date(2025, 1, 15), end_date=date(2025, 1, 15),
                priority=1, status='pending'
            ) for i in range(3)
        ]

        # Mock job execution
        backfill_engine._execute_backfill_job = AsyncMock(return_value=True)
        backfill_engine._mark_job_completed = AsyncMock()
        backfill_engine._mark_job_failed = AsyncMock()

        await backfill_engine._process_jobs_concurrently(jobs, max_concurrent=2)

        # Should execute all jobs
        assert backfill_engine._execute_backfill_job.call_count == 3
        assert backfill_engine._mark_job_completed.call_count == 3
        assert backfill_engine._mark_job_failed.call_count == 0

        # Check statistics
        assert backfill_engine.stats['jobs_completed'] == 3
        assert backfill_engine.stats['jobs_failed'] == 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_handle_job_failures_with_retry(self, backfill_engine):
        """Test job failure handling with retry logic"""
        job = BackfillJob(
            job_id='test-job', vendor='polygon', symbol='AAPL',
            start_date=date(2025, 1, 15), end_date=date(2025, 1, 15),
            priority=1, status='pending', retry_count=0
        )

        # Mock job execution to fail first time, succeed second time
        backfill_engine._execute_backfill_job = AsyncMock(side_effect=[False, True])
        backfill_engine._mark_job_completed = AsyncMock()
        backfill_engine._mark_job_failed = AsyncMock()
        backfill_engine._increment_retry_count = AsyncMock()

        # Set retry limit
        backfill_engine.max_retry_attempts = 2

        await backfill_engine._process_jobs_concurrently([job], max_concurrent=1)

        # Should retry and eventually succeed
        assert backfill_engine._execute_backfill_job.call_count == 2
        assert backfill_engine._mark_job_completed.call_count == 1
        assert backfill_engine._mark_job_failed.call_count == 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_generate_backfill_summary(self, backfill_engine):
        """Test generating backfill summary"""
        # Set up test statistics
        backfill_engine.stats = {
            'jobs_total': 100,
            'jobs_completed': 90,
            'jobs_failed': 5,
            'jobs_cancelled': 5,
            'bars_processed': 450000,
            'total_api_calls': 1000,
            'total_processing_time': 3600.0,
            'vendor_performance': {
                'polygon': {'success_rate': 0.95, 'avg_latency': 200},
                'tiingo': {'success_rate': 0.92, 'avg_latency': 350},
                'fmp': {'success_rate': 0.88, 'avg_latency': 500}
            }
        }

        summary = await backfill_engine._generate_backfill_summary()

        assert summary['success_rate'] == 0.90  # 90/100
        assert summary['bars_per_second'] == 125.0  # 450000/3600
        assert summary['avg_api_latency'] == 350.0  # Average of vendor latencies
        assert len(summary['vendor_performance']) == 3
        assert summary['recommendations']  # Should contain recommendations

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_run_weekly_backfill_complete_flow(self, backfill_engine):
        """Test the complete weekly backfill flow"""
        # Mock all dependencies
        mock_pool = AsyncMock()
        backfill_engine.pool = mock_pool

        backfill_engine._get_backfill_universe = AsyncMock(return_value=[
            {'symbol': 'AAPL', 'instrument_id': 1},
            {'symbol': 'MSFT', 'instrument_id': 2}
        ])
        backfill_engine._process_jobs_concurrently = AsyncMock()
        backfill_engine._generate_backfill_summary = AsyncMock(return_value={
            'success_rate': 0.95,
            'bars_processed': 100000,
            'total_processing_time': 1800.0
        })
        backfill_engine._send_completion_notification = AsyncMock()

        await backfill_engine.run_weekly_backfill()

        # Verify all steps were called
        backfill_engine._get_backfill_universe.assert_called_once()
        backfill_engine._process_jobs_concurrently.assert_called_once()
        backfill_engine._generate_backfill_summary.assert_called_once()
        backfill_engine._send_completion_notification.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_shutdown(self, backfill_engine):
        """Test graceful shutdown"""
        mock_pool = AsyncMock()
        backfill_engine.pool = mock_pool
        backfill_engine.running = True

        await backfill_engine.shutdown()

        assert backfill_engine.running is False
        mock_pool.close.assert_called_once()

class TestAPIErrorHandling:
    """Test API error handling scenarios"""

    @pytest.fixture
    def backfill_engine(self):
        with patch('market_data.realtime.weekly_backfill.Environment'):
            with patch.dict(os.environ, {
                'POLYGON_API_KEY': 'test_key',
                'TIINGO_API_KEY': 'test_key',
                'FMP_API_KEY': 'test_key'
            }):
                engine = WeeklyBackfillEngine()
                return engine

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_polygon_api_rate_limit_handling(self, backfill_engine):
        """Test handling Polygon API rate limits"""
        job = BackfillJob(
            job_id='test-job', vendor='polygon', symbol='AAPL',
            start_date=date(2025, 1, 15), end_date=date(2025, 1, 15),
            priority=1, status='pending'
        )

        mock_response = AsyncMock()
        mock_response.status = 429  # Rate limit

        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        with patch('aiohttp.ClientSession', return_value=mock_session):
            with patch('asyncio.sleep', new_callable=AsyncMock):  # Mock sleep for rate limiting
                result = await backfill_engine._execute_backfill_job(job)
                assert result is False

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_tiingo_api_server_error_handling(self, backfill_engine):
        """Test handling Tiingo API server errors"""
        job = BackfillJob(
            job_id='test-job', vendor='tiingo', symbol='AAPL',
            start_date=date(2025, 1, 15), end_date=date(2025, 1, 15),
            priority=1, status='pending'
        )

        mock_response = AsyncMock()
        mock_response.status = 500  # Server error

        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        with patch('aiohttp.ClientSession', return_value=mock_session):
            result = await backfill_engine._execute_backfill_job(job)
            assert result is False

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_network_timeout_handling(self, backfill_engine):
        """Test handling network timeouts during backfill"""
        job = BackfillJob(
            job_id='test-job', vendor='polygon', symbol='AAPL',
            start_date=date(2025, 1, 15), end_date=date(2025, 1, 15),
            priority=1, status='pending'
        )

        mock_session = AsyncMock()
        mock_session.get.side_effect = asyncio.TimeoutError("Request timeout")

        with patch('aiohttp.ClientSession', return_value=mock_session):
            result = await backfill_engine._execute_backfill_job(job)
            assert result is False

class TestPerformanceAndScaling:
    """Test performance optimization and scaling scenarios"""

    @pytest.fixture
    def backfill_engine(self):
        with patch('market_data.realtime.weekly_backfill.Environment'):
            return WeeklyBackfillEngine()

    def test_large_universe_job_generation(self, backfill_engine):
        """Test generating jobs for large universe"""
        # Simulate large universe (1000 symbols)
        universe = [
            {'symbol': f'SYM{i:04d}', 'instrument_id': i}
            for i in range(1, 1001)
        ]

        vendors = ['polygon', 'tiingo', 'fmp']
        jobs = backfill_engine._generate_backfill_jobs(universe, vendors)

        # Should create jobs efficiently for large universe
        # 1000 symbols × 3 vendors × 7 days = 21,000 jobs
        assert len(jobs) == 21000

        # Verify job structure is maintained
        assert all(job.status == BackfillStatus.PENDING for job in jobs)
        assert all(job.retry_count == 0 for job in jobs)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_concurrent_job_processing_performance(self, backfill_engine):
        """Test performance of concurrent job processing"""
        # Create many jobs
        jobs = [
            BackfillJob(
                job_id=f'job-{i}', vendor='polygon', symbol=f'SYM{i}',
                start_date=date(2025, 1, 15), end_date=date(2025, 1, 15),
                priority=1, status='pending'
            ) for i in range(50)
        ]

        # Mock fast job execution
        backfill_engine._execute_backfill_job = AsyncMock(return_value=True)
        backfill_engine._mark_job_completed = AsyncMock()
        backfill_engine._mark_job_failed = AsyncMock()

        import time
        start_time = time.time()

        await backfill_engine._process_jobs_concurrently(jobs, max_concurrent=10)

        processing_time = time.time() - start_time

        # Should process jobs efficiently
        assert processing_time < 5.0  # Should complete within 5 seconds
        assert backfill_engine._execute_backfill_job.call_count == 50
        assert backfill_engine._mark_job_completed.call_count == 50

class TestEdgeCases:
    """Test edge cases and boundary conditions"""

    @pytest.fixture
    def backfill_engine(self):
        with patch('market_data.realtime.weekly_backfill.Environment'):
            return WeeklyBackfillEngine()

    def test_empty_universe_handling(self, backfill_engine):
        """Test handling empty universe"""
        universe = []
        vendors = ['polygon', 'tiingo']

        jobs = backfill_engine._generate_backfill_jobs(universe, vendors)
        assert jobs == []

    def test_single_day_backfill(self, backfill_engine):
        """Test backfill for single day"""
        backfill_engine.start_date = date(2025, 1, 15)
        backfill_engine.end_date = date(2025, 1, 15)

        universe = [{'symbol': 'AAPL', 'instrument_id': 1}]
        vendors = ['polygon']

        jobs = backfill_engine._generate_backfill_jobs(universe, vendors)

        # Should create exactly one job
        assert len(jobs) == 1
        assert jobs[0].start_date == date(2025, 1, 15)
        assert jobs[0].end_date == date(2025, 1, 15)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_job_with_no_data_available(self, backfill_engine):
        """Test handling job when no data is available from API"""
        job = BackfillJob(
            job_id='test-job', vendor='polygon', symbol='AAPL',
            start_date=date(2025, 1, 15), end_date=date(2025, 1, 15),
            priority=1, status='pending'
        )

        # Mock API response with no data
        mock_response_data = {'results': []}

        mock_response = AsyncMock()
        mock_response.status = 200
        mock_response.json.return_value = mock_response_data

        mock_session = AsyncMock()
        mock_session.get.return_value.__aenter__.return_value = mock_response

        backfill_engine._store_backfill_data = AsyncMock()
        backfill_engine._update_job_progress = AsyncMock()

        with patch('aiohttp.ClientSession', return_value=mock_session):
            result = await backfill_engine._execute_backfill_job(job)

            # Should still succeed even with no data
            assert result is True
            backfill_engine._store_backfill_data.assert_called_once_with(job, [])

if __name__ == '__main__':
    pytest.main([__file__, '-v'])