"""
Comprehensive tests for Enhanced Minute Backfill Orchestrator.

Tests checkpoint functionality, parallel processing, error handling,
and recovery mechanisms for minute-level data backfills.
"""

import pytest
import asyncio
import asyncpg
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
import json
import tempfile
from pathlib import Path

from domains.market_data.services.backfill.enhanced_minute_backfill_orchestrator import (
    EnhancedMinuteBackfillOrchestrator,
    EnhancedBackfillConfig,
    JobSegment,
    JobStatus,
    BackfillProgress,
    run_enhanced_minute_backfill
)
from domains.market_data.services.reconciliation.cross_vendor_reconciler import ReconciliationMethod


class TestJobSegment:
    """Test JobSegment functionality."""

    def test_job_segment_creation(self):
        """Test basic job segment creation."""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 2)

        segment = JobSegment(
            segment_id="",
            symbol="AAPL",
            start_date=start_date,
            end_date=end_date
        )

        # Should auto-generate segment ID
        assert segment.segment_id
        assert len(segment.segment_id) == 12  # MD5 hash truncated
        assert segment.symbol == "AAPL"
        assert segment.start_date == start_date
        assert segment.end_date == end_date
        assert segment.status == JobStatus.PENDING
        assert segment.attempt_count == 0
        assert segment.bars_fetched == {}

    def test_job_segment_deterministic_id(self):
        """Test that segment IDs are deterministic."""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 2)

        segment1 = JobSegment("", "AAPL", start_date, end_date)
        segment2 = JobSegment("", "AAPL", start_date, end_date)

        assert segment1.segment_id == segment2.segment_id

        # Different symbols should have different IDs
        segment3 = JobSegment("", "MSFT", start_date, end_date)
        assert segment1.segment_id != segment3.segment_id


class TestEnhancedBackfillConfig:
    """Test configuration validation and defaults."""

    def test_basic_config_creation(self):
        """Test basic configuration creation."""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 7)

        config = EnhancedBackfillConfig(
            start_date=start_date,
            end_date=end_date,
            symbols=["AAPL", "MSFT"],
            polygon_api_key="test_poly_key",
            tiingo_api_key="test_tiingo_key"
        )

        assert config.start_date == start_date
        assert config.end_date == end_date
        assert config.symbols == ["AAPL", "MSFT"]
        assert config.max_concurrent_symbols == 5
        assert config.max_concurrent_date_ranges == 3
        assert config.chunk_size_days == 7
        assert config.reconciliation_method == ReconciliationMethod.WEIGHTED_AVERAGE

    def test_parallel_processing_config(self):
        """Test parallel processing configuration."""
        config = EnhancedBackfillConfig(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 7),
            max_concurrent_symbols=10,
            max_concurrent_date_ranges=5,
            max_total_workers=25
        )

        assert config.max_concurrent_symbols == 10
        assert config.max_concurrent_date_ranges == 5
        assert config.max_total_workers == 25


class TestBackfillProgress:
    """Test progress tracking functionality."""

    def test_progress_initialization(self):
        """Test progress object initialization."""
        progress = BackfillProgress()

        assert progress.job_id
        assert len(progress.job_id) == 8  # UUID truncated
        assert progress.total_segments == 0
        assert progress.segments_completed == 0
        assert progress.total_bars_fetched == {"polygon": 0, "tiingo": 0}
        assert progress.recent_errors == []
        assert progress.failed_segments == []

    def test_completion_estimate_calculation(self):
        """Test estimated completion time calculation."""
        progress = BackfillProgress()
        progress.total_segments = 100
        progress.segments_completed = 25
        progress.avg_processing_time_per_segment = 10.0  # 10 seconds per segment

        progress.update_completion_estimate()

        assert progress.estimated_completion_time is not None
        # Should be approximately 75 * 10 = 750 seconds from now
        expected_eta = datetime.now() + timedelta(seconds=750)
        assert abs((progress.estimated_completion_time - expected_eta).total_seconds()) < 60


@pytest.fixture
async def mock_db_pool():
    """Create a mock database pool."""
    pool = AsyncMock(spec=asyncpg.Pool)
    return pool


@pytest.fixture
def sample_config():
    """Create a sample configuration for testing."""
    return EnhancedBackfillConfig(
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2024, 1, 3),
        symbols=["AAPL", "MSFT"],
        polygon_api_key="test_poly_key",
        tiingo_api_key="test_tiingo_key",
        chunk_size_days=1,
        max_concurrent_symbols=2,
        max_concurrent_date_ranges=2,
        max_total_workers=4,
        checkpoint_interval_minutes=1,
        auto_checkpoint_segment_count=5
    )


@pytest.fixture
def temp_checkpoint_file():
    """Create a temporary checkpoint file."""
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
        yield f.name
    Path(f.name).unlink(missing_ok=True)


class TestEnhancedMinuteBackfillOrchestrator:
    """Test the main orchestrator functionality."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_orchestrator_initialization(self, mock_db_pool, sample_config):
        """Test orchestrator initialization."""
        orchestrator = EnhancedMinuteBackfillOrchestrator(
            mock_db_pool, sample_config
        )

        assert orchestrator.pool == mock_db_pool
        assert orchestrator.config == sample_config
        assert orchestrator.progress.job_id
        assert orchestrator.job_segments == {}

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_segment_generation(self, mock_db_pool, sample_config):
        """Test job segment generation."""
        orchestrator = EnhancedMinuteBackfillOrchestrator(
            mock_db_pool, sample_config
        )

        orchestrator._generate_job_segments()

        # Should create segments for each symbol and date chunk
        # 2 symbols * 2 days (with 1-day chunks) = 4 segments
        assert len(orchestrator.job_segments) == 4
        assert orchestrator.progress.total_segments == 4

        # Check segment details
        symbols_found = set()
        for segment in orchestrator.job_segments.values():
            assert isinstance(segment, JobSegment)
            assert segment.symbol in ["AAPL", "MSFT"]
            assert segment.status == JobStatus.PENDING
            symbols_found.add(segment.symbol)

        assert symbols_found == {"AAPL", "MSFT"}

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_date_chunk_creation(self, mock_db_pool, sample_config):
        """Test date chunk creation for symbols."""
        orchestrator = EnhancedMinuteBackfillOrchestrator(
            mock_db_pool, sample_config
        )

        chunks = orchestrator._create_date_chunks_for_symbol("AAPL")

        # Should create 2 chunks for 2-day period with 1-day chunk size
        assert len(chunks) == 2

        # Check chunk boundaries
        expected_chunks = [
            (datetime(2024, 1, 1), datetime(2024, 1, 2)),
            (datetime(2024, 1, 2), datetime(2024, 1, 3))
        ]

        for actual, expected in zip(chunks, expected_chunks):
            assert actual == expected

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_checkpoint_save_and_load(self, mock_db_pool, sample_config, temp_checkpoint_file):
        """Test checkpoint save and load functionality."""
        sample_config.checkpoint_file = temp_checkpoint_file

        orchestrator = EnhancedMinuteBackfillOrchestrator(
            mock_db_pool, sample_config
        )

        # Generate some segments and progress
        orchestrator._generate_job_segments()
        orchestrator.progress.segments_completed = 2
        orchestrator.progress.total_bars_reconciled = 1000
        orchestrator.progress.recent_errors = ["test error"]

        # Mark one segment as completed
        first_segment = list(orchestrator.job_segments.values())[0]
        first_segment.status = JobStatus.COMPLETED
        first_segment.bars_reconciled = 500

        # Save checkpoint
        orchestrator.save_checkpoint()

        # Verify checkpoint file exists and has content
        assert Path(temp_checkpoint_file).exists()

        with open(temp_checkpoint_file, 'r') as f:
            checkpoint_data = json.load(f)

        assert checkpoint_data['job_id'] == orchestrator.progress.job_id
        assert checkpoint_data['progress']['segments_completed'] == 2
        assert checkpoint_data['progress']['total_bars_reconciled'] == 1000
        assert 'test error' in checkpoint_data['progress']['recent_errors']
        assert len(checkpoint_data['segments']) == 4

        # Test loading checkpoint in new orchestrator
        new_orchestrator = EnhancedMinuteBackfillOrchestrator(
            mock_db_pool, sample_config
        )

        assert new_orchestrator.progress.job_id == orchestrator.progress.job_id
        assert new_orchestrator.progress.segments_completed == 2
        assert new_orchestrator.progress.total_bars_reconciled == 1000
        assert len(new_orchestrator.job_segments) == 4

        # Check that completed segment status was preserved
        completed_segments = [
            s for s in new_orchestrator.job_segments.values()
            if s.status == JobStatus.COMPLETED
        ]
        assert len(completed_segments) == 1
        assert completed_segments[0].bars_reconciled == 500

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_checkpoint_resume_in_progress_segments(self, mock_db_pool, sample_config, temp_checkpoint_file):
        """Test that in-progress segments are reset to pending on resume."""
        sample_config.checkpoint_file = temp_checkpoint_file

        # Create checkpoint with in-progress segment
        checkpoint_data = {
            'job_id': 'test123',
            'progress': {
                'start_time': datetime.now().isoformat(),
                'total_segments': 2,
                'segments_completed': 0,
                'segments_failed': 0,
                'segments_in_progress': 1,
                'segments_pending': 1,
                'total_bars_fetched': {'polygon': 0, 'tiingo': 0},
                'total_bars_reconciled': 0,
                'total_bars_stored': 0,
                'recent_errors': [],
                'failed_segments': [],
                'avg_processing_time_per_segment': 0.0
            },
            'segments': {
                'seg1': {
                    'segment_id': 'seg1',
                    'symbol': 'AAPL',
                    'start_date': datetime(2024, 1, 1).isoformat(),
                    'end_date': datetime(2024, 1, 2).isoformat(),
                    'status': 'in_progress',
                    'attempt_count': 0,
                    'bars_fetched': {},
                    'bars_reconciled': 0,
                    'bars_stored': 0
                },
                'seg2': {
                    'segment_id': 'seg2',
                    'symbol': 'MSFT',
                    'start_date': datetime(2024, 1, 1).isoformat(),
                    'end_date': datetime(2024, 1, 2).isoformat(),
                    'status': 'pending',
                    'attempt_count': 0,
                    'bars_fetched': {},
                    'bars_reconciled': 0,
                    'bars_stored': 0
                }
            }
        }

        with open(temp_checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f)

        # Load orchestrator
        orchestrator = EnhancedMinuteBackfillOrchestrator(
            mock_db_pool, sample_config
        )

        # Check that in-progress segment was reset to pending
        assert orchestrator.progress.segments_in_progress == 0
        assert orchestrator.progress.segments_pending == 2

        pending_segments = [
            s for s in orchestrator.job_segments.values()
            if s.status == JobStatus.PENDING
        ]
        assert len(pending_segments) == 2

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_single_segment_processing_success(self, mock_db_pool, sample_config):
        """Test successful processing of a single segment."""

        with patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.PolygonMinuteAdapter') as mock_polygon, \
             patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.TiingoIntradayAdapter') as mock_tiingo, \
             patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.CrossVendorReconciler') as mock_reconciler, \
             patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.HybridMinuteDataManager') as mock_storage:

            # Setup mocks
            polygon_adapter = AsyncMock()
            tiingo_adapter = AsyncMock()
            reconciler = AsyncMock()
            storage_manager = AsyncMock()

            mock_polygon.return_value = polygon_adapter
            mock_tiingo.return_value = tiingo_adapter
            mock_reconciler.return_value = reconciler
            mock_storage.return_value = storage_manager

            # Mock data responses
            polygon_data = [MagicMock() for _ in range(100)]  # 100 bars
            tiingo_data = [MagicMock() for _ in range(95)]    # 95 bars
            reconciled_data = [MagicMock() for _ in range(98)] # 98 reconciled bars

            polygon_adapter.fetch_minute_bars_async.return_value = polygon_data
            tiingo_adapter.fetch_minute_bars_async.return_value = tiingo_data
            reconciler.reconcile_minute_data.return_value = reconciled_data
            storage_manager.store_minute_data.return_value = {'stored_cold': 98}

            # Create orchestrator
            orchestrator = EnhancedMinuteBackfillOrchestrator(
                mock_db_pool, sample_config
            )
            orchestrator.polygon_adapter = polygon_adapter
            orchestrator.tiingo_adapter = tiingo_adapter
            orchestrator.reconciler = reconciler
            orchestrator.storage_manager = storage_manager

            # Create test segment
            segment = JobSegment(
                segment_id="test_seg",
                symbol="AAPL",
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2024, 1, 2)
            )

            # Process segment
            result = await orchestrator._process_single_segment(segment)

            # Verify results
            assert result['status'] == 'success'
            assert result['bars_processed'] == 98
            assert segment.status == JobStatus.COMPLETED
            assert segment.bars_fetched == {'polygon': 100, 'tiingo': 95}
            assert segment.bars_reconciled == 98
            assert segment.bars_stored == 98

            # Verify progress was updated
            assert orchestrator.progress.segments_completed == 1
            assert orchestrator.progress.total_bars_fetched == {'polygon': 100, 'tiingo': 95}
            assert orchestrator.progress.total_bars_reconciled == 98
            assert orchestrator.progress.total_bars_stored == 98

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_single_segment_processing_failure(self, mock_db_pool, sample_config):
        """Test handling of segment processing failure."""

        with patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.PolygonMinuteAdapter') as mock_polygon, \
             patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.TiingoIntradayAdapter') as mock_tiingo:

            # Setup mocks
            polygon_adapter = AsyncMock()
            tiingo_adapter = AsyncMock()

            mock_polygon.return_value = polygon_adapter
            mock_tiingo.return_value = tiingo_adapter

            # Mock failure
            polygon_adapter.fetch_minute_bars_async.side_effect = Exception("API Error")
            tiingo_adapter.fetch_minute_bars_async.side_effect = Exception("API Error")

            # Create orchestrator
            orchestrator = EnhancedMinuteBackfillOrchestrator(
                mock_db_pool, sample_config
            )
            orchestrator.polygon_adapter = polygon_adapter
            orchestrator.tiingo_adapter = tiingo_adapter

            # Create test segment
            segment = JobSegment(
                segment_id="test_seg",
                symbol="AAPL",
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2024, 1, 2)
            )

            # Process segment
            result = await orchestrator._process_single_segment(segment)

            # Verify failure handling
            assert result['status'] == 'failed'
            assert 'error' in result
            assert segment.attempt_count == 1
            assert segment.status == JobStatus.PENDING  # Will retry
            assert segment.error_message
            assert len(orchestrator.progress.recent_errors) > 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_single_segment_permanent_failure(self, mock_db_pool, sample_config):
        """Test permanent failure after max retries."""

        # Set max retries to 1 for quick test
        sample_config.max_retries_per_segment = 1

        with patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.PolygonMinuteAdapter') as mock_polygon, \
             patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.TiingoIntradayAdapter') as mock_tiingo:

            # Setup mocks
            polygon_adapter = AsyncMock()
            tiingo_adapter = AsyncMock()

            mock_polygon.return_value = polygon_adapter
            mock_tiingo.return_value = tiingo_adapter

            # Mock failure
            polygon_adapter.fetch_minute_bars_async.side_effect = Exception("Permanent API Error")
            tiingo_adapter.fetch_minute_bars_async.side_effect = Exception("Permanent API Error")

            # Create orchestrator
            orchestrator = EnhancedMinuteBackfillOrchestrator(
                mock_db_pool, sample_config
            )
            orchestrator.polygon_adapter = polygon_adapter
            orchestrator.tiingo_adapter = tiingo_adapter

            # Create test segment with max attempts
            segment = JobSegment(
                segment_id="test_seg",
                symbol="AAPL",
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2024, 1, 2),
                attempt_count=1  # Already at max retries
            )

            # Process segment
            result = await orchestrator._process_single_segment(segment)

            # Verify permanent failure
            assert result['status'] == 'failed'
            assert segment.status == JobStatus.FAILED
            assert orchestrator.progress.segments_failed == 1
            assert segment.segment_id in orchestrator.progress.failed_segments

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_parallel_processing_semaphore_limits(self, mock_db_pool, sample_config):
        """Test that parallel processing respects semaphore limits."""

        # Track concurrent calls
        concurrent_calls = {'polygon': 0, 'tiingo': 0}
        max_concurrent = {'polygon': 0, 'tiingo': 0}

        async def mock_polygon_fetch(*args, **kwargs):
            concurrent_calls['polygon'] += 1
            max_concurrent['polygon'] = max(max_concurrent['polygon'], concurrent_calls['polygon'])
            await asyncio.sleep(0.1)  # Simulate processing time
            concurrent_calls['polygon'] -= 1
            return [MagicMock() for _ in range(10)]

        async def mock_tiingo_fetch(*args, **kwargs):
            concurrent_calls['tiingo'] += 1
            max_concurrent['tiingo'] = max(max_concurrent['tiingo'], concurrent_calls['tiingo'])
            await asyncio.sleep(0.1)  # Simulate processing time
            concurrent_calls['tiingo'] -= 1
            return [MagicMock() for _ in range(8)]

        with patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.PolygonMinuteAdapter') as mock_polygon, \
             patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.TiingoIntradayAdapter') as mock_tiingo, \
             patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.CrossVendorReconciler') as mock_reconciler, \
             patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.HybridMinuteDataManager') as mock_storage:

            # Setup mocks
            polygon_adapter = AsyncMock()
            tiingo_adapter = AsyncMock()
            reconciler = AsyncMock()
            storage_manager = AsyncMock()

            mock_polygon.return_value = polygon_adapter
            mock_tiingo.return_value = tiingo_adapter
            mock_reconciler.return_value = reconciler
            mock_storage.return_value = storage_manager

            polygon_adapter.fetch_minute_bars_async.side_effect = mock_polygon_fetch
            tiingo_adapter.fetch_minute_bars_async.side_effect = mock_tiingo_fetch
            reconciler.reconcile_minute_data.return_value = [MagicMock() for _ in range(9)]
            storage_manager.store_minute_data.return_value = {'stored_cold': 9}

            # Create orchestrator with small limits for testing
            sample_config.max_total_workers = 4
            sample_config.max_concurrent_symbols = 2
            sample_config.max_concurrent_date_ranges = 2

            orchestrator = EnhancedMinuteBackfillOrchestrator(
                mock_db_pool, sample_config
            )
            orchestrator.polygon_adapter = polygon_adapter
            orchestrator.tiingo_adapter = tiingo_adapter
            orchestrator.reconciler = reconciler
            orchestrator.storage_manager = storage_manager

            # Generate segments (2 symbols * 2 days = 4 segments)
            orchestrator._generate_job_segments()

            # Run backfill
            await orchestrator._execute_parallel_backfill()

            # Verify concurrency limits were respected
            # Total workers should not exceed max_total_workers
            total_max_concurrent = max_concurrent['polygon'] + max_concurrent['tiingo']
            assert total_max_concurrent <= sample_config.max_total_workers

            # All segments should be completed
            completed_segments = [
                s for s in orchestrator.job_segments.values()
                if s.status == JobStatus.COMPLETED
            ]
            assert len(completed_segments) == 4


class TestConvenienceFunction:
    """Test the convenience function for running enhanced backfill."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_run_enhanced_minute_backfill_basic(self):
        """Test basic usage of convenience function."""

        with patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.asyncpg.create_pool') as mock_pool_create, \
             patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.EnhancedMinuteBackfillOrchestrator') as mock_orchestrator_class:

            # Setup mocks
            mock_pool = AsyncMock()
            mock_pool_create.return_value = mock_pool

            mock_orchestrator = AsyncMock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.__aenter__.return_value = mock_orchestrator
            mock_orchestrator.run_backfill.return_value = {'status': 'completed'}

            # Run function
            result = await run_enhanced_minute_backfill(
                db_url="postgresql://test",
                symbols=["AAPL", "MSFT"],
                polygon_api_key="poly_key",
                tiingo_api_key="tiingo_key",
                start_date=datetime(2024, 1, 1),
                end_date=datetime(2024, 1, 3),
                max_workers=10
            )

            # Verify results
            assert result == {'status': 'completed'}

            # Verify pool was created and closed
            mock_pool_create.assert_called_once()
            mock_pool.close.assert_called_once()

            # Verify orchestrator was created with correct config
            mock_orchestrator_class.assert_called_once()
            config = mock_orchestrator_class.call_args[0][1]
            assert config.symbols == ["AAPL", "MSFT"]
            assert config.max_total_workers == 10
            assert config.max_concurrent_symbols == 3  # min(5, 10//3)


class TestIntegrationScenarios:
    """Integration tests for real-world scenarios."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_checkpoint_recovery_after_partial_completion(self, mock_db_pool, sample_config, temp_checkpoint_file):
        """Test resuming from checkpoint after partial completion."""

        sample_config.checkpoint_file = temp_checkpoint_file

        with patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.PolygonMinuteAdapter') as mock_polygon, \
             patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.TiingoIntradayAdapter') as mock_tiingo, \
             patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.CrossVendorReconciler') as mock_reconciler, \
             patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.HybridMinuteDataManager') as mock_storage:

            # Setup mocks
            polygon_adapter = AsyncMock()
            tiingo_adapter = AsyncMock()
            reconciler = AsyncMock()
            storage_manager = AsyncMock()

            mock_polygon.return_value = polygon_adapter
            mock_tiingo.return_value = tiingo_adapter
            mock_reconciler.return_value = reconciler
            mock_storage.return_value = storage_manager

            polygon_adapter.fetch_minute_bars_async.return_value = [MagicMock() for _ in range(10)]
            tiingo_adapter.fetch_minute_bars_async.return_value = [MagicMock() for _ in range(8)]
            reconciler.reconcile_minute_data.return_value = [MagicMock() for _ in range(9)]
            storage_manager.store_minute_data.return_value = {'stored_cold': 9}

            # First run: complete some segments then "crash"
            orchestrator1 = EnhancedMinuteBackfillOrchestrator(
                mock_db_pool, sample_config
            )
            orchestrator1.polygon_adapter = polygon_adapter
            orchestrator1.tiingo_adapter = tiingo_adapter
            orchestrator1.reconciler = reconciler
            orchestrator1.storage_manager = storage_manager

            # Generate segments
            orchestrator1._generate_job_segments()
            total_segments = len(orchestrator1.job_segments)

            # Complete first two segments manually
            segments = list(orchestrator1.job_segments.values())
            for i in range(2):
                await orchestrator1._process_single_segment(segments[i])

            # Save checkpoint
            orchestrator1.save_checkpoint()

            # Verify checkpoint
            assert orchestrator1.progress.segments_completed == 2

            # Second run: resume from checkpoint
            orchestrator2 = EnhancedMinuteBackfillOrchestrator(
                mock_db_pool, sample_config
            )
            orchestrator2.polygon_adapter = polygon_adapter
            orchestrator2.tiingo_adapter = tiingo_adapter
            orchestrator2.reconciler = reconciler
            orchestrator2.storage_manager = storage_manager

            # Verify resume state
            assert orchestrator2.progress.segments_completed == 2
            assert len(orchestrator2.job_segments) == total_segments

            completed_segments = [
                s for s in orchestrator2.job_segments.values()
                if s.status == JobStatus.COMPLETED
            ]
            assert len(completed_segments) == 2

            pending_segments = [
                s for s in orchestrator2.job_segments.values()
                if s.status == JobStatus.PENDING
            ]
            assert len(pending_segments) == total_segments - 2

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_failure_threshold_enforcement(self, mock_db_pool, sample_config):
        """Test that high failure rates trigger job failure."""

        # Set low failure threshold for testing
        sample_config.failure_threshold = 0.3  # 30%

        with patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.PolygonMinuteAdapter') as mock_polygon, \
             patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.TiingoIntradayAdapter') as mock_tiingo:

            # Setup mocks to fail
            polygon_adapter = AsyncMock()
            tiingo_adapter = AsyncMock()

            mock_polygon.return_value = polygon_adapter
            mock_tiingo.return_value = tiingo_adapter

            polygon_adapter.fetch_minute_bars_async.side_effect = Exception("Systematic failure")
            tiingo_adapter.fetch_minute_bars_async.side_effect = Exception("Systematic failure")

            # Create orchestrator
            orchestrator = EnhancedMinuteBackfillOrchestrator(
                mock_db_pool, sample_config
            )
            orchestrator.polygon_adapter = polygon_adapter
            orchestrator.tiingo_adapter = tiingo_adapter

            # Mock context manager methods
            orchestrator.__aenter__ = AsyncMock(return_value=orchestrator)
            orchestrator.__aexit__ = AsyncMock(return_value=None)

            # Expect failure due to high failure rate
            with pytest.raises(RuntimeError, match="Failure rate.*exceeds threshold"):
                async with orchestrator:
                    await orchestrator.run_backfill()

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_progress_tracking_and_eta_calculation(self, mock_db_pool, sample_config):
        """Test progress tracking and ETA calculation during processing."""

        with patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.PolygonMinuteAdapter') as mock_polygon, \
             patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.TiingoIntradayAdapter') as mock_tiingo, \
             patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.CrossVendorReconciler') as mock_reconciler, \
             patch('src.market_data.backfill.enhanced_minute_backfill_orchestrator.HybridMinuteDataManager') as mock_storage:

            # Setup mocks with realistic timing
            polygon_adapter = AsyncMock()
            tiingo_adapter = AsyncMock()
            reconciler = AsyncMock()
            storage_manager = AsyncMock()

            mock_polygon.return_value = polygon_adapter
            mock_tiingo.return_value = tiingo_adapter
            mock_reconciler.return_value = reconciler
            mock_storage.return_value = storage_manager

            async def slow_fetch(*args, **kwargs):
                await asyncio.sleep(0.05)  # 50ms delay
                return [MagicMock() for _ in range(10)]

            polygon_adapter.fetch_minute_bars_async.side_effect = slow_fetch
            tiingo_adapter.fetch_minute_bars_async.side_effect = slow_fetch
            reconciler.reconcile_minute_data.return_value = [MagicMock() for _ in range(9)]
            storage_manager.store_minute_data.return_value = {'stored_cold': 9}

            # Create orchestrator
            orchestrator = EnhancedMinuteBackfillOrchestrator(
                mock_db_pool, sample_config
            )
            orchestrator.polygon_adapter = polygon_adapter
            orchestrator.tiingo_adapter = tiingo_adapter
            orchestrator.reconciler = reconciler
            orchestrator.storage_manager = storage_manager

            # Generate segments
            orchestrator._generate_job_segments()

            # Process a few segments to build timing data
            segments = list(orchestrator.job_segments.values())[:2]
            for segment in segments:
                await orchestrator._process_single_segment(segment)

            # Check that progress tracking is working
            assert orchestrator.progress.segments_completed == 2
            assert orchestrator.progress.avg_processing_time_per_segment > 0
            assert len(orchestrator.segment_processing_times) == 2

            # ETA should be calculated
            orchestrator.progress.update_completion_estimate()
            assert orchestrator.progress.estimated_completion_time is not None

            # ETA should be reasonable (not too far in the future)
            eta_delta = orchestrator.progress.estimated_completion_time - datetime.now()
            assert eta_delta.total_seconds() > 0  # Should be in the future
            assert eta_delta.total_seconds() < 300  # But not more than 5 minutes for this small test


if __name__ == "__main__":
    pytest.main([__file__, "-v"])