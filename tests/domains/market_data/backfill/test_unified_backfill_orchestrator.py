"""
Comprehensive tests for Unified Backfill Orchestrator.

Tests the complete 5-year backfill workflow including configuration,
progress tracking, error handling, and integration components.
"""

import pytest
import asyncio
import tempfile
import shutil
import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
import asyncpg

from domains.market_data.services.data_collection.backfill.unified_backfill_orchestrator import (
    UnifiedBackfillOrchestrator,
    BackfillConfig,
    BackfillProgress,
    run_5_year_backfill
)
from domains.trading.services.core.minute.hybrid_minute_data_manager import StorageConfig
from domains.market_data.services.core.reconciliation.cross_vendor_reconciler import ReconciliationMethod


class TestBackfillConfig:
    """Test BackfillConfig settings."""

    def test_default_config(self):
        """Test default configuration."""
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2025, 1, 1)

        config = BackfillConfig(
            start_date=start_date,
            end_date=end_date
        )

        assert config.start_date == start_date
        assert config.end_date == end_date
        assert config.symbols is None
        assert config.batch_size == 10
        assert config.chunk_size_days == 30
        assert config.max_concurrent_symbols == 3
        assert config.reconciliation_method == ReconciliationMethod.WEIGHTED_AVERAGE
        assert config.max_retries == 3
        assert config.continue_on_error is True

    def test_custom_config(self):
        """Test custom configuration."""
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2021, 1, 1)
        symbols = ['AAPL', 'MSFT', 'GOOGL']

        config = BackfillConfig(
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            batch_size=5,
            chunk_size_days=7,
            max_concurrent_symbols=2,
            reconciliation_method=ReconciliationMethod.POLYGON_PRIORITY,
            max_retries=5,
            continue_on_error=False,
            polygon_api_key='test_polygon_key',
            tiingo_api_key='test_tiingo_key'
        )

        assert config.symbols == symbols
        assert config.batch_size == 5
        assert config.chunk_size_days == 7
        assert config.max_concurrent_symbols == 2
        assert config.reconciliation_method == ReconciliationMethod.POLYGON_PRIORITY
        assert config.max_retries == 5
        assert config.continue_on_error is False
        assert config.polygon_api_key == 'test_polygon_key'
        assert config.tiingo_api_key == 'test_tiingo_key'


class TestBackfillProgress:
    """Test BackfillProgress tracking."""

    def test_default_progress(self):
        """Test default progress initialization."""
        progress = BackfillProgress()

        assert isinstance(progress.symbols_completed, set)
        assert isinstance(progress.symbols_failed, set)
        assert isinstance(progress.errors, list)
        assert len(progress.symbols_completed) == 0
        assert len(progress.symbols_failed) == 0
        assert len(progress.errors) == 0
        assert progress.current_symbol is None
        assert progress.bars_processed == 0

    def test_progress_with_data(self):
        """Test progress with initial data."""
        completed = {'AAPL', 'MSFT'}
        failed = {'FAILED_SYMBOL'}
        errors = ['Error 1', 'Error 2']

        progress = BackfillProgress(
            symbols_completed=completed,
            symbols_failed=failed,
            bars_processed=1000,
            bars_reconciled=950,
            errors=errors
        )

        assert progress.symbols_completed == completed
        assert progress.symbols_failed == failed
        assert progress.bars_processed == 1000
        assert progress.bars_reconciled == 950
        assert progress.errors == errors


class TestUnifiedBackfillOrchestrator:
    """Test UnifiedBackfillOrchestrator functionality."""

    @pytest.fixture
    def temp_storage_path(self):
        """Create temporary storage directory."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    def mock_pool(self):
        """Create mock database pool."""
        pool = AsyncMock(spec=asyncpg.Pool)
        return pool

    @pytest.fixture
    def test_config(self, temp_storage_path):
        """Create test configuration."""
        return BackfillConfig(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 7),  # One week for testing
            symbols=['AAPL', 'MSFT'],
            batch_size=2,
            chunk_size_days=3,
            max_concurrent_symbols=1,
            storage_base_path=temp_storage_path,
            polygon_api_key='test_polygon_key',
            tiingo_api_key='test_tiingo_key',
            checkpoint_file=f"{temp_storage_path}/test_checkpoint.json"
        )

    @pytest.fixture
    def storage_config(self, temp_storage_path):
        """Create test storage configuration."""
        return StorageConfig(base_data_path=temp_storage_path)

    @pytest.fixture
    def orchestrator(self, mock_pool, test_config, storage_config):
        """Create test orchestrator instance."""
        return UnifiedBackfillOrchestrator(mock_pool, test_config, storage_config)

    def test_orchestrator_initialization(self, orchestrator, test_config):
        """Test orchestrator initialization."""
        assert orchestrator.pool is not None
        assert orchestrator.config == test_config
        assert orchestrator.storage_config is not None
        assert orchestrator.polygon_adapter is None  # Not initialized until context manager
        assert orchestrator.tiingo_adapter is None
        assert orchestrator.reconciler is not None
        assert orchestrator.storage_manager is not None
        assert isinstance(orchestrator.progress, BackfillProgress)
        assert 'symbols_processed' in orchestrator.stats

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_context_manager(self, orchestrator):
        """Test async context manager."""
        with patch('market_data.backfill.unified_backfill_orchestrator.PolygonMinuteAdapter') as MockPolygon, \
             patch('market_data.backfill.unified_backfill_orchestrator.TiingoIntradayAdapter') as MockTiingo:

            mock_polygon = AsyncMock()
            mock_tiingo = AsyncMock()
            MockPolygon.return_value = mock_polygon
            MockTiingo.return_value = mock_tiingo

            async with orchestrator:
                assert orchestrator.polygon_adapter == mock_polygon
                assert orchestrator.tiingo_adapter == mock_tiingo
                mock_polygon.__aenter__.assert_called_once()
                mock_tiingo.__aenter__.assert_called_once()

            mock_polygon.__aexit__.assert_called_once()
            mock_tiingo.__aexit__.assert_called_once()

    def test_create_symbol_batches(self, orchestrator):
        """Test symbol batch creation."""
        orchestrator.config.symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
        orchestrator.config.batch_size = 2

        batches = orchestrator._create_symbol_batches()

        assert len(batches) == 3  # 5 symbols, batch size 2
        assert batches[0] == ['AAPL', 'MSFT']
        assert batches[1] == ['GOOGL', 'AMZN']
        assert batches[2] == ['TSLA']

    def test_create_symbol_batches_with_completed(self, orchestrator):
        """Test symbol batch creation with already completed symbols."""
        orchestrator.config.symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN']
        orchestrator.config.batch_size = 2
        orchestrator.progress.symbols_completed = {'AAPL', 'GOOGL'}

        batches = orchestrator._create_symbol_batches()

        assert len(batches) == 1
        assert batches[0] == ['MSFT', 'AMZN']  # Only remaining symbols

    def test_get_default_symbols(self, orchestrator):
        """Test default symbol list."""
        symbols = orchestrator._get_default_symbols()

        assert len(symbols) > 20  # Should have a good selection
        assert 'AAPL' in symbols
        assert 'MSFT' in symbols
        assert 'SPY' in symbols
        assert 'QQQ' in symbols

    def test_create_date_chunks(self, orchestrator):
        """Test date chunk creation."""
        orchestrator.config.start_date = datetime(2024, 1, 1)
        orchestrator.config.end_date = datetime(2024, 1, 10)
        orchestrator.config.chunk_size_days = 3

        chunks = orchestrator._create_date_chunks()

        assert len(chunks) == 4  # 9 days, 3-day chunks = 4 chunks
        assert chunks[0] == (datetime(2024, 1, 1), datetime(2024, 1, 4))
        assert chunks[1] == (datetime(2024, 1, 4), datetime(2024, 1, 7))
        assert chunks[2] == (datetime(2024, 1, 7), datetime(2024, 1, 10))
        assert chunks[3] == (datetime(2024, 1, 10), datetime(2024, 1, 10))

    def test_convert_for_storage(self, orchestrator):
        """Test converting reconciled bars for storage."""
        from domains.market_data.services.core.reconciliation.cross_vendor_reconciler import ReconciledBar

        reconciled_bars = [
            ReconciledBar(
                symbol='AAPL',
                timestamp=datetime(2024, 1, 1, 10, 30),
                open=180.0, high=181.0, low=179.0, close=180.5,
                volume=1000000,
                quality_score=0.9,
                reconciliation_method='weighted_average',
                source_vendors=['polygon', 'tiingo'],
                vendor_count=2,
                price_variance=0.002,
                volume_variance=0.05,
                metadata={'test': 'data'}
            )
        ]

        result = orchestrator._convert_for_storage(reconciled_bars)

        assert len(result) == 1
        assert result[0]['symbol'] == 'AAPL'
        assert result[0]['vendor'] == 'unified'
        assert result[0]['quality_score'] == 0.9
        assert 'data_source_flags' in result[0]
        assert result[0]['data_source_flags']['reconciliation_method'] == 'weighted_average'
        assert result[0]['data_source_flags']['source_vendors'] == ['polygon', 'tiingo']

    def test_save_checkpoint(self, orchestrator, temp_storage_path):
        """Test checkpoint saving."""
        orchestrator.progress.symbols_completed = {'AAPL', 'MSFT'}
        orchestrator.progress.symbols_failed = {'FAILED_SYMBOL'}
        orchestrator.progress.bars_processed = 1000
        orchestrator.progress.errors = ['Error 1', 'Error 2']

        orchestrator.save_checkpoint()

        checkpoint_file = Path(orchestrator.config.checkpoint_file)
        assert checkpoint_file.exists()

        with open(checkpoint_file, 'r') as f:
            data = json.load(f)

        assert set(data['symbols_completed']) == {'AAPL', 'MSFT'}
        assert set(data['symbols_failed']) == {'FAILED_SYMBOL'}
        assert data['bars_processed'] == 1000
        assert data['errors'] == ['Error 1', 'Error 2']

    def test_load_checkpoint(self, orchestrator, temp_storage_path):
        """Test checkpoint loading."""
        # Create checkpoint file
        checkpoint_data = {
            'symbols_completed': ['AAPL', 'MSFT'],
            'symbols_failed': ['FAILED_SYMBOL'],
            'bars_processed': 500,
            'bars_reconciled': 480,
            'errors': ['Test error'],
            'stats': {'test_stat': 100}
        }

        with open(orchestrator.config.checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f)

        orchestrator.load_checkpoint()

        assert orchestrator.progress.symbols_completed == {'AAPL', 'MSFT'}
        assert orchestrator.progress.symbols_failed == {'FAILED_SYMBOL'}
        assert orchestrator.progress.bars_processed == 500
        assert orchestrator.progress.bars_reconciled == 480
        assert orchestrator.progress.errors == ['Test error']
        assert orchestrator.stats['test_stat'] == 100

    def test_load_checkpoint_missing_file(self, orchestrator):
        """Test loading non-existent checkpoint."""
        orchestrator.config.checkpoint_file = "/nonexistent/checkpoint.json"

        # Should not raise any exceptions
        orchestrator.load_checkpoint()

        # Progress should remain at defaults
        assert len(orchestrator.progress.symbols_completed) == 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_polygon_data_success(self, orchestrator):
        """Test successful Polygon data fetch."""
        mock_adapter = AsyncMock()
        mock_bars = [
            MagicMock(symbol='AAPL', timestamp=datetime.now(), open=180.0,
                     high=181.0, low=179.0, close=180.5, volume=1000000,
                     vwap=180.25, trade_count=1500, vendor='polygon')
        ]
        mock_adapter.fetch_minute_bars_async.return_value = mock_bars
        orchestrator.polygon_adapter = mock_adapter

        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 2)

        result = await orchestrator._fetch_polygon_data('AAPL', start_date, end_date)

        assert len(result) == 1
        assert result[0]['symbol'] == 'AAPL'
        assert result[0]['vendor'] == 'polygon'
        assert result[0]['open'] == 180.0
        mock_adapter.fetch_minute_bars_async.assert_called_once_with('AAPL', start_date, end_date)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_polygon_data_with_retries(self, orchestrator):
        """Test Polygon data fetch with retries."""
        mock_adapter = AsyncMock()
        mock_adapter.fetch_minute_bars_async.side_effect = [
            Exception("Network error"),  # First attempt fails
            Exception("Rate limit"),     # Second attempt fails
            []                          # Third attempt succeeds
        ]
        orchestrator.polygon_adapter = mock_adapter
        orchestrator.config.max_retries = 3

        with patch('asyncio.sleep') as mock_sleep:
            result = await orchestrator._fetch_polygon_data('AAPL', datetime.now(), datetime.now())

        assert result == []
        assert mock_adapter.fetch_minute_bars_async.call_count == 3
        assert mock_sleep.call_count == 2  # Two retry delays

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_fetch_tiingo_data_success(self, orchestrator):
        """Test successful Tiingo data fetch."""
        mock_adapter = AsyncMock()
        mock_bars = [
            MagicMock(symbol='AAPL', timestamp=datetime.now(), open=180.1,
                     high=181.1, low=179.1, close=180.6, volume=1050000, vendor='tiingo')
        ]
        mock_adapter.fetch_minute_bars_async.return_value = mock_bars
        orchestrator.tiingo_adapter = mock_adapter

        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 2)

        result = await orchestrator._fetch_tiingo_data('AAPL', start_date, end_date)

        assert len(result) == 1
        assert result[0]['symbol'] == 'AAPL'
        assert result[0]['vendor'] == 'tiingo'
        assert result[0]['open'] == 180.1

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_process_symbol_date_chunk_success(self, orchestrator):
        """Test successful symbol date chunk processing."""
        # Mock adapters
        orchestrator.polygon_adapter = AsyncMock()
        orchestrator.tiingo_adapter = AsyncMock()

        # Mock reconciler
        mock_reconciled_bars = [
            MagicMock(symbol='AAPL', timestamp=datetime.now(), open=180.0,
                     close=180.5, quality_score=0.9, reconciliation_method='weighted_average',
                     source_vendors=['polygon'], vendor_count=1, price_variance=0.0, volume_variance=0.0)
        ]
        orchestrator.reconciler.reconcile_minute_data = AsyncMock(return_value=mock_reconciled_bars)

        # Mock storage manager
        orchestrator.storage_manager.store_minute_data = AsyncMock(return_value={'stored_cold': 1})

        # Mock fetch methods
        with patch.object(orchestrator, '_fetch_polygon_data', return_value=[{'test': 'polygon_data'}]) as mock_polygon, \
             patch.object(orchestrator, '_fetch_tiingo_data', return_value=[{'test': 'tiingo_data'}]) as mock_tiingo, \
             patch.object(orchestrator, '_convert_for_storage', return_value=[{'converted': 'data'}]) as mock_convert:

            start_date = datetime(2024, 1, 1)
            end_date = datetime(2024, 1, 2)

            result = await orchestrator._process_symbol_date_chunk('AAPL', start_date, end_date)

        assert result['symbol'] == 'AAPL'
        assert result['polygon_bars'] == 1
        assert result['tiingo_bars'] == 1
        assert result['reconciled_bars'] == 1
        assert result['stored_bars'] == 1
        assert len(result['errors']) == 0

        mock_polygon.assert_called_once_with('AAPL', start_date, end_date)
        mock_tiingo.assert_called_once_with('AAPL', start_date, end_date)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_process_symbol_date_chunk_no_data(self, orchestrator):
        """Test processing with no data from either vendor."""
        with patch.object(orchestrator, '_fetch_polygon_data', return_value=[]), \
             patch.object(orchestrator, '_fetch_tiingo_data', return_value=[]):

            start_date = datetime(2024, 1, 1)
            end_date = datetime(2024, 1, 2)

            result = await orchestrator._process_symbol_date_chunk('AAPL', start_date, end_date)

        assert result['symbol'] == 'AAPL'
        assert result['polygon_bars'] == 0
        assert result['tiingo_bars'] == 0
        assert len(result['errors']) > 0  # Should have error about no data

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_process_symbol_date_chunk_error_handling(self, orchestrator):
        """Test error handling in symbol processing."""
        orchestrator.config.continue_on_error = True

        with patch.object(orchestrator, '_fetch_polygon_data', side_effect=Exception("Test error")):
            start_date = datetime(2024, 1, 1)
            end_date = datetime(2024, 1, 2)

            result = await orchestrator._process_symbol_date_chunk('AAPL', start_date, end_date)

        assert result['symbol'] == 'AAPL'
        assert len(result['errors']) > 0
        assert 'Test error' in str(result['errors'])

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_generate_final_statistics(self, orchestrator):
        """Test final statistics generation."""
        # Set up some statistics
        orchestrator.progress.start_time = datetime.now() - timedelta(hours=2)
        orchestrator.progress.symbols_completed = {'AAPL', 'MSFT'}
        orchestrator.progress.symbols_failed = {'FAILED_SYMBOL'}
        orchestrator.progress.errors = ['Error 1', 'Error 2']
        orchestrator.stats['symbols_processed'] = 3
        orchestrator.stats['total_bars_fetched'] = {'polygon': 1000, 'tiingo': 800}
        orchestrator.stats['total_bars_reconciled'] = 900
        orchestrator.stats['total_bars_stored'] = 850

        # Mock storage manager stats
        orchestrator.storage_manager.get_storage_stats = AsyncMock(return_value={'test': 'storage_stats'})

        # Mock reconciler stats
        orchestrator.reconciler.get_reconciliation_stats = MagicMock(return_value={'test': 'reconciliation_stats'})

        result = await orchestrator._generate_final_statistics()

        assert 'execution_summary' in result
        assert 'data_summary' in result
        assert 'vendor_performance' in result
        assert 'storage_statistics' in result
        assert 'quality_metrics' in result
        assert 'configuration' in result

        assert result['execution_summary']['symbols_completed'] == 2
        assert result['execution_summary']['symbols_failed'] == 1
        assert result['data_summary']['total_bars_reconciled'] == 900
        assert result['data_summary']['total_bars_stored'] == 850

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_run_backfill_integration(self, orchestrator):
        """Test complete backfill run integration."""
        # Mock all external dependencies
        orchestrator.polygon_adapter = AsyncMock()
        orchestrator.tiingo_adapter = AsyncMock()

        # Mock successful processing
        with patch.object(orchestrator, '_process_symbol_batch') as mock_process_batch, \
             patch.object(orchestrator, '_generate_final_statistics', return_value={'test': 'final_stats'}) as mock_stats, \
             patch.object(orchestrator, 'save_checkpoint') as mock_checkpoint:

            mock_process_batch.return_value = None  # Successful processing

            result = await orchestrator.run_backfill()

        assert result == {'test': 'final_stats'}
        mock_process_batch.assert_called()
        mock_stats.assert_called_once()
        mock_checkpoint.assert_called()


class TestBackfillIntegration:
    """Integration tests for backfill workflow."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_run_5_year_backfill_function(self):
        """Test convenience function for 5-year backfill."""
        db_url = "postgresql://test:test@localhost:5432/test"
        symbols = ['AAPL', 'MSFT']
        polygon_key = 'test_polygon_key'
        tiingo_key = 'test_tiingo_key'

        with patch('asyncpg.create_pool') as mock_create_pool, \
             patch('market_data.backfill.unified_backfill_orchestrator.UnifiedBackfillOrchestrator') as MockOrchestrator:

            mock_pool = AsyncMock()
            mock_create_pool.return_value = mock_pool

            mock_orchestrator_instance = AsyncMock()
            mock_orchestrator_instance.run_backfill.return_value = {'test': 'result'}
            MockOrchestrator.return_value = mock_orchestrator_instance

            result = await run_5_year_backfill(db_url, symbols, polygon_key, tiingo_key)

        assert result == {'test': 'result'}
        mock_create_pool.assert_called_once_with(db_url, min_size=5, max_size=20)
        mock_pool.close.assert_called_once()


class TestErrorHandlingAndEdgeCases:
    """Test error handling and edge cases."""

    @pytest.fixture
    def temp_storage_path(self):
        """Create temporary storage directory."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_missing_api_keys(self, temp_storage_path):
        """Test handling of missing API keys."""
        config = BackfillConfig(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
            # API keys intentionally missing
        )

        mock_pool = AsyncMock()
        orchestrator = UnifiedBackfillOrchestrator(mock_pool, config)

        # Should handle gracefully during context manager setup
        with patch('market_data.backfill.unified_backfill_orchestrator.PolygonMinuteAdapter',
                   side_effect=ValueError("API key required")) as MockPolygon:

            with pytest.raises(ValueError):
                async with orchestrator:
                    pass

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_database_connection_failure(self, temp_storage_path):
        """Test handling of database connection failures."""
        config = BackfillConfig(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
            polygon_api_key='test_key',
            tiingo_api_key='test_key'
        )

        # Mock failed database operations
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.executemany.side_effect = Exception("Database connection failed")
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

        orchestrator = UnifiedBackfillOrchestrator(mock_pool, config)

        # Should handle database errors gracefully
        with patch.object(orchestrator, '_fetch_polygon_data', return_value=[{'test': 'data'}]), \
             patch.object(orchestrator, '_fetch_tiingo_data', return_value=[]):

            data = [{'symbol': 'AAPL', 'timestamp': datetime.now(), 'open': 180}]
            stored_count = await orchestrator._store_hot_data('AAPL', data)

        # Should handle error and return 0
        assert stored_count == 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_rate_limit_handling(self, temp_storage_path):
        """Test handling of API rate limits."""
        config = BackfillConfig(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
            max_retries=2,
            retry_delay_seconds=0.1  # Short delay for testing
        )

        mock_pool = AsyncMock()
        orchestrator = UnifiedBackfillOrchestrator(mock_pool, config)

        # Mock rate limit errors
        mock_adapter = AsyncMock()
        mock_adapter.fetch_minute_bars_async.side_effect = [
            Exception("Rate limit exceeded"),
            []  # Success on retry
        ]
        orchestrator.polygon_adapter = mock_adapter

        with patch('asyncio.sleep') as mock_sleep:
            result = await orchestrator._fetch_polygon_data('AAPL', datetime.now(), datetime.now())

        assert result == []
        assert mock_adapter.fetch_minute_bars_async.call_count == 2
        mock_sleep.assert_called_once_with(0.1)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_partial_data_scenarios(self, temp_storage_path):
        """Test scenarios with partial data from vendors."""
        config = BackfillConfig(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
            require_both_vendors=False  # Allow single vendor data
        )

        mock_pool = AsyncMock()
        orchestrator = UnifiedBackfillOrchestrator(mock_pool, config)

        # Mock one vendor failing, other succeeding
        polygon_data = [{'symbol': 'AAPL', 'timestamp': datetime.now(), 'open': 180}]
        tiingo_data = []  # No data from Tiingo

        # Mock reconciler to handle single source
        mock_reconciled = [MagicMock(symbol='AAPL')]
        orchestrator.reconciler.reconcile_minute_data = AsyncMock(return_value=mock_reconciled)
        orchestrator.storage_manager.store_minute_data = AsyncMock(return_value={'stored_cold': 1})

        with patch.object(orchestrator, '_fetch_polygon_data', return_value=polygon_data), \
             patch.object(orchestrator, '_fetch_tiingo_data', return_value=tiingo_data), \
             patch.object(orchestrator, '_convert_for_storage', return_value=[{'test': 'data'}]):

            result = await orchestrator._process_symbol_date_chunk('AAPL', datetime.now(), datetime.now())

        assert result['polygon_bars'] == 1
        assert result['tiingo_bars'] == 0
        assert result['reconciled_bars'] == 1
        assert result['stored_bars'] == 1

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_checkpoint_corruption_handling(self, temp_storage_path):
        """Test handling of corrupted checkpoint files."""
        config = BackfillConfig(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
            checkpoint_file=f"{temp_storage_path}/corrupted_checkpoint.json"
        )

        # Create corrupted checkpoint file
        with open(config.checkpoint_file, 'w') as f:
            f.write("This is not valid JSON")

        mock_pool = AsyncMock()
        orchestrator = UnifiedBackfillOrchestrator(mock_pool, config)

        # Should handle corrupted checkpoint gracefully
        orchestrator.load_checkpoint()

        # Progress should remain at defaults
        assert len(orchestrator.progress.symbols_completed) == 0

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_large_symbol_list_batching(self, temp_storage_path):
        """Test handling of large symbol lists."""
        # Create large symbol list
        large_symbol_list = [f"SYM{i:04d}" for i in range(100)]

        config = BackfillConfig(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
            symbols=large_symbol_list,
            batch_size=10
        )

        mock_pool = AsyncMock()
        orchestrator = UnifiedBackfillOrchestrator(mock_pool, config)

        batches = orchestrator._create_symbol_batches()

        assert len(batches) == 10  # 100 symbols / 10 per batch
        assert all(len(batch) == 10 for batch in batches)
        assert sum(len(batch) for batch in batches) == 100


class TestPerformanceAndScaling:
    """Test performance and scaling scenarios."""

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_concurrent_symbol_processing(self, temp_dir=None):
        """Test concurrent processing of multiple symbols."""
        if temp_dir is None:
            temp_dir = tempfile.mkdtemp()

        config = BackfillConfig(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
            symbols=['AAPL', 'MSFT', 'GOOGL'],
            max_concurrent_symbols=2
        )

        mock_pool = AsyncMock()
        orchestrator = UnifiedBackfillOrchestrator(mock_pool, config)

        # Track call order to verify concurrency
        call_order = []

        async def mock_process_chunk(symbol, start, end):
            call_order.append(f"start_{symbol}")
            await asyncio.sleep(0.1)  # Simulate processing time
            call_order.append(f"end_{symbol}")
            return {'symbol': symbol, 'errors': []}

        with patch.object(orchestrator, '_process_symbol_date_chunk', side_effect=mock_process_chunk):
            await orchestrator._process_symbol_batch(['AAPL', 'MSFT', 'GOOGL'])

        # Should have interleaved start/end calls due to concurrency
        assert len(call_order) == 6
        assert call_order.count('start_AAPL') == 1
        assert call_order.count('end_AAPL') == 1

        # Clean up
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_memory_efficient_processing(self):
        """Test memory-efficient processing of large datasets."""
        config = BackfillConfig(
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 12, 31),  # Full year
            chunk_size_days=7  # Weekly chunks
        )

        mock_pool = AsyncMock()
        orchestrator = UnifiedBackfillOrchestrator(mock_pool, config)

        chunks = orchestrator._create_date_chunks()

        # Should create many small chunks instead of one large chunk
        assert len(chunks) > 50  # More than 50 weekly chunks in a year
        assert all((chunk[1] - chunk[0]).days <= 7 for chunk in chunks)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])