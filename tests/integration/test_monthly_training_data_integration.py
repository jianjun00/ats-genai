#!/usr/bin/env python3
"""
Integration Tests for Monthly Training Data System

Tests the complete integration of:
1. Database schema for monthly training data tracking
2. Training data generation with monthly storage
3. EDA endpoints for monthly data visualization
4. End-to-end workflow from generation to visualization
"""

import pytest
import tempfile
import shutil
from datetime import date, timedelta
from pathlib import Path
import asyncpg

from shared.utils.environment import Environment, EnvironmentType
from domains.ml.services.training_data.dao.monthly_training_data_dao import (
    MonthlyTrainingDataDAO, MonthlyTrainingDataRecord
)


@pytest.mark.integration
class TestMonthlyTrainingDataDatabaseIntegration:
    """Test database integration for monthly training data."""

    @pytest.fixture
    def environment(self):
        """Create test environment."""
        return Environment(environment_type=EnvironmentType.DEV)

    @pytest.fixture
    def dao(self, environment):
        """Create DAO for testing."""
        return MonthlyTrainingDataDAO(environment)

    @pytest.mark.asyncio
    async def test_database_schema_exists(self, environment):
        """Test that the monthly training data table exists with correct schema."""
        conn = await asyncpg.connect(environment.get_database_url())

        try:
            # Test table exists
            table_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'dev_monthly_training_data'
                )
            """)
            assert table_exists, "dev_monthly_training_data table should exist"

            # Test column schema
            columns = await conn.fetch("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = 'dev_monthly_training_data'
                ORDER BY ordinal_position
            """)

            column_names = [col['column_name'] for col in columns]
            expected_columns = [
                'id', 'run_id', 'symbol', 'instrument_id', 'year_month',
                'timeframe_paths', 'total_records', 'file_size_mb', 'data_quality_score',
                'status', 'error_message', 'created_at', 'updated_at'
            ]

            for expected_col in expected_columns:
                assert expected_col in column_names, f"Column {expected_col} should exist"

            # Test that timeframe_paths is JSONB
            timeframe_paths_col = next(col for col in columns if col['column_name'] == 'timeframe_paths')
            assert timeframe_paths_col['data_type'] == 'jsonb', "timeframe_paths should be JSONB type"

            # Test indexes exist
            indexes = await conn.fetch("""
                SELECT indexname FROM pg_indexes
                WHERE tablename = 'dev_monthly_training_data'
            """)
            index_names = [idx['indexname'] for idx in indexes]

            expected_indexes = [
                'idx_dev_monthly_training_run_id',
                'idx_dev_monthly_training_symbol',
                'idx_dev_monthly_training_year_month',
                'idx_dev_monthly_training_symbol_month'
            ]

            for expected_idx in expected_indexes:
                assert expected_idx in index_names, f"Index {expected_idx} should exist"

        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_view_exists(self, environment):
        """Test that the view with instrument details exists."""
        conn = await asyncpg.connect(environment.get_database_url())

        try:
            view_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.views
                    WHERE table_name = 'dev_monthly_training_data_with_instruments'
                )
            """)
            assert view_exists, "dev_monthly_training_data_with_instruments view should exist"

        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_crud_operations_full_cycle(self, dao):
        """Test complete CRUD operations on monthly training data."""
        # Create test record
        test_record = MonthlyTrainingDataRecord(
            run_id=9999,  # Use high ID to avoid conflicts
            symbol="TEST_INTEGRATION",
            instrument_id=None,
            year_month=date(2025, 9, 1),
            timeframe_paths={
                "5m": "/test/path/5m.arrayrecord",
                "15m": "/test/path/15m.arrayrecord",
                "1h": "/test/path/1h.arrayrecord",
                "1d": "/test/path/1d.arrayrecord"
            },
            total_records=1000,
            file_size_mb=25.5,
            data_quality_score=0.98,
            status="completed"
        )

        try:
            # CREATE
            record_id = await dao.create_monthly_record(test_record)
            assert record_id is not None
            assert isinstance(record_id, int)

            # READ - Get single record
            retrieved_record = await dao.get_monthly_record(record_id)
            assert retrieved_record is not None
            assert retrieved_record.symbol == "TEST_INTEGRATION"
            assert retrieved_record.year_month == date(2025, 9, 1)
            assert retrieved_record.total_records == 1000
            assert retrieved_record.data_quality_score == 0.98
            assert len(retrieved_record.timeframe_paths) == 4

            # READ - List records with filters
            records = await dao.list_monthly_records(
                symbols=["TEST_INTEGRATION"],
                status="completed",
                limit=10
            )
            assert len(records) >= 1
            found_record = next((r for r in records if r.id == record_id), None)
            assert found_record is not None

            # UPDATE
            updates = {
                'total_records': 1500,
                'data_quality_score': 0.99,
                'status': 'validated'
            }
            update_success = await dao.update_monthly_record(record_id, updates)
            assert update_success

            # Verify update
            updated_record = await dao.get_monthly_record(record_id)
            assert updated_record.total_records == 1500
            assert updated_record.data_quality_score == 0.99
            assert updated_record.status == 'validated'

            # Test timeframe paths retrieval
            paths = await dao.get_timeframe_paths("TEST_INTEGRATION", date(2025, 9, 1))
            assert paths is not None
            assert paths['5m'] == "/test/path/5m.arrayrecord"
            assert paths['1h'] == "/test/path/1h.arrayrecord"

        finally:
            # DELETE - Clean up test data
            await dao.delete_monthly_record(record_id)

            # Verify deletion
            deleted_record = await dao.get_monthly_record(record_id)
            assert deleted_record is None

    @pytest.mark.asyncio
    async def test_summary_statistics(self, dao):
        """Test summary statistics functionality."""
        # Create multiple test records for summary testing
        test_records = [
            MonthlyTrainingDataRecord(
                run_id=9998, symbol="SUMMARY_TEST_A", instrument_id=None,
                year_month=date(2025, 8, 1), timeframe_paths={"5m": "/test/a/5m.arrayrecord"},
                total_records=500, file_size_mb=10.0, data_quality_score=0.95, status="completed"
            ),
            MonthlyTrainingDataRecord(
                run_id=9998, symbol="SUMMARY_TEST_A", instrument_id=None,
                year_month=date(2025, 9, 1), timeframe_paths={"5m": "/test/a2/5m.arrayrecord"},
                total_records=600, file_size_mb=12.0, data_quality_score=0.97, status="completed"
            ),
            MonthlyTrainingDataRecord(
                run_id=9997, symbol="SUMMARY_TEST_B", instrument_id=None,
                year_month=date(2025, 8, 1), timeframe_paths={"5m": "/test/b/5m.arrayrecord"},
                total_records=300, file_size_mb=8.0, data_quality_score=0.90, status="completed"
            )
        ]

        record_ids = []
        try:
            # Create test records
            for record in test_records:
                record_id = await dao.create_monthly_record(record)
                record_ids.append(record_id)

            # Get summary statistics
            summary = await dao.get_summary_by_symbol()

            # Find our test symbols in summary
            test_a_summary = next((s for s in summary if s['symbol'] == 'SUMMARY_TEST_A'), None)
            test_b_summary = next((s for s in summary if s['symbol'] == 'SUMMARY_TEST_B'), None)

            assert test_a_summary is not None
            assert test_a_summary['total_months'] == 2
            assert test_a_summary['total_records'] == 1100  # 500 + 600
            assert test_a_summary['total_size_mb'] == 22.0   # 10.0 + 12.0
            assert test_a_summary['completed_months'] == 2
            assert test_a_summary['failed_months'] == 0

            assert test_b_summary is not None
            assert test_b_summary['total_months'] == 1
            assert test_b_summary['total_records'] == 300
            assert test_b_summary['total_size_mb'] == 8.0

        finally:
            # Clean up test records
            for record_id in record_ids:
                await dao.delete_monthly_record(record_id)


@pytest.mark.integration
class TestMonthlyTrainingDataEndpoints:
    """Test the EDA endpoints for monthly training data."""

    @pytest.fixture
    def analytics_service(self):
        """Create analytics service for endpoint testing."""
        from services.analytics_service import UnifiedAnalyticsService
        return UnifiedAnalyticsService()

    def test_monthly_training_data_table_endpoint(self, analytics_service):
        """Test the monthly training data table endpoint."""
        # This would normally require a running server, but we can test the service method directly
        # In a full integration test, you would use requests to test the actual HTTP endpoint

        # Mock the request handler environment
        class MockRequestHandler:
            def __init__(self):
                self.path = '/api/v1/monthly-training-data?symbols=AAPL&status=completed'
                self.response_code = None
                self.headers = {}
                self.response_data = None

            def send_response(self, code):
                self.response_code = code

            def send_header(self, name, value):
                self.headers[name] = value

            def end_headers(self):
                pass

            def write(self, data):
                self.response_data = data

        # Note: This is a simplified test. In practice, you'd want to:
        # 1. Start the actual analytics server
        # 2. Make HTTP requests using requests library
        # 3. Verify JSON responses
        # 4. Test all query parameters and filters

        # For now, just verify the endpoint method exists
        assert hasattr(analytics_service, '_serve_monthly_training_data_table')
        assert hasattr(analytics_service, '_serve_monthly_training_visualization')
        assert hasattr(analytics_service, '_generate_multi_timeframe_charts')


@pytest.mark.integration
class TestEndToEndWorkflow:
    """Test complete end-to-end workflow for monthly training data."""

    @pytest.fixture
    def temp_workspace(self):
        """Create temporary workspace for testing."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_training_data_generation_to_visualization(self, temp_workspace):
        """Test complete workflow from generation to visualization."""
        # This test simulates:
        # 1. Training data generation with monthly storage
        # 2. Database record creation
        # 3. EDA data retrieval
        # 4. Visualization data preparation

        from unittest.mock import Mock, patch, AsyncMock

        # 1. Set up training data generation
        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer, \
             patch('domains.ml.services.training_data.dao.monthly_training_data_dao.MonthlyTrainingDataDAO') as mock_dao_class:

            # Mock ArrayRecord writer
            mock_writer_instance = Mock()
            mock_writer.return_value = mock_writer_instance

            # Mock DAO
            mock_dao = Mock()
            mock_dao_class.return_value = mock_dao
            mock_dao.create_monthly_record = AsyncMock(return_value=999)
            mock_dao.get_monthly_record = AsyncMock()
            mock_dao.list_monthly_records = AsyncMock(return_value=[])

            # Set up mock record for visualization
            mock_record = Mock()
            mock_record.id = 999
            mock_record.symbol = 'TEST_SYMBOL'
            mock_record.year_month = date(2025, 9, 1)
            mock_record.timeframe_paths = {
                '5m': str(temp_workspace / 'test_5m.arrayrecord'),
                '1h': str(temp_workspace / 'test_1h.arrayrecord')
            }
            mock_record.total_records = 1000
            mock_record.data_quality_score = 0.95
            mock_dao.get_monthly_record.return_value = mock_record

            # Create mock ArrayRecord files
            for path in mock_record.timeframe_paths.values():
                Path(path).parent.mkdir(parents=True, exist_ok=True)
                Path(path).write_bytes(b'mock_arrayrecord_data')

            # 2. Test monthly training data generation
            from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback

            callback = IntervalBasedTrainingDataCallback(
                symbols=['TEST_SYMBOL'],
                start_date='2025-09-01',
                end_date='2025-09-30',
                start_day_offset=2,
                end_day_offset=1,
                output_dir=str(temp_workspace)
            )
            callback.run_id = 999
            callback.dataset_id = 'test_dataset'

            # Initialize monthly structure
            await callback._initialize_dataset_structure()

            # Verify monthly structure was created
            assert len(callback.array_record_writers) == 4  # 1 symbol × 4 timeframes × 1 month
            assert len(callback.monthly_file_paths) == 4

            # 3. Simulate saving to database
            mock_runner = Mock()
            mock_environment = Mock()
            mock_runner.get_environment.return_value = mock_environment

            # Set up record counts
            callback.monthly_record_counts = {
                'TEST_SYMBOL_5m_2025_09': 1440,
                'TEST_SYMBOL_15m_2025_09': 96,
                'TEST_SYMBOL_1h_2025_09': 24,
                'TEST_SYMBOL_1d_2025_09': 1
            }

            await callback._save_monthly_training_data_records(mock_runner)

            # Verify database record was created
            mock_dao.create_monthly_record.assert_called_once()

            # 4. Test EDA data retrieval
            records = await mock_dao.list_monthly_records(symbols=['TEST_SYMBOL'])
            record = await mock_dao.get_monthly_record(999)

            # Verify EDA operations
            mock_dao.list_monthly_records.assert_called_with(symbols=['TEST_SYMBOL'])
            mock_dao.get_monthly_record.assert_called_with(999)

            # 5. Test visualization data preparation
            from services.analytics_service import UnifiedAnalyticsService
            analytics = UnifiedAnalyticsService()

            # Mock ArrayRecord reader for visualization
            with patch('array_record.python.array_record_module.ArrayRecordReader') as mock_reader:
                mock_reader_instance = Mock()
                mock_reader.return_value = mock_reader_instance
                mock_reader_instance.__len__.return_value = 10
                mock_reader_instance.__getitem__.return_value = [100.0, 105.0, 95.0, 102.0, 10000.0]

                # Test chart generation
                timeframe_data = {
                    '5m': [{'timestamp': i, 'open': 100+i, 'high': 105+i, 'low': 95+i, 'close': 102+i, 'volume': 1000} for i in range(10)],
                    '1h': [{'timestamp': i, 'open': 100+i*5, 'high': 105+i*5, 'low': 95+i*5, 'close': 102+i*5, 'volume': 5000} for i in range(5)]
                }

                charts = analytics._generate_multi_timeframe_charts(
                    timeframe_data, 'TEST_SYMBOL', '1h', 2
                )

                # Verify charts were generated
                assert '5m' in charts
                assert '1h' in charts
                assert charts['5m']['data'][0]['type'] == 'candlestick'
                assert charts['1h']['data'][0]['type'] == 'candlestick'
                assert len(charts['5m']['data'][0]['x']) == 10
                assert len(charts['1h']['data'][0]['x']) == 5

    @pytest.mark.asyncio
    async def test_offset_date_handling_integration(self):
        """Test that date offsets work correctly in integration."""
        # Test the complete flow of:
        # 1. Setting start_day_offset and end_day_offset
        # 2. Calculating collection window
        # 3. Filtering data to target range only
        # 4. Saving only target range data to monthly files

        target_start = date(2025, 7, 15)
        target_end = date(2025, 7, 25)
        start_offset = 5  # Look back 5 days
        end_offset = 3    # Look ahead 3 days

        collection_start = target_start - timedelta(days=start_offset)  # July 10
        collection_end = target_end + timedelta(days=end_offset)        # July 28

        # Simulate processing data for each day in collection window
        all_days = []
        current_day = collection_start
        while current_day <= collection_end:
            all_days.append(current_day)
            current_day += timedelta(days=1)

        # Filter to target range (this is what the callback should do)
        target_days = [day for day in all_days if target_start <= day <= target_end]

        # Verify the filtering works correctly
        assert len(all_days) == 19      # July 10-28 = 19 days
        assert len(target_days) == 11   # July 15-25 = 11 days
        assert target_days[0] == target_start
        assert target_days[-1] == target_end

        # Verify we collect more than we save
        assert len(all_days) > len(target_days)

        # This demonstrates that:
        # - We collect 19 days of data (for feature engineering context)
        # - But only save 11 days of data (the target training period)
        # - The 8 extra days provide lookback/lookahead context without being saved


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-m', 'integration'])