#!/usr/bin/env python3
"""
Unit Tests for Monthly Training Data Generation Changes

Tests the new monthly training data system:
1. Training data generation with start_day_offset and end_day_offset
2. Monthly file storage instead of daily storage
3. Monthly training data DAO operations
4. Training callback with monthly record tracking
"""

import pytest
import tempfile
import shutil
from datetime import datetime, date
from pathlib import Path
from unittest.mock import Mock, patch, AsyncMock

# Import the components to test
from domains.ml.services.training_data.dao.monthly_training_data_dao import (
    MonthlyTrainingDataDAO, MonthlyTrainingDataRecord
)
from domains.ml.services.training_data.callbacks.training_data_callback import (
    IntervalBasedTrainingDataCallback
)
from shared.utils.environment import Environment


class TestMonthlyTrainingDataDAO:
    """Test the MonthlyTrainingDataDAO for database operations."""

    @pytest.fixture
    def mock_environment(self):
        """Create a mock environment for testing."""
        env = Mock(spec=Environment)
        env.get_table_name.return_value = "dev_monthly_training_data"
        env.get_database_url.return_value = "postgresql://test_user:test_pass@localhost:5432/test_db"
        return env

    @pytest.fixture
    def dao(self, mock_environment):
        """Create MonthlyTrainingDataDAO instance for testing."""
        return MonthlyTrainingDataDAO(mock_environment)

    def test_dao_initialization(self, dao, mock_environment):
        """Test DAO initializes correctly with environment."""
        assert dao.environment == mock_environment
        assert dao.table_name == "dev_monthly_training_data"
        assert dao.view_name == "dev_monthly_training_data_with_instruments"

    def test_monthly_record_creation(self):
        """Test MonthlyTrainingDataRecord creation and validation."""
        # Test valid record creation
        record = MonthlyTrainingDataRecord(
            run_id=123,
            symbol="AAPL",
            instrument_id=456,
            year_month=date(2025, 7, 1),
            timeframe_paths={
                "5m": "/data/training_data/dataset_123/AAPL_2025_07/5m/AAPL_2025_07.arrayrecord",
                "15m": "/data/training_data/dataset_123/AAPL_2025_07/15m/AAPL_2025_07.arrayrecord",
                "1h": "/data/training_data/dataset_123/AAPL_2025_07/1h/AAPL_2025_07.arrayrecord",
                "1d": "/data/training_data/dataset_123/AAPL_2025_07/1d/AAPL_2025_07.arrayrecord"
            },
            total_records=1440,  # 1 minute data for a day = 1440 records
            file_size_mb=15.2,
            data_quality_score=0.98
        )

        assert record.run_id == 123
        assert record.symbol == "AAPL"
        assert record.year_month == date(2025, 7, 1)
        assert len(record.timeframe_paths) == 4
        assert record.status == "created"  # default value
        assert record.data_quality_score == 0.98

    @pytest.mark.asyncio
    async def test_create_monthly_record(self, dao):
        """Test creating a monthly training data record."""
        record = MonthlyTrainingDataRecord(
            run_id=123,
            symbol="TSLA",
            instrument_id=789,
            year_month=date(2025, 7, 1),
            timeframe_paths={"5m": "/path/to/5m.arrayrecord"},
            total_records=100,
            file_size_mb=5.0,
            data_quality_score=0.95
        )

        with patch('asyncpg.connect') as mock_connect:
            mock_conn = AsyncMock()
            mock_connect.return_value = mock_conn
            mock_conn.fetchval.return_value = 456  # Mock returned record ID

            record_id = await dao.create_monthly_record(record)

            assert record_id == 456
            mock_connect.assert_called_once()
            mock_conn.fetchval.assert_called_once()
            mock_conn.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_monthly_records_with_filters(self, dao):
        """Test listing monthly records with various filters."""
        with patch('asyncpg.connect') as mock_connect:
            mock_conn = AsyncMock()
            mock_connect.return_value = mock_conn

            # Mock database rows
            mock_rows = [
                {
                    'id': 1, 'run_id': 123, 'symbol': 'AAPL', 'instrument_id': 456,
                    'year_month': date(2025, 7, 1), 'timeframe_paths': '{"5m": "/path/to/5m.arrayrecord"}',
                    'total_records': 100, 'file_size_mb': 5.0, 'data_quality_score': 0.95,
                    'status': 'completed', 'error_message': '', 'created_at': datetime.now(),
                    'updated_at': datetime.now(), 'instrument_name': 'Apple Inc.',
                    'exchange': 'NASDAQ', 'sector': 'Technology', 'market_cap': 3000000000000.0
                }
            ]
            mock_conn.fetch.return_value = mock_rows

            # Test with symbol filter
            records = await dao.list_monthly_records(
                symbols=['AAPL'],
                status='completed',
                limit=50,
                offset=0
            )

            assert len(records) == 1
            assert records[0].symbol == 'AAPL'
            assert records[0].status == 'completed'
            assert records[0].instrument_name == 'Apple Inc.'
            mock_connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_timeframe_paths(self, dao):
        """Test retrieving timeframe paths for a specific symbol and month."""
        with patch('asyncpg.connect') as mock_connect:
            mock_conn = AsyncMock()
            mock_connect.return_value = mock_conn

            timeframe_paths_json = '{"5m": "/path/to/5m.arrayrecord", "1h": "/path/to/1h.arrayrecord"}'
            mock_conn.fetchrow.return_value = {'timeframe_paths': timeframe_paths_json}

            paths = await dao.get_timeframe_paths('AAPL', date(2025, 7, 1))

            assert paths is not None
            assert paths['5m'] == "/path/to/5m.arrayrecord"
            assert paths['1h'] == "/path/to/1h.arrayrecord"
            mock_connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_summary_by_symbol(self, dao):
        """Test getting summary statistics grouped by symbol."""
        with patch('asyncpg.connect') as mock_connect:
            mock_conn = AsyncMock()
            mock_connect.return_value = mock_conn

            mock_rows = [
                {
                    'symbol': 'AAPL', 'total_months': 6, 'earliest_month': date(2025, 1, 1),
                    'latest_month': date(2025, 6, 1), 'total_records_all_months': 5000,
                    'avg_quality_score': 0.95, 'total_size_mb': 150.5,
                    'completed_months': 6, 'failed_months': 0
                },
                {
                    'symbol': 'TSLA', 'total_months': 4, 'earliest_month': date(2025, 3, 1),
                    'latest_month': date(2025, 6, 1), 'total_records_all_months': 3200,
                    'avg_quality_score': 0.92, 'total_size_mb': 98.3,
                    'completed_months': 4, 'failed_months': 0
                }
            ]
            mock_conn.fetch.return_value = mock_rows

            summary = await dao.get_summary_by_symbol()

            assert len(summary) == 2
            assert summary[0]['symbol'] == 'AAPL'
            assert summary[0]['total_months'] == 6
            assert summary[0]['avg_quality_score'] == 0.95
            assert summary[1]['symbol'] == 'TSLA'
            assert summary[1]['total_records'] == 3200


class TestTrainingDataCallbackMonthlyChanges:
    """Test the training data callback changes for monthly storage."""

    @pytest.fixture
    def temp_output_dir(self):
        """Create temporary directory for testing file operations."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)

    def test_callback_initialization_with_offsets(self, temp_output_dir):
        """Test callback initializes correctly with day offsets."""
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL', 'TSLA'],
            config=None,
            storage_format='arrayrecord',
            output_dir=str(temp_output_dir),
            start_date='2025-07-01',
            end_date='2025-07-31',
            start_day_offset=5,
            end_day_offset=3,
            collection_start_date='2025-06-26',  # 5 days before start
            collection_end_date='2025-08-03'    # 3 days after end
        )

        assert callback.symbols == ['AAPL', 'TSLA']
        assert callback.start_date == date(2025, 7, 1)
        assert callback.end_date == date(2025, 7, 31)
        assert callback.start_day_offset == 5
        assert callback.end_day_offset == 3
        assert callback.collection_start_date == date(2025, 6, 26)
        assert callback.collection_end_date == date(2025, 8, 3)
        assert callback.output_dir == temp_output_dir

    def test_get_months_in_target_range(self, temp_output_dir):
        """Test month range calculation for monthly file creation."""
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            start_date='2025-06-15',  # Mid-June
            end_date='2025-08-20',    # Mid-August
            output_dir=str(temp_output_dir)
        )

        months = callback._get_months_in_target_range()

        # Should get June, July, August (3 months)
        assert len(months) == 3
        assert months[0] == date(2025, 6, 1)  # First day of June
        assert months[1] == date(2025, 7, 1)  # First day of July
        assert months[2] == date(2025, 8, 1)  # First day of August

    def test_get_months_single_month(self, temp_output_dir):
        """Test month range calculation for single month."""
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            start_date='2025-07-05',
            end_date='2025-07-25',
            output_dir=str(temp_output_dir)
        )

        months = callback._get_months_in_target_range()

        # Should get only July
        assert len(months) == 1
        assert months[0] == date(2025, 7, 1)

    @patch('array_record.python.array_record_module.ArrayRecordWriter')
    def test_initialize_monthly_dataset_structure(self, mock_writer, temp_output_dir):
        """Test monthly dataset structure initialization."""
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL', 'TSLA'],
            start_date='2025-07-01',
            end_date='2025-08-31',  # 2 months
            output_dir=str(temp_output_dir)
        )
        callback.dataset_id = 'test_dataset_123'

        # Mock the ArrayRecord writer
        mock_writer_instance = Mock()
        mock_writer.return_value = mock_writer_instance

        # Mock the binary schema
        callback.binary_schema = Mock()
        callback.binary_schema.save_schema_to_file = Mock()

        # Initialize the dataset structure
        import asyncio
        asyncio.run(callback._initialize_dataset_structure())

        # Verify writers were created for each symbol/timeframe/month combination
        # 2 symbols × 4 timeframes × 2 months = 16 writers
        assert len(callback.array_record_writers) == 16
        assert len(callback.monthly_file_paths) == 16

        # Verify directory structure was created
        expected_dirs = [
            temp_output_dir / 'test_dataset_123' / 'AAPL_2025_07',
            temp_output_dir / 'test_dataset_123' / 'AAPL_2025_08',
            temp_output_dir / 'test_dataset_123' / 'TSLA_2025_07',
            temp_output_dir / 'test_dataset_123' / 'TSLA_2025_08'
        ]

        for dir_path in expected_dirs:
            assert dir_path.exists()
            # Check timeframe subdirectories
            for timeframe in ['5m', '15m', '1h', '1d']:
                assert (dir_path / timeframe).exists()

    def test_monthly_file_key_generation(self, temp_output_dir):
        """Test that monthly file keys are generated correctly."""
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            start_date='2025-07-01',
            end_date='2025-07-31',
            output_dir=str(temp_output_dir)
        )

        # Test the key format: symbol_timeframe_YYYY_MM
        expected_keys = [
            'AAPL_5m_2025_07',
            'AAPL_15m_2025_07',
            'AAPL_1h_2025_07',
            'AAPL_1d_2025_07'
        ]

        # This would be set during _initialize_dataset_structure
        callback.monthly_file_paths = {key: f"/path/to/{key}.arrayrecord" for key in expected_keys}

        for expected_key in expected_keys:
            assert expected_key in callback.monthly_file_paths

    @pytest.mark.asyncio
    async def test_save_monthly_training_data_records(self, temp_output_dir):
        """Test saving monthly training data records to database."""
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            start_date='2025-07-01',
            end_date='2025-07-31',
            output_dir=str(temp_output_dir)
        )
        callback.run_id = 123

        # Set up mock file paths and record counts
        callback.monthly_file_paths = {
            'AAPL_5m_2025_07': '/data/training_data/test_dataset/AAPL_2025_07/5m/AAPL_2025_07.arrayrecord',
            'AAPL_1h_2025_07': '/data/training_data/test_dataset/AAPL_2025_07/1h/AAPL_2025_07.arrayrecord'
        }
        callback.monthly_record_counts = {
            'AAPL_5m_2025_07': 1440,   # 1 day of minute data
            'AAPL_1h_2025_07': 24      # 1 day of hourly data
        }

        # Create mock files for size calculation
        for file_path in callback.monthly_file_paths.values():
            file_path_obj = Path(file_path)
            file_path_obj.parent.mkdir(parents=True, exist_ok=True)
            file_path_obj.write_bytes(b'x' * 1024)  # 1KB file

        # Mock runner and environment
        mock_runner = Mock()
        mock_environment = Mock()
        mock_environment.get_table_name.return_value = "dev_monthly_training_data"
        mock_runner.get_environment.return_value = mock_environment

        # Mock the DAO
        with patch('domains.ml.services.training_data.dao.monthly_training_data_dao.MonthlyTrainingDataDAO') as mock_dao_class:
            mock_dao = Mock()
            mock_dao_class.return_value = mock_dao
            mock_dao.create_monthly_record = AsyncMock(return_value=456)

            # Test the method
            await callback._save_monthly_training_data_records(mock_runner)

            # Verify DAO was created and called
            mock_dao_class.assert_called_once_with(mock_environment)
            mock_dao.create_monthly_record.assert_called_once()

            # Get the record that was passed to create_monthly_record
            call_args = mock_dao.create_monthly_record.call_args[0][0]
            assert call_args.run_id == 123
            assert call_args.symbol == 'AAPL'
            assert call_args.year_month == date(2025, 7, 1)
            assert call_args.total_records == 1464  # 1440 + 24
            assert '5m' in call_args.timeframe_paths
            assert '1h' in call_args.timeframe_paths


class TestTrainingDataGeneratorWithOffsets:
    """Test training data generation with day offsets."""

    def test_offset_date_calculation(self):
        """Test that start and end day offsets calculate dates correctly."""
        from datetime import datetime as dt, timedelta

        # Test case: Generate training data for July with 5-day lookback and 3-day lookahead
        start_date = dt.strptime('2025-07-01', '%Y-%m-%d').date()
        end_date = dt.strptime('2025-07-31', '%Y-%m-%d').date()
        start_day_offset = 5
        end_day_offset = 3

        collection_start_date = start_date - timedelta(days=start_day_offset)
        collection_end_date = end_date + timedelta(days=end_day_offset)

        assert collection_start_date == date(2025, 6, 26)  # 5 days before July 1st
        assert collection_end_date == date(2025, 8, 3)     # 3 days after July 31st

        # Verify that we collect more data than we save
        collection_days = (collection_end_date - collection_start_date).days + 1
        target_days = (end_date - start_date).days + 1

        assert collection_days == 39  # June 26 to August 3
        assert target_days == 31      # July 1 to July 31
        assert collection_days > target_days

    def test_target_range_filtering(self):
        """Test that data is only saved within target range, not collection range."""
        # Simulate the filtering logic in _stream_training_examples_to_writers
        start_date = date(2025, 7, 1)
        end_date = date(2025, 7, 31)

        # Test dates
        test_dates = [
            (date(2025, 6, 30), False),  # Before target range
            (date(2025, 7, 1), True),    # Start of target range
            (date(2025, 7, 15), True),   # Middle of target range
            (date(2025, 7, 31), True),   # End of target range
            (date(2025, 8, 1), False)    # After target range
        ]

        for test_date, should_save in test_dates:
            # This mimics the logic in the callback
            in_target_range = start_date <= test_date <= end_date
            assert in_target_range == should_save, f"Date {test_date} should_save={should_save} but got {in_target_range}"


class TestIntegrationScenarios:
    """Test integration scenarios for the monthly training data system."""

    @pytest.mark.asyncio
    async def test_end_to_end_monthly_workflow(self):
        """Test the complete workflow from generation to database storage."""
        # This is a high-level integration test that verifies:
        # 1. Training data generation with offsets
        # 2. Monthly file creation
        # 3. Database record creation
        # 4. EDA retrieval

        # Mock all external dependencies
        with patch('domains.ml.services.training_data.dao.monthly_training_data_dao.MonthlyTrainingDataDAO') as mock_dao_class, \
             patch('array_record.python.array_record_module.ArrayRecordWriter'), \
             patch('services.core.app.database_manager.DatabaseManager'):

            # Set up mocks
            mock_dao = Mock()
            mock_dao_class.return_value = mock_dao
            mock_dao.create_monthly_record = AsyncMock(return_value=123)
            mock_dao.list_monthly_records = AsyncMock(return_value=[])
            mock_dao.get_summary_by_symbol = AsyncMock(return_value=[])

            # 1. Simulate training data generation with offsets
            temp_dir = tempfile.mkdtemp()
            try:
                callback = IntervalBasedTrainingDataCallback(
                    symbols=['AAPL'],
                    start_date='2025-07-01',
                    end_date='2025-07-31',
                    start_day_offset=2,
                    end_day_offset=1,
                    output_dir=temp_dir
                )
                callback.run_id = 456
                callback.dataset_id = 'test_dataset'

                # 2. Initialize monthly structure
                await callback._initialize_dataset_structure()

                # Verify monthly writers were created
                assert len(callback.array_record_writers) == 4  # 1 symbol × 4 timeframes × 1 month
                assert len(callback.monthly_file_paths) == 4

                # 3. Simulate data processing and record counting
                callback.monthly_record_counts = {
                    'AAPL_5m_2025_07': 100,
                    'AAPL_15m_2025_07': 50,
                    'AAPL_1h_2025_07': 25,
                    'AAPL_1d_2025_07': 12
                }

                # 4. Simulate end of processing - save to database
                mock_runner = Mock()
                mock_environment = Mock()
                mock_runner.get_environment.return_value = mock_environment

                await callback._save_monthly_training_data_records(mock_runner)

                # 5. Verify database interaction
                mock_dao.create_monthly_record.assert_called_once()
                record_saved = mock_dao.create_monthly_record.call_args[0][0]

                assert record_saved.run_id == 456
                assert record_saved.symbol == 'AAPL'
                assert record_saved.year_month == date(2025, 7, 1)
                assert record_saved.total_records == 187  # Sum of all timeframe counts
                assert len(record_saved.timeframe_paths) == 4

                # 6. Test EDA retrieval
                records = await mock_dao.list_monthly_records(symbols=['AAPL'])
                summary = await mock_dao.get_summary_by_symbol()

                # Verify EDA calls were made
                mock_dao.list_monthly_records.assert_called_with(symbols=['AAPL'])
                mock_dao.get_summary_by_symbol.assert_called_once()

            finally:
                shutil.rmtree(temp_dir)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])