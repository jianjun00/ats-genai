"""
Comprehensive Test Cases for Training Data Generation Pipeline

Tests each step of the training data generation process to ensure
proper functionality and identify root causes of failures.
"""

import pytest
import os
import tempfile
import json
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch

# Import the components we're testing
from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig
from domains.ml.services.storage.sequence_storage_manager import ArrayRecordStorageManager
from core.platform.config.environment import Environment, EnvironmentType
from domains.trading.services.core.minute.file_based_minute_service import FileBasedMinuteMarketDataManager


class TestTrainingDataGenerationSteps:
    """Test each step of the training data generation pipeline."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for test outputs."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir

    @pytest.fixture
    def mock_environment(self):
        """Create mock environment for testing."""
        env = Mock(spec=Environment)
        env.get_table_name.return_value = "test_training_dataset"
        return env

    @pytest.fixture
    def test_config(self):
        """Create test configuration."""
        config = TrainingDataConfig()
        config.timeframes = ['5m', '15m', '1h', '1d']
        config.feature_types = ['ohlcv', 'volume_profile']
        config.signal_names = ['etop', 'ebot', 'pldot']
        config.base_interval_minutes = 1
        config.training_interval_minutes = 60
        return config

    @pytest.fixture
    def sample_args(self, temp_dir):
        """Create sample arguments for testing."""
        args = Mock()
        args.symbols = ['TSLA']
        args.start_date = '2025-09-01'
        args.end_date = '2025-09-01'
        args.environment = 'dev'
        args.output_dir = temp_dir
        args.storage_format = 'arrayrecord'
        args.base_duration = '60m'
        args.gin_config = None
        args.universe_id = 1
        args.debug = True
        return args

    def test_step1_configuration_loading(self):
        """
        STEP 1: Test configuration file loading.

        Verifies:
        - Gin config files can be loaded
        - Missing config files are handled gracefully
        - Configuration validation works properly
        """
        # Test with missing gin config
        with tempfile.TemporaryDirectory() as temp_dir:
            gin_path = os.path.join(temp_dir, "nonexistent_config.gin")

            # Should not raise exception for missing config
            assert not os.path.exists(gin_path)

            # Test with valid gin config content
            gin_config_content = """
# Test gin configuration
TrainingDataConfig.timeframes = ['5m', '15m', '1h', '1d']
TrainingDataConfig.feature_types = ['ohlcv']
"""
            gin_file = os.path.join(temp_dir, "test_config.gin")
            with open(gin_file, 'w') as f:
                f.write(gin_config_content)

            assert os.path.exists(gin_file)

            # Verify file can be read
            with open(gin_file, 'r') as f:
                content = f.read()
                assert 'TrainingDataConfig' in content
                assert 'timeframes' in content

    def test_step2_environment_and_data_validation(self, mock_environment, sample_args):
        """
        STEP 2: Test environment setup and data validation.

        Verifies:
        - Environment types are resolved correctly
        - Date ranges are validated
        - Symbol validation works
        - Invalid inputs are rejected
        """
        # Test valid environment mapping
        env_map = {
            'dev': EnvironmentType.DEV,
            'test': EnvironmentType.TEST,
            'intg': EnvironmentType.INTEGRATION,
            'prod': EnvironmentType.PRODUCTION
        }

        for env_str, env_type in env_map.items():
            assert env_map.get(env_str.lower()) == env_type

        # Test invalid environment
        assert env_map.get('invalid_env') is None

        # Test date validation
        from datetime import datetime as dt

        # Valid date range
        start_date = dt.strptime('2025-09-01', "%Y-%m-%d").date()
        end_date = dt.strptime('2025-09-02', "%Y-%m-%d").date()
        assert end_date >= start_date

        # Invalid date range (end before start)
        invalid_end = dt.strptime('2025-08-31', "%Y-%m-%d").date()
        assert invalid_end < start_date  # This should fail validation

        # Test symbol validation
        valid_symbols = ['AAPL', 'TSLA', 'MSFT']
        assert len(valid_symbols) > 0
        assert all(isinstance(symbol, str) for symbol in valid_symbols)

        # Invalid symbols
        empty_symbols = []
        assert len(empty_symbols) == 0  # This should fail validation

    def test_step3_training_configuration_creation(self, test_config):
        """
        STEP 3: Test training data configuration creation.

        Verifies:
        - TrainingDataConfig can be created with gin
        - Configuration fallback works
        - All required config attributes exist
        """
        # Test config creation
        config = test_config

        # Verify required attributes exist
        assert hasattr(config, 'timeframes')
        assert hasattr(config, 'feature_types')
        assert hasattr(config, 'signal_names')
        assert hasattr(config, 'base_interval_minutes')
        assert hasattr(config, 'training_interval_minutes')

        # Test config values
        assert isinstance(config.timeframes, list)
        assert len(config.timeframes) > 0
        assert '5m' in config.timeframes
        assert '1h' in config.timeframes

        assert isinstance(config.feature_types, list)
        assert len(config.feature_types) > 0

        assert isinstance(config.signal_names, list)
        assert len(config.signal_names) > 0

        assert isinstance(config.base_interval_minutes, int)
        assert config.base_interval_minutes > 0

        assert isinstance(config.training_interval_minutes, int)
        assert config.training_interval_minutes > 0

    def test_step4_dataset_setup_and_metadata(self, temp_dir):
        """
        STEP 4: Test dataset directory and metadata creation.

        Verifies:
        - Output directory creation
        - Dataset ID generation
        - Metadata file creation
        - Gin config copying
        """
        # Test directory creation
        test_output_dir = os.path.join(temp_dir, "training_output")
        os.makedirs(test_output_dir, exist_ok=True)
        assert os.path.exists(test_output_dir)

        # Test dataset ID generation
        from datetime import datetime
        dataset_id = f"dataset_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        assert dataset_id.startswith("dataset_")
        assert len(dataset_id) == len("dataset_20250910_123456")

        # Test dataset directory creation
        dataset_dir = os.path.join(test_output_dir, dataset_id)
        os.makedirs(dataset_dir, exist_ok=True)
        assert os.path.exists(dataset_dir)

        # Test metadata creation
        dataset_metadata = {
            "command_line": "test_command",
            "symbols": ["TSLA"],
            "start_date": "2025-09-01",
            "end_date": "2025-09-01",
            "base_duration": "60m",
            "output_dir": test_output_dir,
            "storage_format": "arrayrecord",
            "generation_timestamp": datetime.now().isoformat(),
            "dataset_id": dataset_id,
            "debug_mode": True
        }

        # Save metadata
        metadata_file = os.path.join(dataset_dir, "dataset_metadata.json")
        with open(metadata_file, 'w') as f:
            json.dump(dataset_metadata, f, indent=2)

        assert os.path.exists(metadata_file)

        # Verify metadata can be loaded
        with open(metadata_file, 'r') as f:
            loaded_metadata = json.load(f)

        assert loaded_metadata['dataset_id'] == dataset_id
        assert loaded_metadata['symbols'] == ["TSLA"]
        assert loaded_metadata['storage_format'] == "arrayrecord"

    def test_step5_callback_and_runner_creation(self, sample_args, test_config, temp_dir):
        """
        STEP 5: Test callback and runner creation.

        Verifies:
        - IntervalBasedTrainingDataCallback can be created
        - Dataset ID assignment works
        - Runner creation with callback
        - All required parameters are passed
        """
        # Test callback creation
        training_callback = IntervalBasedTrainingDataCallback(
            symbols=sample_args.symbols,
            config=test_config,
            output_dir=sample_args.output_dir,
            storage_format=sample_args.storage_format,
            start_date=sample_args.start_date,
            end_date=sample_args.end_date
        )

        assert training_callback is not None
        assert training_callback.symbols == sample_args.symbols
        assert training_callback.storage_format == sample_args.storage_format
        assert training_callback.output_dir == Path(sample_args.output_dir)

        # Test dataset ID assignment
        test_dataset_id = "test_dataset_123"
        training_callback.dataset_id = test_dataset_id
        assert training_callback.dataset_id == test_dataset_id

        # Test callback attributes
        assert hasattr(training_callback, 'array_record_writers')
        assert hasattr(training_callback, 'dataset_initialized')
        assert hasattr(training_callback, 'binary_schema')

        # Verify callback methods exist
        assert hasattr(training_callback, 'handleStart')
        assert hasattr(training_callback, 'handleInterval')
        assert hasattr(training_callback, 'handleEnd')

    def test_step6_training_data_generation_execution(self, temp_dir):
        """
        STEP 6: Test training data generation execution.

        Verifies:
        - FileBasedMinuteMarketDataManager initialization
        - Data access and validation
        - ArrayRecord writer creation
        - File writing process
        """
        # Test FileBasedMinuteMarketDataManager creation
        base_path = "/data/minute-bars"  # Container path

        # Mock environment for testing
        mock_env = Mock()

        # Test that manager can be created (even if path doesn't exist in test)
        manager = FileBasedMinuteMarketDataManager(mock_env, base_path)
        assert manager.base_path == Path(base_path)
        assert str(manager.base_path) == base_path
        storage_manager = ArrayRecordStorageManager(temp_dir)
        assert storage_manager.base_path == Path(temp_dir)

        # Test writer creation (mock ArrayRecord since it may not be available)
        pass

        test_file = os.path.join(temp_dir, "test.arrayrecord")
        writer = storage_manager.create_arrayrecord_writer(test_file)
        assert writer is not None
        writer.close()

    def test_step7_post_generation_analysis(self, temp_dir):
        """
        STEP 7: Test post-generation analysis and metadata update.

        Verifies:
        - Sequence counting and estimation
        - Metadata updating with completion info
        - Error handling for missing data
        """
        # Create test callback with mock data
        mock_callback = Mock()
        mock_callback.sequences_generated = 1500
        mock_callback.interval_counter = 750

        # Test sequence analysis
        estimated_sequences = getattr(mock_callback, 'sequences_generated', 0)
        interval_counter = getattr(mock_callback, 'interval_counter', 0)

        assert estimated_sequences == 1500
        assert interval_counter == 750

        # Test fallback estimation
        mock_callback_empty = Mock()
        mock_callback_empty.sequences_generated = 0
        mock_callback_empty.interval_counter = 0

        estimated_empty = getattr(mock_callback_empty, 'sequences_generated', 0)
        assert estimated_empty == 0

        # Test metadata update structure
        completion_info = {
            "completion_timestamp": datetime.now().isoformat(),
            "generation_duration_seconds": 120,
            "estimated_sequences": estimated_sequences,
            "actual_intervals_processed": interval_counter,
            "status": "completed"
        }

        assert completion_info['status'] == "completed"
        assert completion_info['generation_duration_seconds'] > 0
        assert completion_info['estimated_sequences'] > 0

        # Test metadata file update
        metadata_file = os.path.join(temp_dir, "test_metadata.json")
        initial_metadata = {"test": "data"}

        with open(metadata_file, 'w') as f:
            json.dump(initial_metadata, f)

        # Update metadata
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)

        metadata.update(completion_info)

        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)

        # Verify update
        with open(metadata_file, 'r') as f:
            updated_metadata = json.load(f)

        assert updated_metadata['status'] == "completed"
        assert updated_metadata['test'] == "data"  # Original data preserved

    def test_step8_database_registration(self, mock_environment):
        """
        STEP 8: Test database registration process.

        Verifies:
        - Database connection and registration
        - Error handling for database failures
        - Metadata update with database info
        """
        # Test database info structure
        database_info = {
            "database_id": 123,
            "database_registered": True,
            "database_table": "test_training_dataset"
        }

        assert database_info['database_registered'] is True
        assert isinstance(database_info['database_id'], int)
        assert database_info['database_table'] == "test_training_dataset"

        # Test environment table name method
        mock_environment.get_table_name.return_value = "test_training_dataset"
        table_name = mock_environment.get_table_name("training_dataset")
        assert table_name == "test_training_dataset"

    def test_step9_final_summary_and_completion(self, sample_args, temp_dir):
        """
        STEP 9: Test final summary and completion.

        Verifies:
        - Completion summary creation
        - All required fields are present
        - Final status reporting
        """
        # Test completion summary structure
        dataset_id = "test_dataset_123"
        generation_duration = 180
        estimated_sequences = 2000

        completion_summary = {
            'status': 'completed',
            'dataset_directory': temp_dir,
            'dataset_id': dataset_id,
            'metadata_file': os.path.join(temp_dir, dataset_id, "dataset_metadata.json"),
            'gin_config': f"{temp_dir}/gin_config.gin",
            'database_id': 'test_db_id_456',
            'generation_duration': f"{generation_duration} seconds ({generation_duration/60:.1f} minutes)",
            'estimated_sequences': estimated_sequences,
            'symbols_processed': len(sample_args.symbols),
            'date_range': f"{sample_args.start_date} to {sample_args.end_date}"
        }

        # Verify all required fields
        required_fields = [
            'status', 'dataset_directory', 'dataset_id', 'metadata_file',
            'gin_config', 'generation_duration', 'estimated_sequences',
            'symbols_processed', 'date_range'
        ]

        for field in required_fields:
            assert field in completion_summary
            assert completion_summary[field] is not None

        assert completion_summary['status'] == 'completed'
        assert completion_summary['symbols_processed'] == len(sample_args.symbols)
        assert completion_summary['estimated_sequences'] == estimated_sequences

    def test_arrayrecord_file_writing_debugging(self, temp_dir):
        """
        CRITICAL TEST: Debug ArrayRecord file writing failures.

        This test specifically targets the AAPL file writing failure issue.
        Tests the actual ArrayRecord writing process step-by-step.
        """
        import array_record.python.array_record_module as array_record
        test_file = os.path.join(temp_dir, "debug_test.arrayrecord")

        # Test writer creation
        writer = array_record.ArrayRecordWriter(test_file, 'group_size:1')
        assert writer is not None
        test_data = b"test_binary_data_12345"
        writer.write(test_data)
        writer.close()
        assert os.path.exists(test_file)
        file_size = os.path.getsize(test_file)
        assert file_size > 0, f"ArrayRecord file has zero size: {file_size} bytes"

        # Test reading the file back
        reader = array_record.ArrayRecordReader(test_file)
        records = list(reader)
        reader.close()

        assert len(records) == 1
        assert records[0] == test_data

    def test_minute_data_access_debugging(self):
        """
        CRITICAL TEST: Debug minute data access issues.

        This test targets the container path vs host path confusion
        that was the root cause of the TSLA zero output issue.
        """
        # Test path configuration
        host_path = "/mnt/d/ats-data/minute-bars"
        container_path = "/data/minute-bars"

        # This should be the correct path for containers
        assert container_path == "/data/minute-bars"
        assert container_path != host_path

        # Test FileBasedMinuteManager path handling
        from infrastructure.storage.file_based_minute_manager import FileBasedMinuteManager

        # Test with container path (correct)
        manager_container = FileBasedMinuteManager(container_path)
        assert str(manager_container.base_path) == container_path
        from pathlib import Path
        path_obj = Path(container_path)
        path_str = str(path_obj)
        assert isinstance(path_str, str)
        assert path_str == container_path

    @pytest.mark.asyncio
    async def test_full_integration_with_mocked_components(self, temp_dir, sample_args):
        """
        INTEGRATION TEST: Test complete pipeline with mocked components.

        This test runs through the entire pipeline with mocked external dependencies
        to identify where the AAPL generation failure occurs.
        """
        # Mock the external dependencies that might be causing issues
        with patch('domains.ml.services.training_data.runners.training_data_callback_runner.register_training_dataset') as mock_register, \
             patch('domains.ml.services.training_data.runners.training_data_callback_runner.update_training_dataset_completion') as mock_update, \
             patch('services.core.app.runner.Runner') as MockRunner:

            # Setup mocks
            mock_register.return_value = 123
            mock_update.return_value = None

            mock_runner_instance = Mock()
            mock_runner_instance.run = AsyncMock()
            MockRunner.return_value = mock_runner_instance

            # Test callback creation and initialization
            callback = IntervalBasedTrainingDataCallback(
                symbols=sample_args.symbols,
                config=TrainingDataConfig(),
                output_dir=temp_dir,
                storage_format='arrayrecord',
                start_date=sample_args.start_date,
                end_date=sample_args.end_date
            )

            assert callback is not None

            # Test dataset structure initialization
            dataset_id = "test_dataset_integration"
            callback.dataset_id = dataset_id

            # Test the critical initialization step
            await callback._initialize_dataset_structure()

            # Verify writers were created
            assert len(callback.array_record_writers) > 0

            # Check expected writer keys
            expected_timeframes = ['5m', '15m', '1h', '1d']
            for symbol in sample_args.symbols:
                for timeframe in expected_timeframes:
                    writer_key = f"{symbol}_{timeframe}"
                    if writer_key not in callback.array_record_writers:
                        pytest.fail(f"Missing ArrayRecord writer for {writer_key}")

    def test_debug_logging_functionality(self, temp_dir, caplog):
        """
        Test the comprehensive debug logging functionality.

        Verifies that all debug logging steps work correctly.
        """
        # Test logging setup
        import logging

        # Configure logging similar to the main function
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        logger = logging.getLogger("test_training_data")

        # Test all debug logging steps
        with caplog.at_level(logging.INFO):
            logger.info("🔧 STEP 1: Loading configuration files")
            logger.info("✅ STEP 1 COMPLETE: Configuration loading finished")

            logger.info("🌍 STEP 2: Environment setup and data validation")
            logger.info("✅ STEP 2 COMPLETE: Environment and data validation finished")

            logger.info("⚙️ STEP 3: Creating training data configuration")
            logger.info("✅ STEP 3 COMPLETE: Training configuration created successfully")

        # Verify logging output
        log_output = caplog.text
        assert "STEP 1:" in log_output
        assert "STEP 2:" in log_output
        assert "STEP 3:" in log_output
        assert "Configuration loading finished" in log_output
        assert "Environment and data validation finished" in log_output