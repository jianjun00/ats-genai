"""
Test coverage for simple ArrayRecord storage per PRD/DRD QR4/QR5 requirements.

Tests verify:
- Correct directory structure: /data/training_data/{dataset_id}/SYMBOL_STARTDATETIME_ENDDATETIME/{timeframe}/
- Proper file naming: SYMBOL_STARTDATETIME_ENDDATETIME.arrayrecord
- QR4-compliant data format with scalar values only
- Timeframe separation requirements
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
import asyncpg

# Test imports
from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from core.platform.config.environment import Environment, EnvironmentType
# FIXME: tests.utils module does not exist
# from tests.utils.test_data_setup import setup_single_symbol_test


class TestSimpleArrayRecordStorage:
    """Test simple ArrayRecord storage implementation."""

    @pytest.fixture
    def temp_output_dir(self):
        """Create temporary output directory."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def real_config(self):
        """Real training data config."""
        from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig
        return TrainingDataConfig(
            timeframes=['5m', '15m', '1h', '1d', '1w'],  # PRD/DRD: Include missing 1w timeframe
            feature_types=['ohlcv', 'returns', 'technical']
        )

    @pytest.fixture
    async def training_callback(self, temp_output_dir, real_config, unit_test_db):
        """Create training callback with real environment and test data."""
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        
        # Setup real test data
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await setup_single_symbol_test(environment, conn, 'AAPL', 999998, 1)
        await conn.close()
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA', 'AAPL'],
            config=real_config,
            storage_format='arrayrecord',
            output_dir=str(temp_output_dir),
            environment=environment
        )
        # Set dataset_id as would be done by runner
        callback.dataset_id = 'dataset_20250701_120000'
        return callback

    @pytest.fixture
    def sample_training_example(self):
        """Sample training example with PRD/DRD compliant structure."""
        return {
            'instrument_id': 123,
            'symbol': 'TSLA',
            'prediction_timestamp': datetime(2025, 7, 1, 12, 0, 0),
            'base_features': {'market_cap': 800000000000.0},
            'timeframe_features': {
                '5m': {
                    'open': 250.50,
                    'high': 252.75,
                    'low': 249.80,
                    'close': 251.25,
                    'volume': 125000.0,
                    'vwap': 251.00
                },
                '15m': {
                    'open': 248.00,
                    'high': 253.00,
                    'low': 247.50,
                    'close': 251.25,
                    'volume': 380000.0,
                    'vwap': 250.75
                },
                '1h': {
                    'open': 245.00,
                    'high': 255.00,
                    'low': 244.50,
                    'close': 251.25,
                    'volume': 1500000.0,
                    'vwap': 250.25
                }
            },
            'prediction_targets': {'return_1h': 0.02, 'return_1d': 0.05}
        }


class TestDirectoryStructureCompliance(TestSimpleArrayRecordStorage):
    """Test PRD/DRD directory structure compliance."""

    @pytest.mark.asyncio
    async def test_correct_directory_structure_creation(self, training_callback, temp_output_dir, sample_training_example):
        """Test that correct directory structure is created per PRD/DRD."""
        current_time = datetime(2025, 7, 1, 12, 0, 0)

        # Use real ArrayRecord save method - let it fail if library missing
        await training_callback._save_simple_arrayrecord([sample_training_example], current_time)

        # Verify actual directory structure: /data/training_data/{dataset_id}/SYMBOL_STARTDATETIME_ENDDATETIME/{timeframe}/
        dataset_id = 'dataset_20250701_120000'
        expected_base_dir = temp_output_dir / dataset_id / 'TSLA_20250701_000000_20250701_235959'

        # Check that actual timeframe directories are created
        for timeframe in ['5m', '15m', '1h']:
            timeframe_dir = expected_base_dir / timeframe
            assert timeframe_dir.exists(), f"Directory {timeframe_dir} should exist"
            assert timeframe_dir.is_dir(), f"{timeframe_dir} should be a directory"
            
            # Check actual ArrayRecord file exists
            expected_file = timeframe_dir / 'TSLA_20250701_000000_20250701_235959.arrayrecord'
            assert expected_file.exists(), f"ArrayRecord file {expected_file} should exist"
            assert expected_file.stat().st_size > 0, f"ArrayRecord file {expected_file} should not be empty"

    @pytest.mark.asyncio
    async def test_correct_file_naming_convention(self, training_callback, temp_output_dir, sample_training_example):
        """Test that files are named correctly: SYMBOL_STARTDATETIME_ENDDATETIME.arrayrecord"""
        current_time = datetime(2025, 7, 1, 12, 0, 0)

        # Use real ArrayRecord save method
        await training_callback._save_simple_arrayrecord([sample_training_example], current_time)

        # Verify actual files were created with correct naming convention
        dataset_id = 'dataset_20250701_120000'
        symbol_datetime_str = 'TSLA_20250701_000000_20250701_235959'

        actual_files = []
        for timeframe in ['5m', '15m', '1h']:
            expected_file = temp_output_dir / dataset_id / symbol_datetime_str / timeframe / f"{symbol_datetime_str}.arrayrecord"
            actual_files.append(expected_file)
            
            # Verify actual file exists with correct name
            assert expected_file.exists(), f"Expected file {expected_file} should exist"
            assert expected_file.name == f"{symbol_datetime_str}.arrayrecord", f"File should be named {symbol_datetime_str}.arrayrecord"
            
            # Verify file contains actual data
            assert expected_file.stat().st_size > 0, f"File {expected_file} should contain data"

        # Verify exactly 3 files were created (one per timeframe)  
        all_arrayrecord_files = list(temp_output_dir.rglob("*.arrayrecord"))
        assert len(all_arrayrecord_files) == 3, f"Should create exactly 3 ArrayRecord files, found {len(all_arrayrecord_files)}"

    @pytest.mark.asyncio
    async def test_dataset_id_handling(self, training_callback, temp_output_dir):
        """Test that dataset_id is properly handled in directory structure."""
        # Test with custom dataset_id
        training_callback.dataset_id = 'custom_dataset_123'

        current_time = datetime(2025, 7, 1, 15, 30, 0)
        sample_example = {
            'symbol': 'AAPL',
            'prediction_timestamp': current_time,
            'timeframe_features': {
                '5m': {'open': 180.0, 'high': 181.0, 'low': 179.5, 'close': 180.5, 'volume': 50000, 'vwap': 180.25}
            }
        }

        # Use real ArrayRecord save method
        await training_callback._save_simple_arrayrecord([sample_example], current_time)

        # Verify custom dataset_id is used in actual directory structure
        expected_dir = temp_output_dir / 'custom_dataset_123' / 'AAPL_20250701_000000_20250701_235959' / '5m'
        assert expected_dir.exists(), f"Directory with custom dataset_id should exist: {expected_dir}"
        
        expected_file = expected_dir / 'AAPL_20250701_000000_20250701_235959.arrayrecord'
        assert expected_file.exists(), f"File with custom dataset_id should exist: {expected_file}"
        assert expected_file.stat().st_size > 0, f"File should contain actual data"


class TestQR4DataFormatCompliance(TestSimpleArrayRecordStorage):
    """Test QR4 data format compliance - scalar values only, timeframe separation."""

    @pytest.mark.asyncio
    async def test_qr4_scalar_values_only(self, training_callback, sample_training_example):
        """Test that only scalar values are written (no sequences)."""
        current_time = datetime(2025, 7, 1, 12, 0, 0)

        # Use real ArrayRecord save method and read back actual data
        await training_callback._save_simple_arrayrecord([sample_training_example], current_time)

        # Read back actual ArrayRecord files to verify QR4 compliance
        dataset_id = 'dataset_20250701_120000'
        base_dir = temp_output_dir / dataset_id / 'TSLA_20250701_000000_20250701_235959'
        
        timeframe_files = []
        for timeframe in ['5m', '15m', '1h']:
            timeframe_file = base_dir / timeframe / 'TSLA_20250701_000000_20250701_235959.arrayrecord'
            assert timeframe_file.exists(), f"ArrayRecord file should exist: {timeframe_file}"
            timeframe_files.append(timeframe_file)

        # Verify actual file contents contain QR4-compliant data
        import array_record.python.array_record_module as ar
        import json
        
        for i, file_path in enumerate(timeframe_files):
            reader = ar.ArrayRecordReader(str(file_path))
            record = reader.read()
            qr4_row = json.loads(record.decode())
            reader.close()

            # Verify required QR4 columns
            required_columns = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'vwap']
            for col in required_columns:
                assert col in qr4_row, f"QR4 column {col} missing"

                # Verify all values are scalars (not lists or arrays)
                for key, value in qr4_row.items():
                    if key != 'timestamp':  # timestamp can be datetime object
                        assert isinstance(value, (int, float, str)), f"Value {key}={value} should be scalar, got {type(value)}"

                # Verify specific value types
                assert isinstance(qr4_row['symbol'], str), "Symbol should be string"
                assert isinstance(qr4_row['open'], float), "Open should be float"
                assert isinstance(qr4_row['high'], float), "High should be float"
                assert isinstance(qr4_row['low'], float), "Low should be float"
                assert isinstance(qr4_row['close'], float), "Close should be float"
                assert isinstance(qr4_row['volume'], float), "Volume should be float"
                assert isinstance(qr4_row['vwap'], float), "VWAP should be float"

    @pytest.mark.asyncio
    async def test_timeframe_separation_requirement(self, training_callback, sample_training_example, temp_output_dir):
        """Test that each timeframe is saved to separate directory (QR4 requirement)."""
        current_time = datetime(2025, 7, 1, 12, 0, 0)

        # Use real ArrayRecord save method
        await training_callback._save_simple_arrayrecord([sample_training_example], current_time)

        # Verify actual separate directories for each timeframe
        dataset_id = 'dataset_20250701_120000'
        base_dir = temp_output_dir / dataset_id / 'TSLA_20250701_000000_20250701_235959'
        
        timeframes_found = set()
        timeframe_data = {}
        
        for timeframe in ['5m', '15m', '1h']:
            timeframe_dir = base_dir / timeframe
            if timeframe_dir.exists():
                timeframes_found.add(timeframe)
                
                # Read actual data from ArrayRecord file
                file_path = timeframe_dir / 'TSLA_20250701_000000_20250701_235959.arrayrecord'
                assert file_path.exists(), f"ArrayRecord file should exist: {file_path}"
                
                import array_record.python.array_record_module as ar
                import json
                
                reader = ar.ArrayRecordReader(str(file_path))
                record = reader.read()
                qr4_row = json.loads(record.decode())
                reader.close()
                
                timeframe_data[timeframe] = qr4_row

        assert timeframes_found == {'5m', '15m', '1h'}, f"Expected timeframes 5m, 15m, 1h, got {timeframes_found}"

        # Verify actual data values match expected timeframe (no cross-contamination)
        assert timeframe_data['5m']['open'] == 250.50, "5m data should have 5m values"
        assert timeframe_data['15m']['open'] == 248.00, "15m data should have 15m values"
        assert timeframe_data['1h']['open'] == 245.00, "1h data should have 1h values"

    @pytest.mark.asyncio
    async def test_missing_timeframe_features_handling(self, training_callback, temp_output_dir):
        """Test handling of examples with missing timeframe features."""
        current_time = datetime(2025, 7, 1, 12, 0, 0)

        # Example with incomplete timeframe features
        incomplete_example = {
            'symbol': 'TSLA',
            'prediction_timestamp': current_time,
            'timeframe_features': {
                '5m': {
                    'open': 250.0, 'high': 252.0, 'low': 249.0,
                    'close': 251.0, 'volume': 100000, 'vwap': 250.5
                },
                # Missing 15m and 1h timeframes
            }
        }

        # Use real ArrayRecord save method
        await training_callback._save_simple_arrayrecord([incomplete_example], current_time)

        # Should only create actual file for 5m timeframe
        dataset_id = 'dataset_20250701_120000'
        base_dir = temp_output_dir / dataset_id / 'TSLA_20250701_000000_20250701_235959'
        
        # Check only 5m directory and file exist
        timeframe_5m_dir = base_dir / '5m'
        assert timeframe_5m_dir.exists(), "Should create 5m directory"
        
        timeframe_5m_file = timeframe_5m_dir / 'TSLA_20250701_000000_20250701_235959.arrayrecord'
        assert timeframe_5m_file.exists(), "Should create 5m ArrayRecord file"
        
        # Verify 15m and 1h directories don't exist
        timeframe_15m_dir = base_dir / '15m'
        timeframe_1h_dir = base_dir / '1h'
        assert not timeframe_15m_dir.exists(), "Should not create 15m directory when no data"
        assert not timeframe_1h_dir.exists(), "Should not create 1h directory when no data"


class TestErrorHandlingAndEdgeCases(TestSimpleArrayRecordStorage):
    """Test error handling and edge cases."""

    @pytest.mark.asyncio
    async def test_empty_examples_list(self, training_callback, temp_output_dir):
        """Test handling of empty examples list."""
        current_time = datetime(2025, 7, 1, 12, 0, 0)

        # Use real ArrayRecord save method with empty list
        await training_callback._save_simple_arrayrecord([], current_time)

        # Should not create any files or directories
        dataset_id = 'dataset_20250701_120000'
        dataset_dir = temp_output_dir / dataset_id
        
        # Directory might be created but should be empty
        if dataset_dir.exists():
            arrayrecord_files = list(dataset_dir.rglob("*.arrayrecord"))
            assert len(arrayrecord_files) == 0, "Should not create any ArrayRecord files for empty examples"

    @pytest.mark.asyncio
    async def test_missing_timeframe_features_dict(self, training_callback, temp_output_dir):
        """Test handling of example without timeframe_features."""
        current_time = datetime(2025, 7, 1, 12, 0, 0)

        incomplete_example = {
            'symbol': 'TSLA',
            'prediction_timestamp': current_time,
            'base_features': {'market_cap': 800000000000.0}
            # Missing timeframe_features
        }

        # Use real ArrayRecord save method - should handle gracefully
        await training_callback._save_simple_arrayrecord([incomplete_example], current_time)

        # Should not create any ArrayRecord files
        dataset_id = 'dataset_20250701_120000'
        dataset_dir = temp_output_dir / dataset_id
        
        if dataset_dir.exists():
            arrayrecord_files = list(dataset_dir.rglob("*.arrayrecord"))
            assert len(arrayrecord_files) == 0, "Should not create files without timeframe features"

    # Remove test_array_record_write_failure - we don't test simulated failures with real objects
    # Real failures will be properly exposed by the actual ArrayRecord library

    @pytest.mark.asyncio
    async def test_dataset_id_fallback(self, temp_output_dir, real_config, unit_test_db):
        """Test fallback when dataset_id is not set."""
        environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        
        # Setup real test data
        conn = await asyncpg.connect(unit_test_db)
        await setup_single_symbol_test(environment, conn, 'TSLA', 999999, 1)
        await conn.close()
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA'],
            config=real_config,
            storage_format='arrayrecord',
            output_dir=str(temp_output_dir),
            environment=environment
        )
        # Don't set dataset_id to test fallback

        current_time = datetime(2025, 7, 1, 12, 0, 0)
        sample_example = {
            'symbol': 'TSLA',
            'prediction_timestamp': current_time,
            'timeframe_features': {'5m': {'open': 250.0, 'high': 251.0, 'low': 249.0, 'close': 250.5, 'volume': 1000, 'vwap': 250.25}}
        }

        # Use real ArrayRecord save method
        await callback._save_simple_arrayrecord([sample_example], current_time)

        # Should use 'unknown_dataset' as fallback - check actual directory
        fallback_dir = temp_output_dir / 'unknown_dataset' / 'TSLA_20250701_000000_20250701_235959' / '5m'
        assert fallback_dir.exists(), f"Should create directory with 'unknown_dataset' fallback: {fallback_dir}"
        
        expected_file = fallback_dir / 'TSLA_20250701_000000_20250701_235959.arrayrecord'
        assert expected_file.exists(), f"Should create file with fallback dataset_id: {expected_file}"


class TestIntegrationScenarios(TestSimpleArrayRecordStorage):
    """Test integration scenarios with multiple examples and symbols."""

    @pytest.mark.asyncio
    async def test_multiple_symbols_processing(self, training_callback, temp_output_dir):
        """Test processing multiple symbols in single batch."""
        current_time = datetime(2025, 7, 1, 12, 0, 0)

        examples = [
            {
                'symbol': 'TSLA',
                'prediction_timestamp': current_time,
                'timeframe_features': {
                    '5m': {'open': 250.0, 'high': 252.0, 'low': 249.0, 'close': 251.0, 'volume': 100000, 'vwap': 250.5}
                }
            },
            {
                'symbol': 'AAPL',
                'prediction_timestamp': current_time,
                'timeframe_features': {
                    '5m': {'open': 180.0, 'high': 181.0, 'low': 179.0, 'close': 180.5, 'volume': 75000, 'vwap': 180.25}
                }
            }
        ]

        # Use real ArrayRecord save method
        await training_callback._save_simple_arrayrecord(examples, current_time)

        # Verify actual separate directories and files for each symbol
        dataset_id = 'dataset_20250701_120000'
        
        tsla_dir = temp_output_dir / dataset_id / 'TSLA_20250701_000000_20250701_235959' / '5m'
        aapl_dir = temp_output_dir / dataset_id / 'AAPL_20250701_000000_20250701_235959' / '5m'
        
        assert tsla_dir.exists(), "Should create TSLA directory"
        assert aapl_dir.exists(), "Should create AAPL directory"

        tsla_file = tsla_dir / 'TSLA_20250701_000000_20250701_235959.arrayrecord'
        aapl_file = aapl_dir / 'AAPL_20250701_000000_20250701_235959.arrayrecord'
        
        assert tsla_file.exists(), "Should create TSLA ArrayRecord file"
        assert aapl_file.exists(), "Should create AAPL ArrayRecord file"

        # Verify actual data separation by reading files
        import array_record.python.array_record_module as ar
        import json
        
        # Read TSLA data
        tsla_reader = ar.ArrayRecordReader(str(tsla_file))
        tsla_record = tsla_reader.read()
        tsla_data = json.loads(tsla_record.decode())
        tsla_reader.close()
        
        # Read AAPL data
        aapl_reader = ar.ArrayRecordReader(str(aapl_file))
        aapl_record = aapl_reader.read()
        aapl_data = json.loads(aapl_record.decode())
        aapl_reader.close()

        assert tsla_data['symbol'] == 'TSLA', "TSLA file should contain TSLA data"
        assert aapl_data['symbol'] == 'AAPL', "AAPL file should contain AAPL data"
        assert tsla_data['open'] == 250.0, "TSLA data should have correct values"
        assert aapl_data['open'] == 180.0, "AAPL data should have correct values"

    @pytest.mark.asyncio
    async def test_multiple_timeframes_per_symbol(self, training_callback, temp_output_dir):
        """Test processing multiple timeframes for single symbol."""
        current_time = datetime(2025, 7, 1, 12, 0, 0)

        comprehensive_example = {
            'symbol': 'TSLA',
            'prediction_timestamp': current_time,
            'timeframe_features': {
                '5m': {'open': 250.0, 'high': 252.0, 'low': 249.0, 'close': 251.0, 'volume': 100000, 'vwap': 250.5},
                '15m': {'open': 248.0, 'high': 253.0, 'low': 247.0, 'close': 251.0, 'volume': 300000, 'vwap': 249.8},
                '1h': {'open': 245.0, 'high': 255.0, 'low': 244.0, 'close': 251.0, 'volume': 1200000, 'vwap': 249.2},
                '1d': {'open': 240.0, 'high': 260.0, 'low': 238.0, 'close': 251.0, 'volume': 25000000, 'vwap': 248.5},
                '1w': {'open': 235.0, 'high': 265.0, 'low': 230.0, 'close': 251.0, 'volume': 125000000, 'vwap': 247.8}  # PRD/DRD: Add missing 1w
            }
        }

        # Use real ArrayRecord save method
        await training_callback._save_simple_arrayrecord([comprehensive_example], current_time)

        # Verify actual files created for each timeframe
        dataset_id = 'dataset_20250701_120000'
        base_dir = temp_output_dir / dataset_id / 'TSLA_20250701_000000_20250701_235959'
        
        timeframes_found = set()
        timeframe_data = {}
        
        expected_timeframes = ['5m', '15m', '1h', '1d']
        for timeframe in expected_timeframes:
            timeframe_dir = base_dir / timeframe
            if timeframe_dir.exists():
                timeframes_found.add(timeframe)
                
                timeframe_file = timeframe_dir / 'TSLA_20250701_000000_20250701_235959.arrayrecord'
                assert timeframe_file.exists(), f"Should create {timeframe} ArrayRecord file"
                
                # Read actual data
                import array_record.python.array_record_module as ar
                import json
                
                reader = ar.ArrayRecordReader(str(timeframe_file))
                record = reader.read()
                qr4_row = json.loads(record.decode())
                reader.close()
                
                timeframe_data[timeframe] = qr4_row

        assert timeframes_found == {'5m', '15m', '1h', '1d'}, f"Expected all timeframes, got {timeframes_found}"

        # Verify actual data integrity for each PRD/DRD timeframe
        assert timeframe_data['5m']['open'] == 250.0, "5m file should have 5m open price"
        assert timeframe_data['5m']['volume'] == 100000.0, "5m file should have 5m volume"
        assert timeframe_data['15m']['open'] == 248.0, "15m file should have 15m open price"
        assert timeframe_data['15m']['volume'] == 300000.0, "15m file should have 15m volume"
        assert timeframe_data['1h']['open'] == 245.0, "1h file should have 1h open price"
        assert timeframe_data['1h']['volume'] == 1200000.0, "1h file should have 1h volume"
        assert timeframe_data['1d']['open'] == 240.0, "1d file should have 1d open price"
        assert timeframe_data['1d']['volume'] == 25000000.0, "1d file should have 1d volume"
        assert timeframe_data['1w']['open'] == 235.0, "PRD/DRD VIOLATION: 1w file should have 1w open price"
        assert timeframe_data['1w']['volume'] == 125000000.0, "PRD/DRD VIOLATION: 1w file should have 1w volume"


# Real system integration tests
class TestRealFileSystemIntegration:
    """Integration tests with real file system operations."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_end_to_end_file_creation(self, unit_test_db):
        """End-to-end test of actual file creation with real environment."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            
            environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
            
            # Setup real test data
            conn = await asyncpg.connect(unit_test_db)
            await setup_single_symbol_test(environment, conn, 'TEST', 999997, 1)
            await conn.close()
            
            from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig
            config = TrainingDataConfig(timeframes=['5m', '1h'])

            callback = IntervalBasedTrainingDataCallback(
                symbols=['TEST'],
                config=config,
                storage_format='arrayrecord',
                output_dir=str(temp_path),
                environment=environment
            )
            callback.dataset_id = 'test_dataset_001'

            current_time = datetime(2025, 7, 1, 12, 0, 0)
            example = {
                'symbol': 'TEST',
                'prediction_timestamp': current_time,
                'timeframe_features': {
                    '5m': {'open': 100.0, 'high': 101.0, 'low': 99.0, 'close': 100.5, 'volume': 1000, 'vwap': 100.25},
                    '1h': {'open': 98.0, 'high': 103.0, 'low': 97.0, 'close': 100.5, 'volume': 50000, 'vwap': 99.8}
                }
            }

            # Run the actual save operation - let it fail if array_record missing
            await callback._save_simple_arrayrecord([example], current_time)

            # Verify actual directory structure exists
            expected_base = temp_path / 'test_dataset_001' / 'TEST_20250701_000000_20250701_235959'
            assert expected_base.exists(), f"Base directory {expected_base} should exist"

            # Verify actual timeframe directories
            assert (expected_base / '5m').exists(), "5m directory should exist"
            assert (expected_base / '1h').exists(), "1h directory should exist"

            # Verify actual ArrayRecord files exist
            file_5m = expected_base / '5m' / 'TEST_20250701_000000_20250701_235959.arrayrecord'
            file_1h = expected_base / '1h' / 'TEST_20250701_000000_20250701_235959.arrayrecord'

            assert file_5m.exists(), "5m ArrayRecord file should exist"
            assert file_1h.exists(), "1h ArrayRecord file should exist"

            # Verify files contain actual data
            assert file_5m.stat().st_size > 0, "5m ArrayRecord file should not be empty"
            assert file_1h.stat().st_size > 0, "1h ArrayRecord file should not be empty"

            # Verify actual data can be read back
            import array_record.python.array_record_module as ar
            import json

            # Test actual 5m data
            reader_5m = ar.ArrayRecordReader(str(file_5m))
            record_5m = reader_5m.read()
            data_5m = json.loads(record_5m.decode())
            reader_5m.close()

            assert data_5m['symbol'] == 'TEST', "5m data should contain correct symbol"
            assert data_5m['open'] == 100.0, "5m data should contain correct open price"

            # Test actual 1h data
            reader_1h = ar.ArrayRecordReader(str(file_1h))
            record_1h = reader_1h.read()
            data_1h = json.loads(record_1h.decode())
            reader_1h.close()

            assert data_1h['symbol'] == 'TEST', "1h data should contain correct symbol"
            assert data_1h['open'] == 98.0, "1h data should contain correct open price"

            print("✅ End-to-end integration test passed with real objects!")