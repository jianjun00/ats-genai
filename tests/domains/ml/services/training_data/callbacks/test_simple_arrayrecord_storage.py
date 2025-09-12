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
from datetime import datetime, date
from unittest.mock import AsyncMock, Mock, patch
import json

# Test imports
from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback


class TestSimpleArrayRecordStorage:
    """Test simple ArrayRecord storage implementation."""

    @pytest.fixture
    def temp_output_dir(self):
        """Create temporary output directory."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def mock_config(self):
        """Mock training data config."""
        config = Mock()
        config.timeframes = ['5m', '15m', '1h', '1d']
        config.feature_types = ['ohlcv', 'returns', 'technical']
        return config

    @pytest.fixture
    def training_callback(self, temp_output_dir, mock_config):
        """Create training callback with temporary directory."""
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA', 'AAPL'],
            config=mock_config,
            storage_format='arrayrecord',
            output_dir=str(temp_output_dir)
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

        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer:
            mock_writer.return_value.__enter__.return_value.write = Mock()

            await training_callback._save_simple_arrayrecord([sample_training_example], current_time)

            # Verify directory structure: /data/training_data/{dataset_id}/SYMBOL_STARTDATETIME_ENDDATETIME/{timeframe}/
            dataset_id = 'dataset_20250701_120000'
            expected_base_dir = temp_output_dir / dataset_id / 'TSLA_20250701_000000_20250701_235959'

            # Check that timeframe directories are created
            for timeframe in ['5m', '15m', '1h']:
                timeframe_dir = expected_base_dir / timeframe
                assert timeframe_dir.exists(), f"Directory {timeframe_dir} should exist"
                assert timeframe_dir.is_dir(), f"{timeframe_dir} should be a directory"

    @pytest.mark.asyncio
    async def test_correct_file_naming_convention(self, training_callback, temp_output_dir, sample_training_example):
        """Test that files are named correctly: SYMBOL_STARTDATETIME_ENDDATETIME.arrayrecord"""
        current_time = datetime(2025, 7, 1, 12, 0, 0)

        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer:
            mock_context = mock_writer.return_value.__enter__.return_value
            mock_context.write = Mock()

            await training_callback._save_simple_arrayrecord([sample_training_example], current_time)

            # Verify ArrayRecordWriter was called with correct file paths
            expected_calls = []
            dataset_id = 'dataset_20250701_120000'
            symbol_datetime_str = 'TSLA_20250701_000000_20250701_235959'

            for timeframe in ['5m', '15m', '1h']:
                expected_file = temp_output_dir / dataset_id / symbol_datetime_str / timeframe / f"{symbol_datetime_str}.arrayrecord"
                expected_calls.append(expected_file)

            # Check that ArrayRecordWriter was called for each timeframe
            assert mock_writer.call_count == 3, "ArrayRecordWriter should be called for each timeframe"

            # Verify file paths in calls
            called_paths = [str(call.args[0]) for call in mock_writer.call_args_list]
            for expected_file in expected_calls:
                assert str(expected_file) in called_paths, f"Expected file path {expected_file} not found"

    def test_dataset_id_handling(self, training_callback, temp_output_dir):
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

        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer:
            import asyncio
            asyncio.run(training_callback._save_simple_arrayrecord([sample_example], current_time))

            # Verify custom dataset_id is used in path
            called_path = mock_writer.call_args_list[0].args[0]
            assert 'custom_dataset_123' in called_path, "Custom dataset_id should be in file path"
            assert 'AAPL_20250701_000000_20250701_235959' in called_path, "Symbol and datetime should be in file path"


class TestQR4DataFormatCompliance(TestSimpleArrayRecordStorage):
    """Test QR4 data format compliance - scalar values only, timeframe separation."""

    @pytest.mark.asyncio
    async def test_qr4_scalar_values_only(self, training_callback, sample_training_example):
        """Test that only scalar values are written (no sequences)."""
        current_time = datetime(2025, 7, 1, 12, 0, 0)

        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer:
            mock_context = mock_writer.return_value.__enter__.return_value
            mock_context.write = Mock()

            await training_callback._save_simple_arrayrecord([sample_training_example], current_time)

            # Check that write was called with QR4-compliant data
            write_calls = mock_context.write.call_args_list
            assert len(write_calls) == 3, "Should have 3 write calls for 3 timeframes"

            # Verify data structure for each call
            for call in write_calls:
                qr4_row = call.args[0]

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
    async def test_timeframe_separation_requirement(self, training_callback, sample_training_example):
        """Test that each timeframe is saved to separate directory (QR4 requirement)."""
        current_time = datetime(2025, 7, 1, 12, 0, 0)

        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer:
            mock_context = mock_writer.return_value.__enter__.return_value
            mock_context.write = Mock()

            await training_callback._save_simple_arrayrecord([sample_training_example], current_time)

            # Verify separate calls for each timeframe
            called_paths = [call.args[0] for call in mock_writer.call_args_list]

            timeframes_found = set()
            for path in called_paths:
                # Extract timeframe from path
                if '/5m/' in path:
                    timeframes_found.add('5m')
                elif '/15m/' in path:
                    timeframes_found.add('15m')
                elif '/1h/' in path:
                    timeframes_found.add('1h')

            assert timeframes_found == {'5m', '15m', '1h'}, f"Expected timeframes 5m, 15m, 1h, got {timeframes_found}"

            # Verify no cross-timeframe contamination
            write_calls = mock_context.write.call_args_list
            for i, call in enumerate(write_calls):
                qr4_row = call.args[0]
                path = called_paths[i]

                # Verify data values match expected timeframe
                if '/5m/' in path:
                    assert qr4_row['open'] == 250.50, "5m data should have 5m values"
                elif '/15m/' in path:
                    assert qr4_row['open'] == 248.00, "15m data should have 15m values"
                elif '/1h/' in path:
                    assert qr4_row['open'] == 245.00, "1h data should have 1h values"

    @pytest.mark.asyncio
    async def test_missing_timeframe_features_handling(self, training_callback):
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

        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer:
            mock_context = mock_writer.return_value.__enter__.return_value
            mock_context.write = Mock()

            await training_callback._save_simple_arrayrecord([incomplete_example], current_time)

            # Should only create file for 5m timeframe
            assert mock_writer.call_count == 1, "Should only call ArrayRecordWriter for available timeframe"

            called_path = mock_writer.call_args_list[0].args[0]
            assert '/5m/' in called_path, "Should create 5m directory"


class TestErrorHandlingAndEdgeCases(TestSimpleArrayRecordStorage):
    """Test error handling and edge cases."""

    @pytest.mark.asyncio
    async def test_empty_examples_list(self, training_callback):
        """Test handling of empty examples list."""
        current_time = datetime(2025, 7, 1, 12, 0, 0)

        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer:
            await training_callback._save_simple_arrayrecord([], current_time)

            # Should not attempt to write anything
            assert mock_writer.call_count == 0, "Should not call ArrayRecordWriter for empty examples"

    @pytest.mark.asyncio
    async def test_missing_timeframe_features_dict(self, training_callback):
        """Test handling of example without timeframe_features."""
        current_time = datetime(2025, 7, 1, 12, 0, 0)

        incomplete_example = {
            'symbol': 'TSLA',
            'prediction_timestamp': current_time,
            'base_features': {'market_cap': 800000000000.0}
            # Missing timeframe_features
        }

        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer:
            # Should not raise exception
            await training_callback._save_simple_arrayrecord([incomplete_example], current_time)

            # Should not create any files
            assert mock_writer.call_count == 0, "Should not create files without timeframe features"

    @pytest.mark.asyncio
    async def test_array_record_write_failure(self, training_callback, sample_training_example, temp_output_dir):
        """Test handling of ArrayRecord write failures."""
        current_time = datetime(2025, 7, 1, 12, 0, 0)

        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer:
            # Simulate write failure
            mock_context = mock_writer.return_value.__enter__.return_value
            mock_context.write.side_effect = Exception("Write failed")

            # Should not raise exception (should be caught and logged)
            await training_callback._save_simple_arrayrecord([sample_training_example], current_time)

            # Verify directories were created even though write failed
            dataset_id = 'dataset_20250701_120000'
            expected_dir = temp_output_dir / dataset_id / 'TSLA_20250701_000000_20250701_235959' / '5m'
            assert expected_dir.exists(), "Directories should be created even if write fails"

    def test_dataset_id_fallback(self, temp_output_dir, mock_config):
        """Test fallback when dataset_id is not set."""
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA'],
            config=mock_config,
            storage_format='arrayrecord',
            output_dir=str(temp_output_dir)
        )
        # Don't set dataset_id to test fallback

        current_time = datetime(2025, 7, 1, 12, 0, 0)
        sample_example = {
            'symbol': 'TSLA',
            'prediction_timestamp': current_time,
            'timeframe_features': {'5m': {'open': 250.0, 'high': 251.0, 'low': 249.0, 'close': 250.5, 'volume': 1000, 'vwap': 250.25}}
        }

        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer:
            import asyncio
            asyncio.run(callback._save_simple_arrayrecord([sample_example], current_time))

            # Should use 'unknown_dataset' as fallback
            called_path = mock_writer.call_args_list[0].args[0]
            assert 'unknown_dataset' in called_path, "Should use 'unknown_dataset' as fallback"


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

        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer:
            mock_context = mock_writer.return_value.__enter__.return_value
            mock_context.write = Mock()

            await training_callback._save_simple_arrayrecord(examples, current_time)

            # Should create files for both symbols
            assert mock_writer.call_count == 2, "Should call ArrayRecordWriter for each symbol"

            # Verify separate directories for each symbol
            called_paths = [call.args[0] for call in mock_writer.call_args_list]

            tsla_paths = [path for path in called_paths if 'TSLA_' in path]
            aapl_paths = [path for path in called_paths if 'AAPL_' in path]

            assert len(tsla_paths) == 1, "Should have 1 TSLA file"
            assert len(aapl_paths) == 1, "Should have 1 AAPL file"

            # Verify data separation
            write_calls = mock_context.write.call_args_list
            tsla_data = next(call.args[0] for call, path in zip(write_calls, called_paths) if 'TSLA_' in path)
            aapl_data = next(call.args[0] for call, path in zip(write_calls, called_paths) if 'AAPL_' in path)

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
                '1d': {'open': 240.0, 'high': 260.0, 'low': 238.0, 'close': 251.0, 'volume': 25000000, 'vwap': 248.5}
            }
        }

        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer:
            mock_context = mock_writer.return_value.__enter__.return_value
            mock_context.write = Mock()

            await training_callback._save_simple_arrayrecord([comprehensive_example], current_time)

            # Should create 4 files (one per timeframe)
            assert mock_writer.call_count == 4, "Should call ArrayRecordWriter for each timeframe"

            # Verify correct timeframe directories
            called_paths = [call.args[0] for call in mock_writer.call_args_list]

            timeframes_found = set()
            for path in called_paths:
                if '/5m/' in path: timeframes_found.add('5m')
                elif '/15m/' in path: timeframes_found.add('15m')
                elif '/1h/' in path: timeframes_found.add('1h')
                elif '/1d/' in path: timeframes_found.add('1d')

            assert timeframes_found == {'5m', '15m', '1h', '1d'}, f"Expected all timeframes, got {timeframes_found}"

            # Verify data integrity for each timeframe
            write_calls = mock_context.write.call_args_list
            for call, path in zip(write_calls, called_paths):
                qr4_row = call.args[0]

                if '/5m/' in path:
                    assert qr4_row['open'] == 250.0, "5m file should have 5m open price"
                    assert qr4_row['volume'] == 100000.0, "5m file should have 5m volume"
                elif '/15m/' in path:
                    assert qr4_row['open'] == 248.0, "15m file should have 15m open price"
                    assert qr4_row['volume'] == 300000.0, "15m file should have 15m volume"
                elif '/1h/' in path:
                    assert qr4_row['open'] == 245.0, "1h file should have 1h open price"
                    assert qr4_row['volume'] == 1200000.0, "1h file should have 1h volume"
                elif '/1d/' in path:
                    assert qr4_row['open'] == 240.0, "1d file should have 1d open price"
                    assert qr4_row['volume'] == 25000000.0, "1d file should have 1d volume"


# Integration test with real file system
class TestRealFileSystemIntegration:
    """Integration tests with real file system operations."""

    @pytest.mark.integration
    def test_end_to_end_file_creation(self):
        """End-to-end test of actual file creation (requires array_record)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            callback = IntervalBasedTrainingDataCallback(
                symbols=['TEST'],
                config=Mock(timeframes=['5m', '1h']),
                storage_format='arrayrecord',
                output_dir=str(temp_path)
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

            # Run the actual save operation
            import asyncio
            try:
                asyncio.run(callback._save_simple_arrayrecord([example], current_time))

                # Verify directory structure exists
                expected_base = temp_path / 'test_dataset_001' / 'TEST_20250701_000000_20250701_235959'
                assert expected_base.exists(), f"Base directory {expected_base} should exist"

                # Verify timeframe directories
                assert (expected_base / '5m').exists(), "5m directory should exist"
                assert (expected_base / '1h').exists(), "1h directory should exist"

                # Verify ArrayRecord files exist
                file_5m = expected_base / '5m' / 'TEST_20250701_000000_20250701_235959.arrayrecord'
                file_1h = expected_base / '1h' / 'TEST_20250701_000000_20250701_235959.arrayrecord'

                assert file_5m.exists(), "5m ArrayRecord file should exist"
                assert file_1h.exists(), "1h ArrayRecord file should exist"

                # Verify files are not empty
                assert file_5m.stat().st_size > 0, "5m ArrayRecord file should not be empty"
                assert file_1h.stat().st_size > 0, "1h ArrayRecord file should not be empty"

                # Verify data can be read back
                import array_record.python.array_record_module as ar
                import json

                # Test 5m data
                reader_5m = ar.ArrayRecordReader(str(file_5m))
                record_5m = reader_5m.read()
                data_5m = json.loads(record_5m.decode())
                reader_5m.close()

                assert data_5m['symbol'] == 'TEST', "5m data should contain correct symbol"
                assert data_5m['open'] == 100.0, "5m data should contain correct open price"

                # Test 1h data
                reader_1h = ar.ArrayRecordReader(str(file_1h))
                record_1h = reader_1h.read()
                data_1h = json.loads(record_1h.decode())
                reader_1h.close()

                assert data_1h['symbol'] == 'TEST', "1h data should contain correct symbol"
                assert data_1h['open'] == 98.0, "1h data should contain correct open price"

                print("✅ End-to-end integration test passed!")

            except ImportError:
                pytest.skip("array_record not available for integration test")
            except Exception as e:
                pytest.fail(f"End-to-end test failed: {e}")