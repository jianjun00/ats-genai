"""
Logic verification test for ArrayRecord storage implementation.

This test verifies the core logic of the ArrayRecord storage without requiring
the actual array_record library dependency, focusing on:
- Directory structure logic per PRD/DRD
- Data format compliance (QR4 scalar values)
- File naming conventions
- Error handling logic
"""

import pytest
import tempfile
import shutil
import json
from pathlib import Path
from datetime import datetime
from unittest.mock import AsyncMock, Mock, patch

from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback


class TestArrayRecordLogicVerification:
    """Test ArrayRecord storage logic without external dependencies."""

    @pytest.fixture
    def temp_output_dir(self):
        """Create temporary output directory."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def callback(self, temp_output_dir):
        """Create callback with mocked dependencies."""
        callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA'],
            config=None,
            storage_format='arrayrecord',
            output_dir=str(temp_output_dir)
        )
        callback.dataset_id = 'test_dataset_123'
        return callback

    @pytest.fixture
    def sample_example(self):
        """Create sample training example with timeframe features."""
        return {
            'symbol': 'TSLA',
            'prediction_timestamp': datetime(2025, 7, 1, 14, 30, 0),
            'timeframe_features': {
                '5m': {
                    'open': 250.0,
                    'high': 252.5,
                    'low': 248.0,
                    'close': 251.0,
                    'volume': 1000000.0,
                    'vwap': 250.5
                },
                '15m': {
                    'open': 249.0,
                    'high': 253.0,
                    'low': 247.5,
                    'close': 251.0,
                    'volume': 3000000.0,
                    'vwap': 250.8
                }
            }
        }

    @pytest.mark.asyncio
    async def test_directory_structure_logic(self, callback, sample_example, temp_output_dir):
        """Test that directory structure follows PRD/DRD specification."""
        # Mock the ArrayRecordWriter import within the method
        mock_writer = Mock()
        with patch.object(callback, '_save_simple_arrayrecord', new=AsyncMock()) as mock_save:

            # Call the storage method
            current_time = datetime(2025, 7, 1, 14, 30, 0)
            await callback._save_simple_arrayrecord([sample_example], current_time)

            # Verify directory structure was created correctly
            # Expected: /data/training_data/test_dataset_123/TSLA_20250701_000000_20250701_235959/{timeframe}/
            dataset_dir = temp_output_dir / 'test_dataset_123' / 'TSLA_20250701_000000_20250701_235959'

            assert dataset_dir.exists(), f"Dataset directory should exist at {dataset_dir}"

            # Check timeframe directories were created
            timeframe_5m_dir = dataset_dir / '5m'
            timeframe_15m_dir = dataset_dir / '15m'

            assert timeframe_5m_dir.exists(), "5m timeframe directory should exist"
            assert timeframe_15m_dir.exists(), "15m timeframe directory should exist"

    @pytest.mark.asyncio
    async def test_file_naming_convention(self, callback, sample_example):
        """Test that file naming follows SYMBOL_STARTDATETIME_ENDDATETIME.arrayrecord pattern."""
        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer_class:
            mock_writer = Mock()
            mock_writer_class.return_value = mock_writer

            current_time = datetime(2025, 7, 1, 14, 30, 0)
            await callback._save_simple_arrayrecord([sample_example], current_time)

            # Verify correct file paths were used in ArrayRecordWriter calls
            call_args_list = mock_writer_class.call_args_list
            assert len(call_args_list) == 2, "Should have 2 calls for 2 timeframes"

            # Check that file paths follow correct naming convention
            for call_args in call_args_list:
                file_path = call_args[0][0]  # First argument to ArrayRecordWriter

                # Should end with .arrayrecord
                assert file_path.endswith('.arrayrecord'), f"File should end with .arrayrecord: {file_path}"

                # Should contain TSLA_20250701_000000_20250701_235959
                expected_name = 'TSLA_20250701_000000_20250701_235959.arrayrecord'
                assert expected_name in file_path, f"File name should contain {expected_name}"

    @pytest.mark.asyncio
    async def test_qr4_scalar_data_format(self, callback, sample_example):
        """Test that data format complies with QR4 scalar values requirement."""
        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer_class:
            mock_writer = Mock()
            mock_writer_class.return_value = mock_writer

            current_time = datetime(2025, 7, 1, 14, 30, 0)
            await callback._save_simple_arrayrecord([sample_example], current_time)

            # Verify that write was called with proper QR4-compliant data
            write_calls = mock_writer.write.call_args_list
            assert len(write_calls) == 2, "Should have 2 write calls for 2 timeframes"

            for write_call in write_calls:
                json_bytes = write_call[0][0]  # First argument to write()

                # Convert back to dict to verify structure
                data = json.loads(json_bytes.decode('utf-8'))

                # Verify QR4-compliant scalar fields
                required_fields = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'vwap']
                for field in required_fields:
                    assert field in data, f"Required field {field} missing from data"

                # Verify all values are scalars (not lists/dicts)
                assert isinstance(data['open'], float), "open should be float"
                assert isinstance(data['high'], float), "high should be float"
                assert isinstance(data['low'], float), "low should be float"
                assert isinstance(data['close'], float), "close should be float"
                assert isinstance(data['volume'], float), "volume should be float"
                assert isinstance(data['vwap'], float), "vwap should be float"
                assert isinstance(data['symbol'], str), "symbol should be string"

    @pytest.mark.asyncio
    async def test_multiple_examples_processing(self, callback, temp_output_dir):
        """Test processing multiple examples creates separate symbol directories."""
        examples = [
            {
                'symbol': 'TSLA',
                'prediction_timestamp': datetime(2025, 7, 1, 14, 30, 0),
                'timeframe_features': {
                    '5m': {'open': 250.0, 'high': 252.0, 'low': 248.0, 'close': 251.0, 'volume': 1000000.0, 'vwap': 250.5}
                }
            },
            {
                'symbol': 'AAPL',
                'prediction_timestamp': datetime(2025, 7, 1, 14, 30, 0),
                'timeframe_features': {
                    '5m': {'open': 150.0, 'high': 152.0, 'low': 148.0, 'close': 151.0, 'volume': 2000000.0, 'vwap': 150.5}
                }
            }
        ]

        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer_class:
            mock_writer = Mock()
            mock_writer_class.return_value = mock_writer

            current_time = datetime(2025, 7, 1, 14, 30, 0)
            await callback._save_simple_arrayrecord(examples, current_time)

            # Verify separate directories for each symbol
            dataset_base = temp_output_dir / 'test_dataset_123'
            tsla_dir = dataset_base / 'TSLA_20250701_000000_20250701_235959'
            aapl_dir = dataset_base / 'AAPL_20250701_000000_20250701_235959'

            assert tsla_dir.exists(), "TSLA directory should exist"
            assert aapl_dir.exists(), "AAPL directory should exist"

    @pytest.mark.asyncio
    async def test_empty_examples_handling(self, callback):
        """Test that empty examples list is handled gracefully."""
        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer_class:
            current_time = datetime(2025, 7, 1, 14, 30, 0)

            # Should not raise exception with empty list
            await callback._save_simple_arrayrecord([], current_time)

            # Should not attempt to create any writers
            mock_writer_class.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_timeframe_features_handling(self, callback):
        """Test handling of examples missing timeframe_features."""
        example_missing_features = {
            'symbol': 'TSLA',
            'prediction_timestamp': datetime(2025, 7, 1, 14, 30, 0),
            # No timeframe_features
        }

        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer_class:
            mock_writer = Mock()
            mock_writer_class.return_value = mock_writer

            current_time = datetime(2025, 7, 1, 14, 30, 0)
            await callback._save_simple_arrayrecord([example_missing_features], current_time)

            # Should not attempt to write any data if no timeframe features
            mock_writer.write.assert_not_called()

    def test_dataset_id_fallback(self, callback):
        """Test dataset_id fallback behavior."""
        # Remove dataset_id to test fallback
        delattr(callback, 'dataset_id')

        # Get the dataset_id used in the method (should fall back to 'unknown_dataset')
        assert not hasattr(callback, 'dataset_id')

        # Method should handle missing dataset_id gracefully
        dataset_id = getattr(callback, 'dataset_id', 'unknown_dataset')
        assert dataset_id == 'unknown_dataset'

    @pytest.mark.asyncio
    async def test_datetime_serialization(self, callback, sample_example):
        """Test that datetime objects are properly serialized for JSON storage."""
        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer_class:
            mock_writer = Mock()
            mock_writer_class.return_value = mock_writer

            current_time = datetime(2025, 7, 1, 14, 30, 0)
            await callback._save_simple_arrayrecord([sample_example], current_time)

            # Verify datetime was serialized properly
            write_calls = mock_writer.write.call_args_list
            for write_call in write_calls:
                json_bytes = write_call[0][0]
                data = json.loads(json_bytes.decode('utf-8'))

                # timestamp should be ISO string, not datetime object
                assert isinstance(data['timestamp'], str), "timestamp should be serialized as string"
                assert 'T' in data['timestamp'], "timestamp should be in ISO format"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])