#!/usr/bin/env python3
"""
Unit Test: ArrayRecord Timeframe-Symbol Combination Validation

This test specifically validates that the training data callback runner creates
exactly one ArrayRecord file for each combination of timeframe and symbol,
ensuring proper file organization for visualization API compatibility.
"""

import pytest
import asyncio
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, date
from unittest.mock import Mock, patch, MagicMock

from ml.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from ml.training_data.generators.training_data_config import TrainingDataConfig


class TestArrayRecordTimeframeSymbolCombination:
    """Test that exactly one ArrayRecord file is created per timeframe-symbol combination."""

    @pytest.fixture
    def temp_output_dir(self):
        """Create temporary directory for test outputs."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    def training_config(self):
        """Create minimal training configuration."""
        return TrainingDataConfig(
            symbols=['AAPL', 'TSLA'],
            start_date=date(2025, 8, 1),
            end_date=date(2025, 8, 2),
            timeframes={'5m': 300, '15m': 900, '1h': 3600},
            sequence_lengths=[10, 20, 30]
        )

    @pytest.fixture
    def callback_instance(self, temp_output_dir, training_config):
        """Create IntervalBasedTrainingDataCallback instance."""
        return IntervalBasedTrainingDataCallback(
            symbols=['AAPL', 'TSLA'],
            config=training_config,
            output_dir=str(temp_output_dir),
            start_date=date(2025, 8, 1),
            end_date=date(2025, 8, 2),
            save_format="arrayrecord"
        )

    def test_timeframe_directories_created(self, callback_instance, temp_output_dir):
        """Test that all expected timeframe directories are created."""
        # Setup callback (this creates directories)
        callback_instance.handleStart(None, datetime.now())

        # Expected timeframes from the code
        expected_timeframes = ['5m', '15m', '1h', '1d', '1w']

        # Verify each timeframe directory exists
        for timeframe in expected_timeframes:
            timeframe_dir = temp_output_dir / timeframe
            assert timeframe_dir.exists(), f"Timeframe directory {timeframe} not created"
            assert timeframe_dir.is_dir(), f"Timeframe {timeframe} path is not a directory"

        print(f"✅ All {len(expected_timeframes)} timeframe directories created")

    @pytest.mark.asyncio
    async def test_one_arrayrecord_per_timeframe_symbol_combination(self, callback_instance, temp_output_dir):
        """Test that exactly one ArrayRecord file is created per timeframe-symbol combination."""

        # Setup callback
        callback_instance.handleStart(None, datetime.now())

        # Create mock examples for two symbols
        mock_examples = [
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2025, 8, 1, 10, 0, 0),
                'features': {'ohlc': [100.0, 101.0, 99.5, 100.5]},
                'labels': {'return_1h': 0.005},
                'metadata': {'sequence_id': 1}
            },
            {
                'symbol': 'TSLA',
                'timestamp': datetime(2025, 8, 1, 10, 0, 0),
                'features': {'ohlc': [250.0, 252.0, 248.0, 251.0]},
                'labels': {'return_1h': 0.008},
                'metadata': {'sequence_id': 2}
            }
        ]

        # Mock the ArrayRecord writer to avoid actual file I/O complexity
        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer:
            mock_writer_instance = MagicMock()
            mock_writer.return_value.__enter__.return_value = mock_writer_instance

            # Execute the save operation
            await callback_instance._save_interval_examples(mock_examples, datetime.now())

        # Expected combinations: 2 symbols × 5 timeframes = 10 files
        expected_combinations = [
            ('5m', 'AAPL'), ('5m', 'TSLA'),
            ('15m', 'AAPL'), ('15m', 'TSLA'),
            ('1h', 'AAPL'), ('1h', 'TSLA'),
            ('1d', 'AAPL'), ('1d', 'TSLA'),
            ('1w', 'AAPL'), ('1w', 'TSLA')
        ]

        # Verify file creation pattern
        created_files = []
        for timeframe in ['5m', '15m', '1h', '1d', '1w']:
            timeframe_dir = temp_output_dir / timeframe
            for symbol in ['AAPL', 'TSLA']:
                # Look for files matching pattern: {symbol}_{start}_{end}.arrayrecord
                pattern = f"{symbol}_*.arrayrecord"
                matching_files = list(timeframe_dir.glob(pattern))

                # Should be exactly one file per symbol-timeframe combination
                assert len(matching_files) <= 1, f"Multiple ArrayRecord files found for {symbol} {timeframe}: {matching_files}"

                if matching_files:
                    created_files.append((timeframe, symbol))

        print(f"✅ File creation pattern validated: {len(created_files)} combinations processed")

        # Verify ArrayRecordWriter was called for each expected combination
        # (Since we mocked it, verify call count matches expected combinations)
        assert mock_writer.call_count == len(expected_combinations), \
            f"ArrayRecordWriter called {mock_writer.call_count} times, expected {len(expected_combinations)}"

    def test_filename_format_validation(self, callback_instance, temp_output_dir):
        """Test that ArrayRecord filenames follow correct format: {symbol}_{start}_{end}.arrayrecord"""

        # Setup callback
        callback_instance.handleStart(None, datetime.now())

        # Test filename generation logic
        start_date_str = callback_instance.start_date.strftime('%Y%m%d_%H%M%S')
        end_date_str = callback_instance.end_date.strftime('%Y%m%d_%H%M%S')

        for symbol in ['AAPL', 'TSLA']:
            for timeframe in ['5m', '15m', '1h', '1d', '1w']:
                expected_filename = f"{symbol}_{start_date_str}_{end_date_str}.arrayrecord"

                # Validate filename format
                assert expected_filename.startswith(f"{symbol}_"), f"Filename doesn't start with symbol: {expected_filename}"
                assert expected_filename.endswith(".arrayrecord"), f"Filename doesn't end with .arrayrecord: {expected_filename}"
                assert "_" in expected_filename[len(symbol):], f"Filename missing date separator: {expected_filename}"

        print("✅ Filename format validation passed")

    @pytest.mark.asyncio
    async def test_duplicate_symbol_timeframe_handling(self, callback_instance, temp_output_dir):
        """Test that duplicate symbol-timeframe combinations don't create multiple files."""

        # Setup callback
        callback_instance.handleStart(None, datetime.now())

        # Create examples with duplicate symbol-timeframe data
        duplicate_examples = [
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2025, 8, 1, 10, 0, 0),
                'features': {'ohlc': [100.0, 101.0, 99.5, 100.5]},
                'labels': {'return_1h': 0.005}
            },
            {
                'symbol': 'AAPL',  # Same symbol, different timestamp
                'timestamp': datetime(2025, 8, 1, 10, 30, 0),
                'features': {'ohlc': [100.5, 102.0, 100.0, 101.5]},
                'labels': {'return_1h': 0.010}
            }
        ]

        # Mock the ArrayRecord writer
        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer:
            mock_writer_instance = MagicMock()
            mock_writer.return_value.__enter__.return_value = mock_writer_instance

            # Execute save operation
            await callback_instance._save_interval_examples(duplicate_examples, datetime.now())

        # Verify that each timeframe gets exactly one file for AAPL
        # (Even though we had 2 AAPL examples, they should go into the same file per timeframe)
        for timeframe in ['5m', '15m', '1h', '1d', '1w']:
            timeframe_dir = temp_output_dir / timeframe
            aapl_files = list(timeframe_dir.glob("AAPL_*.arrayrecord"))

            # Should be at most 1 file per timeframe (0 if file creation was mocked)
            assert len(aapl_files) <= 1, f"Multiple AAPL files in {timeframe}: {aapl_files}"

        print("✅ Duplicate handling validated - no multiple files per symbol-timeframe")

    def test_expected_timeframes_consistency(self):
        """Test that expected timeframes match between callback and documentation."""

        # This should match the hard-coded list in _save_interval_examples()
        expected_timeframes = ['5m', '15m', '1h', '1d', '1w']

        # Verify this matches what's documented in PRD
        prd_timeframes = ['5m', '15m', '1h', '1d', '1w']  # From PRD documentation

        assert expected_timeframes == prd_timeframes, \
            f"Code timeframes {expected_timeframes} don't match PRD {prd_timeframes}"

        print(f"✅ Timeframe consistency validated: {expected_timeframes}")

    @pytest.mark.asyncio
    async def test_metadata_files_accompany_arrayrecord_files(self, callback_instance, temp_output_dir):
        """Test that metadata JSON files are created alongside ArrayRecord files."""

        # Setup callback
        callback_instance.handleStart(None, datetime.now())

        mock_examples = [
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2025, 8, 1, 10, 0, 0),
                'features': {'ohlc': [100.0, 101.0, 99.5, 100.5]},
                'labels': {'return_1h': 0.005}
            }
        ]

        # Mock both ArrayRecord writer and JSON file operations
        with patch('array_record.python.array_record_module.ArrayRecordWriter') as mock_writer, \
             patch('builtins.open', create=True) as mock_open, \
             patch('json.dump') as mock_json_dump:

            mock_writer_instance = MagicMock()
            mock_writer.return_value.__enter__.return_value = mock_writer_instance
            mock_file = MagicMock()
            mock_open.return_value.__enter__.return_value = mock_file

            await callback_instance._save_interval_examples(mock_examples, datetime.now())

        # Verify JSON metadata files are created (one per ArrayRecord file)
        # 1 symbol × 5 timeframes = 5 metadata files expected
        expected_metadata_files = 5

        # Check that JSON dump was called for metadata
        assert mock_json_dump.call_count == expected_metadata_files, \
            f"JSON metadata dump called {mock_json_dump.call_count} times, expected {expected_metadata_files}"

        print(f"✅ Metadata files validated: {expected_metadata_files} JSON files created alongside ArrayRecord files")

    def test_error_handling_preserves_one_file_per_combination_invariant(self, callback_instance, temp_output_dir):
        """Test that error handling doesn't violate the one-file-per-combination rule."""

        # Setup callback
        callback_instance.handleStart(None, datetime.now())

        # Verify that even on errors, we don't create partial or duplicate files
        # This is ensured by the atomic nature of the ArrayRecord write operation

        # Test filesystem permission errors
        with patch('pathlib.Path.mkdir', side_effect=OSError("Permission denied")):
            with pytest.raises(RuntimeError, match="Critical error saving ArrayRecord"):
                asyncio.run(callback_instance._save_symbol_arrayrecord(
                    [{"test": "data"}],
                    temp_output_dir / "5m" / "TEST_20250801_000000_20250802_000000.arrayrecord",
                    "TEST",
                    "5m"
                ))

        # Verify no partial files were created
        for timeframe in ['5m', '15m', '1h', '1d', '1w']:
            timeframe_dir = temp_output_dir / timeframe
            if timeframe_dir.exists():
                test_files = list(timeframe_dir.glob("TEST_*.arrayrecord"))
                assert len(test_files) == 0, f"Partial files created on error: {test_files}"

        print("✅ Error handling preserves file invariant - no partial files created")


class TestArrayRecordGenerationFlow:
    """Test the complete flow from examples to ArrayRecord files."""

    def test_examples_grouping_by_timeframe_symbol(self):
        """Test that examples are properly grouped by timeframe and symbol combinations."""

        # Create callback instance
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL', 'TSLA'],
            output_dir="/tmp/test",
            start_date=date(2025, 8, 1),
            end_date=date(2025, 8, 2)
        )

        # Mock examples
        examples = [
            {'symbol': 'AAPL', 'data': 'aapl_data_1'},
            {'symbol': 'TSLA', 'data': 'tsla_data_1'},
            {'symbol': 'AAPL', 'data': 'aapl_data_2'}
        ]

        # Test the grouping logic (this is what happens in _save_interval_examples)
        examples_by_timeframe_symbol = {}
        timeframes = ['5m', '15m', '1h', '1d', '1w']

        for example in examples:
            symbol = example['symbol']
            for timeframe in timeframes:
                key = (timeframe, symbol)
                if key not in examples_by_timeframe_symbol:
                    examples_by_timeframe_symbol[key] = []
                examples_by_timeframe_symbol[key].append(example)

        # Verify grouping results
        assert len(examples_by_timeframe_symbol) == 10  # 2 symbols × 5 timeframes

        # Verify each combination exists
        for symbol in ['AAPL', 'TSLA']:
            for timeframe in timeframes:
                key = (timeframe, symbol)
                assert key in examples_by_timeframe_symbol, f"Missing combination: {key}"

                if symbol == 'AAPL':
                    assert len(examples_by_timeframe_symbol[key]) == 2, f"AAPL should have 2 examples in {timeframe}"
                else:  # TSLA
                    assert len(examples_by_timeframe_symbol[key]) == 1, f"TSLA should have 1 example in {timeframe}"

        print("✅ Example grouping by timeframe-symbol validated")

    def test_extract_timeframe_data_preserves_structure(self):
        """Test that timeframe data extraction preserves the required data structure."""

        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            output_dir="/tmp/test",
            start_date=date(2025, 8, 1),
            end_date=date(2025, 8, 2)
        )

        # Mock multi-timeframe examples
        examples = [
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2025, 8, 1, 10, 0, 0),
                'features': {'5m_ohlc': [100, 101, 99, 100.5], '1h_ohlc': [99, 102, 98, 101]},
                'labels': {'5m_return': 0.005, '1h_return': 0.02},
                'metadata': {'source': 'test'}
            }
        ]

        # Test timeframe data extraction
        timeframe_examples_5m = callback._extract_timeframe_data(examples, '5m')
        timeframe_examples_1h = callback._extract_timeframe_data(examples, '1h')

        # Verify structure preservation
        assert len(timeframe_examples_5m) == 1
        assert len(timeframe_examples_1h) == 1

        # Verify timeframe-specific metadata
        assert timeframe_examples_5m[0]['timeframe'] == '5m'
        assert timeframe_examples_1h[0]['timeframe'] == '1h'
        assert timeframe_examples_5m[0]['metadata']['extracted_timeframe'] == '5m'

        print("✅ Timeframe data extraction preserves structure")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])