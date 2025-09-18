#!/usr/bin/env python3
"""
Integration Test: ArrayRecord File Creation and Location Validation

Tests that ArrayRecord files are actually created in the correct timeframe/symbol structure
by the training data callback runner.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, date
from unittest.mock import patch, MagicMock

# Import the actual callback we're testing
from domains.ml.legacy.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback


class TestArrayRecordFileCreationLocations:
    """Test actual ArrayRecord file creation in correct locations."""

    @pytest.fixture
    def temp_output_dir(self):
        """Create temporary directory for test outputs."""
        temp_dir = tempfile.mkdtemp(prefix="arrayrecord_location_test_")
        yield Path(temp_dir)
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    def callback_instance(self, temp_output_dir):
        """Create callback instance with real parameters."""
        return IntervalBasedTrainingDataCallback(
            symbols=['AAPL', 'TSLA'],
            output_dir=str(temp_output_dir),
            start_date=date(2025, 8, 1),
            end_date=date(2025, 8, 2),
            save_format="arrayrecord"
        )

    @pytest.mark.asyncio
    async def test_arrayrecord_files_created_in_correct_locations(self, callback_instance, temp_output_dir):
        """Test that ArrayRecord files are actually created in timeframe subdirectories."""

        # Setup callback to create directory structure
        callback_instance.handleStart(None, datetime.now())

        # Verify timeframe directories were created
        expected_timeframes = ['5m', '15m', '1h', '1d', '1w']
        for timeframe in expected_timeframes:
            timeframe_dir = temp_output_dir / timeframe
            assert timeframe_dir.exists(), f"Timeframe directory {timeframe} not created"
            assert timeframe_dir.is_dir(), f"{timeframe} is not a directory"

        # Create mock examples for testing
        mock_examples = [
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2025, 8, 1, 10, 0, 0),
                'features': {'ohlc': [100.0, 101.0, 99.5, 100.5]},
                'labels': {'return_1h': 0.005},
                'metadata': {'test': True}
            },
            {
                'symbol': 'TSLA',
                'timestamp': datetime(2025, 8, 1, 10, 0, 0),
                'features': {'ohlc': [250.0, 252.0, 248.0, 251.0]},
                'labels': {'return_1h': 0.008},
                'metadata': {'test': True}
            }
        ]

        # Mock ArrayRecordWriter to create actual files without complex binary writing
        files_created = []

        def mock_arrayrecord_writer(file_path, *args, **kwargs):
            # Create the actual file
            Path(file_path).touch()
            files_created.append(Path(file_path))

            # Return mock context manager
            mock_writer = MagicMock()
            mock_writer.__enter__ = MagicMock(return_value=mock_writer)
            mock_writer.__exit__ = MagicMock(return_value=None)
            return mock_writer

        # Mock the ArrayRecord import and file creation
        with patch('array_record.python.array_record_module.ArrayRecordWriter', side_effect=mock_arrayrecord_writer):
            # Execute the actual file creation logic
            await callback_instance._save_interval_examples(mock_examples, datetime.now())

        # Verify files were created
        assert len(files_created) > 0, "No ArrayRecord files were created"

        # Expected: 2 symbols × 5 timeframes = 10 files
        expected_file_count = 2 * 5
        assert len(files_created) == expected_file_count, \
            f"Expected {expected_file_count} files, got {len(files_created)}"

        # Verify file locations match timeframe/symbol structure
        files_by_location = {}
        for file_path in files_created:
            # Extract timeframe from parent directory
            timeframe = file_path.parent.name
            # Extract symbol from filename
            symbol = file_path.name.split('_')[0]

            key = (timeframe, symbol)
            if key not in files_by_location:
                files_by_location[key] = []
            files_by_location[key].append(file_path)

        # Verify each timeframe-symbol combination has exactly one file
        expected_combinations = set()
        for timeframe in expected_timeframes:
            for symbol in ['AAPL', 'TSLA']:
                expected_combinations.add((timeframe, symbol))

        actual_combinations = set(files_by_location.keys())

        assert actual_combinations == expected_combinations, \
            f"File combinations don't match. Expected: {expected_combinations}, Got: {actual_combinations}"

        # Verify exactly one file per combination
        for (timeframe, symbol), file_list in files_by_location.items():
            assert len(file_list) == 1, \
                f"Expected 1 file for {timeframe}/{symbol}, got {len(file_list)}: {file_list}"

        print(f"✅ Created {len(files_created)} ArrayRecord files in correct locations")
        for file_path in sorted(files_created):
            relative_path = file_path.relative_to(temp_output_dir)
            print(f"   {relative_path}")

    @pytest.mark.asyncio
    async def test_arrayrecord_filename_format(self, callback_instance, temp_output_dir):
        """Test that ArrayRecord files use correct naming format."""

        callback_instance.handleStart(None, datetime.now())

        mock_examples = [
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2025, 8, 1, 10, 0, 0),
                'features': {'test': 'data'},
                'labels': {'test': 0.1}
            }
        ]

        files_created = []

        def mock_arrayrecord_writer(file_path, *args, **kwargs):
            Path(file_path).touch()
            files_created.append(Path(file_path))
            mock_writer = MagicMock()
            mock_writer.__enter__ = MagicMock(return_value=mock_writer)
            mock_writer.__exit__ = MagicMock(return_value=None)
            return mock_writer

        with patch('array_record.python.array_record_module.ArrayRecordWriter', side_effect=mock_arrayrecord_writer):
            await callback_instance._save_interval_examples(mock_examples, datetime.now())

        # Verify filename format for each created file
        for file_path in files_created:
            filename = file_path.name

            # Should be: {SYMBOL}_{YYYYMMDD_HHMMSS}_{YYYYMMDD_HHMMSS}.arrayrecord
            assert filename.endswith('.arrayrecord'), f"File doesn't end with .arrayrecord: {filename}"

            # Extract parts
            name_without_ext = filename.replace('.arrayrecord', '')
            parts = name_without_ext.split('_')

            assert len(parts) >= 5, f"Filename format incorrect: {filename} (expected SYMBOL_YYYYMMDD_HHMMSS_YYYYMMDD_HHMMSS.arrayrecord)"

            symbol = parts[0]
            assert symbol in ['AAPL', 'TSLA'], f"Unexpected symbol in filename: {symbol}"

            # Verify date format (YYYYMMDD)
            start_date_part = parts[1]
            assert len(start_date_part) == 8, f"Invalid date format in filename: {start_date_part}"

            print(f"✅ Filename format valid: {filename}")

    @pytest.mark.asyncio
    async def test_metadata_json_files_created_alongside_arrayrecord(self, callback_instance, temp_output_dir):
        """Test that metadata JSON files are created in same location as ArrayRecord files."""

        callback_instance.handleStart(None, datetime.now())

        mock_examples = [
            {
                'symbol': 'AAPL',
                'timestamp': datetime(2025, 8, 1, 10, 0, 0),
                'features': {'test': 'data'},
                'labels': {'test': 0.1}
            }
        ]

        arrayrecord_files_created = []
        json_files_created = []

        def mock_arrayrecord_writer(file_path, *args, **kwargs):
            Path(file_path).touch()
            arrayrecord_files_created.append(Path(file_path))
            mock_writer = MagicMock()
            mock_writer.__enter__ = MagicMock(return_value=mock_writer)
            mock_writer.__exit__ = MagicMock(return_value=None)
            return mock_writer

        def mock_open(file_path, mode='r', **kwargs):
            if 'w' in mode and str(file_path).endswith('.json'):
                Path(file_path).touch()
                json_files_created.append(Path(file_path))
            mock_file = MagicMock()
            mock_file.__enter__ = MagicMock(return_value=mock_file)
            mock_file.__exit__ = MagicMock(return_value=None)
            return mock_file

        with patch('array_record.python.array_record_module.ArrayRecordWriter', side_effect=mock_arrayrecord_writer), \
             patch('builtins.open', side_effect=mock_open), \
             patch('json.dump'):

            await callback_instance._save_interval_examples(mock_examples, datetime.now())

        # Verify equal number of ArrayRecord and JSON files
        assert len(arrayrecord_files_created) == len(json_files_created), \
            f"Mismatch: {len(arrayrecord_files_created)} ArrayRecord files, {len(json_files_created)} JSON files"

        # Verify JSON files are in same directories as ArrayRecord files
        arrayrecord_dirs = {f.parent for f in arrayrecord_files_created}
        json_dirs = {f.parent for f in json_files_created}

        assert arrayrecord_dirs == json_dirs, \
            f"JSON files not in same directories as ArrayRecord files: {arrayrecord_dirs} vs {json_dirs}"

        print(f"✅ Created {len(json_files_created)} metadata JSON files alongside ArrayRecord files")

    def test_directory_structure_matches_expected_pattern(self, callback_instance, temp_output_dir):
        """Test that directory structure matches the pattern expected by visualization API."""

        # Create directory structure
        callback_instance.handleStart(None, datetime.now())

        # Verify the pattern: /output_dir/{timeframe}/
        expected_structure = [
            temp_output_dir / "5m",
            temp_output_dir / "15m",
            temp_output_dir / "1h",
            temp_output_dir / "1d",
            temp_output_dir / "1w"
        ]

        for expected_dir in expected_structure:
            assert expected_dir.exists(), f"Expected directory not created: {expected_dir}"
            assert expected_dir.is_dir(), f"Path exists but is not a directory: {expected_dir}"

        # Verify no unexpected directories
        created_dirs = [d for d in temp_output_dir.iterdir() if d.is_dir() and d.name != 'metadata']
        expected_names = {'5m', '15m', '1h', '1d', '1w'}
        created_names = {d.name for d in created_dirs}

        unexpected_dirs = created_names - expected_names
        assert len(unexpected_dirs) == 0, f"Unexpected directories created: {unexpected_dirs}"

        missing_dirs = expected_names - created_names
        assert len(missing_dirs) == 0, f"Expected directories not created: {missing_dirs}"

        print(f"✅ Directory structure matches expected pattern: {expected_names}")

    @pytest.mark.asyncio
    async def test_real_file_paths_match_visualization_api_glob_patterns(self, callback_instance, temp_output_dir):
        """Test that created files can be discovered using visualization API glob patterns."""

        callback_instance.handleStart(None, datetime.now())

        mock_examples = [
            {'symbol': 'AAPL', 'timestamp': datetime.now(), 'features': {}, 'labels': {}},
            {'symbol': 'TSLA', 'timestamp': datetime.now(), 'features': {}, 'labels': {}}
        ]

        files_created = []

        def mock_arrayrecord_writer(file_path, *args, **kwargs):
            Path(file_path).touch()
            files_created.append(Path(file_path))
            mock_writer = MagicMock()
            mock_writer.__enter__ = MagicMock(return_value=mock_writer)
            mock_writer.__exit__ = MagicMock(return_value=None)
            return mock_writer

        with patch('array_record.python.array_record_module.ArrayRecordWriter', side_effect=mock_arrayrecord_writer):
            await callback_instance._save_interval_examples(mock_examples, datetime.now())

        # Test glob patterns that visualization API would use
        # Pattern from analytics service: {output_dir}/{timeframe}/{symbol}_{dates}.arrayrecord

        # Test pattern 1: Find all ArrayRecord files
        all_files = list(temp_output_dir.rglob("*.arrayrecord"))
        assert len(all_files) == len(files_created), \
            f"Glob *.arrayrecord found {len(all_files)} files, expected {len(files_created)}"

        # Test pattern 2: Find files by timeframe
        for timeframe in ['5m', '15m', '1h', '1d', '1w']:
            timeframe_files = list(temp_output_dir.glob(f"{timeframe}/*.arrayrecord"))
            expected_count = 2  # AAPL and TSLA
            assert len(timeframe_files) == expected_count, \
                f"Timeframe {timeframe} should have {expected_count} files, found {len(timeframe_files)}"

        # Test pattern 3: Find files by symbol across timeframes
        for symbol in ['AAPL', 'TSLA']:
            symbol_files = list(temp_output_dir.rglob(f"{symbol}_*.arrayrecord"))
            expected_count = 5  # 5 timeframes
            assert len(symbol_files) == expected_count, \
                f"Symbol {symbol} should have {expected_count} files, found {len(symbol_files)}"

        # Test pattern 4: Complex pattern matching visualization API search
        pattern_files = list(temp_output_dir.glob("*/*_*.arrayrecord"))
        assert len(pattern_files) == len(files_created), \
            f"Pattern '*/*_*.arrayrecord' found {len(pattern_files)} files, expected {len(files_created)}"

        print(f"✅ All {len(files_created)} files discoverable via visualization API glob patterns")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])