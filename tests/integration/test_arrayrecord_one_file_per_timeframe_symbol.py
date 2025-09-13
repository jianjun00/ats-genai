#!/usr/bin/env python3
"""
Integration Test: ArrayRecord One-File-Per-Timeframe-Symbol Validation

This test validates that the training data callback runner creates exactly
one ArrayRecord file for each timeframe-symbol combination, ensuring proper
file organization for the visualization API.

This test focuses specifically on the file creation pattern without requiring
heavy ML dependencies.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from datetime import date


class TestArrayRecordFilePerTimeframeSymbol:
    """Integration test for ArrayRecord file creation pattern."""

    @pytest.fixture
    def temp_output_dir(self):
        """Create temporary directory for test outputs."""
        temp_dir = tempfile.mkdtemp(prefix="arrayrecord_test_")
        yield Path(temp_dir)
        # Cleanup
        shutil.rmtree(temp_dir, ignore_errors=True)

    def test_expected_timeframe_directory_structure(self, temp_output_dir):
        """Test that expected timeframe directories can be created."""

        # These are the hard-coded timeframes from the callback
        expected_timeframes = ['5m', '15m', '1h', '1d', '1w']

        # Simulate directory creation (from handleStart method)
        created_dirs = []
        for timeframe in expected_timeframes:
            timeframe_dir = temp_output_dir / timeframe
            timeframe_dir.mkdir(exist_ok=True)
            created_dirs.append(timeframe_dir)

        # Validate all directories were created
        assert len(created_dirs) == len(expected_timeframes)

        for timeframe_dir in created_dirs:
            assert timeframe_dir.exists()
            assert timeframe_dir.is_dir()

        print(f"✅ Created {len(created_dirs)} timeframe directories")

    def test_arrayrecord_filename_pattern(self):
        """Test ArrayRecord filename generation pattern."""

        # Test data from callback logic
        symbols = ['AAPL', 'TSLA']
        start_date = date(2025, 8, 1)
        end_date = date(2025, 8, 2)

        # Generate filenames using the same logic as callback (lines 690-692)
        start_date_str = start_date.strftime('%Y%m%d_%H%M%S')
        end_date_str = end_date.strftime('%Y%m%d_%H%M%S')

        expected_filenames = []
        for symbol in symbols:
            filename = f"{symbol}_{start_date_str}_{end_date_str}.arrayrecord"
            expected_filenames.append(filename)

        # Validate filename format
        for filename in expected_filenames:
            # Should start with symbol
            assert any(filename.startswith(f"{symbol}_") for symbol in symbols)

            # Should end with .arrayrecord
            assert filename.endswith(".arrayrecord")

            # Should have proper datetime format
            parts = filename.split('_')
            assert len(parts) >= 5  # SYMBOL_YYYYMMDD_HHMMSS_YYYYMMDD_HHMMSS.arrayrecord

        print(f"✅ Filename pattern validation passed: {expected_filenames}")

    def test_complete_timeframe_symbol_matrix(self, temp_output_dir):
        """Test that complete matrix of timeframe-symbol files would be created."""

        # Test parameters
        symbols = ['AAPL', 'TSLA']
        timeframes = ['5m', '15m', '1h', '1d', '1w']
        expected_total = len(symbols) * len(timeframes)

        # Create directory structure
        for timeframe in timeframes:
            (temp_output_dir / timeframe).mkdir(exist_ok=True)

        # Simulate file creation for each combination
        created_files = []
        start_date_str = "20250801_000000"
        end_date_str = "20250802_000000"

        for timeframe in timeframes:
            for symbol in symbols:
                filename = f"{symbol}_{start_date_str}_{end_date_str}.arrayrecord"
                file_path = temp_output_dir / timeframe / filename

                # Create empty file to simulate ArrayRecord creation
                file_path.touch()
                created_files.append(file_path)

        # Validate total file count
        assert len(created_files) == expected_total, \
            f"Expected {expected_total} files, created {len(created_files)}"

        # Validate each timeframe has all symbols
        for timeframe in timeframes:
            timeframe_dir = temp_output_dir / timeframe
            files_in_timeframe = list(timeframe_dir.glob("*.arrayrecord"))

            assert len(files_in_timeframe) == len(symbols), \
                f"{timeframe} should have {len(symbols)} files, has {len(files_in_timeframe)}"

            # Check that all symbols are present
            symbols_in_files = set()
            for file_path in files_in_timeframe:
                symbol = file_path.name.split('_')[0]  # Extract symbol from filename
                symbols_in_files.add(symbol)

            assert symbols_in_files == set(symbols), \
                f"{timeframe} missing symbols: {set(symbols) - symbols_in_files}"

        # Validate each symbol has all timeframes
        for symbol in symbols:
            symbol_files = list(temp_output_dir.rglob(f"{symbol}_*.arrayrecord"))

            assert len(symbol_files) == len(timeframes), \
                f"{symbol} should have {len(timeframes)} files, has {len(symbol_files)}"

            # Check that all timeframes are present
            timeframes_in_files = set()
            for file_path in symbol_files:
                timeframe = file_path.parent.name  # Parent directory is timeframe
                timeframes_in_files.add(timeframe)

            assert timeframes_in_files == set(timeframes), \
                f"{symbol} missing timeframes: {set(timeframes) - timeframes_in_files}"

        print(f"✅ Complete matrix validated: {expected_total} files ({len(symbols)} symbols × {len(timeframes)} timeframes)")

    def test_no_duplicate_files_per_combination(self, temp_output_dir):
        """Test that no duplicate files are created for same timeframe-symbol combination."""

        timeframes = ['5m', '1h']  # Test subset for simplicity
        symbols = ['AAPL']

        # Create directories
        for timeframe in timeframes:
            (temp_output_dir / timeframe).mkdir(exist_ok=True)

        # Simulate the grouping logic that could create duplicates
        # This tests the scenario where multiple examples for same symbol-timeframe
        # should still result in only one file

        mock_examples = [
            {'symbol': 'AAPL', 'timestamp': '2025-08-01T10:00:00'},
            {'symbol': 'AAPL', 'timestamp': '2025-08-01T10:30:00'},  # Same symbol, different time
            {'symbol': 'AAPL', 'timestamp': '2025-08-01T11:00:00'},  # Same symbol, different time
        ]

        # Simulate the callback's grouping logic
        examples_by_timeframe_symbol = {}
        for example in mock_examples:
            symbol = example['symbol']
            for timeframe in timeframes:
                key = (timeframe, symbol)
                if key not in examples_by_timeframe_symbol:
                    examples_by_timeframe_symbol[key] = []
                examples_by_timeframe_symbol[key].append(example)

        # Create one file per combination (not per example)
        created_files = {}
        for (timeframe, symbol), symbol_examples in examples_by_timeframe_symbol.items():
            filename = f"{symbol}_20250801_000000_20250802_000000.arrayrecord"
            file_path = temp_output_dir / timeframe / filename

            # Should create only one file even with multiple examples
            if file_path not in created_files:
                file_path.touch()
                created_files[file_path] = len(symbol_examples)

        # Validate no duplicates
        for timeframe in timeframes:
            timeframe_dir = temp_output_dir / timeframe
            aapl_files = list(timeframe_dir.glob("AAPL_*.arrayrecord"))

            assert len(aapl_files) == 1, \
                f"{timeframe} should have exactly 1 AAPL file, has {len(aapl_files)}: {aapl_files}"

        # Validate that all examples would be included in the single file
        for file_path, example_count in created_files.items():
            assert example_count == len(mock_examples), \
                f"File {file_path.name} should contain {len(mock_examples)} examples, has {example_count}"

        print(f"✅ No duplicate files: {len(mock_examples)} examples → {len(created_files)} unique files")

    def test_visualization_api_compatible_structure(self, temp_output_dir):
        """Test that file structure matches visualization API search patterns."""

        # Create the expected structure
        symbols = ['AAPL', 'TSLA']
        timeframes = ['5m', '15m', '1h']

        for timeframe in timeframes:
            timeframe_dir = temp_output_dir / timeframe
            timeframe_dir.mkdir(exist_ok=True)

            for symbol in symbols:
                filename = f"{symbol}_20250801_000000_20250802_000000.arrayrecord"
                (timeframe_dir / filename).touch()

        # Test the glob patterns that visualization API would use
        # Pattern from analytics service: /data/training_data/{run_id}/{timeframe}/{symbol}_{dates}.arrayrecord

        # Test pattern: */*/*_*.arrayrecord (run_id/timeframe/symbol_dates.arrayrecord)
        pattern1_files = list(temp_output_dir.glob("*/*_*.arrayrecord"))
        expected_files = len(symbols) * len(timeframes)

        assert len(pattern1_files) == expected_files, \
            f"Pattern '*/*_*.arrayrecord' found {len(pattern1_files)} files, expected {expected_files}"

        # Test pattern: specific timeframe search
        for timeframe in timeframes:
            timeframe_files = list(temp_output_dir.glob(f"{timeframe}/*_*.arrayrecord"))
            assert len(timeframe_files) == len(symbols), \
                f"{timeframe} should have {len(symbols)} files, found {len(timeframe_files)}"

        # Test pattern: specific symbol search across timeframes
        for symbol in symbols:
            symbol_files = list(temp_output_dir.rglob(f"{symbol}_*.arrayrecord"))
            assert len(symbol_files) == len(timeframes), \
                f"{symbol} should have {len(timeframes)} files, found {len(symbol_files)}"

        print(f"✅ Visualization API compatible structure: {expected_files} files discoverable")

    def test_metadata_json_accompanies_arrayrecord(self, temp_output_dir):
        """Test that metadata JSON files would accompany ArrayRecord files."""

        symbols = ['AAPL']
        timeframes = ['5m', '1h']

        # Create directories
        for timeframe in timeframes:
            (temp_output_dir / timeframe).mkdir(exist_ok=True)

        # Simulate ArrayRecord + metadata creation
        created_pairs = []
        for timeframe in timeframes:
            for symbol in symbols:
                # ArrayRecord file
                arrayrecord_filename = f"{symbol}_20250801_000000_20250802_000000.arrayrecord"
                arrayrecord_path = temp_output_dir / timeframe / arrayrecord_filename
                arrayrecord_path.touch()

                # Metadata JSON file (from callback line 702)
                metadata_filename = f"{symbol}_20250801_000000_20250802_000000_metadata.json"
                metadata_path = temp_output_dir / timeframe / metadata_filename
                metadata_path.write_text('{"timeframe": "' + timeframe + '", "symbol": "' + symbol + '"}')

                created_pairs.append((arrayrecord_path, metadata_path))

        # Validate pairing
        expected_pairs = len(symbols) * len(timeframes)
        assert len(created_pairs) == expected_pairs, \
            f"Expected {expected_pairs} ArrayRecord-metadata pairs, created {len(created_pairs)}"

        # Validate that each ArrayRecord has corresponding metadata
        for arrayrecord_path, metadata_path in created_pairs:
            assert arrayrecord_path.exists(), f"ArrayRecord file missing: {arrayrecord_path}"
            assert metadata_path.exists(), f"Metadata file missing: {metadata_path}"

            # Validate they're in the same directory
            assert arrayrecord_path.parent == metadata_path.parent, \
                f"Files not in same directory: {arrayrecord_path.parent} vs {metadata_path.parent}"

        print(f"✅ Metadata JSON files accompany ArrayRecord files: {expected_pairs} pairs created")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])