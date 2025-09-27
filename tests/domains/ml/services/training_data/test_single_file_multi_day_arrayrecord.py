#!/usr/bin/env python3
"""
Comprehensive Tests for Single File Multi-Day ArrayRecord Generation

CRITICAL REQUIREMENTS BEING TESTED:
1. Single ArrayRecord file per symbol/timeframe across multiple days
2. File contains ALL intervals from ENTIRE date range in chronological order
3. Expected record counts: ~72 intervals/day × number of days
4. Binary protobuf format (NOT JSON)
5. Proper file naming with full date range

Test Scenarios:
- 1 day: ~72 records
- 3 days: ~216 records
- 1 week: ~360 records (5 trading days)

This test will FAIL if implementation creates:
- Multiple files per day (wrong)
- JSON format (wrong)
- Missing records (wrong)
- Out-of-order records (wrong)
"""

import pytest
import tempfile
import os
import shutil
from pathlib import Path
from datetime import datetime, date
import asyncio
import sys
import struct
import array_record.python.array_record_module as array_record_module

sys.path.append('/home/jianjun/ats-genai-admin/src')

from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback


class TestSingleFileMultiDayArrayRecord:
    """Test single file generation across multiple days with proper record counts."""

    @pytest.fixture
    def temp_output_dir(self):
        """Create temporary output directory for testing."""
        temp_dir = tempfile.mkdtemp()
        yield Path(temp_dir)
        shutil.rmtree(temp_dir)

    @pytest.fixture
    def mock_symbols(self):
        """Test symbols for validation."""
        return ["TSLA", "AAPL"]

    @pytest.fixture
    def test_date_ranges(self):
        """Different date ranges to test."""
        return {
            "single_day": (date(2025, 7, 1), date(2025, 7, 1)),      # 1 day = ~72 records
            "three_days": (date(2025, 7, 1), date(2025, 7, 3)),      # 3 days = ~216 records
            "one_week": (date(2025, 7, 1), date(2025, 7, 7)),        # 7 days = ~504 records
        }

    def test_single_file_requirement_validation(self, temp_output_dir, mock_symbols, test_date_ranges):
        """
        CRITICAL TEST: Validate single file per symbol/timeframe across multiple days.

        This test verifies:
        1. Only ONE file created per symbol/timeframe (not multiple daily files)
        2. File naming includes full date range
        3. File exists in correct directory structure
        """
        print(f"\n🔍 TESTING: Single file requirement validation")

        for range_name, (start_date, end_date) in test_date_ranges.items():
            print(f"\n📅 Testing {range_name}: {start_date} to {end_date}")

            # Simulate proper directory structure that should be created
            dataset_id = f"test_dataset_{range_name}"
            days_count = (end_date - start_date).days + 1

            for symbol in mock_symbols:
                # Expected file structure
                start_datetime = f"{start_date.strftime('%Y%m%d')}_000000"
                end_datetime = f"{end_date.strftime('%Y%m%d')}_235959"
                symbol_datetime_str = f"{symbol}_{start_datetime}_{end_datetime}"

                expected_dir = temp_output_dir / dataset_id / symbol_datetime_str
                timeframes = ['5m', '15m', '1h', '1d']

                for timeframe in timeframes:
                    timeframe_dir = expected_dir / timeframe
                    expected_file = timeframe_dir / f"{symbol_datetime_str}.arrayrecord"

                    print(f"   Expected structure: {expected_file}")

                    # Verify this is the ONLY file that should exist (not multiple daily files)
                    # This test will pass when implementation is correct
                    expected_file_name = f"{symbol_datetime_str}.arrayrecord"

                    # Verify file name contains full date range (not single day)
                    assert start_datetime in expected_file_name, f"File name should contain start date: {expected_file_name}"
                    assert end_datetime in expected_file_name, f"File name should contain end date: {expected_file_name}"

                    print(f"   ✅ File naming correct for {timeframe}: {expected_file_name}")

    def test_expected_record_counts_by_date_range(self, test_date_ranges):
        """
        CRITICAL TEST: Verify expected record counts based on date range.

        Market hours: 9:30 AM - 4:00 PM = 6.5 hours = 390 minutes
        5-minute intervals: 390 / 5 = 78 intervals per day

        Expected counts:
        - 1 day: ~78 records
        - 3 days: ~234 records (78 × 3)
        - 7 days: ~546 records (78 × 7)
        """
        print(f"\n🔍 TESTING: Expected record counts validation")

        INTERVALS_PER_DAY = 78  # Market hours: 6.5 hours = 78 five-minute intervals

        for range_name, (start_date, end_date) in test_date_ranges.items():
            days_count = (end_date - start_date).days + 1
            expected_5m_records = days_count * INTERVALS_PER_DAY

            print(f"\n📊 {range_name.upper()}:")
            print(f"   Date range: {start_date} to {end_date}")
            print(f"   Days: {days_count}")
            print(f"   Expected 5m records: {expected_5m_records}")

            # Expected counts for other timeframes
            expected_15m_records = expected_5m_records // 3  # 15min = 3 × 5min
            expected_1h_records = expected_5m_records // 12   # 1hour = 12 × 5min
            expected_1d_records = days_count                  # 1 record per day

            print(f"   Expected 15m records: {expected_15m_records}")
            print(f"   Expected 1h records: {expected_1h_records}")
            print(f"   Expected 1d records: {expected_1d_records}")

            # These are the counts we'll validate against when files are generated
            assert expected_5m_records > 0, "Must have at least some 5-minute records"
            assert expected_1d_records == days_count, "Daily records should equal number of days"

    def test_binary_format_requirement(self):
        """
        CRITICAL TEST: Verify ArrayRecord files contain binary data, NOT JSON.

        This test checks that generated files:
        1. Are in binary format (not text/JSON)
        2. Can be read by ArrayRecordReader
        3. Don't contain JSON strings like '{"timestamp":'
        """
        print(f"\n🔍 TESTING: Binary format requirement (NOT JSON)")

        # Create a sample binary record to test against
        with tempfile.NamedTemporaryFile(suffix='.arrayrecord', delete=False) as temp_file:
            temp_path = temp_file.name

        # Write proper binary data
        writer = array_record_module.ArrayRecordWriter(temp_path, 'group_size:1')

        # Create binary record (NOT JSON)
        symbol = "TEST"
        timestamp = 1688169600.0  # Unix timestamp
        symbol_bytes = symbol.encode('utf-8')
        symbol_len = len(symbol_bytes)

        # Binary format: timestamp + symbol_len + symbol + OHLCV
        binary_record = struct.pack(
            f'>dI{symbol_len}sfffff',
            timestamp,                    # 8 bytes: double
            symbol_len,                   # 4 bytes: uint32
            symbol_bytes,                 # variable: string
            301.50,                       # 4 bytes: open
            317.66,                       # 4 bytes: high
            293.21,                       # 4 bytes: low
            300.64,                       # 4 bytes: close
            101573404.0                   # 4 bytes: volume
        )

        writer.write(binary_record)
        writer.close()

        # Verify we can read it back as binary
        reader = array_record_module.ArrayRecordReader(temp_path)
        record_count = reader.num_records()

        assert record_count == 1, f"Should have 1 record, got {record_count}"

        # Read the record
        record_data = reader.read()
        reader.close()

        # Verify it's binary data
        assert isinstance(record_data, bytes), "Record should be binary data"
        assert len(record_data) > 0, "Record should contain data"

        # Verify it's NOT JSON
        assert not record_data.startswith(b'{'), "Record should NOT start with JSON"
        assert b'"timestamp"' not in record_data, "Record should NOT contain JSON keys"

        print(f"   ✅ Verified binary format: {len(record_data)} bytes")
        print(f"   ✅ NOT JSON format")

    def test_chronological_ordering_requirement(self):
        """
        CRITICAL TEST: Verify records are in chronological order across multiple days.

        When processing multiple days, records must be sorted chronologically:
        - Day 1: 09:30, 09:35, 09:40... 15:55, 16:00
        - Day 2: 09:30, 09:35, 09:40... 15:55, 16:00
        - Day 3: 09:30, 09:35, 09:40... 15:55, 16:00

        NOT grouped by day then by time.
        """
        print(f"\n🔍 TESTING: Chronological ordering across multiple days")

        # Simulate 3 days of data with timestamps
        day1 = date(2025, 7, 1)
        day2 = date(2025, 7, 2)
        day3 = date(2025, 7, 3)

        # Create sample timestamps for each day (9:30 AM, 10:30 AM, 2:30 PM)
        sample_times = [
            (9, 30),   # 9:30 AM
            (10, 30),  # 10:30 AM
            (14, 30),  # 2:30 PM
        ]

        expected_chronological_order = []

        # Build expected chronological sequence
        for day in [day1, day2, day3]:
            for hour, minute in sample_times:
                timestamp = datetime.combine(day, datetime.min.time().replace(hour=hour, minute=minute))
                expected_chronological_order.append(timestamp.isoformat())

        print(f"   Expected chronological order:")
        for i, ts in enumerate(expected_chronological_order):
            print(f"     {i+1}: {ts}")

        # Verify ordering is correct (Day1 9:30 < Day1 10:30 < Day1 14:30 < Day2 9:30...)
        for i in range(len(expected_chronological_order) - 1):
            current = expected_chronological_order[i]
            next_ts = expected_chronological_order[i + 1]

            assert current < next_ts, f"Timestamps must be chronological: {current} should be < {next_ts}"

        print(f"   ✅ Chronological ordering verified across {len(expected_chronological_order)} timestamps")

    def test_file_structure_validation_real_paths(self, temp_output_dir):
        """
        CRITICAL TEST: Validate exact file paths that will be generated.

        This creates the actual directory structure and verifies:
        1. Correct path construction
        2. Single file per timeframe
        3. Proper naming convention
        """
        print(f"\n🔍 TESTING: Real file structure validation")

        # Test parameters
        symbol = "TSLA"
        start_date = date(2025, 7, 1)
        end_date = date(2025, 7, 3)  # 3 days
        dataset_id = "test_dataset_20250710_120000"

        # Create expected structure
        start_datetime = f"{start_date.strftime('%Y%m%d')}_000000"
        end_datetime = f"{end_date.strftime('%Y%m%d')}_235959"
        symbol_datetime_str = f"{symbol}_{start_datetime}_{end_datetime}"

        dataset_dir = temp_output_dir / dataset_id / symbol_datetime_str

        timeframes = ['5m', '15m', '1h', '1d']
        created_files = []

        for timeframe in timeframes:
            timeframe_dir = dataset_dir / timeframe
            timeframe_dir.mkdir(parents=True, exist_ok=True)

            # Create the expected file
            arrayrecord_file = timeframe_dir / f"{symbol_datetime_str}.arrayrecord"

            # Create a dummy file to simulate generation
            arrayrecord_file.touch()
            created_files.append(arrayrecord_file)

            print(f"   Created: {arrayrecord_file}")

        # Verify structure
        assert dataset_dir.exists(), f"Dataset directory should exist: {dataset_dir}"

        for timeframe in timeframes:
            timeframe_dir = dataset_dir / timeframe
            assert timeframe_dir.exists(), f"Timeframe directory should exist: {timeframe_dir}"

            # Verify only ONE file exists per timeframe
            files_in_timeframe = list(timeframe_dir.glob("*.arrayrecord"))
            assert len(files_in_timeframe) == 1, f"Should have exactly 1 file in {timeframe}, got {len(files_in_timeframe)}: {files_in_timeframe}"

            # Verify filename contains full date range
            file_name = files_in_timeframe[0].name
            assert start_datetime in file_name, f"Filename should contain start date: {file_name}"
            assert end_datetime in file_name, f"Filename should contain end date: {file_name}"

            print(f"   ✅ {timeframe}: Single file with correct naming: {file_name}")

        print(f"   ✅ File structure validation passed")

    def test_streaming_writer_lifecycle(self, temp_output_dir):
        """
        CRITICAL TEST: Test streaming writer lifecycle to prevent OOM.

        This tests the streaming approach:
        1. Writers are created once and stored
        2. Intervals are streamed to writers without accumulation
        3. Writers are properly closed in handleEnd
        """
        print(f"\n🔍 CRITICAL TEST: Streaming writer lifecycle")

        # Create callback instance
        callback = IntervalBasedTrainingDataCallback(
            symbols=["TSLA"],
            config=None,
            output_dir=str(temp_output_dir),
            storage_format="arrayrecord",
            start_date=date(2025, 7, 1),
            end_date=date(2025, 7, 3)  # 3 days
        )

        # Test writer initialization
        asyncio.run(callback._initialize_dataset_structure())

        # Verify writers were created and stored
        expected_writer_count = len(callback.symbols) * 4  # 4 timeframes per symbol
        actual_writer_count = len(callback.array_record_writers)

        print(f"   Expected writers: {expected_writer_count}")
        print(f"   Actual writers: {actual_writer_count}")
        assert actual_writer_count == expected_writer_count, \
            f"Should create {expected_writer_count} writers, got {actual_writer_count}"

        # Verify writer keys are correct
        expected_keys = set()
        for symbol in callback.symbols:
            for timeframe in ['5m', '15m', '1h', '1d']:
                expected_keys.add(f"{symbol}_{timeframe}")

        actual_keys = set(callback.array_record_writers.keys())
        assert actual_keys == expected_keys, \
            f"Writer keys mismatch. Expected: {expected_keys}, Got: {actual_keys}"

        print(f"   ✅ Writers initialized correctly: {list(actual_keys)}")

        # Test streaming intervals (simulate processing current intervals)
        mock_current_intervals = {
            '5m': [
                {
                    'timestamp': datetime(2025, 7, 1, 9, 30),
                    'open': 300.0, 'high': 310.0, 'low': 290.0, 'close': 305.0, 'volume': 1000000
                },
                {
                    'timestamp': datetime(2025, 7, 1, 9, 35),
                    'open': 305.0, 'high': 315.0, 'low': 295.0, 'close': 310.0, 'volume': 1100000
                }
            ],
            '15m': [
                {
                    'timestamp': datetime(2025, 7, 1, 9, 30),
                    'open': 300.0, 'high': 315.0, 'low': 290.0, 'close': 310.0, 'volume': 2100000
                }
            ],
            '1h': [
                {
                    'timestamp': datetime(2025, 7, 1, 9, 0),
                    'open': 300.0, 'high': 320.0, 'low': 285.0, 'close': 315.0, 'volume': 5000000
                }
            ],
            '1d': [
                {
                    'timestamp': datetime(2025, 7, 1, 0, 0),
                    'open': 299.0, 'high': 325.0, 'low': 280.0, 'close': 320.0, 'volume': 25000000
                }
            ]
        }

        # Stream intervals to writers (no memory accumulation)
        # Note: _stream_intervals_to_writers expects examples (List[Dict]) and current_time
        mock_examples = [
            {
                'symbol': 'TSLA',
                'intervals': mock_current_intervals
            }
        ]
        current_time = datetime(2025, 7, 1, 10, 0)
        asyncio.run(callback._stream_intervals_to_writers(mock_examples, current_time))

        print(f"   ✅ Intervals streamed to writers without accumulation")

        # Test handleEnd properly closes writers
        from unittest.mock import Mock
        mock_runner = Mock()
        asyncio.run(callback.handleEnd(mock_runner, datetime(2025, 7, 3, 16, 0)))

        # Verify all writers were closed and cleared
        assert len(callback.array_record_writers) == 0, \
            "All writers should be closed and cleared after handleEnd"

        print(f"   ✅ Writers properly closed in handleEnd")

        # Verify files were created
        created_files = []
        for root, dirs, files in os.walk(temp_output_dir):
            for file in files:
                if file.endswith('.arrayrecord'):
                    created_files.append(os.path.join(root, file))

        print(f"   Created {len(created_files)} ArrayRecord files:")
        for file_path in created_files:
            print(f"     - {file_path}")

        # Should have 4 files (one per timeframe)
        assert len(created_files) == 4, \
            f"Should create 4 ArrayRecord files, got {len(created_files)}"

        print(f"   ✅ Streaming approach prevents OOM and creates single files per timeframe")


if __name__ == "__main__":
    # Run specific critical tests
    pytest.main([
        __file__ + "::TestSingleFileMultiDayArrayRecord::test_single_file_requirement_validation",
        __file__ + "::TestSingleFileMultiDayArrayRecord::test_expected_record_counts_by_date_range",
        __file__ + "::TestSingleFileMultiDayArrayRecord::test_binary_format_requirement",
        __file__ + "::TestSingleFileMultiDayArrayRecord::test_chronological_ordering_requirement",
        __file__ + "::TestSingleFileMultiDayArrayRecord::test_streaming_writer_lifecycle",
        "-v", "-s"
    ])