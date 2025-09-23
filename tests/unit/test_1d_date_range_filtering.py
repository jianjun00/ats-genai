#!/usr/bin/env python3
"""
Test date range filtering for 1d ArrayRecord data generation.

The issue may be that 1d data filtering is incorrectly excluding all records
when the date range spans a single day or multiple days.

This test investigates date range logic specifically for 1d timeframes.
"""

import pytest
import tempfile
import asyncio
import logging
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Dict, Any, List
import array_record.python.array_record_module as array_record

from core.platform.config.environment import Environment, EnvironmentType  
from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback


class Test1dDateRangeFiltering:
    """Test cases to debug 1d date range filtering logic."""

    @pytest.fixture
    def test_output_dir(self):
        """Create temporary directory for test output."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def mock_runner_for_date_testing(self):
        """Create mock runner specifically for date range testing."""
        from unittest.mock import Mock, AsyncMock
        
        # Setup debug logging for date filtering
        logger = logging.getLogger("date_range_debug")
        logger.setLevel(logging.DEBUG)
        
        runner = Mock()
        env = Mock(spec=Environment)
        env.env_type = EnvironmentType.TEST
        env.get_connection = AsyncMock()
        
        # Mock the feature DAO to return simple categorization
        feature_dao = Mock()
        feature_dao.get_feature_categorization = AsyncMock(return_value={
            'symbol': 'ohlcv_basic',
            'timestamp': 'ohlcv_basic', 
            'open': 'ohlcv_basic',
            'high': 'ohlcv_basic',
            'low': 'ohlcv_basic',
            'close': 'ohlcv_basic',
            'volume': 'ohlcv_basic',
        })
        
        runner.environment = env
        runner.feature_dao = feature_dao
        return runner

    @pytest.fixture
    def daily_training_examples_multiple_days(self):
        """Create training examples spanning multiple days for date range testing."""
        examples = []
        
        # Create examples for 3 consecutive days with explicit timestamps
        base_dates = [
            date(2025, 7, 15),  # Day 1
            date(2025, 7, 16),  # Day 2  
            date(2025, 7, 17),  # Day 3
        ]
        
        for day_idx, target_date in enumerate(base_dates):
            # Create 1d data with proper daily timestamps
            daily_timestamp = datetime.combine(target_date, datetime.min.time())
            
            example = {
                'target_date': target_date,
                'symbol': 'AAPL',
                'timeframes': {
                    '1d': {
                        '1d_timestamp': daily_timestamp.timestamp(),
                        '1d_symbol': 'AAPL',
                        '1d_open': 207.50 + day_idx,
                        '1d_high': 208.00 + day_idx, 
                        '1d_low': 207.30 + day_idx,
                        '1d_close': 207.75 + day_idx,
                        '1d_volume': 500000 + (day_idx * 10000),
                    }
                }
            }
            examples.append(example)
            
        print(f"🗓️ Created daily examples for dates: {[ex['target_date'] for ex in examples]}")
        return examples

    @pytest.mark.asyncio 
    async def test_1d_date_range_single_day_filtering(self, test_output_dir,
                                                     mock_runner_for_date_testing,
                                                     daily_training_examples_multiple_days):
        """Test 1d date filtering for single day range."""
        print("🗓️ TESTING 1D DATE RANGE FILTERING - SINGLE DAY")
        
        # Test with single day range
        target_date = date(2025, 7, 15)
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            start_date=target_date,
            end_date=target_date,  # Same day
            output_dir=str(test_output_dir)
        )
        
        # Process examples and check what gets through the date filter
        filtered_examples = []
        for example in daily_training_examples_multiple_days:
            example_date = example['target_date']
            
            # Debug the date filtering logic
            print(f"📅 Checking example date: {example_date}")
            print(f"📅 Target range: {target_date} to {target_date}")
            print(f"📅 Date in range: {target_date <= example_date <= target_date}")
            
            if target_date <= example_date <= target_date:
                filtered_examples.append(example)
                print(f"✅ Example for {example_date} INCLUDED")
            else:
                print(f"❌ Example for {example_date} EXCLUDED")
        
        print(f"📊 Filtered examples count: {len(filtered_examples)}")
        assert len(filtered_examples) == 1, f"Expected 1 example for single day, got {len(filtered_examples)}"
        
        # Initialize writers and process the filtered examples
        await callback.initialize_monthly_writers()
        
        for example in filtered_examples:
            timeframe_data = example['timeframes']['1d']
            await callback.add_training_example(timeframe_data)
        
        await callback.finalize()
        
        # Check that 1d file was created and contains data
        daily_file = test_output_dir / "test_1d_single_day" / "AAPL_2025_07" / "1d" / "AAPL_2025_07.arrayrecord"
        assert daily_file.exists(), f"1d file should exist: {daily_file}"
        
        # Read the file and verify it contains the expected record
        try:
            reader = array_record.ArrayRecordReader(str(daily_file))
            records = list(reader)
            print(f"📊 1d file contains {len(records)} records")
            assert len(records) == 1, f"Expected 1 record in 1d file, got {len(records)}"
            
            # Verify the record content
            record = records[0]
            assert record['symbol'] == 'AAPL'
            assert record['open'] == pytest.approx(207.50, rel=1e-2)
            print("✅ 1d single day filtering test PASSED")
        except Exception as e:
            print(f"❌ Failed to read 1d file: {e}")
            raise

    @pytest.mark.asyncio
    async def test_1d_date_range_multi_day_filtering(self, test_output_dir,
                                                    mock_runner_for_date_testing, 
                                                    daily_training_examples_multiple_days):
        """Test 1d date filtering for multi-day range."""
        print("🗓️ TESTING 1D DATE RANGE FILTERING - MULTI DAY")
        
        # Test with multi-day range
        start_date = date(2025, 7, 15)
        end_date = date(2025, 7, 17)
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=['AAPL'],
            start_date=start_date,
            end_date=end_date,
            output_dir=str(test_output_dir)
        )
        
        # Process examples and check what gets through the date filter
        filtered_examples = []
        for example in daily_training_examples_multiple_days:
            example_date = example['target_date']
            
            # Debug the date filtering logic
            print(f"📅 Checking example date: {example_date}")
            print(f"📅 Target range: {start_date} to {end_date}")
            print(f"📅 Date in range: {start_date <= example_date <= end_date}")
            
            if start_date <= example_date <= end_date:
                filtered_examples.append(example)
                print(f"✅ Example for {example_date} INCLUDED")
            else:
                print(f"❌ Example for {example_date} EXCLUDED")
        
        print(f"📊 Filtered examples count: {len(filtered_examples)}")
        assert len(filtered_examples) == 3, f"Expected 3 examples for 3-day range, got {len(filtered_examples)}"
        
        # Initialize writers and process the filtered examples
        await callback.initialize_monthly_writers()
        
        for example in filtered_examples:
            timeframe_data = example['timeframes']['1d']
            await callback.add_training_example(timeframe_data)
        
        await callback.finalize()
        
        # Check that 1d file was created and contains data
        daily_file = test_output_dir / "test_1d_multi_day" / "AAPL_2025_07" / "1d" / "AAPL_2025_07.arrayrecord"
        assert daily_file.exists(), f"1d file should exist: {daily_file}"
        
        # Read the file and verify it contains the expected records
        try:
            reader = array_record.ArrayRecordReader(str(daily_file))
            records = list(reader)
            print(f"📊 1d file contains {len(records)} records")
            assert len(records) == 3, f"Expected 3 records in 1d file, got {len(records)}"
            
            # Verify the records are in chronological order and have correct data
            for i, record in enumerate(records):
                assert record['symbol'] == 'AAPL'
                expected_open = 207.50 + i
                assert record['open'] == pytest.approx(expected_open, rel=1e-2)
                print(f"✅ Record {i}: open={record['open']:.2f} (expected={expected_open:.2f})")
            
            print("✅ 1d multi-day filtering test PASSED")
        except Exception as e:
            print(f"❌ Failed to read 1d file: {e}")
            raise

    @pytest.mark.asyncio 
    async def test_1d_timestamp_format_investigation(self, test_output_dir,
                                                    mock_runner_for_date_testing):
        """Investigate timestamp format differences between timeframes."""
        print("🕐 INVESTIGATING 1D TIMESTAMP FORMAT DIFFERENCES")
        
        # Create examples with different timestamp formats to test edge cases
        target_date = date(2025, 7, 15)
        
        timestamp_formats = [
            {
                'name': 'midnight_timestamp',
                'timestamp': datetime(2025, 7, 15, 0, 0, 0).timestamp(),
                'description': 'Midnight start of day'
            },
            {
                'name': 'market_open_timestamp', 
                'timestamp': datetime(2025, 7, 15, 9, 30, 0).timestamp(),
                'description': 'Market open time'
            },
            {
                'name': 'market_close_timestamp',
                'timestamp': datetime(2025, 7, 15, 16, 0, 0).timestamp(), 
                'description': 'Market close time'
            },
            {
                'name': 'end_of_day_timestamp',
                'timestamp': datetime(2025, 7, 15, 23, 59, 59).timestamp(),
                'description': 'End of day'
            }
        ]
        
        for fmt in timestamp_formats:
            print(f"\n🕐 Testing timestamp format: {fmt['name']} ({fmt['description']})")
            
            callback = IntervalBasedTrainingDataCallback(
                symbols=['AAPL'],
                start_date=target_date,
                end_date=target_date,
                output_dir=str(test_output_dir)
            )
            
            await callback.initialize_monthly_writers()
            
            # Create example with this timestamp format
            example_data = {
                '1d_timestamp': fmt['timestamp'],
                '1d_symbol': 'AAPL',
                '1d_open': 207.50,
                '1d_high': 208.00,
                '1d_low': 207.30,
                '1d_close': 207.75,
                '1d_volume': 500000,
            }
            
            print(f"📊 Adding example with timestamp: {fmt['timestamp']} ({datetime.fromtimestamp(fmt['timestamp'])})")
            await callback.add_training_example(example_data)
            await callback.finalize()
            
            # Check if file was created and readable
            daily_file = test_output_dir / f"test_1d_timestamp_{fmt['name']}" / "AAPL_2025_07" / "1d" / "AAPL_2025_07.arrayrecord"
            
            if daily_file.exists():
                try:
                    reader = array_record.ArrayRecordReader(str(daily_file))
                    records = list(reader)
                    print(f"✅ {fmt['name']}: File created successfully, {len(records)} records")
                    
                    if records:
                        record = records[0]
                        stored_timestamp = record['timestamp']
                        print(f"📊 Stored timestamp: {stored_timestamp} ({datetime.fromtimestamp(stored_timestamp)})")
                except Exception as e:
                    print(f"❌ {fmt['name']}: File exists but read failed: {e}")
            else:
                print(f"❌ {fmt['name']}: File was not created")
        
        print("\n📊 Timestamp format investigation completed")