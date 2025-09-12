#!/usr/bin/env python3

"""
Test to reproduce and debug the issue where training data generation
only produces 7 sequences instead of the expected ~352 sequences
for TSLA July 2025 data.

Expected: 22 trading days * 16 intervals/day (60m base duration) = 352 sequences
Actual: Only 7 sequences per timeframe

This test will help us identify where the limitation is occurring.
"""

import sys
import os
sys.path.insert(0, 'src')

import pytest
import asyncio
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
import pandas as pd
from array_record.python import array_record_module

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_tsla_july_2025_sequence_count_issue():
    """
    Test that reproduces the 7-sequence limitation issue.
    
    This test will:
    1. Verify we have sufficient TSLA July 2025 minute bar data
    2. Run training data generation with controlled parameters
    3. Count the actual sequences generated
    4. Compare with expected count based on data availability
    5. Identify where the limitation occurs
    """
    print("🔍 DEBUGGING: Training Data Sequence Count Issue")
    print("=" * 60)
    
    # STEP 1: Verify source data availability
    print("\n📊 STEP 1: Verifying TSLA July 2025 source data")
    
    tsla_file = Path("/mnt/d/ats-data/minute-bars/firstrate/T/TSLA/2025/07/TSLA_2025_07.parquet")
    assert tsla_file.exists(), f"TSLA data file not found: {tsla_file}"
    
    df = pd.read_parquet(tsla_file)
    total_minutes = len(df)
    date_range = f"{df['timestamp'].min()} to {df['timestamp'].max()}"
    unique_days = df['timestamp'].dt.date.unique()
    trading_days = len(unique_days)
    
    print(f"✅ TSLA data verified:")
    print(f"   File: {tsla_file}")
    print(f"   Total minutes: {total_minutes:,}")
    print(f"   Date range: {date_range}")
    print(f"   Trading days: {trading_days}")
    
    # Calculate expected intervals with 60m base duration
    # Each trading day has ~960 minutes (16 hours * 60 minutes)
    first_day_data = df[df['timestamp'].dt.date == unique_days[0]]
    minutes_per_day = len(first_day_data)
    intervals_per_day_60m = minutes_per_day // 60
    expected_total_intervals = trading_days * intervals_per_day_60m
    
    print(f"✅ Expected interval calculation:")
    print(f"   Minutes per trading day: {minutes_per_day}")
    print(f"   60m intervals per day: {intervals_per_day_60m}")
    print(f"   Expected total 60m intervals: {expected_total_intervals}")
    
    # STEP 2: Run minimal training data generation
    print(f"\n🔄 STEP 2: Running training data generation")
    
    from shared.utils.environment import Environment, EnvironmentType
    from domains.ml.services.training_data.runners.training_data_callback_runner import main as runner_main
    from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig
    import tempfile
    
    # Create temporary output directory
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"   Output directory: {temp_dir}")
        
        # Set up minimal arguments for testing
        import argparse
        
        # Mock command line arguments
        test_args = [
            '--symbols', 'TSLA',
            '--start-date', '2025-07-01',
            '--end-date', '2025-07-01',  # Test with just one day first
            '--environment', 'dev',
            '--output-dir', temp_dir,
            '--base-duration', '60m',
            '--debug'
        ]
        
        # Temporarily override sys.argv
        original_argv = sys.argv.copy()
        sys.argv = ['test'] + test_args
        
        try:
            print("   Executing training data generation...")
            await runner_main()
            print("   ✅ Training data generation completed")
            
        except Exception as e:
            print(f"   ❌ Training data generation failed: {e}")
            raise
        finally:
            sys.argv = original_argv
        
        # STEP 3: Count generated sequences
        print(f"\n📈 STEP 3: Counting generated sequences")
        
        output_path = Path(temp_dir)
        dataset_dirs = list(output_path.glob("dataset_*"))
        
        if not dataset_dirs:
            print("❌ No dataset directories found!")
            assert False, "No training data generated"
        
        latest_dataset = max(dataset_dirs, key=lambda p: p.name)
        print(f"   Latest dataset: {latest_dataset.name}")
        
        # Count sequences in each timeframe
        sequence_counts = {}
        timeframes = ['5m', '15m', '1h', '1d']
        
        for timeframe in timeframes:
            tf_files = list(latest_dataset.glob(f"*/{timeframe}/*.arrayrecord"))
            
            if tf_files:
                arrayrecord_file = tf_files[0]
                reader = array_record_module.ArrayRecordReader(str(arrayrecord_file))
                record_count = reader.num_records()
                sequence_counts[timeframe] = record_count
                print(f"   {timeframe}: {record_count} sequences ({arrayrecord_file.stat().st_size:,} bytes)")
            else:
                sequence_counts[timeframe] = 0
                print(f"   {timeframe}: 0 sequences (no file found)")
        
        # STEP 4: Analyze the discrepancy
        print(f"\n🔍 STEP 4: Analyzing sequence count discrepancy")
        
        actual_sequences = sequence_counts.get('1h', 0)
        
        # For single day test, expected would be ~16 intervals
        expected_single_day = minutes_per_day // 60
        
        print(f"📊 Results for single day test (2025-07-01):")
        print(f"   Expected 60m intervals: {expected_single_day}")
        print(f"   Actual sequences generated: {actual_sequences}")
        print(f"   Discrepancy: {expected_single_day - actual_sequences} missing sequences")
        
        if actual_sequences < expected_single_day:
            print(f"\n🚨 ISSUE REPRODUCED!")
            print(f"   We should have {expected_single_day} sequences but only got {actual_sequences}")
            print(f"   This suggests the runner is not processing all available intervals")
            
            # Return diagnostic info for debugging
            return {
                'expected_sequences': expected_single_day,
                'actual_sequences': actual_sequences,
                'data_minutes': minutes_per_day,
                'dataset_path': str(latest_dataset),
                'sequence_counts': sequence_counts,
                'issue_reproduced': True
            }
        else:
            print(f"✅ Sequence count looks correct for single day test")
            return {
                'expected_sequences': expected_single_day,
                'actual_sequences': actual_sequences,
                'issue_reproduced': False
            }

async def test_debug_runner_interval_processing():
    """
    Debug test to understand how the Runner processes intervals.
    
    This will help us identify if the issue is in:
    1. Runner interval generation
    2. Callback interval handling  
    3. Data filtering/selection
    4. ArrayRecord writing
    """
    print("\n🔧 DEBUGGING: Runner Interval Processing")
    print("=" * 50)
    
    from services.core.app.runner import Runner
    from shared.utils.environment import Environment, EnvironmentType
    from domains.market_data.services.core.minute.file_based_minute_market_data_manager import FileBasedMinuteMarketDataManager
    from domains.trading.services.state.universe_state_builder import UniverseStateBuilder
    from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
    from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig
    import tempfile
    
    # Create test environment
    environment = Environment(None, EnvironmentType.DEV)
    
    # Create minute data manager
    minute_data_manager = FileBasedMinuteMarketDataManager(
        base_path="/mnt/d/ats-data/minute-bars",
        environment=environment
    )
    
    # Create universe state builder
    universe_state_builder = UniverseStateBuilder(
        environment=environment,
        universe_id=1
    )
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create training config
        config = TrainingDataConfig()
        
        # Create callback
        training_callback = IntervalBasedTrainingDataCallback(
            symbols=['TSLA'],
            config=config,
            output_dir=temp_dir,
            start_date='2025-07-01',
            end_date='2025-07-01'  # Single day for debugging
        )
        
        # Add debugging to track intervals processed
        original_handle_interval = training_callback.handleInterval
        intervals_processed = []
        
        def debug_handle_interval(runner, current_time):
            intervals_processed.append(current_time)
            print(f"🔄 Processing interval: {current_time}")
            return original_handle_interval(runner, current_time)
        
        training_callback.handleInterval = debug_handle_interval
        
        # Create runner
        runner = Runner(
            start_date=date(2025, 7, 1),
            end_date=date(2025, 7, 1),
            environment=environment,
            universe_id=1,
            callbacks=[universe_state_builder, training_callback],
            market_data_manager=minute_data_manager,
            base_duration='60m'
        )
        
        print(f"🚀 Starting runner for single day debug...")
        
        try:
            await runner.run()
            print(f"✅ Runner completed successfully")
            
            print(f"\n📊 Debug Results:")
            print(f"   Intervals processed: {len(intervals_processed)}")
            print(f"   Interval timestamps:")
            for i, timestamp in enumerate(intervals_processed[:10]):  # Show first 10
                print(f"     {i+1}: {timestamp}")
            if len(intervals_processed) > 10:
                print(f"     ... ({len(intervals_processed) - 10} more intervals)")
            
            return {
                'intervals_processed_count': len(intervals_processed),
                'intervals_processed': intervals_processed,
                'expected_intervals': 16  # ~16 hours of 60m intervals per day
            }
            
        except Exception as e:
            print(f"❌ Runner failed: {e}")
            raise

if __name__ == "__main__":
    async def main():
        print("🧪 TRAINING SEQUENCE COUNT DEBUG TEST")
        print("="*60)
        
        try:
            # Test 1: Reproduce the sequence count issue
            result1 = await test_tsla_july_2025_sequence_count_issue()
            
            # Test 2: Debug runner interval processing
            result2 = await test_debug_runner_interval_processing()
            
            print(f"\n🎯 FINAL ANALYSIS:")
            print(f"   Issue reproduced: {result1.get('issue_reproduced', False)}")
            print(f"   Expected sequences: {result1.get('expected_sequences', 0)}")  
            print(f"   Actual sequences: {result1.get('actual_sequences', 0)}")
            print(f"   Runner intervals processed: {result2.get('intervals_processed_count', 0)}")
            
            if result1.get('issue_reproduced'):
                print(f"\n🔍 ROOT CAUSE ANALYSIS NEEDED:")
                print(f"   The runner should process ~16 intervals per day (60m base duration)")
                print(f"   But we're only getting {result1.get('actual_sequences', 0)} sequences")
                print(f"   Check if the issue is in Runner interval generation or Callback processing")
            
        except Exception as e:
            print(f"❌ Test failed with error: {e}")
            raise
    
    asyncio.run(main())