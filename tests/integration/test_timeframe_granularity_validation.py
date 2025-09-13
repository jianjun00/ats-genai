#!/usr/bin/env python3
"""
🧪 CRITICAL TEST: Timeframe Granularity Validation

This test validates that each timeframe generates records at its correct native frequency:
- 5m: 12 records per hour (every 5 minutes)
- 15m: 4 records per hour (every 15 minutes)  
- 1h: 1 record per hour (every hour)
- 1d: 1 record per day (every day)
- 1w: 1 record per week (every week)

🚨 CRITICAL ISSUE DETECTION:
Currently all timeframes generate 1 record/hour instead of native frequency.
This test will FAIL until the granularity issue is fixed.
"""

import pytest
import asyncio
import tempfile
import shutil
import os
from datetime import datetime, timedelta
from pathlib import Path
import array_record.python.array_record_module as array_record
import struct

# Import training data system
import sys
sys.path.insert(0, '/workspace/src')

from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig
from services.core.app.runner import Runner
from shared.utils.environment import Environment, EnvironmentType


class TimeframeGranularityTest:
    """Test suite for validating correct timeframe granularity."""
    
    def __init__(self):
        self.test_output_dir = None
        self.test_symbol = 'TSLA'
        # Test a single trading day to verify granularity
        self.test_start_date = '2025-07-01'  # Known trading day with data
        self.test_end_date = '2025-07-01'
        
        # Expected records per timeframe for 1 trading day
        # Assuming 6.5 hours of market data (9:30 AM - 4:00 PM EST)
        self.expected_records = {
            '5m': 78,   # 6.5 hours * 12 records/hour = 78 records
            '15m': 26,  # 6.5 hours * 4 records/hour = 26 records
            '1h': 7,    # 6.5 hours * 1 record/hour = 7 records (rounded up)
            '1d': 1,    # 1 day = 1 record
            '1w': 1     # 1 day within a week = 1 record
        }
        
    async def setup_test_environment(self):
        """Setup isolated test environment."""
        self.test_output_dir = tempfile.mkdtemp(prefix='timeframe_granularity_test_')
        print(f"📁 Test output directory: {self.test_output_dir}")
        return True
        
    async def cleanup_test_environment(self):
        """Cleanup test environment."""
        if self.test_output_dir and os.path.exists(self.test_output_dir):
            shutil.rmtree(self.test_output_dir)
            print(f"🧹 Cleaned up test directory: {self.test_output_dir}")
    
    async def generate_test_training_data(self):
        """Generate training data for timeframe granularity testing."""
        print("🔄 Generating training data for timeframe granularity test...")
        
        config = TrainingDataConfig(
            base_interval_minutes=1,
            training_interval_minutes=60,  # This might be the issue - forcing 60m intervals
            timeframes=["5m", "15m", "1h", "1d", "1w"],
            feature_types=["ohlcv", "indicators"],
            signal_names=["etop", "ebot", "pldot"]
        )
        
        callback = IntervalBasedTrainingDataCallback(
            symbols=[self.test_symbol],
            config=config,
            start_date=self.test_start_date,
            end_date=self.test_end_date,
            output_dir=self.test_output_dir,
            storage_format='arrayrecord'
        )
        
        environment = Environment(env_type=EnvironmentType.DEV)
        runner = Runner(
            environment=environment,
            universe_id=1,  # Use default universe
            callbacks=[callback],  # Pass the training callback instance directly
            base_duration='60m',  # This might be the source of the granularity issue
            start_date=self.test_start_date,
            end_date=self.test_end_date
        )
        
        # Generate with context manager for proper cleanup
        with callback:
            await runner.run()
            
        print("✅ Training data generation completed")
    
    def analyze_timeframe_records(self):
        """Analyze generated files and count records per timeframe."""
        print("\n🔍 Analyzing timeframe record counts...")
        
        timeframe_counts = {}
        
        # Find all generated ArrayRecord files
        for root, dirs, files in os.walk(self.test_output_dir):
            for file in files:
                if file.endswith('.arrayrecord'):
                    file_path = os.path.join(root, file)
                    
                    # Extract timeframe from directory structure
                    path_parts = root.split(os.sep)
                    if len(path_parts) >= 2:
                        timeframe = path_parts[-1]  # e.g., '5m', '15m', '1h', '1d'
                        
                        if timeframe in ['5m', '15m', '1h', '1d', '1w']:
                            try:
                                reader = array_record.ArrayRecordReader(str(file_path))
                                record_count = reader.num_records()
                                
                                timeframe_counts[timeframe] = record_count
                                print(f"   📊 {timeframe}: {record_count} records")
                                
                            except Exception as e:
                                print(f"   ❌ Error reading {timeframe} file: {e}")
        
        return timeframe_counts
    
    def validate_timeframe_granularity(self, actual_counts):
        """Validate that record counts match expected granularity."""
        print("\n🎯 Validating timeframe granularity...")
        
        validation_results = {}
        
        for timeframe, expected_count in self.expected_records.items():
            actual_count = actual_counts.get(timeframe, 0)
            
            # Allow some tolerance for market hours variations
            tolerance = 0.2  # 20% tolerance
            min_expected = int(expected_count * (1 - tolerance))
            max_expected = int(expected_count * (1 + tolerance))
            
            is_valid = min_expected <= actual_count <= max_expected
            
            validation_results[timeframe] = {
                'expected': expected_count,
                'actual': actual_count,
                'valid': is_valid,
                'tolerance_range': f"{min_expected}-{max_expected}"
            }
            
            status = "✅ PASS" if is_valid else "❌ FAIL"
            print(f"   {timeframe}: {status} - Expected: {expected_count}, Actual: {actual_count}, Range: {min_expected}-{max_expected}")
        
        return validation_results
    
    def detect_granularity_issues(self, validation_results):
        """Detect specific granularity issues and provide diagnostic information."""
        print("\n🚨 GRANULARITY ISSUE DETECTION:")
        
        issues_detected = []
        
        for timeframe, result in validation_results.items():
            if not result['valid']:
                expected = result['expected']
                actual = result['actual']
                
                if actual == 1 and timeframe in ['5m', '15m'] and expected > 1:
                    issues_detected.append({
                        'timeframe': timeframe,
                        'issue': 'HOURLY_GENERATION_BUG',
                        'description': f'{timeframe} generating 1 record/hour instead of native frequency',
                        'expected_behavior': f'Should generate {expected} records per day',
                        'likely_cause': 'Runner interval generation or aggregation logic issue'
                    })
                elif actual == expected and timeframe == '1h':
                    # 1h might be working correctly
                    continue
                else:
                    issues_detected.append({
                        'timeframe': timeframe,
                        'issue': 'INCORRECT_FREQUENCY',
                        'description': f'{timeframe} generating {actual} records instead of {expected}',
                        'expected_behavior': f'Should generate records at native {timeframe} frequency',
                        'likely_cause': 'Timeframe aggregation or interval calculation issue'
                    })
        
        if issues_detected:
            print("🚨 CRITICAL ISSUES DETECTED:")
            for i, issue in enumerate(issues_detected, 1):
                print(f"\n   Issue {i}: {issue['issue']}")
                print(f"   Timeframe: {issue['timeframe']}")
                print(f"   Description: {issue['description']}")
                print(f"   Expected: {issue['expected_behavior']}")
                print(f"   Likely Cause: {issue['likely_cause']}")
        else:
            print("✅ No granularity issues detected")
            
        return issues_detected


@pytest.mark.asyncio
async def test_timeframe_granularity_validation():
    """
    🧪 MASTER TEST: Validate timeframe granularity correctness
    
    This test will FAIL until the granularity issue is fixed.
    It validates that each timeframe generates records at its native frequency.
    """
    print("\n" + "="*80)
    print("🧪 TIMEFRAME GRANULARITY VALIDATION TEST")
    print("="*80)
    
    test_suite = TimeframeGranularityTest()
    
    try:
        # Setup test environment
        await test_suite.setup_test_environment()
        
        # Generate training data
        await test_suite.generate_test_training_data()
        
        # Analyze record counts per timeframe
        actual_counts = test_suite.analyze_timeframe_records()
        
        # Validate granularity
        validation_results = test_suite.validate_timeframe_granularity(actual_counts)
        
        # Detect specific issues
        issues = test_suite.detect_granularity_issues(validation_results)
        
        # Report results
        print("\n" + "="*80)
        print("🎯 TEST RESULTS SUMMARY")
        print("="*80)
        
        total_timeframes = len(test_suite.expected_records)
        passing_timeframes = sum(1 for result in validation_results.values() if result['valid'])
        
        print(f"📊 Timeframes Tested: {total_timeframes}")
        print(f"✅ Passing: {passing_timeframes}")
        print(f"❌ Failing: {total_timeframes - passing_timeframes}")
        print(f"🚨 Issues Detected: {len(issues)}")
        
        if issues:
            print("\n🚨 CRITICAL: Granularity issues detected - timeframes not generating at native frequency")
            print("💡 ACTION REQUIRED: Fix Runner interval generation and timeframe aggregation logic")
            
            # This test should FAIL until the granularity issue is fixed
            pytest.fail(f"Timeframe granularity validation failed - {len(issues)} issues detected")
        else:
            print("\n🎉 SUCCESS: All timeframes generating at correct native frequency")
            
        print("="*80)
        
    finally:
        await test_suite.cleanup_test_environment()


@pytest.mark.asyncio
async def test_missing_weekly_timeframe():
    """
    🧪 TEST: Validate 1w (weekly) timeframe support
    
    This test validates that the weekly timeframe is properly supported.
    """
    print("\n🧪 Testing weekly (1w) timeframe support...")
    
    test_suite = TimeframeGranularityTest()
    
    try:
        await test_suite.setup_test_environment()
        await test_suite.generate_test_training_data()
        
        actual_counts = test_suite.analyze_timeframe_records()
        
        # Check if 1w timeframe exists
        if '1w' not in actual_counts:
            pytest.fail("❌ CRITICAL: 1w (weekly) timeframe is missing - needs to be implemented")
        elif actual_counts['1w'] == 0:
            pytest.fail("❌ CRITICAL: 1w (weekly) timeframe exists but contains no records")
        else:
            print(f"✅ Weekly timeframe found with {actual_counts['1w']} records")
            
    finally:
        await test_suite.cleanup_test_environment()


if __name__ == "__main__":
    """Direct execution for development testing."""
    print("🧪 Direct execution of timeframe granularity validation tests")
    
    async def run_tests():
        await test_timeframe_granularity_validation()
        await test_missing_weekly_timeframe()
    
    asyncio.run(run_tests())