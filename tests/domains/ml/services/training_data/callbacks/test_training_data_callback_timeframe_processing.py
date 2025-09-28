#!/usr/bin/env python3

"""
Thorough test to verify that training_data_callback and timeseries_sequence_training_generator
properly build separate outputs for each timeframe with correct aggregation.

Expected behavior:
1. With 1m market data input, 5m timeframe should aggregate 5 minutes of 1m data
2. With 1m market data input, 15m timeframe should aggregate 15 minutes of 1m data  
3. Each timeframe should have DIFFERENT OHLCV values based on proper aggregation
4. handleInterval should call build_timeframe_features separately for each timeframe
5. Each timeframe output should be stored separately in ArrayRecord files

Current Bug:
- build_timeframe_features and handleInterval only build once for 5m for all timeframes
- All timeframes get identical values instead of properly aggregated values
"""

import asyncio
import logging
import pytest
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List
from pathlib import Path
import tempfile
import pandas as pd
import numpy as np

# Add src to path if needed
sys.path.insert(0, 'src')

from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from domains.ml.services.training_data.timeseries_sequence_training_generator import TimeSeriesSequenceTrainingGenerator, TrainingDataConfig
from core.platform.config.environment import Environment, EnvironmentType
# FIXME: tests.utils module does not exist
# from tests.utils.test_data_setup import setup_single_symbol_test
import asyncpg

class RealUniverseStateManagerWrapper:
    """Real universe state manager wrapper with test data."""
    
    def __init__(self, environment, test_data):
        from domains.trading.services.state.universe_state_manager import UniverseStateManager
        self.real_manager = UniverseStateManager(env=environment)
        self.test_data = test_data
        
    def _generate_test_data(self) -> Dict[datetime, Dict]:
        """Generate test data for real system."""
        data = {}
        start_time = datetime(2025, 7, 1, 13, 30)
        base_price = 207.50
        
        for i in range(30):  # 30 minutes of data
            timestamp = start_time + timedelta(minutes=i)
            
            # Simulate realistic price movement
            price_change = np.random.normal(0, 0.05)  # Small random changes
            current_price = base_price + price_change + (i * 0.01)  # Slight upward trend
            
            # Generate OHLCV data
            open_price = current_price
            high_price = open_price + abs(np.random.normal(0, 0.02))
            low_price = open_price - abs(np.random.normal(0, 0.02))
            close_price = open_price + np.random.normal(0, 0.01)
            volume = int(np.random.normal(40000, 5000))
            
            data[timestamp] = {
                'open': round(open_price, 2),
                'high': round(high_price, 2),
                'low': round(low_price, 2),
                'close': round(close_price, 2),
                'volume': volume,
                'timestamp': timestamp
            }
            
        return data
    
    async def get_lag_prices(self, instrument_id: int, cur_datetime: datetime, lag_periods: int, time_interval: str = '1m') -> pd.DataFrame:
        """Use real manager with test data fallback."""
        print(f"🔍 REAL: get_lag_prices called with instrument_id={instrument_id}, cur_datetime={cur_datetime}, lag_periods={lag_periods}, time_interval={time_interval}")
        
        # Try real manager first, fallback to test data
        return await self.real_manager.get_lag_prices(instrument_id, cur_datetime, lag_periods, time_interval)
    def _fallback_lag_prices(self, instrument_id: int, cur_datetime: datetime, lag_periods: int, time_interval: str) -> pd.DataFrame:
        
        # Get the requested time interval in minutes
        interval_minutes = self._parse_timeframe_minutes(time_interval)
        
        # Calculate how many 1m periods we need to aggregate
        total_minutes_needed = lag_periods * interval_minutes
        
        # Get 1m data for the required period
        end_time = cur_datetime
        start_time = end_time - timedelta(minutes=total_minutes_needed)
        
        minute_records = []
        for timestamp, data in self.minute_data.items():
            if start_time <= timestamp <= end_time:
                minute_records.append(data)
        
        if not minute_records:
            print(f"❌ MOCK: No minute data found for time range {start_time} to {end_time}")
            return pd.DataFrame()
        
        # Aggregate minute data into requested timeframe intervals
        aggregated_records = []
        
        for i in range(lag_periods):
            interval_start = end_time - timedelta(minutes=(i + 1) * interval_minutes)
            interval_end = end_time - timedelta(minutes=i * interval_minutes)
            
            # Get 1m records for this interval
            interval_minutes_data = [
                record for record in minute_records
                if interval_start <= record['timestamp'] < interval_end
            ]
            
            if interval_minutes_data:
                # Aggregate OHLCV data properly
                open_price = interval_minutes_data[0]['open']  # First open
                high_price = max(record['high'] for record in interval_minutes_data)  # Max high
                low_price = min(record['low'] for record in interval_minutes_data)    # Min low
                close_price = interval_minutes_data[-1]['close']  # Last close
                total_volume = sum(record['volume'] for record in interval_minutes_data)  # Sum volume
                
                aggregated_record = {
                    'timestamp': interval_end,
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price,
                    'volume': total_volume
                }
                aggregated_records.append(aggregated_record)
                
                print(f"📊 MOCK: Aggregated {time_interval} interval {interval_start} to {interval_end}:")
                print(f"   Input: {len(interval_minutes_data)} minute records")
                print(f"   Output: O={open_price:.2f}, H={high_price:.2f}, L={low_price:.2f}, C={close_price:.2f}, V={total_volume}")
        
        if not aggregated_records:
            return pd.DataFrame()
        
        df = pd.DataFrame(aggregated_records)
        print(f"✅ MOCK: Returning {len(df)} aggregated {time_interval} records")
        return df
    
    async def get_lead_prices(self, instrument_id: int, cur_datetime: datetime, lead_periods: int, time_interval: str = '1m') -> pd.DataFrame:
        """Use real manager for lead prices."""
        return await self.real_manager.get_lead_prices(instrument_id, cur_datetime, lead_periods, time_interval)
    async def get_lagged_signals(self, instrument_id: int, cur_datetime: datetime, lag_periods: int, time_interval: str = '1m', signal_names: List[str] = None) -> pd.DataFrame:
        """Mock lagged signals."""
        # For testing, return simple signals
        if not signal_names:
            return pd.DataFrame()
        
        records = []
        for i in range(lag_periods):
            signal_record = {'timestamp': cur_datetime - timedelta(minutes=i)}
            for signal_name in signal_names:
                signal_record[signal_name] = np.random.normal(50, 10)  # Mock signal value
            records.append(signal_record)
        
        return pd.DataFrame(records) if records else pd.DataFrame()
    
    def _parse_timeframe_minutes(self, timeframe: str) -> int:
        """Parse timeframe string to minutes."""
        if timeframe.endswith('m'):
            return int(timeframe[:-1])
        elif timeframe.endswith('h'):
            return int(timeframe[:-1]) * 60
        elif timeframe.endswith('d'):
            return int(timeframe[:-1]) * 1440
        else:
            return 1  # Default to 1 minute

class RealRunnerWrapper:
    """Real runner wrapper for testing."""
    
    def __init__(self, universe_manager: RealUniverseStateManagerWrapper, environment: Environment):
        self.universe_manager = universe_manager
        self.environment = environment
        
    def get_universe_state_manager(self):
        return self.universe_manager
    
    def get_environment(self):
        return self.environment

class TestTrainingDataCallbackTimeframeProcessing:
    """Test training data callback timeframe processing."""
    
    def __init__(self, unit_test_db):
        self.environment = Environment(env_type=EnvironmentType.TEST, db_url=unit_test_db)
        self.test_data = self._generate_test_data()
        self.real_universe_manager = RealUniverseStateManagerWrapper(self.environment, self.test_data)
        self.real_runner = RealRunnerWrapper(self.real_universe_manager, self.environment)
        
    async def setup_test_data(self):
        """Setup real test data."""
        conn = await asyncpg.connect(self.environment.get_database_url())
        await setup_single_symbol_test(self.environment, conn, 'AAPL', 999999, 1)
        await conn.close()
        
    def _generate_test_data(self) -> Dict[datetime, Dict]:
        """Generate test data."""
        data = {}
        start_time = datetime(2025, 7, 1, 13, 30)
        base_price = 207.50
        
        for i in range(30):  # 30 minutes of data
            timestamp = start_time + timedelta(minutes=i)
            
            # Simulate realistic price movement
            price_change = np.random.normal(0, 0.05)  # Small random changes
            current_price = base_price + price_change + (i * 0.01)  # Slight upward trend
            
            # Generate OHLCV data
            open_price = current_price
            high_price = open_price + abs(np.random.normal(0, 0.02))
            low_price = open_price - abs(np.random.normal(0, 0.02))
            close_price = open_price + np.random.normal(0, 0.01)
            volume = int(np.random.normal(40000, 5000))
            
            data[timestamp] = {
                'open': round(open_price, 2),
                'high': round(high_price, 2),
                'low': round(low_price, 2),
                'close': round(close_price, 2),
                'volume': volume,
                'timestamp': timestamp
            }
            
        return data
        
    async def test_build_timeframe_features_separate_outputs(self):
        """Test that build_timeframe_features produces separate outputs for each timeframe."""
        print("\n🧪 TEST: build_timeframe_features produces separate outputs for each timeframe")
        print("=" * 80)
        
        # Create training generator with multiple timeframes
        config = TrainingDataConfig(
            feature_types=['ohlcv', 'technical'],
            signal_names=['sma_20', 'rsi_14']
        )
        
        generator = TimeSeriesSequenceTrainingGenerator(
            env=self.real_runner.get_environment(),
            config=config,
            universe_manager=self.real_universe_manager
        )
        
        # Override timeframes to test specific intervals
        generator.timeframes = ['5m', '15m']
        
        # Test timestamp
        test_time = datetime(2025, 7, 1, 13, 45)
        instrument_id = 1
        
        print(f"📅 Testing at {test_time}")
        print(f"🎯 Target timeframes: {generator.timeframes}")
        
        # Build timeframe features
        timeframe_features = await generator.build_timeframe_features(instrument_id, test_time)
        
        print(f"\n📊 RESULTS:")
        print(f"   Returned timeframes: {list(timeframe_features.keys())}")
        
        # Verify we get separate outputs for each timeframe
        assert '5m' in timeframe_features, "Missing 5m timeframe in output"
        assert '15m' in timeframe_features, "Missing 15m timeframe in output"
        
        features_5m = timeframe_features['5m']
        features_15m = timeframe_features['15m']
        
        print(f"   5m features: {len(features_5m)} items")
        print(f"   15m features: {len(features_15m)} items")
        
        # Check that features have OHLCV data
        assert 'open' in features_5m or '5m_open' in features_5m, "Missing open price in 5m features"
        assert 'open' in features_15m or '15m_open' in features_15m, "Missing open price in 15m features"
        
        # Extract OHLCV values for comparison
        ohlcv_5m = self._extract_ohlcv_from_features(features_5m, '5m')
        ohlcv_15m = self._extract_ohlcv_from_features(features_15m, '15m')
        
        print(f"\n💰 OHLCV COMPARISON:")
        print(f"   5m:  O={ohlcv_5m['open']:.2f}, H={ohlcv_5m['high']:.2f}, L={ohlcv_5m['low']:.2f}, C={ohlcv_5m['close']:.2f}, V={ohlcv_5m['volume']}")
        print(f"   15m: O={ohlcv_15m['open']:.2f}, H={ohlcv_15m['high']:.2f}, L={ohlcv_15m['low']:.2f}, C={ohlcv_15m['close']:.2f}, V={ohlcv_15m['volume']}")
        
        # CRITICAL TEST: Verify that timeframes have DIFFERENT aggregated values
        if (ohlcv_5m['open'] == ohlcv_15m['open'] and 
            ohlcv_5m['high'] == ohlcv_15m['high'] and 
            ohlcv_5m['close'] == ohlcv_15m['close']):
            print("🔴 BUG CONFIRMED: 5m and 15m timeframes have identical OHLCV values!")
            print("   This indicates build_timeframe_features is not properly aggregating different timeframes")
            return False
        else:
            print("✅ SUCCESS: 5m and 15m timeframes have different OHLCV values (proper aggregation)")
            return True
    
    async def test_handleInterval_processes_each_timeframe_separately(self):
        """Test that handleInterval processes each timeframe separately."""
        print("\n🧪 TEST: handleInterval processes each timeframe separately")
        print("=" * 80)
        
        # Create temporary output directory
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create training callback with multiple timeframes
            config = TrainingDataConfig(
                feature_types=['ohlcv', 'technical'],
                signal_names=['sma_20']
            )
            
            callback = IntervalBasedTrainingDataCallback(
                symbols=['AAPL'],
                config=config,
                storage_format='arrayrecord',
                output_dir=temp_dir,
                start_date=datetime(2025, 7, 1).date(),
                end_date=datetime(2025, 7, 1).date()
            )
            
            # Initialize with mock runner
            callback.handleStart(self.real_runner, datetime(2025, 7, 1))
            
            # Override training generator timeframes for testing
            if callback.training_generator:
                callback.training_generator.timeframes = ['5m', '15m']
            
            test_time = datetime(2025, 7, 1, 13, 45)
            
            print(f"📅 Processing interval at {test_time}")
            print(f"🎯 Target timeframes: {callback.training_generator.timeframes if callback.training_generator else 'None'}")
            
            # Process interval
            await callback.handleInterval(self.real_runner, test_time)
            
            # Check output files
            output_path = Path(temp_dir)
            arrayrecord_files = list(output_path.rglob("*.arrayrecord"))
            
            print(f"\n📁 OUTPUT FILES:")
            print(f"   Total ArrayRecord files: {len(arrayrecord_files)}")
            for file_path in arrayrecord_files:
                print(f"   - {file_path.name}")
            
            # Verify separate files for each timeframe
            timeframe_files = {}
            for file_path in arrayrecord_files:
                if '_5m_' in file_path.name:
                    timeframe_files['5m'] = file_path
                elif '_15m_' in file_path.name:
                    timeframe_files['15m'] = file_path
            
            print(f"\n🗂️ TIMEFRAME FILES:")
            for timeframe, file_path in timeframe_files.items():
                print(f"   {timeframe}: {file_path.name if file_path else 'MISSING'}")
            
            # CRITICAL TEST: Verify separate files for each timeframe
            if '5m' not in timeframe_files or '15m' not in timeframe_files:
                print("🔴 BUG CONFIRMED: Missing separate ArrayRecord files for different timeframes!")
                print("   This indicates handleInterval is not processing each timeframe separately")
                return False
            else:
                print("✅ SUCCESS: Separate ArrayRecord files created for each timeframe")
                return True
    
    def _extract_ohlcv_from_features(self, features: Dict, timeframe: str) -> Dict:
        """Extract OHLCV values from features dictionary."""
        ohlcv = {}
        
        # Try timeframe-prefixed keys first
        for metric in ['open', 'high', 'low', 'close', 'volume']:
            prefixed_key = f"{timeframe}_{metric}"
            if prefixed_key in features:
                ohlcv[metric] = features[prefixed_key]
            elif metric in features:
                ohlcv[metric] = features[metric]
            else:
                ohlcv[metric] = 0.0
        
        return ohlcv
    
    async def run_all_tests(self):
        """Run all timeframe processing tests."""
        print("🧪 TIMEFRAME PROCESSING TESTS")
        print("=" * 80)
        print("Testing training_data_callback and timeseries_sequence_training_generator")
        print("with 1m market data to verify proper timeframe aggregation.")
        print()
        
        test_results = {}
        
        # Test 1: build_timeframe_features separate outputs
        test_results['build_timeframe_features'] = await self.test_build_timeframe_features_separate_outputs()
        
        # Test 2: handleInterval processes each timeframe separately
        test_results['handleInterval'] = await self.test_handleInterval_processes_each_timeframe_separately()
        
        print("\n" + "=" * 80)
        print("📋 TEST RESULTS SUMMARY:")
        print("=" * 80)
        
        all_passed = True
        for test_name, result in test_results.items():
            status = "✅ PASS" if result else "🔴 FAIL"
            print(f"{status} {test_name}")
            if not result:
                all_passed = False
        
        if all_passed:
            print("\n🎉 ALL TESTS PASSED - Timeframe processing working correctly")
        else:
            print("\n💥 TESTS FAILED - Timeframe processing bugs confirmed:")
            print("   1. build_timeframe_features may be reusing data across timeframes")
            print("   2. handleInterval may not be processing each timeframe separately")
            print("   3. Aggregation logic may not be working properly")
        
        return all_passed

# Create pytest fixture for database
@pytest.fixture
async def test_suite(unit_test_db):
    """Create test suite with real database."""
    suite = TestTrainingDataCallbackTimeframeProcessing(unit_test_db)
    await suite.setup_test_data()
    return suite

@pytest.mark.asyncio
async def test_build_timeframe_features_separate_outputs(test_suite):
    """Test that build_timeframe_features produces separate outputs for each timeframe."""
    result = await test_suite.test_build_timeframe_features_separate_outputs()
    assert result, "Build timeframe features should produce separate outputs"

@pytest.mark.asyncio
async def test_handleInterval_processes_each_timeframe_separately(test_suite):
    """Test that handleInterval processes each timeframe separately."""
    result = await test_suite.test_handleInterval_processes_each_timeframe_separately()
    assert result, "HandleInterval should process each timeframe separately"

if __name__ == "__main__":
    # Run with pytest
    import pytest
    pytest.main([__file__, '-v'])