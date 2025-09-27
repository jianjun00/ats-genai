#!/usr/bin/env python3

"""
Test to verify that TimeSeriesSequenceTrainingGenerator uses pre-computed UniverseStateInterval
objects for FUTURE data instead of rebuilding it with get_lead_prices().

This test confirms the architectural fix where training data generators should:
1. Use pre-computed future UniverseStateInterval objects from UniverseStateBuilder  
2. Extract future OHLCV and indicator data directly from InstrumentInterval objects
3. Generate identical results to what UniverseStateBuilder computed for future periods

The fix eliminates future data duplication and ensures consistency between:
- UniverseStateBuilder computed future values 
- Training data generator extracted future values
"""

import asyncio
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import pandas as pd
import numpy as np

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

from domains.ml.services.training_data.timeseries_sequence_training_generator import TimeSeriesSequenceTrainingGenerator, TrainingDataConfig
from domains.trading.services.state.universe_state_manager import UniverseStateManager
from domains.trading.services.state.universe_state import UniverseStateInterval
from domains.trading.services.state.instrument_interval import InstrumentInterval
from core.business.calendars.time_duration import TimeDuration
from core.platform.config.environment import Environment

class MockUniverseStateManagerWithFutureIntervals:
    """
    Mock universe state manager that returns pre-computed future UniverseStateInterval objects.
    This simulates the proper architectural flow where UniverseStateBuilder has already
    computed future OHLCV and indicator data and stored it in UniverseStateInterval objects.
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # Create pre-computed universe state intervals for current and future timeframes
        self.current_intervals, self.future_intervals = self._create_precomputed_intervals()
        
    def _create_precomputed_intervals(self) -> tuple:
        """Create pre-computed current and future UniverseStateInterval objects for testing."""
        current_intervals = {}
        future_intervals = {}
        
        # Test timeframes
        timeframes = ['5m', '15m']
        current_time = datetime(2025, 7, 1, 13, 45)
        
        for timeframe in timeframes:
            current_intervals[timeframe] = {}
            future_intervals[timeframe] = {}
            
            # Create current interval data
            if timeframe == '5m':
                # Current 5m interval: 13:40-13:45
                current_start = datetime(2025, 7, 1, 13, 40) 
                current_end = datetime(2025, 7, 1, 13, 45)
                current_instrument_interval = InstrumentInterval(
                    instrument_id=31,
                    start_date_time=current_start,
                    end_date_time=current_end,
                    open=207.71,
                    high=207.86,
                    low=207.65,
                    close=207.83,
                    traded_volume=238895,
                    traded_dollar=49655432.5
                )
                
                # Future 5m interval: 13:45-13:50 (one 5m period ahead)
                future_start = datetime(2025, 7, 1, 13, 45)
                future_end = datetime(2025, 7, 1, 13, 50)
                future_instrument_interval = InstrumentInterval(
                    instrument_id=31,
                    start_date_time=future_start,
                    end_date_time=future_end,
                    open=207.83,  # Future open (starts from current close)
                    high=208.12,  # Future high (different from current)
                    low=207.76,   # Future low
                    close=208.05, # Future close
                    traded_volume=245120,  # Different future volume
                    traded_dollar=50942115.2
                )
                
            elif timeframe == '15m':
                # Current 15m interval: 13:30-13:45
                current_start = datetime(2025, 7, 1, 13, 30)
                current_end = datetime(2025, 7, 1, 13, 45)
                current_instrument_interval = InstrumentInterval(
                    instrument_id=31,
                    start_date_time=current_start,
                    end_date_time=current_end,
                    open=207.55,
                    high=207.86,
                    low=207.42,
                    close=207.83,
                    traded_volume=677157,
                    traded_dollar=140864321.5
                )
                
                # Future 15m interval: 13:45-14:00 (one 15m period ahead)
                future_start = datetime(2025, 7, 1, 13, 45)
                future_end = datetime(2025, 7, 1, 14, 0)
                future_instrument_interval = InstrumentInterval(
                    instrument_id=31,
                    start_date_time=future_start,
                    end_date_time=future_end,
                    open=207.83,  # Future open (starts from current close)
                    high=208.25,  # Future high (different aggregated value)
                    low=207.61,   # Future low
                    close=208.18, # Future close
                    traded_volume=701850,  # Different future aggregated volume
                    traded_dollar=145912876.3
                )
            
            # Create current UniverseStateInterval
            current_universe_interval = UniverseStateInterval(
                duration=TimeDuration(timeframe),
                start_date_time=current_start,
                end_date_time=current_end,
                factor_intervals=[],
                instrument_intervals={31: current_instrument_interval},
                instrument_indicator_intervals={
                    # Current technical indicators
                    'sma_20': {31: type('IndicatorInterval', (), {'value': 207.5 + (5 if timeframe == '5m' else 2)})()},
                    'rsi_14': {31: type('IndicatorInterval', (), {'value': 52.3 if timeframe == '5m' else 54.1})()}
                },
                universe_id=1
            )
            
            # Create future UniverseStateInterval
            future_universe_interval = UniverseStateInterval(
                duration=TimeDuration(timeframe),
                start_date_time=future_start,
                end_date_time=future_end,
                factor_intervals=[],
                instrument_intervals={31: future_instrument_interval},
                instrument_indicator_intervals={
                    # Future technical indicators (different from current)
                    'sma_20': {31: type('IndicatorInterval', (), {'value': 207.8 + (3 if timeframe == '5m' else 1)})()},
                    'rsi_14': {31: type('IndicatorInterval', (), {'value': 58.7 if timeframe == '5m' else 61.2})()}
                },
                universe_id=1
            )
            
            current_intervals[timeframe][current_time] = current_universe_interval
            future_intervals[timeframe][current_time] = future_universe_interval
            
        return current_intervals, future_intervals
        
    def get_universe_state_interval(self, timeframe: str, current_time: datetime, run_id: str = None) -> Optional[UniverseStateInterval]:
        """Mock implementation that returns pre-computed current intervals."""
        print(f"🏗️ MOCK: get_universe_state_interval called for {timeframe} at {current_time}")
        
        if timeframe in self.current_intervals:
            # Find the closest matching time
            for interval_time, interval in self.current_intervals[timeframe].items():
                # Check if current_time falls within this interval
                time_diff = abs((current_time - interval_time).total_seconds())
                if time_diff < 300:  # Within 5 minutes
                    print(f"✅ MOCK: Returning pre-computed current UniverseStateInterval for {timeframe}")
                    return interval
        
        print(f"❌ MOCK: No pre-computed current UniverseStateInterval found for {timeframe} at {current_time}")
        return None
    
    def get_future_universe_state_interval(self, timeframe: str, current_time: datetime, lead_periods: int = 1, run_id: str = None) -> Optional[UniverseStateInterval]:
        """Mock implementation that returns pre-computed future intervals."""
        print(f"🔮 MOCK: get_future_universe_state_interval called for {timeframe} at {current_time} (lead_periods={lead_periods})")
        
        if timeframe in self.future_intervals:
            # Find the closest matching time
            for interval_time, interval in self.future_intervals[timeframe].items():
                # Check if current_time matches
                time_diff = abs((current_time - interval_time).total_seconds())
                if time_diff < 300:  # Within 5 minutes
                    print(f"✅ MOCK: Returning pre-computed future UniverseStateInterval for {timeframe}")
                    return interval
        
        print(f"❌ MOCK: No pre-computed future UniverseStateInterval found for {timeframe} at {current_time}")
        return None
    
    def get_lead_prices(self, instrument_id: int, cur_datetime: datetime, lead_periods: int, time_interval: str = '1m') -> pd.DataFrame:
        """This method should NEVER be called since we removed fallbacks."""
        raise RuntimeError("get_lead_prices should NEVER be called - training data generator should only use UniverseStateInterval objects!")
    
    def get_lag_prices(self, instrument_id: int, cur_datetime: datetime, lag_periods: int, time_interval: str = '1m') -> pd.DataFrame:
        """This method should NEVER be called since we removed fallbacks."""
        raise RuntimeError("get_lag_prices should NEVER be called - training data generator should only use UniverseStateInterval objects!")
    
    async def get_lagged_signals(self, instrument_id: int, cur_datetime: datetime, lag_periods: int, time_interval: str = '1m', signal_names: List[str] = None) -> pd.DataFrame:
        """Mock lagged signals (not used since we get indicators from UniverseStateInterval)."""
        return pd.DataFrame()

class TestTrainingDataUsesFutureUniverseStateIntervals:
    """Test that training data generators use future UniverseStateInterval objects properly."""
    
    def __init__(self):
        self.mock_universe_manager = MockUniverseStateManagerWithFutureIntervals()
        
    async def test_get_timeframe_data_uses_future_universe_state_interval(self):
        """Test that get_timeframe_data uses future UniverseStateInterval instead of get_lead_prices."""
        print("\n🧪 TEST: get_timeframe_data uses future UniverseStateInterval instead of get_lead_prices")
        print("=" * 80)
        
        # Create training generator
        config = TrainingDataConfig(
            feature_types=['ohlcv', 'technical'],
            signal_names=['sma_20', 'rsi_14']
        )
        
        generator = TimeSeriesSequenceTrainingGenerator(
            env=Environment(env_type="test"),
            config=config,
            universe_manager=self.mock_universe_manager
        )
        
        # Test parameters
        instrument_id = 31  # AAPL
        test_datetime = datetime(2025, 7, 1, 13, 45)
        
        print(f"📅 Test datetime: {test_datetime}")
        print(f"🎯 Target instrument: {instrument_id}")
        
        # Test 5m future timeframe data extraction
        print(f"\n🔍 Testing 5m future timeframe data extraction...")
        if generator.sequence_builder:
            future_data_5m = await generator.sequence_builder.get_timeframe_data(instrument_id, test_datetime, '5m', is_future=True)
        else:
            print("❌ Sequence builder not available")
            return False
        
        # Test 15m future timeframe data extraction
        print(f"\n🔍 Testing 15m future timeframe data extraction...")
        future_data_15m = await generator.sequence_builder.get_timeframe_data(instrument_id, test_datetime, '15m', is_future=True)
        
        print(f"\n📊 FUTURE EXTRACTION RESULTS:")
        print(f"   5m future data fields: {len(future_data_5m)} items")
        print(f"   15m future data fields: {len(future_data_15m)} items")
        
        # Extract OHLCV values
        future_ohlcv_5m = self._extract_ohlcv_values(future_data_5m, '5m')
        future_ohlcv_15m = self._extract_ohlcv_values(future_data_15m, '15m')
        
        print(f"\n🔮 EXTRACTED FUTURE OHLCV VALUES:")
        print(f"   5m future:  O={future_ohlcv_5m['open']:.2f}, H={future_ohlcv_5m['high']:.2f}, L={future_ohlcv_5m['low']:.2f}, C={future_ohlcv_5m['close']:.2f}, V={future_ohlcv_5m['volume']}")
        print(f"   15m future: O={future_ohlcv_15m['open']:.2f}, H={future_ohlcv_15m['high']:.2f}, L={future_ohlcv_15m['low']:.2f}, C={future_ohlcv_15m['close']:.2f}, V={future_ohlcv_15m['volume']}")
        
        # CRITICAL TEST 1: Verify we got valid future data (no fallbacks exist)
        has_valid_future_data_5m = (future_ohlcv_5m['open'] > 0)
        has_valid_future_data_15m = (future_ohlcv_15m['open'] > 0)
        
        if not (has_valid_future_data_5m and has_valid_future_data_15m):
            print(f"\n🔴 FAILURE: No valid future data received!")
            print(f"   5m future data valid: {has_valid_future_data_5m}")
            print(f"   15m future data valid: {has_valid_future_data_15m}")
            print(f"   This means future UniverseStateInterval approach is not working")
            return False
        
        # CRITICAL TEST 2: Verify different future timeframes have different aggregated values
        future_values_are_different = (
            future_ohlcv_5m['open'] != future_ohlcv_15m['open'] or
            future_ohlcv_5m['volume'] != future_ohlcv_15m['volume'] or
            future_ohlcv_5m['close'] != future_ohlcv_15m['close']
        )
        
        if not future_values_are_different:
            print(f"\n🔴 FAILURE: 5m and 15m future timeframes have identical values!")
            print(f"   This suggests proper future aggregation is not working")
            return False
        
        # CRITICAL TEST 3: Verify expected pre-computed future values are used
        expected_5m_future_open = 207.83   # Future open should be current close
        expected_5m_future_close = 208.05
        expected_15m_future_open = 207.83  # Same future open but different aggregated values
        expected_15m_future_close = 208.18 # Different future close due to aggregation
        expected_15m_future_volume = 701850
        
        future_values_match_expected = (
            abs(future_ohlcv_5m['open'] - expected_5m_future_open) < 0.01 and
            abs(future_ohlcv_5m['close'] - expected_5m_future_close) < 0.01 and
            abs(future_ohlcv_15m['open'] - expected_15m_future_open) < 0.01 and
            abs(future_ohlcv_15m['close'] - expected_15m_future_close) < 0.01 and
            abs(future_ohlcv_15m['volume'] - expected_15m_future_volume) < 100
        )
        
        if not future_values_match_expected:
            print(f"\n🔴 FAILURE: Future values don't match expected pre-computed values!")
            print(f"   Expected 5m future open: {expected_5m_future_open}, got: {future_ohlcv_5m['open']}")
            print(f"   Expected 5m future close: {expected_5m_future_close}, got: {future_ohlcv_5m['close']}")
            print(f"   Expected 15m future open: {expected_15m_future_open}, got: {future_ohlcv_15m['open']}")
            print(f"   Expected 15m future close: {expected_15m_future_close}, got: {future_ohlcv_15m['close']}")
            print(f"   Expected 15m future volume: {expected_15m_future_volume}, got: {future_ohlcv_15m['volume']}")
            return False
        
        print(f"\n✅ SUCCESS: All future architectural tests passed!")
        print(f"   ✅ Future UniverseStateInterval approach used (no fallbacks exist)")
        print(f"   ✅ Different future timeframes have different aggregated values")
        print(f"   ✅ Future values match expected pre-computed results") 
        print(f"   ✅ Future training data uses same values as UniverseStateBuilder computed")
        
        return True
    
    async def test_future_vs_current_data_consistency(self):
        """Test that future data correctly follows from current data."""
        print("\n🧪 TEST: Future data correctly follows from current data")
        print("=" * 80)
        
        config = TrainingDataConfig(
            feature_types=['ohlcv', 'technical'],
            signal_names=['sma_20', 'rsi_14']
        )
        
        generator = TimeSeriesSequenceTrainingGenerator(
            env=Environment(env_type="test"),
            config=config,
            universe_manager=self.mock_universe_manager
        )
        
        instrument_id = 31
        test_datetime = datetime(2025, 7, 1, 13, 45)
        
        print(f"🔍 Testing current vs future data consistency for {test_datetime}")
        
        if generator.sequence_builder:
            # Get current 5m data
            current_data_5m = await generator.sequence_builder.get_timeframe_data(instrument_id, test_datetime, '5m', is_future=False)
            # Get future 5m data
            future_data_5m = await generator.sequence_builder.get_timeframe_data(instrument_id, test_datetime, '5m', is_future=True)
        else:
            print("❌ Sequence builder not available")
            return False
        
        # Extract OHLCV values
        current_ohlcv = self._extract_ohlcv_values(current_data_5m, '5m')
        future_ohlcv = self._extract_ohlcv_values(future_data_5m, '5m')
        
        print(f"\n📊 CURRENT vs FUTURE COMPARISON:")
        print(f"   Current 5m: O={current_ohlcv['open']:.2f}, C={current_ohlcv['close']:.2f}")
        print(f"   Future 5m:  O={future_ohlcv['open']:.2f}, C={future_ohlcv['close']:.2f}")
        
        # Test: Future open should equal current close (continuity)
        continuity_correct = abs(future_ohlcv['open'] - current_ohlcv['close']) < 0.01
        
        if continuity_correct:
            print(f"✅ SUCCESS: Future open ({future_ohlcv['open']:.2f}) correctly equals current close ({current_ohlcv['close']:.2f})")
            return True
        else:
            print(f"🔴 FAILURE: Future open ({future_ohlcv['open']:.2f}) does not equal current close ({current_ohlcv['close']:.2f})")
            print(f"   This indicates data continuity problems")
            return False
    
    def _extract_ohlcv_values(self, feature_data: Dict, timeframe: str) -> Dict:
        """Extract OHLCV values from feature data dictionary."""
        ohlcv = {'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0, 'volume': 0}
        
        # Try different possible key formats
        for ohlcv_key in ['open', 'high', 'low', 'close', 'volume']:
            # Try timeframe-prefixed key first
            prefixed_key = f"{timeframe}_{ohlcv_key}"
            if prefixed_key in feature_data:
                ohlcv[ohlcv_key] = feature_data[prefixed_key]
            elif ohlcv_key in feature_data:
                ohlcv[ohlcv_key] = feature_data[ohlcv_key]
            # Keep 0.0 default if not found
        
        return ohlcv
    
    async def run_all_tests(self):
        """Run all future UniverseStateInterval architecture tests."""
        print("🧪 FUTURE UNIVERSE STATE INTERVAL ARCHITECTURE TESTS")
        print("=" * 80)
        print("Testing that TimeSeriesSequenceTrainingGenerator uses pre-computed")
        print("FUTURE UniverseStateInterval objects instead of rebuilding data with get_lead_prices().")
        print()
        
        test_results = {}
        
        # Test 1: Future UniverseStateInterval usage
        test_results['future_universe_state_interval_usage'] = await self.test_get_timeframe_data_uses_future_universe_state_interval()
        
        # Test 2: Current vs Future data continuity
        test_results['future_data_continuity'] = await self.test_future_vs_current_data_consistency()
        
        print("\n" + "=" * 80)
        print("📋 FUTURE ARCHITECTURE TEST RESULTS:")
        print("=" * 80)
        
        all_passed = True
        for test_name, result in test_results.items():
            status = "✅ PASS" if result else "🔴 FAIL"
            print(f"{status} {test_name}")
            if not result:
                all_passed = False
        
        if all_passed:
            print("\n🎉 ALL TESTS PASSED - Future UniverseStateInterval architecture working correctly")
            print("✅ Future training data generators now use pre-computed intervals EXCLUSIVELY")
            print("✅ No more duplicate future computation - get_lead_prices eliminated")
            print("✅ Consistent future data between UniverseStateBuilder and training generators")
            print("✅ Proper data continuity between current and future intervals")
        else:
            print("\n💥 FUTURE ARCHITECTURE TESTS FAILED:")
            print("🔴 Future training data generators are NOT using UniverseStateInterval properly")
            print("🔴 No fallbacks exist - system must use UniverseStateInterval exclusively")
            print("🔴 Future data inconsistency between builder and generator")
            print("\n🛠️ REQUIRED FIXES:")
            print("1. Fix get_future_universe_state_interval() to return proper future intervals")
            print("2. Ensure rolling cache is populated with future UniverseStateInterval data") 
            print("3. Complete future indicator extraction from UniverseStateInterval objects")
        
        return all_passed

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the tests
    test_suite = TestTrainingDataUsesFutureUniverseStateIntervals()
    result = asyncio.run(test_suite.run_all_tests())
    
    if not result:
        print(f"\n🚨 CONCLUSION: Future UniverseStateInterval architecture needs fixes")
        exit(1)
    else:
        print(f"\n✅ CONCLUSION: Future UniverseStateInterval architecture working correctly")