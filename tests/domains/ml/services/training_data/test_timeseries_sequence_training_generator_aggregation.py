#!/usr/bin/env python3

"""
Thorough test for TimeSeriesSequenceTrainingGenerator to verify proper timeframe aggregation.

This test uses realistic 1m market data and verifies that:
1. get_timeframe_data properly aggregates different timeframe periods  
2. 5m data aggregates 5 minutes of 1m data with correct OHLCV calculation
3. 15m data aggregates 15 minutes of 1m data with correct OHLCV calculation
4. Different timeframes produce DIFFERENT aggregated values
5. Aggregation follows proper OHLCV rules: first open, last close, max high, min low, sum volume

Expected Issue:
- get_timeframe_data currently calls get_lag_prices with lag_periods=1 for all timeframes
- This means all timeframes get the same single interval data instead of properly aggregated data
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
from core.platform.config.environment import Environment

class MockUniverseStateManager:
    """
    Mock universe state manager with realistic 1m data and proper timeframe aggregation.
    This simulates what the real UniverseStateManager SHOULD do for different timeframes.
    """
    
    def __init__(self):
        # Generate realistic 1-minute market data
        self.minute_data = self._create_realistic_market_data()
        print(f"📊 Mock data created: {len(self.minute_data)} minute intervals")
        
    def _create_realistic_market_data(self) -> Dict[datetime, Dict]:
        """Create 30 minutes of realistic AAPL-like market data."""
        data = {}
        start_time = datetime(2025, 7, 1, 13, 30)
        
        # Starting price similar to AAPL
        base_price = 207.50
        
        for i in range(30):  # 30 minutes of data
            timestamp = start_time + timedelta(minutes=i)
            
            # Simulate realistic intraday price movement
            price_drift = (i * 0.02)  # Slight upward trend
            volatility = np.random.normal(0, 0.08)  # Random volatility
            current_base = base_price + price_drift + volatility
            
            # Generate realistic OHLCV for this minute
            open_price = round(current_base, 2)
            high_price = round(open_price + abs(np.random.normal(0, 0.05)), 2)
            low_price = round(open_price - abs(np.random.normal(0, 0.05)), 2)
            close_price = round(open_price + np.random.normal(0, 0.03), 2)
            volume = int(np.random.normal(42000, 8000))
            
            # Ensure high >= max(open, close) and low <= min(open, close)
            high_price = max(high_price, open_price, close_price)
            low_price = min(low_price, open_price, close_price)
            
            data[timestamp] = {
                'timestamp': timestamp,
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': volume
            }
            
        return data
    
    def get_lag_prices(self, instrument_id: int, cur_datetime: datetime, lag_periods: int, time_interval: str = '1m') -> pd.DataFrame:
        """
        Mock lag prices that demonstrate PROPER timeframe aggregation.
        
        This shows what the real implementation SHOULD do:
        - For 5m timeframe: aggregate 5 minutes of 1m data  
        - For 15m timeframe: aggregate 15 minutes of 1m data
        - Each timeframe should return different aggregated OHLCV values
        """
        print(f"🔍 MOCK get_lag_prices: instrument_id={instrument_id}, cur_datetime={cur_datetime}")
        print(f"   lag_periods={lag_periods}, time_interval={time_interval}")
        
        # Parse timeframe interval in minutes
        interval_minutes = self._parse_interval_minutes(time_interval)
        print(f"   Parsed interval: {interval_minutes} minutes per period")
        
        # Calculate total time span needed
        total_minutes_needed = lag_periods * interval_minutes
        print(f"   Total minutes needed: {lag_periods} periods × {interval_minutes} min = {total_minutes_needed} min")
        
        # Get data for the required time span
        end_time = cur_datetime
        start_time = end_time - timedelta(minutes=total_minutes_needed)
        print(f"   Data range: {start_time} to {end_time}")
        
        # Extract 1m data for this range
        relevant_minute_data = []
        for timestamp, data in self.minute_data.items():
            if start_time <= timestamp < end_time:
                relevant_minute_data.append(data)
        
        relevant_minute_data.sort(key=lambda x: x['timestamp'])
        print(f"   Found {len(relevant_minute_data)} minute records in range")
        
        if not relevant_minute_data:
            print(f"   ❌ No data found for range")
            return pd.DataFrame()
        
        # Aggregate into requested timeframe periods
        aggregated_records = []
        
        for period_idx in range(lag_periods):
            # Calculate time range for this period (working backwards from cur_datetime)
            period_end = cur_datetime - timedelta(minutes=period_idx * interval_minutes)
            period_start = period_end - timedelta(minutes=interval_minutes)
            
            print(f"   📅 Period {period_idx + 1}: {period_start} to {period_end}")
            
            # Get 1m records for this specific period
            period_minute_records = [
                record for record in relevant_minute_data
                if period_start <= record['timestamp'] < period_end
            ]
            
            print(f"      Found {len(period_minute_records)} minute records for this period")
            
            if period_minute_records:
                # Apply proper OHLCV aggregation rules
                aggregated = self._aggregate_ohlcv_data(period_minute_records, period_end)
                aggregated_records.append(aggregated)
                
                print(f"      📊 Aggregated result: O={aggregated['open']:.2f}, H={aggregated['high']:.2f}")
                print(f"         L={aggregated['low']:.2f}, C={aggregated['close']:.2f}, V={aggregated['volume']}")
            else:
                print(f"      ⚠️ No data for this period")
        
        if not aggregated_records:
            print(f"   ❌ No aggregated records created")
            return pd.DataFrame()
        
        # Create DataFrame with aggregated data
        df = pd.DataFrame(aggregated_records)
        
        print(f"   ✅ Returning {len(df)} aggregated {time_interval} records")
        if len(df) > 0:
            print(f"      Sample: O={df.iloc[0]['open']:.2f}, H={df.iloc[0]['high']:.2f}, C={df.iloc[0]['close']:.2f}")
        
        return df
    
    def _aggregate_ohlcv_data(self, minute_records: List[Dict], period_end: datetime) -> Dict:
        """Aggregate 1-minute records into a single OHLCV record using proper rules."""
        if not minute_records:
            return {
                'timestamp': period_end,
                'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0, 'volume': 0
            }
        
        # Sort by timestamp to ensure proper order
        sorted_records = sorted(minute_records, key=lambda x: x['timestamp'])
        
        # Apply OHLCV aggregation rules
        open_price = sorted_records[0]['open']      # First open
        close_price = sorted_records[-1]['close']   # Last close
        high_price = max(record['high'] for record in sorted_records)  # Max high
        low_price = min(record['low'] for record in sorted_records)    # Min low
        total_volume = sum(record['volume'] for record in sorted_records)  # Sum volume
        
        return {
            'timestamp': period_end,
            'open': open_price,
            'high': high_price,
            'low': low_price,
            'close': close_price,
            'volume': total_volume
        }
    
    def _parse_interval_minutes(self, time_interval: str) -> int:
        """Parse time interval string to minutes."""
        if time_interval.endswith('m'):
            return int(time_interval[:-1])
        elif time_interval.endswith('h'):
            return int(time_interval[:-1]) * 60
        elif time_interval.endswith('d'):
            return int(time_interval[:-1]) * 1440
        else:
            return 1  # Default to 1 minute
    
    def get_lead_prices(self, instrument_id: int, cur_datetime: datetime, lead_periods: int, time_interval: str = '1m') -> pd.DataFrame:
        """Mock lead prices (future data) - not needed for this test."""
        return pd.DataFrame()
    
    async def get_lagged_signals(self, instrument_id: int, cur_datetime: datetime, lag_periods: int, time_interval: str = '1m', signal_names: List[str] = None) -> pd.DataFrame:
        """Mock technical indicators."""
        if not signal_names:
            return pd.DataFrame()
        
        records = []
        for i in range(lag_periods):
            timestamp = cur_datetime - timedelta(minutes=i)
            signal_record = {'timestamp': timestamp}
            
            for signal_name in signal_names:
                # Generate mock signal values
                if signal_name.startswith('sma'):
                    signal_record[signal_name] = np.random.normal(207, 2)  # Mock SMA around current price
                elif signal_name.startswith('rsi'):
                    signal_record[signal_name] = np.random.uniform(30, 70)  # Mock RSI
                else:
                    signal_record[signal_name] = np.random.normal(50, 10)
            
            records.append(signal_record)
        
        return pd.DataFrame(records) if records else pd.DataFrame()

class TestTimeSeriesSequenceTrainingGeneratorAggregation:
    """Test proper timeframe aggregation in TimeSeriesSequenceTrainingGenerator."""
    
    def __init__(self):
        self.mock_universe_manager = MockUniverseStateManager()
        
    async def test_get_timeframe_data_proper_aggregation(self):
        """
        Test that get_timeframe_data properly aggregates different timeframes.
        
        This is the CORE test - it verifies that:
        1. 5m data aggregates 5 minutes of 1m data
        2. 15m data aggregates 15 minutes of 1m data  
        3. Different timeframes produce different aggregated values
        """
        print("\n🧪 TEST: get_timeframe_data proper timeframe aggregation")
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
        instrument_id = 1
        test_datetime = datetime(2025, 7, 1, 13, 45)  # 15 minutes after data start
        
        print(f"📅 Test datetime: {test_datetime}")
        print(f"🎯 Testing timeframes: 5m and 15m")
        
        # Get 5m timeframe data via sequence builder
        print(f"\n🔍 Getting 5m timeframe data...")
        if generator.sequence_builder:
            data_5m = await generator.sequence_builder.get_timeframe_data(instrument_id, test_datetime, '5m', is_future=False)
        else:
            print("❌ Sequence builder not available")
            return False
        
        # Get 15m timeframe data via sequence builder
        print(f"\n🔍 Getting 15m timeframe data...")
        data_15m = await generator.sequence_builder.get_timeframe_data(instrument_id, test_datetime, '15m', is_future=False)
        
        print(f"\n📊 RESULTS:")
        print(f"   5m data fields: {len(data_5m)} items")
        print(f"   15m data fields: {len(data_15m)} items")
        
        # Extract OHLCV values
        ohlcv_5m = self._extract_ohlcv_values(data_5m, '5m')
        ohlcv_15m = self._extract_ohlcv_values(data_15m, '15m')
        
        print(f"\n💰 EXTRACTED OHLCV VALUES:")
        print(f"   5m:  O={ohlcv_5m['open']:.2f}, H={ohlcv_5m['high']:.2f}, L={ohlcv_5m['low']:.2f}, C={ohlcv_5m['close']:.2f}, V={ohlcv_5m['volume']}")
        print(f"   15m: O={ohlcv_15m['open']:.2f}, H={ohlcv_15m['high']:.2f}, L={ohlcv_15m['low']:.2f}, C={ohlcv_15m['close']:.2f}, V={ohlcv_15m['volume']}")
        
        # CRITICAL TEST: Check if values are different (indicating proper aggregation)
        values_are_different = (
            ohlcv_5m['open'] != ohlcv_15m['open'] or
            ohlcv_5m['high'] != ohlcv_15m['high'] or 
            ohlcv_5m['low'] != ohlcv_15m['low'] or
            ohlcv_5m['close'] != ohlcv_15m['close'] or
            ohlcv_5m['volume'] != ohlcv_15m['volume']
        )
        
        if not values_are_different:
            print("\n🔴 BUG CONFIRMED: 5m and 15m timeframes have IDENTICAL values!")
            print("   This indicates get_timeframe_data is NOT properly aggregating different timeframes")
            print("   Expected: 15m should aggregate 3x more data than 5m and have different values")
            return False
        else:
            print("\n✅ SUCCESS: 5m and 15m timeframes have DIFFERENT values (proper aggregation working)")
            
            # Additional validation: 15m volume should be higher (more data aggregated)
            if ohlcv_15m['volume'] > ohlcv_5m['volume']:
                print(f"✅ Volume validation passed: 15m volume ({ohlcv_15m['volume']}) > 5m volume ({ohlcv_5m['volume']})")
            else:
                print(f"⚠️ Volume validation concern: 15m volume ({ohlcv_15m['volume']}) not > 5m volume ({ohlcv_5m['volume']})")
            
            return True
    
    async def test_multiple_timeframes_different_aggregation_periods(self):
        """Test that multiple timeframes aggregate different numbers of periods correctly."""
        print("\n🧪 TEST: Multiple timeframes aggregate different periods correctly")
        print("=" * 80)
        
        config = TrainingDataConfig(
            feature_types=['ohlcv'],
            signal_names=[]
        )
        
        generator = TimeSeriesSequenceTrainingGenerator(
            env=Environment(env_type="test"),
            config=config,
            universe_manager=self.mock_universe_manager
        )
        
        # Test parameters
        instrument_id = 1
        test_datetime = datetime(2025, 7, 1, 13, 50)  # 20 minutes after data start
        test_timeframes = ['1m', '5m', '15m']
        
        print(f"📅 Test datetime: {test_datetime}")
        print(f"🎯 Testing timeframes: {test_timeframes}")
        
        # Get data for each timeframe
        timeframe_data = {}
        if not generator.sequence_builder:
            print("❌ Sequence builder not available")
            return False
            
        for timeframe in test_timeframes:
            print(f"\n🔍 Getting {timeframe} data...")
            data = await generator.sequence_builder.get_timeframe_data(instrument_id, test_datetime, timeframe, is_future=False)
            timeframe_data[timeframe] = data
        
        print(f"\n📊 AGGREGATION COMPARISON:")
        
        # Compare volumes (should increase with longer timeframes due to more data aggregation)
        volumes = {}
        for timeframe, data in timeframe_data.items():
            ohlcv = self._extract_ohlcv_values(data, timeframe)
            volumes[timeframe] = ohlcv['volume']
            print(f"   {timeframe}: Volume = {ohlcv['volume']}")
        
        # Logical test: longer timeframes should aggregate more data
        expected_volume_order = volumes['1m'] <= volumes['5m'] <= volumes['15m']
        
        if expected_volume_order:
            print(f"\n✅ SUCCESS: Volume increases with timeframe length (proper aggregation)")
            print(f"   1m ({volumes['1m']}) ≤ 5m ({volumes['5m']}) ≤ 15m ({volumes['15m']})")
            return True
        else:
            print(f"\n🔴 BUG CONFIRMED: Volume does NOT increase with timeframe length!")
            print(f"   This suggests aggregation is not working properly")
            print(f"   1m: {volumes['1m']}, 5m: {volumes['5m']}, 15m: {volumes['15m']}")
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
        """Run all aggregation tests."""
        print("🧪 TIMESERIES SEQUENCE TRAINING GENERATOR AGGREGATION TESTS")
        print("=" * 80)
        print("Testing TimeSeriesSequenceTrainingGenerator with realistic 1m market data")
        print("to verify that different timeframes properly aggregate underlying data.")
        print()
        
        test_results = {}
        
        # Test 1: Basic 5m vs 15m aggregation
        test_results['timeframe_aggregation'] = await self.test_get_timeframe_data_proper_aggregation()
        
        # Test 2: Multiple timeframes with expected volume ordering
        test_results['multiple_timeframes'] = await self.test_multiple_timeframes_different_aggregation_periods()
        
        print("\n" + "=" * 80)
        print("📋 AGGREGATION TEST RESULTS:")
        print("=" * 80)
        
        all_passed = True
        for test_name, result in test_results.items():
            status = "✅ PASS" if result else "🔴 FAIL" 
            print(f"{status} {test_name}")
            if not result:
                all_passed = False
        
        if all_passed:
            print("\n🎉 ALL TESTS PASSED - TimeSeriesSequenceTrainingGenerator aggregation working correctly")
        else:
            print("\n💥 AGGREGATION TESTS FAILED:")
            print("   🔴 get_timeframe_data is NOT properly aggregating different timeframes")
            print("   🔴 All timeframes are returning identical or incorrect values")
            print("   🔴 This confirms the bug: lag_periods=1 used for all timeframes")
            print("\n🛠️ REQUIRED FIXES:")
            print("   1. get_timeframe_data should call get_lag_prices with timeframe-appropriate periods")
            print("   2. 5m timeframe should aggregate 5 minutes of base data")
            print("   3. 15m timeframe should aggregate 15 minutes of base data")
            print("   4. Each timeframe should return different aggregated OHLCV values")
        
        return all_passed

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the tests
    test_suite = TestTimeSeriesSequenceTrainingGeneratorAggregation()
    result = asyncio.run(test_suite.run_all_tests())
    
    if not result:
        print(f"\n🚨 CONCLUSION: TimeSeriesSequenceTrainingGenerator aggregation bugs confirmed")
        print(f"   The fix should modify get_timeframe_data to use proper lag_periods for each timeframe")
        exit(1)
    else:
        print(f"\n✅ CONCLUSION: TimeSeriesSequenceTrainingGenerator aggregation working correctly")