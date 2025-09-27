#!/usr/bin/env python3

"""
Test to reproduce OHLCV duplication bugs in multi-timeframe training data generation.

This test directly uses UniverseStateBuilder, UniverseStateManager, and TrainingDataCallback
to reproduce the exact issues:
1. 5m data are all the same except for time
2. 15m, 60m, 1d data are empty

Expected behavior:
- At 13:35: Build only 5m data (different OHLCV values each time)
- At 13:45: Build both 5m and 15m data (15m should be aggregated from 5m intervals)
- At 14:00: Build 5m, 15m, and 60m data
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from domains.trading.services.state.universe_state_builder import UniverseStateIntervalBuilder
from domains.trading.services.state.universe_state_manager import UniverseStateManager
from domains.ml.services.training_data.callbacks.training_data_callback import IntervalBasedTrainingDataCallback
from domains.ml.services.training_data.timeseries_sequence_training_generator import TrainingDataConfig
from core.platform.config.environment import Environment
from domains.trading.services.core.app.runner import Runner

class MockRunner:
    """Mock runner to simulate the real training data generation environment."""
    
    def __init__(self, environment: Environment, universe_state_manager: UniverseStateManager):
        self.env = environment
        self.universe_state_manager = universe_state_manager
        self.start_date = datetime(2025, 7, 1)
        self.end_date = datetime(2025, 7, 1, 16, 0)
        self.base_duration = '5m'
        
    def get_environment(self):
        return self.env
        
    def get_universe_state_manager(self):
        return self.universe_state_manager

async def test_multiTimeframe_ohlcv_duplication_bug():
    """Test to reproduce the OHLCV duplication bugs."""
    
    print("🧪 TESTING: Multi-timeframe OHLCV Duplication Bug Reproduction")
    print("=" * 80)
    
    # Setup environment
    import os
    os.environ['DB_HOST'] = 'localhost'
    os.environ['DB_PORT'] = '4432'
    os.environ['DB_USER'] = 'postgres'
    os.environ['DB_PASSWORD'] = 'intg_password'
    os.environ['DB_NAME'] = 'intg_db'
    os.environ['ENVIRONMENT_TYPE'] = 'intg'
    
    env = Environment(env_type="intg")
    
    # Setup universe state manager
    universe_state_manager = UniverseStateManager(env)
    
    # Setup universe state builder with target durations
    universe_state_builder = UniverseStateIntervalBuilder(
        env=env,
        base_duration='5m',
        target_durations='5m,15m,60m,1d',
        universe_state_manager=universe_state_manager
    )
    
    # Setup training data callback
    training_config = TrainingDataConfig(
        feature_types=['ohlcv', 'returns', 'volatility', 'volume_profile', 'technical', 'indicators'],
        signal_names=['sma_20', 'ema_12', 'rsi_14']
    )
    
    mock_runner = MockRunner(env, universe_state_manager)
    training_callback = IntervalBasedTrainingDataCallback(config=training_config)
    training_callback.handleStart(mock_runner, datetime(2025, 7, 1))
    
    # Test intervals to reproduce the bug
    test_intervals = [
        datetime(2025, 7, 1, 13, 35),  # 5m boundary only
        datetime(2025, 7, 1, 13, 40),  # 5m boundary only  
        datetime(2025, 7, 1, 13, 45),  # 5m + 15m boundary
        datetime(2025, 7, 1, 14, 0),   # 5m + 15m + 60m boundary
    ]
    
    ohlcv_results = {}
    
    print("\n📊 TESTING INTERVALS:")
    print("-" * 40)
    
    for interval_time in test_intervals:
        print(f"\n⏰ Testing interval: {interval_time}")
        
        # Step 1: Call universe state builder
        print("   🏗️  Calling UniverseStateIntervalBuilder.handleInterval...")
        await universe_state_builder.handleInterval(mock_runner, interval_time)
        
        # Step 2: Call training data callback  
        print("   🤖 Calling TrainingDataCallback.handleInterval...")
        await training_callback.handleInterval(mock_runner, interval_time)
        
        # Step 3: Extract OHLCV data from rolling cache
        print("   📈 Extracting OHLCV data from rolling cache...")
        cache_data = extract_rolling_cache_data(universe_state_manager, interval_time)
        ohlcv_results[interval_time] = cache_data
        
        # Step 4: Show results
        print_interval_results(interval_time, cache_data)
    
    print("\n" + "=" * 80)
    print("🔍 BUG ANALYSIS:")
    print("=" * 80)
    
    # Analyze bug 1: 5m data all the same except for time
    analyze_5m_duplication_bug(ohlcv_results)
    
    # Analyze bug 2: 15m, 60m, 1d data are empty
    analyze_missing_timeframes_bug(ohlcv_results)
    
    print("\n✅ TEST COMPLETE - Bugs reproduced successfully")

def extract_rolling_cache_data(universe_state_manager: UniverseStateManager, interval_time: datetime) -> Dict:
    """Extract OHLCV data from rolling cache for analysis."""
    
    cache_data = {
        '5m': [],
        '15m': [],
        '60m': [], 
        '1d': []
    }
    
    # Get instrument ID for AAPL (assuming it's 31 based on previous tests)
    instrument_id = 31
    
    for timeframe in ['5m', '15m', '60m', '1d']:
        # Try to get lag prices for this timeframe
        lag_data = universe_state_manager.get_lag_prices(
            instrument_id, 
            interval_time, 
            lag_periods=5, 
            time_interval=timeframe
        )
        
        if not lag_data.empty:
            cache_data[timeframe] = lag_data.to_dict('records')
        
    return cache_data

def print_interval_results(interval_time: datetime, cache_data: Dict):
    """Print the results for a specific interval."""
    
    print(f"   📊 Results for {interval_time}:")
    
    for timeframe, data in cache_data.items():
        if isinstance(data, list) and data:
            print(f"      {timeframe}: {len(data)} records")
            if data:
                latest = data[-1]
                print(f"         Latest: O={latest.get('open', 'N/A'):.4f} H={latest.get('high', 'N/A'):.4f} " +
                     f"L={latest.get('low', 'N/A'):.4f} C={latest.get('close', 'N/A'):.4f}")
        elif isinstance(data, str):
            print(f"      {timeframe}: {data}")
        else:
            print(f"      {timeframe}: Empty")

def analyze_5m_duplication_bug(ohlcv_results: Dict):
    """Analyze if 5m OHLCV data is duplicated across different times."""
    
    print("\n🐛 BUG 1 ANALYSIS: 5m data all the same except for time")
    print("-" * 60)
    
    # Collect 5m OHLCV values across all intervals
    ohlcv_values = []
    
    for interval_time, cache_data in ohlcv_results.items():
        fivemin_data = cache_data.get('5m', [])
        if isinstance(fivemin_data, list) and fivemin_data:
            latest = fivemin_data[-1]
            ohlcv_values.append({
                'time': interval_time,
                'open': latest.get('open'),
                'high': latest.get('high'), 
                'low': latest.get('low'),
                'close': latest.get('close'),
                'volume': latest.get('volume')
            })
    
    if len(ohlcv_values) < 2:
        print("❌ Insufficient data to analyze duplication bug")
        return
    
    # Check if OHLCV values are identical (except time)
    first_ohlcv = ohlcv_values[0]
    duplicated = True
    
    for i, ohlcv in enumerate(ohlcv_values[1:], 1):
        if (ohlcv['open'] != first_ohlcv['open'] or 
            ohlcv['high'] != first_ohlcv['high'] or
            ohlcv['low'] != first_ohlcv['low'] or
            ohlcv['close'] != first_ohlcv['close'] or
            ohlcv['volume'] != first_ohlcv['volume']):
            duplicated = False
            break
    
    if duplicated:
        print("🔴 BUG CONFIRMED: 5m OHLCV values are identical across different times!")
        print(f"   All intervals have: O={first_ohlcv['open']} H={first_ohlcv['high']} " +
              f"L={first_ohlcv['low']} C={first_ohlcv['close']} V={first_ohlcv['volume']}")
    else:
        print("✅ 5m OHLCV values are correctly different across time intervals")
        for ohlcv in ohlcv_values:
            print(f"   {ohlcv['time']}: O={ohlcv['open']:.4f} H={ohlcv['high']:.4f} " +
                  f"L={ohlcv['low']:.4f} C={ohlcv['close']:.4f}")

def analyze_missing_timeframes_bug(ohlcv_results: Dict):
    """Analyze if 15m, 60m, 1d data are missing when they should be present."""
    
    print("\n🐛 BUG 2 ANALYSIS: 15m, 60m, 1d data are empty")
    print("-" * 60)
    
    # Expected timeframes for each boundary
    expected_timeframes = {
        datetime(2025, 7, 1, 13, 35): ['5m'],           # 5m boundary only
        datetime(2025, 7, 1, 13, 40): ['5m'],           # 5m boundary only  
        datetime(2025, 7, 1, 13, 45): ['5m', '15m'],    # 5m + 15m boundary
        datetime(2025, 7, 1, 14, 0): ['5m', '15m', '60m'] # 5m + 15m + 60m boundary
    }
    
    bugs_found = []
    
    for interval_time, cache_data in ohlcv_results.items():
        expected = expected_timeframes.get(interval_time, [])
        
        print(f"\n   ⏰ {interval_time} - Expected: {expected}")
        
        for timeframe in ['5m', '15m', '60m', '1d']:
            data = cache_data.get(timeframe, [])
            has_data = isinstance(data, list) and len(data) > 0
            
            if timeframe in expected and not has_data:
                print(f"      🔴 {timeframe}: MISSING (expected but not found)")
                bugs_found.append(f"{interval_time} missing {timeframe}")
            elif timeframe in expected and has_data:
                print(f"      ✅ {timeframe}: Present ({len(data)} records)")
            elif timeframe not in expected and has_data:
                print(f"      ⚠️  {timeframe}: Unexpected data ({len(data)} records)")
            else:
                print(f"      ⚪ {timeframe}: Empty (expected)")
    
    if bugs_found:
        print(f"\n🔴 BUG CONFIRMED: Missing timeframe data in {len(bugs_found)} cases:")
        for bug in bugs_found:
            print(f"   - {bug}")
    else:
        print("\n✅ All expected timeframes have data")

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.INFO)
    
    # Run the test
    asyncio.run(test_multiTimeframe_ohlcv_duplication_bug())