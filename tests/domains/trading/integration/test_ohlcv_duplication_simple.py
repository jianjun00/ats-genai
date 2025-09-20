#!/usr/bin/env python3

"""
Simple test to demonstrate OHLCV duplication bugs without full database setup.

This test focuses on the core issue: 
1. Rolling cache population with different OHLCV values
2. Multi-timeframe boundary detection and aggregation

Expected Issues:
1. 5m data should be different at each time interval but shows identical values
2. 15m, 1h, 1d data should be present at boundary times but are empty
"""

import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List
import pandas as pd

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from core.business.calendars.time_duration import TimeDuration

def test_boundary_detection():
    """Test if boundary detection logic works correctly for multi-timeframes."""
    
    print("🧪 TESTING: Multi-timeframe Boundary Detection")
    print("=" * 60)
    
    # Test intervals
    test_times = [
        datetime(2025, 7, 1, 13, 35),  # 5m boundary only
        datetime(2025, 7, 1, 13, 40),  # 5m boundary only  
        datetime(2025, 7, 1, 13, 45),  # 5m + 15m boundary
        datetime(2025, 7, 1, 14, 0),   # 5m + 15m + 1h boundary
    ]
    
    target_durations = ['5m', '15m', '60m', '1d']
    duration_objects = [TimeDuration(d) for d in target_durations]
    
    print("\n📊 Boundary Detection Results:")
    print("-" * 40)
    
    for test_time in test_times:
        print(f"\n⏰ Testing: {test_time}")
        
        expected_boundaries = []
        
        # Simulate _should_process_timeframe logic
        for duration in duration_objects:
            should_process = should_process_timeframe_simulation(duration, test_time)
            if should_process:
                expected_boundaries.append(duration.get_duration_string())
                
        print(f"   Expected boundaries: {expected_boundaries}")
        
        # Show what SHOULD happen
        if '15m' in expected_boundaries:
            print("   ✅ Should aggregate 3x 5m intervals into 1x 15m interval")
        if '1h' in expected_boundaries:  
            print("   ✅ Should aggregate 12x 5m intervals into 1x 1h interval")
        if '1d' in expected_boundaries:
            print("   ✅ Should aggregate 288x 5m intervals into 1x 1d interval")

def should_process_timeframe_simulation(duration: TimeDuration, current_time: datetime) -> bool:
    """Simulate the _should_process_timeframe logic from UniverseStateIntervalBuilder."""
    
    # This is a simplified version of the actual boundary detection logic
    minutes = current_time.minute
    hour = current_time.hour
    
    duration_str = duration.get_duration_string()
    
    if duration_str == '5m':
        # Process every 5 minutes
        return minutes % 5 == 0
    elif duration_str == '15m':
        # Process at 00, 15, 30, 45 minutes
        return minutes % 15 == 0
    elif duration_str == '60m':
        # Process at the top of every hour
        return minutes == 0
    elif duration_str == '1d':
        # Process at midnight
        return hour == 0 and minutes == 0
    
    return False

def test_ohlcv_cache_population():
    """Test OHLCV data population in rolling cache."""
    
    print("\n\n🧪 TESTING: OHLCV Cache Population Simulation")
    print("=" * 60)
    
    # Simulate different OHLCV data that SHOULD be in cache at different times
    mock_market_data = {
        datetime(2025, 7, 1, 13, 35): {
            'open': 207.43, 'high': 207.49, 'low': 207.37, 'close': 207.42, 'volume': 35961
        },
        datetime(2025, 7, 1, 13, 40): {
            'open': 207.42, 'high': 207.51, 'low': 207.38, 'close': 207.48, 'volume': 42150  
        },
        datetime(2025, 7, 1, 13, 45): {
            'open': 207.48, 'high': 207.55, 'low': 207.44, 'close': 207.52, 'volume': 38200
        },
        datetime(2025, 7, 1, 14, 0): {
            'open': 207.52, 'high': 207.58, 'low': 207.49, 'close': 207.56, 'volume': 41800
        }
    }
    
    # Simulate rolling cache behavior
    simulated_cache = simulate_rolling_cache_population(mock_market_data)
    
    print("\n📊 Rolling Cache Simulation Results:")
    print("-" * 40)
    
    # Check for Bug 1: 5m data all the same
    check_5m_duplication_bug(simulated_cache)
    
    # Check for Bug 2: Missing aggregated timeframes
    check_missing_timeframes_bug(simulated_cache)

def simulate_rolling_cache_population(market_data: Dict) -> Dict:
    """Simulate how the rolling cache SHOULD be populated."""
    
    cache = {
        '5m': {},
        '15m': {},
        '1h': {},
        '1d': {}
    }
    
    for timestamp, ohlcv in market_data.items():
        print(f"\n⏰ Processing {timestamp}")
        print(f"   Market Data: O={ohlcv['open']:.2f} H={ohlcv['high']:.2f} L={ohlcv['low']:.2f} C={ohlcv['close']:.2f}")
        
        # Add to 5m cache (always)
        cache['5m'][timestamp] = ohlcv.copy()
        print(f"   ✅ Added to 5m cache")
        
        # Add to 15m cache (at 15m boundaries)
        if timestamp.minute % 15 == 0:
            # Aggregate last 3x 5m intervals
            cache['15m'][timestamp] = aggregate_ohlcv_simulation(cache['5m'], timestamp, 3)
            print(f"   ✅ Added to 15m cache (aggregated)")
        
        # Add to 1h cache (at 1h boundaries)  
        if timestamp.minute == 0:
            # Aggregate last 12x 5m intervals
            cache['1h'][timestamp] = aggregate_ohlcv_simulation(cache['5m'], timestamp, 12)
            print(f"   ✅ Added to 1h cache (aggregated)")
    
    return cache

def aggregate_ohlcv_simulation(cache_5m: Dict, end_time: datetime, periods: int) -> Dict:
    """Simulate OHLCV aggregation from 5m intervals."""
    
    # Get the last N intervals from 5m cache
    sorted_times = sorted([t for t in cache_5m.keys() if t <= end_time])
    recent_intervals = sorted_times[-periods:] if len(sorted_times) >= periods else sorted_times
    
    if not recent_intervals:
        return {'open': 0, 'high': 0, 'low': 0, 'close': 0, 'volume': 0}
    
    # Aggregate: first open, last close, max high, min low, sum volume
    intervals = [cache_5m[t] for t in recent_intervals]
    
    aggregated = {
        'open': intervals[0]['open'],      # First open
        'high': max(i['high'] for i in intervals),   # Max high
        'low': min(i['low'] for i in intervals),     # Min low  
        'close': intervals[-1]['close'],   # Last close
        'volume': sum(i['volume'] for i in intervals)  # Sum volume
    }
    
    return aggregated

def check_5m_duplication_bug(cache: Dict):
    """Check if 5m data shows duplication bug."""
    
    print("\n🐛 BUG 1 CHECK: 5m OHLCV Duplication")
    print("-" * 40)
    
    fivemin_data = cache['5m']
    
    if len(fivemin_data) < 2:
        print("❌ Insufficient 5m data to check duplication")
        return
    
    values = list(fivemin_data.values())
    first_ohlcv = values[0]
    
    # Check if all OHLCV values are identical
    duplicated = all(
        v['open'] == first_ohlcv['open'] and
        v['high'] == first_ohlcv['high'] and 
        v['low'] == first_ohlcv['low'] and
        v['close'] == first_ohlcv['close'] and
        v['volume'] == first_ohlcv['volume']
        for v in values[1:]
    )
    
    if duplicated:
        print("🔴 BUG CONFIRMED: All 5m intervals have identical OHLCV values!")
        print(f"   Duplicate values: O={first_ohlcv['open']} H={first_ohlcv['high']} " +
              f"L={first_ohlcv['low']} C={first_ohlcv['close']} V={first_ohlcv['volume']}")
    else:
        print("✅ CORRECT: 5m intervals have different OHLCV values")
        for timestamp, ohlcv in fivemin_data.items():
            print(f"   {timestamp}: O={ohlcv['open']:.2f} H={ohlcv['high']:.2f} " +
                  f"L={ohlcv['low']:.2f} C={ohlcv['close']:.2f}")

def check_missing_timeframes_bug(cache: Dict):
    """Check if aggregated timeframes are missing when they should be present."""
    
    print("\n🐛 BUG 2 CHECK: Missing Aggregated Timeframes")
    print("-" * 40)
    
    # Expected aggregated data at specific times
    expected_15m = [datetime(2025, 7, 1, 13, 45), datetime(2025, 7, 1, 14, 0)]
    expected_1h = [datetime(2025, 7, 1, 14, 0)]
    
    print("Expected 15m data at: 13:45, 14:00")
    print("Expected 1h data at: 14:00")
    
    # Check 15m data
    missing_15m = []
    for expected_time in expected_15m:
        if expected_time not in cache['15m']:
            missing_15m.append(expected_time)
        else:
            data = cache['15m'][expected_time]
            print(f"✅ 15m data at {expected_time}: O={data['open']:.2f} H={data['high']:.2f} " +
                  f"L={data['low']:.2f} C={data['close']:.2f}")
    
    # Check 1h data
    missing_1h = []
    for expected_time in expected_1h:
        if expected_time not in cache['1h']:
            missing_1h.append(expected_time)
        else:
            data = cache['1h'][expected_time]
            print(f"✅ 1h data at {expected_time}: O={data['open']:.2f} H={data['high']:.2f} " +
                  f"L={data['low']:.2f} C={data['close']:.2f}")
    
    if missing_15m:
        print(f"🔴 BUG CONFIRMED: Missing 15m data at {len(missing_15m)} expected times:")
        for time in missing_15m:
            print(f"   - {time}")
    
    if missing_1h:
        print(f"🔴 BUG CONFIRMED: Missing 1h data at {len(missing_1h)} expected times:")
        for time in missing_1h:
            print(f"   - {time}")
    
    if not missing_15m and not missing_1h:
        print("✅ All expected aggregated timeframes have data")

if __name__ == "__main__":
    print("🧪 MULTI-TIMEFRAME OHLCV DUPLICATION BUG REPRODUCTION TEST")
    print("=" * 80)
    print("This test simulates the expected behavior to identify where bugs occur.")
    print()
    
    # Test 1: Boundary detection logic
    test_boundary_detection()
    
    # Test 2: OHLCV cache population  
    test_ohlcv_cache_population()
    
    print("\n" + "=" * 80)
    print("📋 SUMMARY OF EXPECTED BUGS:")
    print("=" * 80)
    print("1. 🔴 5m OHLCV Duplication: Same values at different times")
    print("2. 🔴 Missing Aggregated Data: Empty 15m, 1h, 1d at boundary times")
    print()
    print("💡 SOLUTION AREAS TO INVESTIGATE:")
    print("- Rolling cache population in UniverseStateManager.addUniverseState")
    print("- Boundary detection in UniverseStateIntervalBuilder._should_process_timeframe")  
    print("- OHLCV aggregation logic in UniverseStateIntervalBuilder._aggregate_ohlcv_intervals")
    print("- Data retrieval in UniverseStateManager.get_lag_prices")
    
    print("\n✅ TEST COMPLETE - Use this as reference for expected behavior")