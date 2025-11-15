#!/usr/bin/env python3
"""
Test script to reproduce and debug the 9:00:00 timestamp issue for 1d/1w intervals.
"""
import sys
import os
sys.path.append('src')
os.environ['ENVIRONMENT'] = 'dev'

from datetime import datetime, timedelta
import pytz
from core.business.calendars.time_duration import TimeDuration

def test_trading_day_end_timestamp():
    """Test what happens when handleTradingDayEnd is called at different times."""
    
    print("🔍 [TEST] Testing trading day end timestamp calculation...")
    
    # Simulate different times when handleTradingDayEnd might be called
    test_times = [
        "2025-05-14 09:00:00",  # 9:00 AM - the problematic timestamp we're seeing
        "2025-05-14 16:00:00",  # 4:00 PM - when it should be called
        "2025-05-14 20:00:00",  # 8:00 PM - after hours
    ]
    
    duration_1d = TimeDuration("1d")
    
    for time_str in test_times:
        current_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        current_time = pytz.UTC.localize(current_time)
        
        # This is what the interval creation code does
        start_time = duration_1d.get_start_time(current_time)
        end_time = current_time
        
        print(f"\n🔍 [TEST] If handleTradingDayEnd called at: {current_time}")
        print(f"🔍 [TEST]   Daily interval start_time: {start_time}")
        print(f"🔍 [TEST]   Daily interval end_time: {end_time}")
        print(f"🔍 [TEST]   Duration: {end_time - start_time}")

def test_runner_event_generation():
    """Test when Runner generates trading_day_end events."""
    
    print("\n🔍 [TEST] Testing Runner event generation...")
    
    # Simulate what Runner.iter_events does
    trading_end_hour = 16  # 4:00 PM
    trading_end_minute = 0
    
    # Test date
    day = datetime(2025, 5, 14).date()
    
    # This mimics Runner's logic
    sod_time = datetime.combine(day, datetime.min.time())
    sod_time = pytz.UTC.localize(sod_time)
    
    trading_day_end_time = sod_time.replace(
        hour=trading_end_hour, 
        minute=trading_end_minute, 
        second=0, 
        microsecond=0
    )
    
    print(f"\n🔍 [TEST] Runner should generate trading_day_end at: {trading_day_end_time}")
    print(f"🔍 [TEST] This should result in 1d intervals ending at: {trading_day_end_time}")

def test_time_duration_calculation():
    """Test TimeDuration.get_start_time calculation."""
    
    print("\n🔍 [TEST] Testing TimeDuration calculation...")
    
    duration_1d = TimeDuration("1d")
    
    # Test with the problematic 9:00 AM time
    end_time_9am = datetime(2025, 5, 14, 9, 0, 0)
    end_time_9am = pytz.UTC.localize(end_time_9am)
    
    start_time_9am = duration_1d.get_start_time(end_time_9am)
    
    print(f"\n🔍 [TEST] TimeDuration('1d').get_start_time() test:")
    print(f"🔍 [TEST]   end_time: {end_time_9am}")
    print(f"🔍 [TEST]   start_time: {start_time_9am}")
    print(f"🔍 [TEST]   This creates a daily bar from {start_time_9am} to {end_time_9am}")
    
    # Test with proper 4:00 PM time
    end_time_4pm = datetime(2025, 5, 14, 16, 0, 0)
    end_time_4pm = pytz.UTC.localize(end_time_4pm)
    
    start_time_4pm = duration_1d.get_start_time(end_time_4pm)
    
    print(f"\n🔍 [TEST] Proper 4:00 PM calculation:")
    print(f"🔍 [TEST]   end_time: {end_time_4pm}")
    print(f"🔍 [TEST]   start_time: {start_time_4pm}")
    print(f"🔍 [TEST]   This creates a daily bar from {start_time_4pm} to {end_time_4pm}")

if __name__ == "__main__":
    print("=" * 80)
    print("🔍 TIMESTAMP DEBUG TEST")
    print("=" * 80)
    
    test_trading_day_end_timestamp()
    test_runner_event_generation()
    test_time_duration_calculation()
    
    print("\n" + "=" * 80)
    print("🔍 ANALYSIS:")
    print("=" * 80)
    print("If you see 1d records with 9:00:00 timestamps, it means:")
    print("1. handleTradingDayEnd is being called at 9:00 AM instead of 4:00 PM")
    print("2. OR there's a fallback creating daily intervals at wrong times")
    print("3. OR timezone conversion is shifting 4:00 PM to 9:00 AM")
    print("=" * 80)