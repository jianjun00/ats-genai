#!/usr/bin/env python3
"""
Test script to simulate Runner event generation and see when trading_day_end events are generated.
"""
import sys
import os
sys.path.append('src')
os.environ['ENVIRONMENT'] = 'dev'

from datetime import datetime, date
import pytz
from core.platform.config_env.environment import Environment

def test_runner_event_generation():
    """Simulate Runner's iter_events method to see when trading_day_end events are generated."""
    
    print("🔍 [RUNNER_TEST] Testing Runner event generation logic...")
    
    # Simulate Runner configuration
    trading_start_hour = 9
    trading_start_minute = 35
    trading_end_hour = 16
    trading_end_minute = 0
    timezone = 'America/New_York'
    
    # Test date
    test_date = date(2025, 5, 14)  # May 14, 2025 (a Wednesday)
    
    print(f"🔍 [RUNNER_TEST] Configuration:")
    print(f"🔍 [RUNNER_TEST]   trading_start: {trading_start_hour}:{trading_start_minute:02d}")
    print(f"🔍 [RUNNER_TEST]   trading_end: {trading_end_hour}:{trading_end_minute:02d}")
    print(f"🔍 [RUNNER_TEST]   timezone: {timezone}")
    print(f"🔍 [RUNNER_TEST]   test_date: {test_date}")
    
    # Simulate the logic from Runner.iter_events
    sod_time = datetime.combine(test_date, datetime.min.time())
    sod_time = pytz.UTC.localize(sod_time)
    
    print(f"\n🔍 [RUNNER_TEST] Start of day (SOD) time: {sod_time}")
    
    # Market open time
    market_open_local = pytz.timezone(timezone).localize(
        datetime.combine(test_date, datetime.min.time().replace(
            hour=trading_start_hour, 
            minute=trading_start_minute
        ))
    )
    market_open_utc = market_open_local.astimezone(pytz.UTC)
    
    print(f"🔍 [RUNNER_TEST] Market open (local): {market_open_local}")
    print(f"🔍 [RUNNER_TEST] Market open (UTC): {market_open_utc}")
    
    # Market close time  
    market_close_local = pytz.timezone(timezone).localize(
        datetime.combine(test_date, datetime.min.time().replace(
            hour=trading_end_hour,
            minute=trading_end_minute
        ))
    )
    market_close_utc = market_close_local.astimezone(pytz.UTC)
    
    print(f"🔍 [RUNNER_TEST] Market close (local): {market_close_local}")
    print(f"🔍 [RUNNER_TEST] Market close (UTC): {market_close_utc}")
    
    # Trading day end event (what Runner actually generates)
    trading_day_end_time = sod_time.replace(
        hour=trading_end_hour, 
        minute=trading_end_minute, 
        second=0, 
        microsecond=0
    )
    
    print(f"\n🔍 [RUNNER_TEST] ⚠️  PROBLEM FOUND!")
    print(f"🔍 [RUNNER_TEST] Runner generates trading_day_end at: {trading_day_end_time}")
    print(f"🔍 [RUNNER_TEST] This is using UTC time, NOT timezone-aware time!")
    print(f"🔍 [RUNNER_TEST] Should be: {market_close_utc}")
    
    print(f"\n🔍 [RUNNER_TEST] Time difference:")
    print(f"🔍 [RUNNER_TEST]   Current (wrong): {trading_day_end_time}")
    print(f"🔍 [RUNNER_TEST]   Should be (correct): {market_close_utc}")
    print(f"🔍 [RUNNER_TEST]   Difference: {market_close_utc - trading_day_end_time}")

def test_timezone_conversion():
    """Test timezone conversion issues."""
    
    print(f"\n🔍 [TIMEZONE_TEST] Testing timezone conversion...")
    
    # May 14, 2025 in different timezones
    et_tz = pytz.timezone('America/New_York')
    
    # 4:00 PM ET
    local_4pm = et_tz.localize(datetime(2025, 5, 14, 16, 0, 0))
    utc_4pm = local_4pm.astimezone(pytz.UTC)
    
    print(f"🔍 [TIMEZONE_TEST] 4:00 PM ET: {local_4pm}")
    print(f"🔍 [TIMEZONE_TEST] 4:00 PM ET in UTC: {utc_4pm}")
    print(f"🔍 [TIMEZONE_TEST] UTC hour: {utc_4pm.hour} (should be 20 or 21 depending on DST)")
    
    # What if Runner sets hour=16 directly on UTC?
    wrong_utc = pytz.UTC.localize(datetime(2025, 5, 14, 16, 0, 0))
    print(f"🔍 [TIMEZONE_TEST] Wrong UTC (hour=16): {wrong_utc}")
    print(f"🔍 [TIMEZONE_TEST] Wrong UTC as ET: {wrong_utc.astimezone(et_tz)}")

if __name__ == "__main__":
    print("=" * 80)
    print("🔍 RUNNER EVENT GENERATION TEST")
    print("=" * 80)
    
    test_runner_event_generation()
    test_timezone_conversion()
    
    print("\n" + "=" * 80)
    print("🔍 ROOT CAUSE IDENTIFIED:")
    print("=" * 80)
    print("The Runner is setting trading_day_end_time by replacing hours directly on UTC time")
    print("instead of using proper timezone conversion. This causes:")
    print("1. 4:00 PM ET should be ~20:00 UTC (depending on DST)")
    print("2. But Runner sets hour=16 on UTC time, making it 4:00 PM UTC")
    print("3. This results in daily intervals being created at wrong times")
    print("=" * 80)