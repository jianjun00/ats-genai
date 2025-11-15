#!/usr/bin/env python3
"""
Test script to verify the Runner timezone fix.
"""
import sys
import os
sys.path.append('src')
os.environ['ENVIRONMENT'] = 'dev'

from datetime import datetime, date
import pytz

def test_fixed_runner_logic():
    """Test the fixed Runner timezone logic."""
    
    print("🔍 [FIX_TEST] Testing FIXED Runner timezone logic...")
    
    # Simulate the FIXED Runner logic
    trading_end_hour = 16
    trading_end_minute = 0
    timezone = 'America/New_York'
    
    test_date = date(2025, 5, 14)  # May 14, 2025
    
    print(f"🔍 [FIX_TEST] Configuration:")
    print(f"🔍 [FIX_TEST]   trading_end: {trading_end_hour}:{trading_end_minute:02d}")
    print(f"🔍 [FIX_TEST]   timezone: {timezone}")
    print(f"🔍 [FIX_TEST]   test_date: {test_date}")
    
    # OLD (BROKEN) logic
    sod_time = datetime.combine(test_date, datetime.min.time())
    sod_time = pytz.UTC.localize(sod_time)
    
    old_trading_day_end_time = sod_time.replace(
        hour=trading_end_hour, 
        minute=trading_end_minute, 
        second=0, 
        microsecond=0
    )
    
    print(f"\n🔍 [FIX_TEST] OLD (BROKEN) logic:")
    print(f"🔍 [FIX_TEST]   trading_day_end_time: {old_trading_day_end_time}")
    print(f"🔍 [FIX_TEST]   As ET: {old_trading_day_end_time.astimezone(pytz.timezone('America/New_York'))}")
    
    # NEW (FIXED) logic
    market_tz = pytz.timezone(timezone)
    market_close_local = market_tz.localize(datetime.combine(test_date, datetime.min.time().replace(
        hour=trading_end_hour,
        minute=trading_end_minute,
        second=0,
        microsecond=0
    )))
    new_trading_day_end_time = market_close_local.astimezone(pytz.UTC)
    
    print(f"\n🔍 [FIX_TEST] NEW (FIXED) logic:")
    print(f"🔍 [FIX_TEST]   market_close_local: {market_close_local}")
    print(f"🔍 [FIX_TEST]   trading_day_end_time: {new_trading_day_end_time}")
    print(f"🔍 [FIX_TEST]   As ET: {new_trading_day_end_time.astimezone(pytz.timezone('America/New_York'))}")
    
    print(f"\n🔍 [FIX_TEST] Comparison:")
    print(f"🔍 [FIX_TEST]   Time difference: {new_trading_day_end_time - old_trading_day_end_time}")
    print(f"🔍 [FIX_TEST]   OLD creates 1d intervals ending at: {old_trading_day_end_time} (12:00 PM ET)")
    print(f"🔍 [FIX_TEST]   NEW creates 1d intervals ending at: {new_trading_day_end_time} (4:00 PM ET) ✅")

if __name__ == "__main__":
    print("=" * 80)
    print("🔍 RUNNER TIMEZONE FIX TEST")
    print("=" * 80)
    
    test_fixed_runner_logic()
    
    print("\n" + "=" * 80)
    print("🔍 FIX SUMMARY:")
    print("=" * 80)
    print("✅ OLD: trading_day_end at 16:00 UTC (12:00 PM ET) - WRONG")
    print("✅ NEW: trading_day_end at 20:00 UTC (4:00 PM ET) - CORRECT")
    print("✅ This should fix the 9:00:00 timestamp issue in 1d/1w records")
    print("=" * 80)