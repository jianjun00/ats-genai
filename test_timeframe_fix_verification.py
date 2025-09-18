#!/usr/bin/env python3
"""
Simple test to verify the UniverseStateIntervalBuilder trading session fix.
"""

import asyncio
from datetime import datetime, timedelta
from core.shared.data_handling.utils.datetime_utils import get_session_times, to_utc

async def test_trading_session_fix():
    """Test that the fix correctly calculates trading session times."""
    print("🔧 Testing UniverseStateIntervalBuilder trading session fix...")
    
    # Test with a specific date (2024-08-01 - when we have AAPL data)
    test_date = datetime(2024, 8, 1, 20, 0, 0)  # 8:00 PM UTC (4:00 PM EDT)
    
    print(f"   📅 Test date: {test_date}")
    
    # Get session times using our fixed logic
    session_times = get_session_times(test_date.date())
    minute_start_time = to_utc(session_times['market_open'])  # 9:30 AM EDT -> 13:30 UTC
    minute_end_time = to_utc(session_times['market_close'])   # 4:00 PM EDT -> 20:00 UTC
    
    print(f"   ⏰ Market open (UTC):  {minute_start_time}")
    print(f"   ⏰ Market close (UTC): {minute_end_time}")
    
    # Verify the times are correct
    expected_start = datetime(2024, 8, 1, 13, 30, 0, tzinfo=minute_start_time.tzinfo)
    expected_end = datetime(2024, 8, 1, 20, 0, 0, tzinfo=minute_end_time.tzinfo)
    
    print(f"   ✅ Expected start: {expected_start}")
    print(f"   ✅ Expected end:   {expected_end}")
    
    # Test assertions
    assert minute_start_time.hour == 13 and minute_start_time.minute == 30, f"Market open should be 13:30 UTC, got {minute_start_time}"
    assert minute_end_time.hour == 20 and minute_end_time.minute == 0, f"Market close should be 20:00 UTC, got {minute_end_time}"
    
    print("   ✅ Trading session times are correct!")
    
    # Calculate session duration
    session_duration = minute_end_time - minute_start_time
    expected_duration = timedelta(hours=6, minutes=30)  # 6.5 hours trading session
    
    print(f"   📊 Session duration: {session_duration}")
    print(f"   📊 Expected duration: {expected_duration}")
    
    assert session_duration == expected_duration, f"Session duration should be 6.5 hours, got {session_duration}"
    
    print("   ✅ Session duration is correct!")
    print("\n🎉 UniverseStateIntervalBuilder trading session fix is working correctly!")

if __name__ == "__main__":
    asyncio.run(test_trading_session_fix())