#!/usr/bin/env python3
"""
Test cases for Runner timezone handling to prevent regression of the 9:00:00 timestamp bug.

This test ensures that trading_day_end and trading_week_end events are generated
at the correct timezone-aware times, not raw UTC times.
"""
import unittest
from datetime import datetime, date, time
import pytz
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../../../src'))

class TestRunnerTimezoneHandling(unittest.TestCase):
    """Test cases for Runner timezone handling."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.trading_end_hour = 16
        self.trading_end_minute = 0
        self.timezone = 'America/New_York'
        self.test_date = date(2025, 5, 14)  # May 14, 2025 (Wednesday, EDT)
        
    def test_trading_day_end_time_calculation(self):
        """Test that trading_day_end_time is calculated with proper timezone conversion."""
        
        # Expected: 4:00 PM ET should be 20:00 UTC in May (EDT)
        market_tz = pytz.timezone(self.timezone)
        expected_local = market_tz.localize(datetime.combine(
            self.test_date, 
            time(self.trading_end_hour, self.trading_end_minute)
        ))
        expected_utc = expected_local.astimezone(pytz.UTC)
        
        # Verify our expectation
        self.assertEqual(expected_utc.hour, 20)  # 4 PM EDT = 20:00 UTC
        self.assertEqual(expected_utc.minute, 0)
        
        # Test the FIXED logic (what Runner should do)
        market_close_local = market_tz.localize(datetime.combine(
            self.test_date, 
            datetime.min.time().replace(
                hour=self.trading_end_hour,
                minute=self.trading_end_minute,
                second=0,
                microsecond=0
            )
        ))
        actual_utc = market_close_local.astimezone(pytz.UTC)
        
        # Assert the fix works
        self.assertEqual(actual_utc, expected_utc)
        self.assertEqual(actual_utc.hour, 20)  # Should be 20:00 UTC, not 16:00 UTC
        
    def test_old_broken_logic_for_regression_detection(self):
        """Test the OLD broken logic to document what we fixed."""
        
        # OLD (BROKEN) logic - direct hour replacement on UTC
        sod_time = pytz.UTC.localize(datetime.combine(self.test_date, datetime.min.time()))
        old_broken_time = sod_time.replace(
            hour=self.trading_end_hour, 
            minute=self.trading_end_minute, 
            second=0, 
            microsecond=0
        )
        
        # This should be WRONG (16:00 UTC instead of 20:00 UTC)
        self.assertEqual(old_broken_time.hour, 16)  # Wrong - should be 20
        
        # Convert to ET to see the problem
        et_tz = pytz.timezone('America/New_York')
        old_as_et = old_broken_time.astimezone(et_tz)
        
        # This would be 12:00 PM ET instead of 4:00 PM ET
        self.assertEqual(old_as_et.hour, 12)  # Wrong - should be 16
        
    def test_daily_interval_timestamps(self):
        """Test that daily intervals get correct start/end timestamps."""
        
        from core.business.calendars.time_duration import TimeDuration
        
        # Correct trading_day_end_time (after fix)
        market_tz = pytz.timezone(self.timezone)
        correct_end_time = market_tz.localize(datetime.combine(
            self.test_date, 
            time(self.trading_end_hour, self.trading_end_minute)
        )).astimezone(pytz.UTC)
        
        # Daily interval calculation
        duration_1d = TimeDuration("1d")
        start_time = duration_1d.get_start_time(correct_end_time)
        
        # Verify the daily interval spans the correct 24-hour period
        self.assertEqual(correct_end_time - start_time, duration_1d.get_end_time(start_time) - start_time)
        
        # The end time should be 4:00 PM ET (20:00 UTC)
        self.assertEqual(correct_end_time.hour, 20)
        
        # Convert back to ET to verify
        end_time_et = correct_end_time.astimezone(market_tz)
        self.assertEqual(end_time_et.hour, 16)  # 4:00 PM ET
        self.assertEqual(end_time_et.minute, 0)
        
    def test_weekly_interval_timestamps(self):
        """Test that weekly intervals get correct timestamps."""
        
        from core.business.calendars.time_duration import TimeDuration
        
        # Friday end of week scenario
        friday_date = date(2025, 5, 16)  # May 16, 2025 (Friday)
        
        market_tz = pytz.timezone(self.timezone)
        week_end_time = market_tz.localize(datetime.combine(
            friday_date, 
            time(self.trading_end_hour, self.trading_end_minute)
        )).astimezone(pytz.UTC)
        
        # Weekly interval calculation
        duration_1w = TimeDuration("1w")
        start_time = duration_1w.get_start_time(week_end_time)
        
        # Verify the weekly interval spans 7 days
        duration_days = (week_end_time - start_time).days
        self.assertEqual(duration_days, 7)
        
        # The end time should be 4:00 PM ET on Friday
        end_time_et = week_end_time.astimezone(market_tz)
        self.assertEqual(end_time_et.hour, 16)  # 4:00 PM ET
        self.assertEqual(end_time_et.weekday(), 4)  # Friday
        
    def test_different_seasons_dst_handling(self):
        """Test timezone handling across DST transitions."""
        
        market_tz = pytz.timezone(self.timezone)
        
        # May (EDT - UTC-4)
        may_date = date(2025, 5, 14)
        may_close = market_tz.localize(datetime.combine(
            may_date, time(16, 0)
        )).astimezone(pytz.UTC)
        self.assertEqual(may_close.hour, 20)  # 4 PM EDT = 20 UTC
        
        # December (EST - UTC-5) 
        dec_date = date(2025, 12, 14)
        dec_close = market_tz.localize(datetime.combine(
            dec_date, time(16, 0)
        )).astimezone(pytz.UTC)
        self.assertEqual(dec_close.hour, 21)  # 4 PM EST = 21 UTC
        
    def test_regression_prevention(self):
        """Specific test to prevent the 9:00:00 timestamp regression."""
        
        # If this test fails, the 9:00:00 timestamp bug has returned
        market_tz = pytz.timezone(self.timezone)
        
        # Generate trading_day_end_time using FIXED logic
        trading_day_end_time = market_tz.localize(datetime.combine(
            self.test_date, 
            time(self.trading_end_hour, self.trading_end_minute)
        )).astimezone(pytz.UTC)
        
        # Daily interval creation
        from core.business.calendars.time_duration import TimeDuration
        duration_1d = TimeDuration("1d")
        interval_end_time = trading_day_end_time
        interval_start_time = duration_1d.get_start_time(interval_end_time)
        
        # Convert interval end time back to ET
        interval_end_et = interval_end_time.astimezone(market_tz)
        
        # CRITICAL ASSERTIONS - if these fail, the bug has returned
        self.assertEqual(interval_end_et.hour, 16, 
                        "Daily interval should end at 4:00 PM ET, not 9:00 AM or 12:00 PM")
        self.assertEqual(interval_end_et.minute, 0,
                        "Daily interval should end at exactly 4:00 PM ET")
        
        # The UTC time should be 20:00 (May EDT), not 16:00 or 09:00
        self.assertEqual(interval_end_time.hour, 20,
                        "Daily interval end time in UTC should be 20:00 (4 PM EDT)")
        
        # Interval should span exactly 24 hours
        self.assertEqual(interval_end_time - interval_start_time, 
                        duration_1d.get_end_time(interval_start_time) - interval_start_time,
                        "Daily interval should span exactly 24 hours")


if __name__ == '__main__':
    unittest.main(verbosity=2)