#!/usr/bin/env python3
"""
Comprehensive tests for Runner interval generation.

This test file catches the critical bug where Runner only generates one interval
per day instead of multiple intraday intervals based on base_duration.
"""

import pytest
import sys
import os
from datetime import datetime, timedelta
from typing import List, Tuple
from unittest.mock import Mock, patch

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../src'))

from services.app.runner import Runner
from core.platform.config.environment import Environment, EnvironmentType


class TestRunnerIntervalGeneration:
    """Test Runner's interval generation logic for different base_duration values."""

    def setup_method(self):
        """Setup test environment and mocks."""
        # Create mock environment
        self.mock_env = Mock(spec=Environment)
        self.mock_env.env_type = EnvironmentType.TEST

        # Mock dependencies to avoid database calls
        self.mock_security_master = Mock()
        self.mock_universe_state_manager = Mock()
        self.mock_universe_manager = Mock()
        self.mock_market_data_manager = Mock()
        self.mock_market_data_manager.exchange = 'NYSE'

    def create_runner(self, start_date: str, end_date: str, base_duration: str) -> Runner:
        """Create a Runner instance with mocked dependencies."""
        with patch('services.app.runner.create_run_context'):
            runner = Runner(
                start_date=start_date,
                end_date=end_date,
                environment=self.mock_env,
                universe_id=1,
                callbacks=[],
                base_duration=base_duration,
                security_master=self.mock_security_master,
                universe_state_manager=self.mock_universe_state_manager,
                universe_manager=self.mock_universe_manager,
                market_data_manager=self.mock_market_data_manager,
                enable_run_isolation=False  # Disable to avoid run context creation
            )
        return runner

    def extract_intervals(self, events: List[Tuple[datetime, str]]) -> List[datetime]:
        """Extract just the interval events from the event stream."""
        return [event_time for event_time, event_type in events if event_type == 'interval']

    @patch('services.app.runner.ExchangeCalendar')
    def test_60_minute_intervals_single_day(self, mock_calendar_class):
        """Test that 60-minute base_duration generates 24 intervals for a single day."""
        # Mock trading day
        mock_calendar = Mock()
        mock_calendar.all_trading_days.return_value = [datetime(2025, 7, 1).date()]
        mock_calendar_class.return_value = mock_calendar

        runner = self.create_runner('2025-07-01', '2025-07-01', '60m')
        events = list(runner.iter_events())
        intervals = self.extract_intervals(events)

        # Should generate 24 intervals for 60-minute base_duration
        expected_intervals = [
            datetime(2025, 7, 1, hour, 0) for hour in range(24)
        ]

        print(f"🔍 [TEST] Expected {len(expected_intervals)} intervals for 60m base_duration")
        print(f"🔍 [TEST] Actual intervals found: {len(intervals)}")
        print(f"🔍 [TEST] First few intervals: {intervals[:5] if intervals else 'None'}")

        assert len(intervals) == 24, f"Expected 24 intervals for 60m duration, got {len(intervals)}"

        for i, expected_time in enumerate(expected_intervals):
            assert intervals[i] == expected_time, f"Interval {i}: expected {expected_time}, got {intervals[i] if i < len(intervals) else 'missing'}"

    @patch('services.app.runner.ExchangeCalendar')
    def test_30_minute_intervals_single_day(self, mock_calendar_class):
        """Test that 30-minute base_duration generates 48 intervals for a single day."""
        # Mock trading day
        mock_calendar = Mock()
        mock_calendar.all_trading_days.return_value = [datetime(2025, 7, 1).date()]
        mock_calendar_class.return_value = mock_calendar

        runner = self.create_runner('2025-07-01', '2025-07-01', '30m')
        events = list(runner.iter_events())
        intervals = self.extract_intervals(events)

        # Should generate 48 intervals for 30-minute base_duration
        expected_intervals = []
        current = datetime(2025, 7, 1, 0, 0)
        for _ in range(48):  # 24 hours * 2 intervals per hour
            expected_intervals.append(current)
            current += timedelta(minutes=30)

        print(f"🔍 [TEST] Expected {len(expected_intervals)} intervals for 30m base_duration")
        print(f"🔍 [TEST] Actual intervals found: {len(intervals)}")

        assert len(intervals) == 48, f"Expected 48 intervals for 30m duration, got {len(intervals)}"

        for i, expected_time in enumerate(expected_intervals):
            assert intervals[i] == expected_time, f"Interval {i}: expected {expected_time}, got {intervals[i] if i < len(intervals) else 'missing'}"

    @patch('services.app.runner.ExchangeCalendar')
    def test_15_minute_intervals_single_day(self, mock_calendar_class):
        """Test that 15-minute base_duration generates 96 intervals for a single day."""
        # Mock trading day
        mock_calendar = Mock()
        mock_calendar.all_trading_days.return_value = [datetime(2025, 7, 1).date()]
        mock_calendar_class.return_value = mock_calendar

        runner = self.create_runner('2025-07-01', '2025-07-01', '15m')
        events = list(runner.iter_events())
        intervals = self.extract_intervals(events)

        # Should generate 96 intervals for 15-minute base_duration
        expected_intervals = []
        current = datetime(2025, 7, 1, 0, 0)
        for _ in range(96):  # 24 hours * 4 intervals per hour
            expected_intervals.append(current)
            current += timedelta(minutes=15)

        print(f"🔍 [TEST] Expected {len(expected_intervals)} intervals for 15m base_duration")
        print(f"🔍 [TEST] Actual intervals found: {len(intervals)}")

        assert len(intervals) == 96, f"Expected 96 intervals for 15m duration, got {len(intervals)}"

    @patch('services.app.runner.ExchangeCalendar')
    def test_5_minute_intervals_market_hours_subset(self, mock_calendar_class):
        """Test 5-minute intervals and verify market hours are included."""
        # Mock trading day
        mock_calendar = Mock()
        mock_calendar.all_trading_days.return_value = [datetime(2025, 7, 1).date()]
        mock_calendar_class.return_value = mock_calendar

        runner = self.create_runner('2025-07-01', '2025-07-01', '5m')
        events = list(runner.iter_events())
        intervals = self.extract_intervals(events)

        # Should generate 288 intervals for 5-minute base_duration (24 hours * 12 per hour)
        expected_count = 288

        print(f"🔍 [TEST] Expected {expected_count} intervals for 5m base_duration")
        print(f"🔍 [TEST] Actual intervals found: {len(intervals)}")

        assert len(intervals) == expected_count, f"Expected {expected_count} intervals for 5m duration, got {len(intervals)}"

        # Verify market hours intervals are included
        market_start = datetime(2025, 7, 1, 8, 0)  # 8am UTC (pre-market)
        market_end = datetime(2025, 7, 1, 21, 0)   # 9pm UTC (after-hours)

        market_intervals = [interval for interval in intervals if market_start <= interval <= market_end]
        expected_market_intervals = 13 * 12  # 13 hours * 12 intervals per hour

        print(f"🔍 [TEST] Market hours intervals (8am-9pm UTC): {len(market_intervals)} out of {len(intervals)}")

        assert len(market_intervals) >= expected_market_intervals, f"Should have at least {expected_market_intervals} market hour intervals, got {len(market_intervals)}"

    @patch('services.app.runner.ExchangeCalendar')
    def test_multiple_trading_days(self, mock_calendar_class):
        """Test interval generation across multiple trading days."""
        # Mock 3 trading days
        trading_days = [
            datetime(2025, 7, 1).date(),
            datetime(2025, 7, 2).date(),
            datetime(2025, 7, 3).date()
        ]
        mock_calendar = Mock()
        mock_calendar.all_trading_days.return_value = trading_days
        mock_calendar_class.return_value = mock_calendar

        runner = self.create_runner('2025-07-01', '2025-07-03', '60m')
        events = list(runner.iter_events())
        intervals = self.extract_intervals(events)

        # Should generate 24 intervals per day * 3 days = 72 intervals
        expected_count = 24 * 3

        print(f"🔍 [TEST] Expected {expected_count} intervals for 3 days with 60m base_duration")
        print(f"🔍 [TEST] Actual intervals found: {len(intervals)}")

        assert len(intervals) == expected_count, f"Expected {expected_count} intervals for 3 days, got {len(intervals)}"

        # Verify intervals for each day
        day1_intervals = [i for i in intervals if i.date() == datetime(2025, 7, 1).date()]
        day2_intervals = [i for i in intervals if i.date() == datetime(2025, 7, 2).date()]
        day3_intervals = [i for i in intervals if i.date() == datetime(2025, 7, 3).date()]

        assert len(day1_intervals) == 24, f"Day 1 should have 24 intervals, got {len(day1_intervals)}"
        assert len(day2_intervals) == 24, f"Day 2 should have 24 intervals, got {len(day2_intervals)}"
        assert len(day3_intervals) == 24, f"Day 3 should have 24 intervals, got {len(day3_intervals)}"

    @patch('services.app.runner.ExchangeCalendar')
    def test_no_trading_days(self, mock_calendar_class):
        """Test behavior when no trading days are found."""
        # Mock no trading days (e.g., weekend)
        mock_calendar = Mock()
        mock_calendar.all_trading_days.return_value = []
        mock_calendar_class.return_value = mock_calendar

        runner = self.create_runner('2025-07-05', '2025-07-06', '60m')  # Weekend
        events = list(runner.iter_events())
        intervals = self.extract_intervals(events)

        # Should generate no intervals when there are no trading days
        assert len(intervals) == 0, f"Expected 0 intervals for non-trading days, got {len(intervals)}"

    @patch('services.app.runner.ExchangeCalendar')
    def test_interval_timing_precision(self, mock_calendar_class):
        """Test that intervals are generated at precise times."""
        # Mock trading day
        mock_calendar = Mock()
        mock_calendar.all_trading_days.return_value = [datetime(2025, 7, 1).date()]
        mock_calendar_class.return_value = mock_calendar

        runner = self.create_runner('2025-07-01', '2025-07-01', '60m')
        events = list(runner.iter_events())
        intervals = self.extract_intervals(events)

        # Verify precise timing
        expected_times = [
            datetime(2025, 7, 1, 0, 0),   # 00:00
            datetime(2025, 7, 1, 1, 0),   # 01:00
            datetime(2025, 7, 1, 2, 0),   # 02:00
            datetime(2025, 7, 1, 8, 0),   # 08:00 - Market hours!
            datetime(2025, 7, 1, 9, 0),   # 09:00 - Market hours!
            datetime(2025, 7, 1, 15, 0),  # 15:00 - Market hours!
            datetime(2025, 7, 1, 23, 0),  # 23:00
        ]

        for expected_time in expected_times:
            assert expected_time in intervals, f"Expected interval at {expected_time} not found in {intervals[:10]}..."

    def test_duration_parsing(self):
        """Test that different duration strings are parsed correctly."""
        test_cases = [
            ('60m', 60),
            ('30m', 30),
            ('15m', 15),
            ('5m', 5),
            ('1h', 60),  # Should be equivalent to 60m
        ]

        for duration_str, expected_minutes in test_cases:
            runner = self.create_runner('2025-07-01', '2025-07-01', duration_str)
            duration_minutes = runner.duration.get_duration_minutes()

            assert duration_minutes == expected_minutes, f"Duration '{duration_str}' should parse to {expected_minutes} minutes, got {duration_minutes}"

    def test_advance_time_method(self):
        """Test the _advance_time method works correctly for different durations."""
        base_time = datetime(2025, 7, 1, 8, 0)  # 8am

        test_cases = [
            ('60m', timedelta(hours=1)),
            ('30m', timedelta(minutes=30)),
            ('15m', timedelta(minutes=15)),
            ('5m', timedelta(minutes=5)),
        ]

        for duration_str, expected_delta in test_cases:
            runner = self.create_runner('2025-07-01', '2025-07-01', duration_str)
            advanced_time = runner._advance_time(base_time)
            expected_time = base_time + expected_delta

            assert advanced_time == expected_time, f"Advancing {base_time} by {duration_str} should give {expected_time}, got {advanced_time}"

    @patch('services.app.runner.ExchangeCalendar')
    def test_market_hours_interval_coverage(self, mock_calendar_class):
        """Test that market hours (8am-9pm UTC) are properly covered by intervals."""
        # Mock trading day
        mock_calendar = Mock()
        mock_calendar.all_trading_days.return_value = [datetime(2025, 7, 1).date()]
        mock_calendar_class.return_value = mock_calendar

        runner = self.create_runner('2025-07-01', '2025-07-01', '60m')
        events = list(runner.iter_events())
        intervals = self.extract_intervals(events)

        # Market hours in UTC: 8am to 9pm (inclusive)
        market_start = datetime(2025, 7, 1, 8, 0)
        market_end = datetime(2025, 7, 1, 21, 0)

        market_intervals = [interval for interval in intervals if market_start <= interval <= market_end]

        print(f"🔍 [TEST] Market hours coverage analysis:")
        print(f"   Total intervals: {len(intervals)}")
        print(f"   Market hour intervals: {len(market_intervals)}")
        print(f"   Market intervals: {[t.strftime('%H:%M') for t in market_intervals]}")

        # Should have 14 market hour intervals (8:00, 9:00, ..., 21:00)
        expected_market_intervals = 14
        assert len(market_intervals) == expected_market_intervals, f"Expected {expected_market_intervals} market hour intervals, got {len(market_intervals)}"

        # Verify specific critical market hours are included
        critical_hours = [8, 9, 15, 16, 20, 21]  # Pre-market, open, close, after-hours
        for hour in critical_hours:
            expected_time = datetime(2025, 7, 1, hour, 0)
            assert expected_time in intervals, f"Critical market hour {hour}:00 not found in intervals"

    @patch('services.app.runner.ExchangeCalendar')
    def test_training_data_generation_scenario(self, mock_calendar_class):
        """Test the exact scenario that was failing in training data generation."""
        # Mock single trading day
        mock_calendar = Mock()
        mock_calendar.all_trading_days.return_value = [datetime(2025, 7, 1).date()]
        mock_calendar_class.return_value = mock_calendar

        runner = self.create_runner('2025-07-01', '2025-07-01', '60m')
        events = list(runner.iter_events())
        intervals = self.extract_intervals(events)

        print(f"🔍 [TEST] Training data generation scenario:")
        print(f"   Date range: 2025-07-01 to 2025-07-01")
        print(f"   Base duration: 60m")
        print(f"   Total intervals generated: {len(intervals)}")

        # Before fix: Only 1 interval at midnight (no market data)
        # After fix: 24 intervals including market hours (abundant data)
        assert len(intervals) > 1, "Should generate multiple intervals, not just midnight"
        assert len(intervals) == 24, f"Should generate 24 hourly intervals, got {len(intervals)}"

        # Verify midnight interval exists (no data expected)
        midnight = datetime(2025, 7, 1, 0, 0)
        assert midnight in intervals, "Midnight interval should still be included"

        # Verify market hour intervals exist (data available)
        market_hour_8am = datetime(2025, 7, 1, 8, 0)
        market_hour_9am = datetime(2025, 7, 1, 9, 0)
        market_hour_3pm = datetime(2025, 7, 1, 15, 0)

        assert market_hour_8am in intervals, "8am interval missing - critical for training data"
        assert market_hour_9am in intervals, "9am interval missing - critical for training data"
        assert market_hour_3pm in intervals, "3pm interval missing - critical for training data"

        print(f"✅ [TEST] Training data scenario fixed - market hours accessible")

    @patch('services.app.runner.ExchangeCalendar')
    def test_interval_generation_regression_prevention(self, mock_calendar_class):
        """Regression test to prevent the bug from reoccurring."""
        # Mock trading day
        mock_calendar = Mock()
        mock_calendar.all_trading_days.return_value = [datetime(2025, 7, 1).date()]
        mock_calendar_class.return_value = mock_calendar

        # Test all common durations
        duration_test_cases = [
            ('60m', 24, 'hourly'),
            ('30m', 48, 'half-hourly'),
            ('15m', 96, 'quarter-hourly'),
            ('5m', 288, 'five-minute')
        ]

        for duration_str, expected_count, description in duration_test_cases:
            runner = self.create_runner('2025-07-01', '2025-07-01', duration_str)
            events = list(runner.iter_events())
            intervals = self.extract_intervals(events)

            print(f"🔍 [REGRESSION] {description} intervals ({duration_str}): {len(intervals)}")

            # The critical assertion: NEVER go back to 1 interval per day
            assert len(intervals) > 1, f"REGRESSION: {duration_str} only generated 1 interval (the original bug)"
            assert len(intervals) == expected_count, f"REGRESSION: {duration_str} generated {len(intervals)} intervals, expected {expected_count}"

            # Verify interval timing precision
            if len(intervals) >= 2:
                first_interval = intervals[0]
                second_interval = intervals[1]
                expected_delta = timedelta(minutes=int(duration_str.rstrip('m')))
                actual_delta = second_interval - first_interval
                assert actual_delta == expected_delta, f"REGRESSION: Interval spacing incorrect for {duration_str}"

        print("✅ [REGRESSION] All duration tests pass - bug cannot reoccur")

    @patch('services.app.runner.ExchangeCalendar')
    def test_weekend_and_holiday_handling(self, mock_calendar_class):
        """Test that weekends and holidays are handled correctly."""
        # Test weekend (no trading days)
        mock_calendar = Mock()
        mock_calendar.all_trading_days.return_value = []  # Weekend
        mock_calendar_class.return_value = mock_calendar

        runner = self.create_runner('2025-07-05', '2025-07-06', '60m')  # Saturday-Sunday
        events = list(runner.iter_events())
        intervals = self.extract_intervals(events)

        assert len(intervals) == 0, f"Weekend should generate 0 intervals, got {len(intervals)}"

        # Test single trading day among non-trading days
        mock_calendar.all_trading_days.return_value = [datetime(2025, 7, 2).date()]  # Only Wednesday

        runner = self.create_runner('2025-07-01', '2025-07-03', '60m')  # Tue-Thu, only Wed trading
        events = list(runner.iter_events())
        intervals = self.extract_intervals(events)

        # Should only generate intervals for the single trading day
        assert len(intervals) == 24, f"Single trading day should generate 24 intervals, got {len(intervals)}"

        # All intervals should be on the trading day
        trading_day_intervals = [i for i in intervals if i.date() == datetime(2025, 7, 2).date()]
        assert len(trading_day_intervals) == 24, f"All intervals should be on trading day, got {len(trading_day_intervals)}"


class TestRunnerEventSequence:
    """Test the complete event sequence generated by Runner."""

    def setup_method(self):
        """Setup test environment."""
        self.mock_env = Mock(spec=Environment)
        self.mock_env.env_type = EnvironmentType.TEST

    @patch('services.app.runner.ExchangeCalendar')
    def test_event_sequence_order(self, mock_calendar_class):
        """Test that events are generated in the correct order."""
        # Mock single trading day
        mock_calendar = Mock()
        mock_calendar.all_trading_days.return_value = [datetime(2025, 7, 1).date()]
        mock_calendar_class.return_value = mock_calendar

        with patch('services.app.runner.create_run_context'):
            runner = Runner(
                start_date='2025-07-01',
                end_date='2025-07-01',
                environment=self.mock_env,
                universe_id=1,
                callbacks=[],
                base_duration='60m',
                enable_run_isolation=False
            )

        events = list(runner.iter_events())
        event_types = [event_type for _, event_type in events]

        # Expected sequence: start, sod, intervals..., eod, end, end
        expected_start = ['start', 'sod']
        expected_end = ['eod', 'end', 'end']

        assert event_types[0] == 'start', f"First event should be 'start', got {event_types[0]}"
        assert event_types[1] == 'sod', f"Second event should be 'sod', got {event_types[1]}"
        assert event_types[-3] == 'eod', f"Third-to-last event should be 'eod', got {event_types[-3]}"
        assert event_types[-2] == 'end', f"Second-to-last event should be 'end', got {event_types[-2]}"
        assert event_types[-1] == 'end', f"Last event should be 'end', got {event_types[-1]}"

        # Count intervals between sod and eod
        interval_count = event_types.count('interval')
        assert interval_count == 24, f"Should have 24 interval events for 60m duration, got {interval_count}"


if __name__ == '__main__':
    # Run tests with verbose output
    pytest.main([__file__, '-v', '-s'])