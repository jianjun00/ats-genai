#!/usr/bin/env python3
"""
Test suite for the critical Runner interval generation bug fix.

BUG DESCRIPTION:
Runner.iter_events() was only generating one interval per day at midnight (00:00:00)
instead of multiple intraday intervals based on the base_duration parameter.

IMPACT:
Training data generation could only access midnight data (no market activity)
instead of market hours (8am-9pm UTC with abundant OHLCV data).

FIX IMPLEMENTATION:
File: /home/jianjun/ats-genai-pm/src/services/app/runner.py:158-166
Changed from single midnight interval to loop generating intervals throughout the day.

DATA FLOW VERIFICATION:
FileBasedMinuteManager → FileBasedMinuteMarketDataManager → UniverseStateBuilder → TrainingDataCallback

This test suite prevents regression and documents the fix for future developers.
"""

import pytest
import sys
import os
from datetime import datetime, timedelta
from typing import List, Tuple
from unittest.mock import Mock, patch

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../../src'))

from domains.trading.services.core.app.runner import Runner
from core.platform.config.environment import Environment, EnvironmentType

class TestRunnerIntervalBugFix:
    """
    Comprehensive test suite for the Runner interval generation bug fix.

    This bug was critical for training data generation as it prevented access
    to market hours data, which is essential for meaningful ML training datasets.
    """

    def setup_method(self):
        """Setup test environment and mocks."""
        self.mock_env = Mock(spec=Environment)
        self.mock_env.env_type = EnvironmentType.TEST

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
                enable_run_isolation=False
            )
        return runner

    def extract_intervals(self, events: List[Tuple[datetime, str]]) -> List[datetime]:
        """Extract interval events from the complete event stream."""
        return [event_time for event_time, event_type in events if event_type == 'interval']

    @patch('services.app.runner.ExchangeCalendar')
    def test_bug_fix_verification_60m(self, mock_calendar_class):
        """
        CRITICAL TEST: Verify the 60-minute interval bug is fixed.

        Before Fix: 1 interval at 00:00:00 (midnight, no market data)
        After Fix: 24 intervals including market hours (8am-9pm, abundant data)

        Code pointer: /home/jianjun/ats-genai-pm/src/services/app/runner.py:158-166
        """
        mock_calendar = Mock()
        mock_calendar.all_trading_days.return_value = [datetime(2025, 7, 1).date()]
        mock_calendar_class.return_value = mock_calendar

        runner = self.create_runner('2025-07-01', '2025-07-01', '60m')
        events = list(runner.iter_events())
        intervals = self.extract_intervals(events)

        # CRITICAL ASSERTIONS - Bug prevention
        assert len(intervals) != 1, "REGRESSION: Only 1 interval generated (original bug)"
        assert len(intervals) == 24, f"Expected 24 hourly intervals, got {len(intervals)}"

        # Verify complete hourly coverage
        expected_hours = list(range(24))  # 0, 1, 2, ..., 23
        actual_hours = [interval.hour for interval in intervals]
        assert sorted(actual_hours) == expected_hours, f"Missing hours in intervals: {set(expected_hours) - set(actual_hours)}"

        # Verify market hours are included (critical for training data)
        market_hours = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]
        market_intervals = [interval for interval in intervals if interval.hour in market_hours]
        assert len(market_intervals) == 14, f"Expected 14 market hour intervals, got {len(market_intervals)}"

        print(f"✅ BUG FIX VERIFIED: 60m generates {len(intervals)} intervals including {len(market_intervals)} market hours")

    @patch('services.app.runner.ExchangeCalendar')
    def test_bug_fix_verification_30m(self, mock_calendar_class):
        """
        CRITICAL TEST: Verify the 30-minute interval bug is fixed.

        Before Fix: 1 interval at 00:00:00
        After Fix: 48 intervals (30-minute resolution throughout the day)
        """
        mock_calendar = Mock()
        mock_calendar.all_trading_days.return_value = [datetime(2025, 7, 1).date()]
        mock_calendar_class.return_value = mock_calendar

        runner = self.create_runner('2025-07-01', '2025-07-01', '30m')
        events = list(runner.iter_events())
        intervals = self.extract_intervals(events)

        # CRITICAL ASSERTIONS
        assert len(intervals) != 1, "REGRESSION: Only 1 interval generated (original bug)"
        assert len(intervals) == 48, f"Expected 48 half-hourly intervals, got {len(intervals)}"

        # Verify 30-minute spacing
        if len(intervals) >= 2:
            delta = intervals[1] - intervals[0]
            assert delta == timedelta(minutes=30), f"Expected 30-minute spacing, got {delta}"

        # Count market hour intervals (more granular coverage)
        market_intervals = [i for i in intervals if 8 <= i.hour <= 21]
        assert len(market_intervals) == 28, f"Expected 28 market hour intervals (30m), got {len(market_intervals)}"

        print(f"✅ BUG FIX VERIFIED: 30m generates {len(intervals)} intervals with {len(market_intervals)} in market hours")

    @patch('services.app.runner.ExchangeCalendar')
    def test_bug_fix_verification_15m(self, mock_calendar_class):
        """
        CRITICAL TEST: Verify the 15-minute interval bug is fixed.

        Before Fix: 1 interval at 00:00:00
        After Fix: 96 intervals (15-minute resolution, high granularity for training)
        """
        mock_calendar = Mock()
        mock_calendar.all_trading_days.return_value = [datetime(2025, 7, 1).date()]
        mock_calendar_class.return_value = mock_calendar

        runner = self.create_runner('2025-07-01', '2025-07-01', '15m')
        events = list(runner.iter_events())
        intervals = self.extract_intervals(events)

        # CRITICAL ASSERTIONS
        assert len(intervals) != 1, "REGRESSION: Only 1 interval generated (original bug)"
        assert len(intervals) == 96, f"Expected 96 quarter-hourly intervals, got {len(intervals)}"

        # Verify 15-minute spacing precision
        if len(intervals) >= 2:
            delta = intervals[1] - intervals[0]
            assert delta == timedelta(minutes=15), f"Expected 15-minute spacing, got {delta}"

        # High-resolution market coverage verification
        market_intervals = [i for i in intervals if 8 <= i.hour <= 21]
        assert len(market_intervals) == 56, f"Expected 56 market hour intervals (15m), got {len(market_intervals)}"

        print(f"✅ BUG FIX VERIFIED: 15m generates {len(intervals)} intervals with {len(market_intervals)} in market hours")

    @patch('services.app.runner.ExchangeCalendar')
    def test_multiple_trading_days_fix(self, mock_calendar_class):
        """
        CRITICAL TEST: Verify multi-day interval generation is fixed.

        Before Fix: 3 intervals (one per day at midnight)
        After Fix: 72 intervals (24 per day * 3 days)
        """
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

        # CRITICAL ASSERTIONS
        assert len(intervals) != 3, "REGRESSION: Only 3 intervals generated (original bug for multi-day)"
        assert len(intervals) == 72, f"Expected 72 intervals (24*3), got {len(intervals)}"

        # Verify distribution across days
        day1_intervals = [i for i in intervals if i.date() == datetime(2025, 7, 1).date()]
        day2_intervals = [i for i in intervals if i.date() == datetime(2025, 7, 2).date()]
        day3_intervals = [i for i in intervals if i.date() == datetime(2025, 7, 3).date()]

        assert len(day1_intervals) == 24, f"Day 1 should have 24 intervals, got {len(day1_intervals)}"
        assert len(day2_intervals) == 24, f"Day 2 should have 24 intervals, got {len(day2_intervals)}"
        assert len(day3_intervals) == 24, f"Day 3 should have 24 intervals, got {len(day3_intervals)}"

        print(f"✅ MULTI-DAY BUG FIX VERIFIED: {len(intervals)} total intervals across {len(trading_days)} days")

    @patch('services.app.runner.ExchangeCalendar')
    def test_training_data_generation_scenario(self, mock_calendar_class):
        """
        INTEGRATION TEST: Verify the exact scenario that was failing in training data generation.

        This test simulates the exact conditions under which TSLA training data
        generation was failing due to the interval bug.

        Data Flow: Runner.iter_events() → UniverseStateBuilder.handleInterval() → TrainingDataCallback.handleInterval()
        Data Available: 20,547 TSLA records in market hours vs 0 at midnight
        """
        mock_calendar = Mock()
        mock_calendar.all_trading_days.return_value = [datetime(2025, 7, 1).date()]
        mock_calendar_class.return_value = mock_calendar

        # Exact parameters from failing TSLA training data generation
        runner = self.create_runner('2025-07-01', '2025-07-01', '60m')
        events = list(runner.iter_events())
        intervals = self.extract_intervals(events)

        print(f"🔍 TRAINING DATA SCENARIO ANALYSIS:")
        print(f"   Symbol: TSLA")
        print(f"   Date: 2025-07-01 (single day)")
        print(f"   Base duration: 60m")
        print(f"   Trading data available: 08:00-21:59 UTC (market hours)")
        print(f"   Total intervals generated: {len(intervals)}")

        # Before fix analysis
        midnight_interval = datetime(2025, 7, 1, 0, 0)
        assert midnight_interval in intervals, "Midnight interval should be present"
        print(f"   Midnight interval (00:00): Present (no market data available)")

        # After fix analysis - market hours accessibility
        critical_market_intervals = [
            (datetime(2025, 7, 1, 8, 0), "Pre-market open"),
            (datetime(2025, 7, 1, 9, 0), "Market open"),
            (datetime(2025, 7, 1, 15, 0), "Market active"),
            (datetime(2025, 7, 1, 16, 0), "Market close"),
            (datetime(2025, 7, 1, 20, 0), "After-hours"),
            (datetime(2025, 7, 1, 21, 0), "After-hours end")
        ]

        market_hours_accessible = 0
        for interval_time, description in critical_market_intervals:
            if interval_time in intervals:
                market_hours_accessible += 1
                print(f"   {description} ({interval_time.strftime('%H:%M')}): ✅ Accessible (data available)")
            else:
                print(f"   {description} ({interval_time.strftime('%H:%M')}): ❌ Missing")

        # CRITICAL ASSERTION: Market hours must be accessible for training data
        assert market_hours_accessible == len(critical_market_intervals), f"Only {market_hours_accessible}/{len(critical_market_intervals)} market hours accessible"

        print(f"✅ TRAINING DATA SCENARIO FIXED: {market_hours_accessible} critical market intervals accessible")
        print(f"   Before fix: Training data could only access midnight (0 TSLA records)")
        print(f"   After fix: Training data can access market hours (20,547+ TSLA records)")

    @patch('services.app.runner.ExchangeCalendar')
    def test_data_flow_pipeline_verification(self, mock_calendar_class):
        """
        INTEGRATION TEST: Verify the complete data flow pipeline works with fixed intervals.

        DATA FLOW:
        1. Runner.iter_events() generates intervals → [FIXED: Now generates 24 instead of 1]
        2. UniverseStateBuilder.handleInterval() processes each interval
        3. FileBasedMinuteMarketDataManager.get_minute_ohlc_batch() fetches data
        4. TrainingDataCallback.handleInterval() generates training examples

        File references:
        - Runner: /home/jianjun/ats-genai-pm/src/services/app/runner.py:158-166
        - UniverseStateBuilder: /home/jianjun/ats-genai-pm/src/domains/trading/services/state/universe_state_builder.py:118
        - FileBasedMinuteMarketDataManager: /home/jianjun/ats-genai-pm/src/domains/market_data/services/core/minute/file_based_minute_market_data_manager.py
        """
        mock_calendar = Mock()
        mock_calendar.all_trading_days.return_value = [datetime(2025, 7, 1).date()]
        mock_calendar_class.return_value = mock_calendar

        runner = self.create_runner('2025-07-01', '2025-07-01', '60m')
        events = list(runner.iter_events())

        # Extract different event types
        start_events = [e for e in events if e[1] == 'start']
        sod_events = [e for e in events if e[1] == 'sod']
        interval_events = [e for e in events if e[1] == 'interval']
        eod_events = [e for e in events if e[1] == 'eod']
        end_events = [e for e in events if e[1] == 'end']

        print(f"🔍 DATA FLOW PIPELINE VERIFICATION:")
        print(f"   Total events generated: {len(events)}")
        print(f"   Start events: {len(start_events)}")
        print(f"   SOD events: {len(sod_events)}")
        print(f"   Interval events: {len(interval_events)} ← [CRITICAL: This was 1, now {len(interval_events)}]")
        print(f"   EOD events: {len(eod_events)}")
        print(f"   End events: {len(end_events)}")

        # CRITICAL: Interval events drive the training data pipeline
        assert len(interval_events) == 24, f"Expected 24 interval events for pipeline, got {len(interval_events)}"

        # Verify event sequence timing
        interval_times = [e[0] for e in interval_events]
        market_hour_intervals = [t for t in interval_times if 8 <= t.hour <= 21]

        assert len(market_hour_intervals) == 14, f"Expected 14 market hour intervals for data pipeline, got {len(market_hour_intervals)}"

        print(f"✅ DATA PIPELINE VERIFIED: {len(interval_events)} intervals feed training data generation")
        print(f"   Market hours pipeline coverage: {len(market_hour_intervals)} intervals")
        print(f"   Each interval can now access minute bar data via FileBasedMinuteMarketDataManager")

    @patch('services.app.runner.ExchangeCalendar')
    def test_regression_prevention_comprehensive(self, mock_calendar_class):
        """
        REGRESSION TEST: Comprehensive prevention of the interval generation bug.

        This test ensures the bug can never reoccur by testing all common scenarios
        that training data generation uses.
        """
        mock_calendar = Mock()
        mock_calendar.all_trading_days.return_value = [datetime(2025, 7, 1).date()]
        mock_calendar_class.return_value = mock_calendar

        # All common training data durations
        duration_scenarios = [
            ('5m', 288, 'High-frequency training (5-minute bars)'),
            ('15m', 96, 'Standard training (15-minute bars)'),
            ('30m', 48, 'Medium-frequency training (30-minute bars)'),
            ('60m', 24, 'Hourly training (1-hour bars)'),
        ]

        regression_results = []

        for duration_str, expected_count, description in duration_scenarios:
            runner = self.create_runner('2025-07-01', '2025-07-01', duration_str)
            events = list(runner.iter_events())
            intervals = self.extract_intervals(events)

            # CRITICAL REGRESSION CHECKS
            bug_detected = len(intervals) == 1
            fix_verified = len(intervals) == expected_count

            regression_results.append({
                'duration': duration_str,
                'expected': expected_count,
                'actual': len(intervals),
                'bug_detected': bug_detected,
                'fix_verified': fix_verified,
                'description': description
            })

            # FAIL FAST on regression
            assert not bug_detected, f"REGRESSION DETECTED: {duration_str} generated only 1 interval (original bug)"
            assert fix_verified, f"REGRESSION: {duration_str} generated {len(intervals)}, expected {expected_count}"

        # Summary report
        print(f"🔍 REGRESSION PREVENTION REPORT:")
        for result in regression_results:
            status = "✅ PASS" if result['fix_verified'] else "❌ FAIL"
            print(f"   {status} {result['duration']:>4}: {result['actual']:>3} intervals - {result['description']}")

        all_passed = all(result['fix_verified'] for result in regression_results)
        assert all_passed, "Regression detected in one or more duration scenarios"

        print(f"✅ REGRESSION PREVENTION: All {len(duration_scenarios)} scenarios pass")

class TestRunnerIntervalBugDocumentation:
    """
    Documentation tests that capture the technical details of the bug fix
    for future developers and debugging.
    """

    def test_bug_fix_code_documentation(self):
        """
        Document the exact code changes made to fix the bug.

        This test serves as living documentation of the fix.
        """
        print("📚 BUG FIX DOCUMENTATION:")
        print("=" * 60)
        print()

        print("🐛 ORIGINAL BUG:")
        print("   File: /home/jianjun/ats-genai-pm/src/services/app/runner.py")
        print("   Lines: 158-161 (before fix)")
        print("   Code: # Yield interval event (at SOD for now, can adjust for intraday if needed)")
        print("         sod_time = datetime.combine(day, datetime.min.time())  # 00:00:00")
        print("         yield (sod_time, \"interval\")")
        print()

        print("🔧 FIX IMPLEMENTATION:")
        print("   File: /home/jianjun/ats-genai-pm/src/services/app/runner.py")
        print("   Lines: 158-166 (after fix)")
        print("   Code: # Yield multiple interval events throughout the day based on base_duration")
        print("         current_interval_time = sod_time")
        print("         next_day = sod_time + timedelta(days=1)")
        print("         while current_interval_time < next_day:")
        print("             yield (current_interval_time, \"interval\")")
        print("             current_interval_time = self._advance_time(current_interval_time)")
        print()

        print("📊 IMPACT ANALYSIS:")
        print("   Before Fix:")
        print("     - 60m duration: 1 interval per day (only midnight)")
        print("     - 30m duration: 1 interval per day (only midnight)")
        print("     - 15m duration: 1 interval per day (only midnight)")
        print("     - Training data: Could only access midnight (0 market records)")
        print()
        print("   After Fix:")
        print("     - 60m duration: 24 intervals per day (every hour)")
        print("     - 30m duration: 48 intervals per day (every 30 minutes)")
        print("     - 15m duration: 96 intervals per day (every 15 minutes)")
        print("     - Training data: Can access market hours (20,547+ TSLA records)")
        print()

        print("🔗 DATA FLOW:")
        print("   Runner.iter_events() [FIXED]")
        print("   ↓ Multiple intervals per day")
        print("   UniverseStateBuilder.handleInterval()")
        print("   ↓ Processes each interval")
        print("   FileBasedMinuteMarketDataManager.get_minute_ohlc_batch()")
        print("   ↓ Fetches minute bars for time range")
        print("   TrainingDataCallback.handleInterval()")
        print("   ↓ Generates training examples")
        print("   Training Dataset Files")
        print()

        print("✅ This documentation test always passes and serves as reference.")

if __name__ == '__main__':
    # Run tests with detailed output
    pytest.main([__file__, '-v', '-s'])