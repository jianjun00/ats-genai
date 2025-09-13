"""
Comprehensive tests for TimeDuration time range logic.

Tests the new get_start_time method and verifies that time ranges are calculated
correctly for feature extraction: [current_time - base_duration, current_time].
"""

import pytest
from datetime import datetime, timedelta

from core.business.calendars.time_duration import TimeDuration


class TestTimeDurationRangeLogic:
    """Test cases for TimeDuration start time calculation."""

    def test_get_start_time_5_minutes(self):
        """Test start time calculation for 5-minute duration."""
        duration = TimeDuration("5m")
        end_time = datetime(2025, 7, 1, 14, 30, 0)  # 2:30 PM

        start_time = duration.get_start_time(end_time)
        expected_start = datetime(2025, 7, 1, 14, 25, 0)  # 2:25 PM

        assert start_time == expected_start
        assert (end_time - start_time).total_seconds() == 300  # 5 minutes

    def test_get_start_time_15_minutes(self):
        """Test start time calculation for 15-minute duration."""
        duration = TimeDuration("15m")
        end_time = datetime(2025, 7, 1, 14, 30, 0)  # 2:30 PM

        start_time = duration.get_start_time(end_time)
        expected_start = datetime(2025, 7, 1, 14, 15, 0)  # 2:15 PM

        assert start_time == expected_start
        assert (end_time - start_time).total_seconds() == 900  # 15 minutes

    def test_get_start_time_60_minutes(self):
        """Test start time calculation for 60-minute duration."""
        duration = TimeDuration("60m")
        end_time = datetime(2025, 7, 1, 15, 0, 0)  # 3:00 PM

        start_time = duration.get_start_time(end_time)
        expected_start = datetime(2025, 7, 1, 14, 0, 0)  # 2:00 PM

        assert start_time == expected_start
        assert (end_time - start_time).total_seconds() == 3600  # 60 minutes

    def test_get_start_time_1_day(self):
        """Test start time calculation for daily duration."""
        duration = TimeDuration("1d")
        end_time = datetime(2025, 7, 2, 9, 0, 0)  # July 2, 9:00 AM

        start_time = duration.get_start_time(end_time)
        expected_start = datetime(2025, 7, 1, 9, 0, 0)  # July 1, 9:00 AM

        assert start_time == expected_start
        assert (end_time - start_time).days == 1

    def test_get_start_time_1_week(self):
        """Test start time calculation for weekly duration."""
        duration = TimeDuration("1w")
        end_time = datetime(2025, 7, 8, 12, 0, 0)  # Week later

        start_time = duration.get_start_time(end_time)
        expected_start = datetime(2025, 7, 1, 12, 0, 0)  # Week earlier

        assert start_time == expected_start
        assert (end_time - start_time).days == 7

    def test_get_start_time_1_month(self):
        """Test start time calculation for monthly duration."""
        duration = TimeDuration("1m")
        end_time = datetime(2025, 8, 1, 10, 0, 0)  # August 1

        start_time = duration.get_start_time(end_time)
        expected_start = datetime(2025, 7, 1, 10, 0, 0)  # July 1

        assert start_time == expected_start

    def test_get_start_time_consistency_with_get_end_time(self):
        """Test that get_start_time and get_end_time are consistent."""
        duration = TimeDuration("60m")
        reference_time = datetime(2025, 7, 1, 14, 0, 0)

        # Test forward: start -> end
        end_time = duration.get_end_time(reference_time)
        assert end_time == datetime(2025, 7, 1, 15, 0, 0)

        # Test backward: end -> start
        calculated_start = duration.get_start_time(end_time)
        assert calculated_start == reference_time

        # Round trip should be identity
        assert calculated_start == reference_time

    def test_time_range_logic_for_feature_extraction(self):
        """Test time range logic matches feature extraction requirements."""
        duration = TimeDuration("60m")
        current_time = datetime(2025, 7, 1, 15, 0, 0)  # 3:00 PM

        # For feature extraction, we want past data: [current_time - duration, current_time]
        feature_start = duration.get_start_time(current_time)
        feature_end = current_time

        # This should give us [2:00 PM, 3:00 PM] - past hour of data
        expected_start = datetime(2025, 7, 1, 14, 0, 0)
        expected_end = datetime(2025, 7, 1, 15, 0, 0)

        assert feature_start == expected_start
        assert feature_end == expected_end

        # Verify this is a past time range (start < end <= current_time)
        assert feature_start < feature_end
        assert feature_end <= current_time

    def test_multiple_duration_types_time_ranges(self):
        """Test time ranges for multiple duration types."""
        current_time = datetime(2025, 7, 1, 15, 30, 0)  # 3:30 PM

        test_cases = [
            ("5m", datetime(2025, 7, 1, 15, 25, 0)),   # [3:25 PM, 3:30 PM]
            ("15m", datetime(2025, 7, 1, 15, 15, 0)),  # [3:15 PM, 3:30 PM]
            ("60m", datetime(2025, 7, 1, 14, 30, 0)),  # [2:30 PM, 3:30 PM]
        ]

        for duration_str, expected_start in test_cases:
            duration = TimeDuration(duration_str)
            calculated_start = duration.get_start_time(current_time)

            assert calculated_start == expected_start, f"Failed for duration {duration_str}"
            assert calculated_start < current_time, f"Start time should be before current time for {duration_str}"

    def test_edge_cases_time_boundaries(self):
        """Test edge cases around time boundaries."""
        duration = TimeDuration("60m")

        # Test at midnight
        midnight = datetime(2025, 7, 2, 0, 0, 0)
        start_time = duration.get_start_time(midnight)
        assert start_time == datetime(2025, 7, 1, 23, 0, 0)  # Previous day 11 PM

        # Test at month boundary
        month_start = datetime(2025, 8, 1, 0, 0, 0)
        monthly_duration = TimeDuration("1m")
        month_back = monthly_duration.get_start_time(month_start)
        assert month_back == datetime(2025, 7, 1, 0, 0, 0)

    def test_time_range_validation_for_training_data(self):
        """Test that time ranges are correct for training data generation."""
        # This simulates the actual training data scenario
        current_interval_time = datetime(2025, 7, 1, 14, 0, 0)  # Current processing time
        base_duration = TimeDuration("60m")

        # OLD LOGIC (incorrect): [current_time, current_time + duration] - looks at future
        old_start = current_interval_time
        old_end = base_duration.get_end_time(current_interval_time)
        assert old_start == datetime(2025, 7, 1, 14, 0, 0)
        assert old_end == datetime(2025, 7, 1, 15, 0, 0)  # Future data!

        # NEW LOGIC (correct): [current_time - duration, current_time] - looks at past
        new_start = base_duration.get_start_time(current_interval_time)
        new_end = current_interval_time
        assert new_start == datetime(2025, 7, 1, 13, 0, 0)  # Past data ✅
        assert new_end == datetime(2025, 7, 1, 14, 0, 0)   # Current time ✅

        # Verify the fix: new logic looks at past, old logic looked at future
        assert new_start < new_end <= current_interval_time  # Past data ✅
        assert old_start == current_interval_time < old_end  # Future data ❌

    def test_intraday_vs_daily_duration_behavior(self):
        """Test different behavior for intraday vs daily durations."""
        current_time = datetime(2025, 7, 1, 14, 30, 0)

        # Intraday duration
        intraday = TimeDuration("30m")
        intraday_start = intraday.get_start_time(current_time)
        assert intraday_start == datetime(2025, 7, 1, 14, 0, 0)
        assert intraday.is_intraday()

        # Daily duration
        daily = TimeDuration("1d")
        daily_start = daily.get_start_time(current_time)
        assert daily_start == datetime(2025, 6, 30, 14, 30, 0)  # Same time, previous day
        assert daily.is_daily_or_longer()


class TestTimeDurationErrorHandling:
    """Test error handling in TimeDuration time calculations."""

    def test_invalid_duration_type_get_start_time(self):
        """Test error handling for invalid duration types."""
        # This should not happen with proper construction, but test anyway
        duration = TimeDuration("5m")
        duration.duration_type = "invalid"  # Force invalid state

        current_time = datetime(2025, 7, 1, 14, 0, 0)

        with pytest.raises(ValueError, match="Unsupported duration type"):
            duration.get_start_time(current_time)


class TestTimeRangeIntegrationScenarios:
    """Integration test scenarios for time range logic."""

    def test_training_data_pipeline_time_ranges(self):
        """Test complete training data pipeline time range logic."""
        # Simulate training data generation scenario
        processing_times = [
            datetime(2025, 7, 1, 13, 35, 0),  # 1:35 PM (market hours)
            datetime(2025, 7, 1, 14, 35, 0),  # 2:35 PM (market hours)
            datetime(2025, 7, 1, 15, 35, 0),  # 3:35 PM (market hours)
        ]

        base_duration = TimeDuration("60m")

        for current_time in processing_times:
            # Calculate feature extraction time range: [current_time - duration, current_time]
            feature_start = base_duration.get_start_time(current_time)
            feature_end = current_time

            # Verify this gives us past hour of data for features
            assert feature_end - feature_start == timedelta(hours=1)
            assert feature_start < feature_end <= current_time

            print(f"Processing at {current_time}: features from [{feature_start}, {feature_end}]")

    def test_multi_timeframe_aggregation_time_ranges(self):
        """Test time ranges for multi-timeframe aggregation."""
        current_time = datetime(2025, 7, 1, 15, 0, 0)
        timeframes = ["5m", "15m", "60m"]

        for tf_str in timeframes:
            duration = TimeDuration(tf_str)

            # For each timeframe, calculate the past data range
            start_time = duration.get_start_time(current_time)
            end_time = current_time

            # Verify all ranges end at current_time but start at different points
            assert end_time == current_time
            assert start_time < current_time

            # Verify duration is correct
            if tf_str == "5m":
                assert (end_time - start_time).total_seconds() == 300
            elif tf_str == "15m":
                assert (end_time - start_time).total_seconds() == 900
            elif tf_str == "60m":
                assert (end_time - start_time).total_seconds() == 3600