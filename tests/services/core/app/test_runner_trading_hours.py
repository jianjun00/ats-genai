"""
Comprehensive tests for Runner trading hours filtering functionality.

Tests the new trading hours filter to ensure training data generation only occurs
during regular market hours (9:35 AM - 4:00 PM Eastern Time).
"""

from datetime import datetime, date
from unittest.mock import Mock, patch

from services.core.app.runner import Runner
from core.platform.config.environment import Environment, EnvironmentType

class TestRunnerTradingHours:
    """Test cases for Runner trading hours filtering."""

    def setup_method(self):
        """Setup test fixtures."""
        self.mock_env = Mock(spec=Environment)
        self.mock_env.environment_type = EnvironmentType.DEV
        self.mock_env.env_type = EnvironmentType.DEV

        # Create minimal runner for testing
        self.runner = Runner(
            start_date="2025-07-01",
            end_date="2025-07-01",
            environment=self.mock_env,
            universe_id=1,
            callbacks=[],
            base_duration="60m",
            enable_trading_hours_filter=True,
            trading_start_hour=9,
            trading_start_minute=35,
            trading_end_hour=16,
            trading_end_minute=0,
            timezone='America/New_York'
        )

    def test_trading_hours_initialization(self):
        """Test trading hours parameters are properly initialized."""
        assert self.runner.trading_start_hour == 9
        assert self.runner.trading_start_minute == 35
        assert self.runner.trading_end_hour == 16
        assert self.runner.trading_end_minute == 0
        assert self.runner.timezone == 'America/New_York'
        assert self.runner.enable_trading_hours_filter is True

    def test_is_within_trading_hours_during_market(self):
        """Test time check during regular trading hours."""
        # 2:00 PM EDT = 18:00 UTC (during EDT/daylight saving)
        market_time_utc = datetime(2025, 7, 1, 18, 0, 0)

        assert self.runner._is_within_trading_hours(market_time_utc) is True

    def test_is_within_trading_hours_before_market(self):
        """Test time check before market open."""
        # 1:00 AM UTC = 9:00 PM EDT previous day (way before market)
        before_market_utc = datetime(2025, 7, 1, 1, 0, 0)

        assert self.runner._is_within_trading_hours(before_market_utc) is False

    def test_is_within_trading_hours_after_market(self):
        """Test time check after market close."""
        # 11:00 PM UTC = 7:00 PM EDT (after market close)
        after_market_utc = datetime(2025, 7, 1, 23, 0, 0)

        assert self.runner._is_within_trading_hours(after_market_utc) is False

    def test_is_within_trading_hours_at_market_open(self):
        """Test time check exactly at market open."""
        # 9:35 AM EDT = 13:35 UTC (during EDT)
        market_open_utc = datetime(2025, 7, 1, 13, 35, 0)

        assert self.runner._is_within_trading_hours(market_open_utc) is True

    def test_is_within_trading_hours_at_market_close(self):
        """Test time check exactly at market close."""
        # 4:00 PM EDT = 20:00 UTC (during EDT)
        market_close_utc = datetime(2025, 7, 1, 20, 0, 0)

        assert self.runner._is_within_trading_hours(market_close_utc) is True

    def test_trading_hours_filter_disabled(self):
        """Test that filter can be disabled."""
        mock_env = Mock(spec=Environment)
        mock_env.env_type = EnvironmentType.DEV

        runner_no_filter = Runner(
            start_date="2025-07-01",
            end_date="2025-07-01",
            environment=mock_env,
            universe_id=1,
            callbacks=[],
            base_duration="60m",
            enable_trading_hours_filter=False  # Disabled
        )

        # Any time should return True when filter is disabled
        middle_of_night = datetime(2025, 7, 1, 2, 0, 0)
        assert runner_no_filter._is_within_trading_hours(middle_of_night) is True

    def test_timezone_conversion_during_est(self):
        """Test timezone conversion during EST (standard time)."""
        mock_env = Mock(spec=Environment)
        mock_env.env_type = EnvironmentType.DEV

        # Create runner for winter time (EST)
        runner_est = Runner(
            start_date="2025-01-15",
            end_date="2025-01-15",
            environment=mock_env,
            universe_id=1,
            callbacks=[],
            base_duration="60m",
            enable_trading_hours_filter=True,
            trading_start_hour=9,
            trading_start_minute=35,
            trading_end_hour=16,
            trading_end_minute=0,
            timezone='America/New_York'
        )

        # 2:00 PM EST = 19:00 UTC (during EST/standard time)
        market_time_utc = datetime(2025, 1, 15, 19, 0, 0)

        assert runner_est._is_within_trading_hours(market_time_utc) is True

    @patch('services.core.app.runner.logging')
    def test_trading_hours_error_handling(self, mock_logging):
        """Test error handling in trading hours check."""
        mock_env = Mock(spec=Environment)
        mock_env.env_type = EnvironmentType.DEV

        # Create runner with invalid timezone
        runner_bad_tz = Runner(
            start_date="2025-07-01",
            end_date="2025-07-01",
            environment=mock_env,
            universe_id=1,
            callbacks=[],
            base_duration="60m",
            timezone='Invalid/Timezone'
        )

        test_time = datetime(2025, 7, 1, 15, 0, 0)

        # Should default to True and log warning on error
        assert runner_bad_tz._is_within_trading_hours(test_time) is True
        mock_logging.warning.assert_called_once()

    def test_trading_hours_boundary_conditions(self):
        """Test edge cases around trading hours boundaries."""
        # Just before market open: 9:34 AM EDT = 13:34 UTC
        just_before_open = datetime(2025, 7, 1, 13, 34, 0)
        assert self.runner._is_within_trading_hours(just_before_open) is False

        # Just after market close: 4:01 PM EDT = 20:01 UTC
        just_after_close = datetime(2025, 7, 1, 20, 1, 0)
        assert self.runner._is_within_trading_hours(just_after_close) is False

    @patch('services.core.app.runner.Runner._advance_time')
    @patch('services.core.app.runner.Runner._is_within_trading_hours')
    def test_iter_events_filters_intervals(self, mock_is_within_hours, mock_advance_time):
        """Test that iter_events properly filters intervals based on trading hours."""
        # Setup mocks
        mock_advance_time.side_effect = [
            datetime(2025, 7, 1, 1, 0, 0),   # 1:00 AM (outside hours)
            datetime(2025, 7, 1, 2, 0, 0),   # 2:00 AM (outside hours)
            datetime(2025, 7, 1, 15, 0, 0),  # 11:00 AM EDT (inside hours)
            datetime(2025, 7, 2, 0, 0, 0)    # Next day (exit condition)
        ]

        mock_is_within_hours.side_effect = [False, False, True, False]

        # Mock exchange calendar and market data manager
        with patch('services.core.app.runner.ExchangeCalendar') as mock_calendar:
            mock_calendar.return_value.all_trading_days.return_value = [date(2025, 7, 1)]

            # Mock market data manager
            self.runner.market_data_manager = Mock()
            self.runner.market_data_manager.exchange = 'NYSE'

            # Mock duration
            self.runner.duration = Mock()
            self.runner.duration.get_duration_minutes.return_value = 60

            events = list(self.runner.iter_events())

            # Should have start, sod, one interval (only the one within trading hours), eod, end
            interval_events = [e for e in events if e[1] == 'interval']

            # Only one interval should be yielded (the one within trading hours)
            assert len(interval_events) == 1
            assert interval_events[0][0] == datetime(2025, 7, 1, 15, 0, 0)

class TestTrainingDataProblemReproduction:
    """Test to reproduce the original zero-value problem and verify fix."""

    def test_original_problem_reproduction(self):
        """Reproduce the original problem: 1:00 AM UTC generates zero values."""
        mock_env = Mock(spec=Environment)
        mock_env.env_type = EnvironmentType.DEV

        # Create runner with trading hours filter DISABLED (original behavior)
        runner_no_filter = Runner(
            start_date="2025-07-01",
            end_date="2025-07-01",
            environment=mock_env,
            universe_id=1,
            callbacks=[],
            base_duration="60m",
            enable_trading_hours_filter=False  # This was the original problem
        )

        # 1:00 AM UTC should be allowed without filter
        problematic_time = datetime(2025, 7, 1, 1, 0, 0)
        assert runner_no_filter._is_within_trading_hours(problematic_time) is True

    def test_fixed_behavior_with_trading_hours(self):
        """Verify the fix: 1:00 AM UTC is now filtered out."""
        mock_env = Mock(spec=Environment)
        mock_env.env_type = EnvironmentType.DEV

        # Create runner with trading hours filter ENABLED (fixed behavior)
        runner_with_filter = Runner(
            start_date="2025-07-01",
            end_date="2025-07-01",
            environment=mock_env,
            universe_id=1,
            callbacks=[],
            base_duration="60m",
            enable_trading_hours_filter=True  # This is the fix
        )

        # 1:00 AM UTC should now be filtered out
        problematic_time = datetime(2025, 7, 1, 1, 0, 0)
        assert runner_with_filter._is_within_trading_hours(problematic_time) is False

    def test_market_hours_generate_intervals(self):
        """Verify that market hours do generate intervals."""
        mock_env = Mock(spec=Environment)
        mock_env.env_type = EnvironmentType.DEV

        runner = Runner(
            start_date="2025-07-01",
            end_date="2025-07-01",
            environment=mock_env,
            universe_id=1,
            callbacks=[],
            base_duration="60m",
            enable_trading_hours_filter=True
        )

        # Test several market hours times
        market_times = [
            datetime(2025, 7, 1, 14, 0, 0),  # 10:00 AM EDT
            datetime(2025, 7, 1, 16, 0, 0),  # 12:00 PM EDT
            datetime(2025, 7, 1, 19, 30, 0), # 3:30 PM EDT
        ]

        for market_time in market_times:
            assert runner._is_within_trading_hours(market_time) is True, f"Failed for {market_time}"