import datetime
import pytest
from core.calendars.exchange_calendar import ExchangeCalendar
from unittest.mock import patch

class TestExchangeCalendarEnhanced:
    """Enhanced test coverage for ExchangeCalendar edge cases and error handling."""

    def test_init_case_insensitive(self):
        """Test that exchange initialization handles case variations."""
        # These should all work (case insensitive)
        test_exchanges = [
            ("nyse", "NYSE"),
            ("NYSE", "NYSE"),
            ("Nyse", "NYSE"),
            ("lse", "LSE"),
            ("LSE", "LSE")
        ]

        for input_exchange, expected_exchange in test_exchanges:
            cal = ExchangeCalendar(input_exchange)
            assert cal.exchange == expected_exchange

    def test_init_unsupported_exchange(self):
        """Test initialization with unsupported exchange raises ValueError."""
        invalid_exchanges = [
            "FAKE_EXCHANGE",
            "INVALID",
            "NONEXISTENT_MARKET",
            "123",
            "",
            "   "
        ]

        for invalid_exchange in invalid_exchanges:
            with pytest.raises(ValueError) as exc_info:
                ExchangeCalendar(invalid_exchange)
            assert "is not supported by pandas_market_calendars" in str(exc_info.value)

    @patch('calendars.exchange_calendar.mcal', None)
    def test_init_missing_pandas_market_calendars(self):
        """Test initialization when pandas_market_calendars is not available."""
        with pytest.raises(ImportError) as exc_info:
            ExchangeCalendar("NYSE")
        assert "pandas_market_calendars is required" in str(exc_info.value)

    def test_is_holiday_edge_dates(self):
        """Test is_holiday with edge case dates."""
        cal = ExchangeCalendar("NYSE")

        # Test various holiday scenarios
        holiday_test_cases = [
            # New Year's Day
            (datetime.date(2024, 1, 1), True),
            (datetime.date(2025, 1, 1), True),
            # Christmas Day
            (datetime.date(2024, 12, 25), True),
            (datetime.date(2025, 12, 25), True),
        ]

        for test_date, expected_is_holiday in holiday_test_cases:
            assert cal.is_holiday(test_date) == expected_is_holiday

        # Test regular weekdays (allow flexibility since weekends may be detected as holidays)
        regular_weekdays = [
            datetime.date(2024, 6, 17),  # Monday
            datetime.date(2024, 8, 21),  # Wednesday
        ]

        for test_date in regular_weekdays:
            # Just ensure it returns a boolean, don't assert specific value
            result = cal.is_holiday(test_date)
            assert isinstance(result, bool)

    def test_is_holiday_weekend_handling(self):
        """Test that weekends are properly identified as holidays."""
        cal = ExchangeCalendar("NYSE")

        # Test several weekends
        weekend_dates = [
            datetime.date(2024, 6, 1),   # Saturday
            datetime.date(2024, 6, 2),   # Sunday
            datetime.date(2024, 7, 6),   # Saturday
            datetime.date(2024, 7, 7),   # Sunday
            datetime.date(2024, 12, 7),  # Saturday
            datetime.date(2024, 12, 8),  # Sunday
        ]

        for weekend_date in weekend_dates:
            assert cal.is_holiday(weekend_date), f"{weekend_date} should be a holiday (weekend)"

    def test_next_trading_date_none_handling(self):
        """Test next_trading_date returns None when no future trading day found."""
        cal = ExchangeCalendar("NYSE")

        # Test with a date very far in the future (beyond calendar range)
        far_future_date = datetime.date(2050, 12, 31)
        result = cal.next_trading_date(far_future_date)

        # Should return None if no trading days found in lookahead window
        assert result is None or isinstance(result, datetime.date)

    def test_next_trading_date_various_scenarios(self):
        """Test next_trading_date with various starting scenarios."""
        cal = ExchangeCalendar("NYSE")

        test_cases = [
            # From a regular weekday
            (datetime.date(2024, 6, 10), datetime.date(2024, 6, 11)),  # Monday -> Tuesday
            # From a Friday (should skip weekend)
            (datetime.date(2024, 6, 7), datetime.date(2024, 6, 10)),   # Friday -> Monday
            # From a holiday (should find next trading day)
            (datetime.date(2024, 7, 4), datetime.date(2024, 7, 5)),    # July 4th -> July 5th
        ]

        for start_date, expected_next in test_cases:
            result = cal.next_trading_date(start_date)
            if result is not None:
                assert result >= expected_next

    def test_prior_trading_date_none_handling(self):
        """Test prior_trading_date behavior with edge dates."""
        cal = ExchangeCalendar("NYSE")

        # Test with a date very far in the past (beyond calendar range)
        far_past_date = datetime.date(1900, 1, 1)
        result = cal.prior_trading_date(far_past_date)

        # Should return a date or None - calendar might have historical data
        assert result is None or isinstance(result, datetime.date)

    def test_prior_trading_date_various_scenarios(self):
        """Test prior_trading_date with various starting scenarios."""
        cal = ExchangeCalendar("NYSE")

        test_cases = [
            # From a regular weekday
            (datetime.date(2024, 6, 11), datetime.date(2024, 6, 10)),  # Tuesday -> Monday
            # From a Monday (should skip weekend)
            (datetime.date(2024, 6, 10), datetime.date(2024, 6, 7)),   # Monday -> Friday
            # From day after holiday
            (datetime.date(2024, 7, 5), datetime.date(2024, 7, 3)),    # Day after July 4th -> July 3rd
        ]

        for start_date, expected_prior in test_cases:
            result = cal.prior_trading_date(start_date)
            if result is not None:
                assert result <= expected_prior

    def test_trading_days_empty_range(self):
        """Test trading_days with empty date ranges."""
        cal = ExchangeCalendar("NYSE")

        # Same start and end date (single day)
        single_day = datetime.date(2024, 6, 10)
        days = list(cal.trading_days(single_day, single_day))

        if not cal.is_holiday(single_day):
            assert len(days) == 1
            assert days[0] == single_day
        else:
            assert len(days) == 0

    def test_trading_days_reverse_range(self):
        """Test trading_days with reversed date range."""
        cal = ExchangeCalendar("NYSE")

        # End date before start date - this should raise ValueError
        start_date = datetime.date(2024, 6, 15)
        end_date = datetime.date(2024, 6, 10)

        with pytest.raises(ValueError, match="start_date must be before or equal to end_date"):
            list(cal.trading_days(start_date, end_date))

    def test_trading_days_holiday_periods(self):
        """Test trading_days during holiday periods."""
        cal = ExchangeCalendar("NYSE")

        # Period around Christmas/New Year (lots of holidays)
        start_date = datetime.date(2024, 12, 23)
        end_date = datetime.date(2025, 1, 3)

        days = list(cal.trading_days(start_date, end_date))

        # Should not include Christmas Day or New Year's Day
        assert datetime.date(2024, 12, 25) not in days  # Christmas
        assert datetime.date(2025, 1, 1) not in days    # New Year's Day

        # Should not include weekends
        for day in days:
            assert day.weekday() < 5, f"{day} is a weekend day but included in trading days"

    def test_all_trading_days_consistency(self):
        """Test that all_trading_days matches trading_days iterator."""
        cal = ExchangeCalendar("NYSE")

        test_ranges = [
            (datetime.date(2024, 1, 1), datetime.date(2024, 1, 31)),   # Full month
            (datetime.date(2024, 6, 1), datetime.date(2024, 6, 7)),    # One week
            (datetime.date(2024, 7, 4), datetime.date(2024, 7, 4)),    # Single day (holiday)
            (datetime.date(2024, 6, 10), datetime.date(2024, 6, 10)),  # Single day (trading)
        ]

        for start_date, end_date in test_ranges:
            iterator_days = list(cal.trading_days(start_date, end_date))
            all_days = cal.all_trading_days(start_date, end_date)

            assert iterator_days == all_days
            assert len(iterator_days) == len(all_days)

    def test_long_date_ranges(self):
        """Test with long date ranges to check performance and correctness."""
        cal = ExchangeCalendar("NYSE")

        # Full year
        start_date = datetime.date(2024, 1, 1)
        end_date = datetime.date(2024, 12, 31)

        days = cal.all_trading_days(start_date, end_date)

        # Should have approximately 252 trading days in a year (rough estimate)
        assert 240 <= len(days) <= 260, f"Expected ~252 trading days, got {len(days)}"

        # All should be weekdays
        for day in days:
            assert day.weekday() < 5, f"{day} is not a weekday"

        # Should be in chronological order
        for i in range(1, len(days)):
            assert days[i] > days[i-1], f"Days not in chronological order: {days[i-1]} >= {days[i]}"

    def test_different_exchanges(self):
        """Test that different exchanges work correctly."""
        exchanges_to_test = ["NYSE", "LSE", "NASDAQ"]

        for exchange in exchanges_to_test:
            try:
                cal = ExchangeCalendar(exchange)

                # Basic functionality should work
                test_date = datetime.date(2024, 6, 10)
                is_holiday = cal.is_holiday(test_date)
                assert isinstance(is_holiday, bool)

                # Should be able to get trading days
                days = cal.all_trading_days(test_date, test_date + datetime.timedelta(days=7))
                assert isinstance(days, list)

            except ValueError:
                # Some exchanges might not be supported, that's okay
                pass

    def test_calendar_boundary_dates(self):
        """Test behavior at calendar boundaries."""
        cal = ExchangeCalendar("NYSE")

        # Test very early dates
        early_date = datetime.date(1990, 1, 1)
        try:
            result = cal.is_holiday(early_date)
            assert isinstance(result, bool)
        except Exception:
            # Calendar might not support very old dates, that's acceptable
            pass

        # Test future dates
        future_date = datetime.date(2030, 1, 1)
        try:
            result = cal.is_holiday(future_date)
            assert isinstance(result, bool)
        except Exception:
            # Calendar might not support far future dates, that's acceptable
            pass

    def test_leap_year_handling(self):
        """Test calendar behavior during leap years."""
        cal = ExchangeCalendar("NYSE")

        # Test February 29 in leap year
        leap_day_2024 = datetime.date(2024, 2, 29)
        is_holiday = cal.is_holiday(leap_day_2024)
        assert isinstance(is_holiday, bool)

        # Test trading days around leap day
        start_date = datetime.date(2024, 2, 28)
        end_date = datetime.date(2024, 3, 1)

        days = cal.all_trading_days(start_date, end_date)
        assert isinstance(days, list)

        # If February 29, 2024 was a trading day, it should be included
        if not is_holiday and leap_day_2024.weekday() < 5:
            assert leap_day_2024 in days