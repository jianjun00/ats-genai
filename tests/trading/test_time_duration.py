import pytest
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from trading.time_duration import TimeDuration, DurationType


class TestTimeDuration:
    """Test cases for TimeDuration class."""
    
    def test_init_with_duration_type_enum(self):
        """Test initialization with DurationType enum."""
        duration = TimeDuration(DurationType.MINUTES_5)
        assert duration.duration_type == DurationType.MINUTES_5
        assert duration.get_duration_string() == "5m"
    
    def test_init_with_string(self):
        """Test initialization with string duration type."""
        duration = TimeDuration("15m")
        assert duration.duration_type == DurationType.MINUTES_15
        assert duration.get_duration_string() == "15m"
    
    def test_init_with_invalid_string(self):
        """Test initialization with invalid string raises ValueError."""
        with pytest.raises(ValueError, match="Invalid duration type: invalid"):
            TimeDuration("invalid")
    
    def test_get_end_time_5_minutes(self):
        """Test get_end_time for 5-minute duration."""
        duration = TimeDuration("5m")
        start_time = datetime(2023, 1, 1, 9, 30, 0)
        expected_end = datetime(2023, 1, 1, 9, 35, 0)
        assert duration.get_end_time(start_time) == expected_end
    
    def test_get_end_time_15_minutes(self):
        """Test get_end_time for 15-minute duration."""
        duration = TimeDuration("15m")
        start_time = datetime(2023, 1, 1, 9, 30, 0)
        expected_end = datetime(2023, 1, 1, 9, 45, 0)
        assert duration.get_end_time(start_time) == expected_end
    
    def test_get_end_time_60_minutes(self):
        """Test get_end_time for 60-minute duration."""
        duration = TimeDuration("60m")
        start_time = datetime(2023, 1, 1, 9, 30, 0)
        expected_end = datetime(2023, 1, 1, 10, 30, 0)
        assert duration.get_end_time(start_time) == expected_end
    
    def test_get_end_time_daily(self):
        """Test get_end_time for daily duration."""
        duration = TimeDuration("1d")
        start_time = datetime(2023, 1, 1, 9, 30, 0)
        expected_end = datetime(2023, 1, 2, 9, 30, 0)
        assert duration.get_end_time(start_time) == expected_end
    
    def test_get_end_time_weekly(self):
        """Test get_end_time for weekly duration."""
        duration = TimeDuration("1w")
        start_time = datetime(2023, 1, 1, 9, 30, 0)  # Sunday
        expected_end = datetime(2023, 1, 8, 9, 30, 0)  # Next Sunday
        assert duration.get_end_time(start_time) == expected_end
    
    def test_get_end_time_monthly(self):
        """Test get_end_time for monthly duration."""
        duration = TimeDuration("1m")
        start_time = datetime(2023, 1, 15, 9, 30, 0)
        expected_end = datetime(2023, 2, 15, 9, 30, 0)
        assert duration.get_end_time(start_time) == expected_end
    
    def test_get_end_time_monthly_end_of_month(self):
        """Test get_end_time for monthly duration at end of month."""
        duration = TimeDuration("1m")
        start_time = datetime(2023, 1, 31, 9, 30, 0)
        # February doesn't have 31 days, should go to Feb 28
        expected_end = datetime(2023, 2, 28, 9, 30, 0)
        assert duration.get_end_time(start_time) == expected_end
    
    def test_get_end_time_quarterly(self):
        """Test get_end_time for quarterly duration."""
        duration = TimeDuration("1q")
        start_time = datetime(2023, 1, 15, 9, 30, 0)
        expected_end = datetime(2023, 4, 15, 9, 30, 0)
        assert duration.get_end_time(start_time) == expected_end
    
    def test_get_end_time_yearly(self):
        """Test get_end_time for yearly duration."""
        duration = TimeDuration("1y")
        start_time = datetime(2023, 1, 15, 9, 30, 0)
        expected_end = datetime(2024, 1, 15, 9, 30, 0)
        assert duration.get_end_time(start_time) == expected_end
    
    def test_get_end_time_leap_year(self):
        """Test get_end_time handles leap year correctly."""
        duration = TimeDuration("1y")
        start_time = datetime(2020, 2, 29, 9, 30, 0)  # Leap year
        expected_end = datetime(2021, 2, 28, 9, 30, 0)  # Non-leap year
        assert duration.get_end_time(start_time) == expected_end
    
    def test_get_duration_minutes(self):
        """Test get_duration_minutes for minute-based durations."""
        assert TimeDuration("5m").get_duration_minutes() == 5
        assert TimeDuration("15m").get_duration_minutes() == 15
        assert TimeDuration("60m").get_duration_minutes() == 60
        
        # Date-based durations should return None
        assert TimeDuration("1d").get_duration_minutes() is None
        assert TimeDuration("1w").get_duration_minutes() is None
        assert TimeDuration("1m").get_duration_minutes() is None
        assert TimeDuration("1q").get_duration_minutes() is None
        assert TimeDuration("1y").get_duration_minutes() is None
    
    def test_is_intraday(self):
        """Test is_intraday method."""
        assert TimeDuration("5m").is_intraday() is True
        assert TimeDuration("15m").is_intraday() is True
        assert TimeDuration("60m").is_intraday() is True
        
        assert TimeDuration("1d").is_intraday() is False
        assert TimeDuration("1w").is_intraday() is False
        assert TimeDuration("1m").is_intraday() is False
        assert TimeDuration("1q").is_intraday() is False
        assert TimeDuration("1y").is_intraday() is False
    
    def test_is_daily_or_longer(self):
        """Test is_daily_or_longer method."""
        assert TimeDuration("5m").is_daily_or_longer() is False
        assert TimeDuration("15m").is_daily_or_longer() is False
        assert TimeDuration("60m").is_daily_or_longer() is False
        
        assert TimeDuration("1d").is_daily_or_longer() is True
        assert TimeDuration("1w").is_daily_or_longer() is True
        assert TimeDuration("1m").is_daily_or_longer() is True
        assert TimeDuration("1q").is_daily_or_longer() is True
        assert TimeDuration("1y").is_daily_or_longer() is True
    
    def test_string_representations(self):
        """Test string representations."""
        duration = TimeDuration("5m")
        assert str(duration) == "TimeDuration(5m)"
        assert repr(duration) == "TimeDuration(duration_type=DurationType.MINUTES_5)"
    
    def test_equality(self):
        """Test equality comparison."""
        duration1 = TimeDuration("5m")
        duration2 = TimeDuration("5m")
        duration3 = TimeDuration("15m")
        
        assert duration1 == duration2
        assert duration1 != duration3
        assert duration1 != "5m"  # Different type
    
    def test_hashable(self):
        """Test that TimeDuration is hashable."""
        duration1 = TimeDuration("5m")
        duration2 = TimeDuration("5m")
        duration3 = TimeDuration("15m")
        
        # Should be able to use in sets and as dict keys
        duration_set = {duration1, duration2, duration3}
        assert len(duration_set) == 2  # duration1 and duration2 are equal
        
        duration_dict = {duration1: "five_minutes", duration3: "fifteen_minutes"}
        assert len(duration_dict) == 2
    
    def test_get_all_supported_durations(self):
        """Test get_all_supported_durations class method."""
        supported = TimeDuration.get_all_supported_durations()
        expected = ["5m", "15m", "60m", "1d", "1w", "1m", "1q", "1y"]
        assert supported == expected
    
    def test_factory_methods(self):
        """Test factory methods for creating TimeDuration instances."""
        assert TimeDuration.create_5_minutes() == TimeDuration("5m")
        assert TimeDuration.create_15_minutes() == TimeDuration("15m")
        assert TimeDuration.create_60_minutes() == TimeDuration("60m")
        assert TimeDuration.create_daily() == TimeDuration("1d")
        assert TimeDuration.create_weekly() == TimeDuration("1w")
        assert TimeDuration.create_monthly() == TimeDuration("1m")
        assert TimeDuration.create_quarterly() == TimeDuration("1q")
        assert TimeDuration.create_yearly() == TimeDuration("1y")
    
    def test_all_duration_types_covered(self):
        """Test that all duration types can be created and used."""
        all_durations = ["5m", "15m", "60m", "1d", "1w", "1m", "1q", "1y"]
        start_time = datetime(2023, 6, 15, 10, 30, 0)  # Mid-year, mid-month
        
        for duration_str in all_durations:
            duration = TimeDuration(duration_str)
            end_time = duration.get_end_time(start_time)
            
            # End time should always be after start time
            assert end_time > start_time
            
            # Duration string should match
            assert duration.get_duration_string() == duration_str
    
    def test_edge_cases_month_boundaries(self):
        """Test edge cases around month boundaries."""
        duration = TimeDuration("1m")
        
        # Test various month-end dates
        test_cases = [
            (datetime(2023, 1, 31, 12, 0, 0), datetime(2023, 2, 28, 12, 0, 0)),  # Jan 31 -> Feb 28
            (datetime(2023, 3, 31, 12, 0, 0), datetime(2023, 4, 30, 12, 0, 0)),  # Mar 31 -> Apr 30
            (datetime(2023, 5, 31, 12, 0, 0), datetime(2023, 6, 30, 12, 0, 0)),  # May 31 -> Jun 30
        ]
        
        for start_time, expected_end in test_cases:
            actual_end = duration.get_end_time(start_time)
            assert actual_end == expected_end
    
    def test_quarter_boundaries(self):
        """Test quarterly duration across different quarters."""
        duration = TimeDuration("1q")
        
        test_cases = [
            (datetime(2023, 1, 15, 12, 0, 0), datetime(2023, 4, 15, 12, 0, 0)),  # Q1 -> Q2
            (datetime(2023, 4, 15, 12, 0, 0), datetime(2023, 7, 15, 12, 0, 0)),  # Q2 -> Q3
            (datetime(2023, 7, 15, 12, 0, 0), datetime(2023, 10, 15, 12, 0, 0)), # Q3 -> Q4
            (datetime(2023, 10, 15, 12, 0, 0), datetime(2024, 1, 15, 12, 0, 0)), # Q4 -> Q1 next year
        ]
        
        for start_time, expected_end in test_cases:
            actual_end = duration.get_end_time(start_time)
            assert actual_end == expected_end
