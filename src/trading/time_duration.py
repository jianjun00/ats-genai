from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from enum import Enum
from typing import Union


class DurationType(Enum):
    """Enumeration of supported duration types."""
    MINUTES_5 = "5m"
    MINUTES_15 = "15m"
    MINUTES_60 = "60m"
    DAILY = "1d"
    WEEKLY = "1w"
    MONTHLY = "1m"
    QUARTERLY = "1q"
    YEARLY = "1y"


class TimeDuration:
    """
    A class to represent time durations and calculate end times from start times.
    
    Supports various duration types:
    - 5 minutes, 15 minutes, 60 minutes
    - 1 day, 1 week, 1 month, 1 quarter, 1 year
    """
    
    def __init__(self, duration_type: Union[DurationType, str]):
        """
        Initialize TimeDuration with a duration type.
        
        Args:
            duration_type: Either a DurationType enum or string representation
        """
        if isinstance(duration_type, str):
            # Convert string to DurationType
            duration_map = {
                "5m": DurationType.MINUTES_5,
                "15m": DurationType.MINUTES_15,
                "60m": DurationType.MINUTES_60,
                "1d": DurationType.DAILY,
                "1w": DurationType.WEEKLY,
                "1m": DurationType.MONTHLY,
                "1q": DurationType.QUARTERLY,
                "1y": DurationType.YEARLY
            }
            if duration_type not in duration_map:
                raise ValueError(f"Invalid duration type: {duration_type}. "
                               f"Supported types: {list(duration_map.keys())}")
            self.duration_type = duration_map[duration_type]
        else:
            self.duration_type = duration_type
    
    def get_end_time(self, start_time: datetime) -> datetime:
        """
        Calculate the end time based on the start time and duration type.
        
        Args:
            start_time: The starting datetime
            
        Returns:
            The calculated end datetime
        """
        if self.duration_type == DurationType.MINUTES_5:
            return start_time + timedelta(minutes=5)
        elif self.duration_type == DurationType.MINUTES_15:
            return start_time + timedelta(minutes=15)
        elif self.duration_type == DurationType.MINUTES_60:
            return start_time + timedelta(hours=1)
        elif self.duration_type == DurationType.DAILY:
            return start_time + timedelta(days=1)
        elif self.duration_type == DurationType.WEEKLY:
            return start_time + timedelta(weeks=1)
        elif self.duration_type == DurationType.MONTHLY:
            return start_time + relativedelta(months=1)
        elif self.duration_type == DurationType.QUARTERLY:
            return start_time + relativedelta(months=3)
        elif self.duration_type == DurationType.YEARLY:
            return start_time + relativedelta(years=1)
        else:
            raise ValueError(f"Unsupported duration type: {self.duration_type}")
    
    def get_duration_string(self) -> str:
        """Get the string representation of the duration type."""
        return self.duration_type.value
    
    def get_duration_minutes(self) -> Union[int, None]:
        """
        Get the duration in minutes for minute-based durations.
        Returns None for date-based durations (day, week, month, quarter, year).
        """
        if self.duration_type == DurationType.MINUTES_5:
            return 5
        elif self.duration_type == DurationType.MINUTES_15:
            return 15
        elif self.duration_type == DurationType.MINUTES_60:
            return 60
        else:
            return None  # Date-based durations don't have fixed minute values
    
    def is_intraday(self) -> bool:
        """Check if this is an intraday duration (minutes/hours)."""
        return self.duration_type in [
            DurationType.MINUTES_5,
            DurationType.MINUTES_15,
            DurationType.MINUTES_60
        ]
    
    def is_daily_or_longer(self) -> bool:
        """Check if this is a daily or longer duration."""
        return self.duration_type in [
            DurationType.DAILY,
            DurationType.WEEKLY,
            DurationType.MONTHLY,
            DurationType.QUARTERLY,
            DurationType.YEARLY
        ]
    
    def __str__(self) -> str:
        """String representation of the TimeDuration."""
        return f"TimeDuration({self.duration_type.value})"
    
    def __repr__(self) -> str:
        """Detailed string representation of the TimeDuration."""
        return f"TimeDuration(duration_type={self.duration_type})"
    
    def __eq__(self, other) -> bool:
        """Check equality with another TimeDuration."""
        if not isinstance(other, TimeDuration):
            return False
        return self.duration_type == other.duration_type
    
    def __hash__(self) -> int:
        """Make TimeDuration hashable."""
        return hash(self.duration_type)
    
    @classmethod
    def get_all_supported_durations(cls) -> list[str]:
        """Get a list of all supported duration strings."""
        return [duration.value for duration in DurationType]
    
    @classmethod
    def create_5_minutes(cls) -> 'TimeDuration':
        """Factory method to create a 5-minute duration."""
        return cls(DurationType.MINUTES_5)
    
    @classmethod
    def create_15_minutes(cls) -> 'TimeDuration':
        """Factory method to create a 15-minute duration."""
        return cls(DurationType.MINUTES_15)
    
    @classmethod
    def create_60_minutes(cls) -> 'TimeDuration':
        """Factory method to create a 60-minute duration."""
        return cls(DurationType.MINUTES_60)
    
    @classmethod
    def create_daily(cls) -> 'TimeDuration':
        """Factory method to create a daily duration."""
        return cls(DurationType.DAILY)
    
    @classmethod
    def create_weekly(cls) -> 'TimeDuration':
        """Factory method to create a weekly duration."""
        return cls(DurationType.WEEKLY)
    
    @classmethod
    def create_monthly(cls) -> 'TimeDuration':
        """Factory method to create a monthly duration."""
        return cls(DurationType.MONTHLY)
    
    @classmethod
    def create_quarterly(cls) -> 'TimeDuration':
        """Factory method to create a quarterly duration."""
        return cls(DurationType.QUARTERLY)
    
    @classmethod
    def create_yearly(cls) -> 'TimeDuration':
        """Factory method to create a yearly duration."""
        return cls(DurationType.YEARLY)
