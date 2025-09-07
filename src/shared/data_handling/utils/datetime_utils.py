"""
Date and time utilities for ATS-GenAI.

This module provides standardized date/time handling with timezone awareness
and market calendar integration.
"""

from datetime import datetime, date, time, timedelta
from typing import Optional, Union, List
from zoneinfo import ZoneInfo



# Market timezones
US_EASTERN = ZoneInfo("America/New_York")
US_CENTRAL = ZoneInfo("America/Chicago")
US_PACIFIC = ZoneInfo("America/Los_Angeles")
UTC = ZoneInfo("UTC")

# Market hours (US Eastern Time)
MARKET_OPEN_TIME = time(9, 30)  # 9:30 AM
MARKET_CLOSE_TIME = time(16, 0)  # 4:00 PM
PRE_MARKET_OPEN = time(4, 0)    # 4:00 AM
AFTER_HOURS_CLOSE = time(20, 0)  # 8:00 PM


def get_current_time(timezone: Optional[ZoneInfo] = None) -> datetime:
    """Get current time in specified timezone."""
    tz = timezone or UTC
    return datetime.now(tz)


def get_current_market_time() -> datetime:
    """Get current time in US Eastern (market timezone)."""
    return get_current_time(US_EASTERN)


def to_utc(dt: datetime) -> datetime:
    """Convert datetime to UTC."""
    if dt.tzinfo is None:
        # Assume local timezone if naive
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(UTC)


def to_market_time(dt: datetime) -> datetime:
    """Convert datetime to US Eastern (market timezone)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(US_EASTERN)


def is_market_hours(dt: Optional[datetime] = None) -> bool:
    """Check if given time is during regular market hours."""
    if dt is None:
        dt = get_current_market_time()
    else:
        dt = to_market_time(dt)
    
    # Check if weekday (Monday=0, Sunday=6)
    if dt.weekday() > 4:  # Saturday or Sunday
        return False
    
    # Check if between market hours
    current_time = dt.time()
    return MARKET_OPEN_TIME <= current_time <= MARKET_CLOSE_TIME


def is_pre_market(dt: Optional[datetime] = None) -> bool:
    """Check if given time is during pre-market hours."""
    if dt is None:
        dt = get_current_market_time()
    else:
        dt = to_market_time(dt)
    
    if dt.weekday() > 4:  # Weekend
        return False
    
    current_time = dt.time()
    return PRE_MARKET_OPEN <= current_time < MARKET_OPEN_TIME


def is_after_hours(dt: Optional[datetime] = None) -> bool:
    """Check if given time is during after-hours trading."""
    if dt is None:
        dt = get_current_market_time()
    else:
        dt = to_market_time(dt)
    
    if dt.weekday() > 4:  # Weekend
        return False
    
    current_time = dt.time()
    return MARKET_CLOSE_TIME < current_time <= AFTER_HOURS_CLOSE


def get_trading_session(dt: Optional[datetime] = None) -> str:
    """Get current trading session."""
    if is_pre_market(dt):
        return "pre_market"
    elif is_market_hours(dt):
        return "market_hours"
    elif is_after_hours(dt):
        return "after_hours"
    else:
        return "closed"


def get_next_market_open(dt: Optional[datetime] = None) -> datetime:
    """Get next market open time."""
    if dt is None:
        dt = get_current_market_time()
    else:
        dt = to_market_time(dt)
    
    # If it's before market open today and it's a weekday
    if dt.weekday() < 5 and dt.time() < MARKET_OPEN_TIME:
        return dt.replace(
            hour=MARKET_OPEN_TIME.hour,
            minute=MARKET_OPEN_TIME.minute,
            second=0,
            microsecond=0
        )
    
    # Otherwise, next business day
    days_ahead = 1
    if dt.weekday() == 4:  # Friday
        days_ahead = 3  # Skip to Monday
    elif dt.weekday() == 5:  # Saturday
        days_ahead = 2  # Skip to Monday
    
    next_open = dt + timedelta(days=days_ahead)
    return next_open.replace(
        hour=MARKET_OPEN_TIME.hour,
        minute=MARKET_OPEN_TIME.minute,
        second=0,
        microsecond=0
    )


def get_next_market_close(dt: Optional[datetime] = None) -> datetime:
    """Get next market close time."""
    if dt is None:
        dt = get_current_market_time()
    else:
        dt = to_market_time(dt)
    
    # If it's before market close today and it's a weekday
    if dt.weekday() < 5 and dt.time() < MARKET_CLOSE_TIME:
        return dt.replace(
            hour=MARKET_CLOSE_TIME.hour,
            minute=MARKET_CLOSE_TIME.minute,
            second=0,
            microsecond=0
        )
    
    # Otherwise, next business day
    days_ahead = 1
    if dt.weekday() == 4:  # Friday
        days_ahead = 3  # Skip to Monday
    elif dt.weekday() == 5:  # Saturday
        days_ahead = 2  # Skip to Monday
    
    next_close = dt + timedelta(days=days_ahead)
    return next_close.replace(
        hour=MARKET_CLOSE_TIME.hour,
        minute=MARKET_CLOSE_TIME.minute,
        second=0,
        microsecond=0
    )


def generate_business_days(
    start_date: Union[date, datetime],
    end_date: Union[date, datetime]
) -> List[date]:
    """Generate list of business days between start and end dates."""
    if isinstance(start_date, datetime):
        start_date = start_date.date()
    if isinstance(end_date, datetime):
        end_date = end_date.date()
    
    business_days = []
    current_date = start_date
    
    while current_date <= end_date:
        if current_date.weekday() < 5:  # Monday-Friday
            business_days.append(current_date)
        current_date += timedelta(days=1)
    
    return business_days


def get_trading_days_in_range(
    start_date: Union[date, datetime],
    end_date: Union[date, datetime]
) -> int:
    """Get number of trading days in date range."""
    return len(generate_business_days(start_date, end_date))


def format_datetime_for_api(dt: datetime, vendor: str = "polygon") -> str:
    """Format datetime for specific API vendor."""
    utc_dt = to_utc(dt)
    
    if vendor.lower() == "polygon":
        return utc_dt.strftime("%Y-%m-%d")
    elif vendor.lower() == "tiingo":
        return utc_dt.strftime("%Y-%m-%d")
    elif vendor.lower() == "alpha_vantage":
        return utc_dt.strftime("%Y-%m-%d")
    else:
        return utc_dt.isoformat()


def parse_api_datetime(date_str: str, vendor: str = "polygon") -> datetime:
    """Parse datetime from API vendor format."""
    if vendor.lower() == "polygon":
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
        except ValueError:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    elif vendor.lower() == "tiingo":
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    else:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))


def get_session_times(session_date: Union[date, datetime]) -> dict:
    """Get all session times for a given date."""
    if isinstance(session_date, datetime):
        session_date = session_date.date()
    
    # Create datetime for the session date in market timezone
    base_dt = datetime.combine(session_date, time(0, 0), tzinfo=US_EASTERN)
    
    return {
        "pre_market_open": base_dt.replace(
            hour=PRE_MARKET_OPEN.hour,
            minute=PRE_MARKET_OPEN.minute
        ),
        "market_open": base_dt.replace(
            hour=MARKET_OPEN_TIME.hour,
            minute=MARKET_OPEN_TIME.minute
        ),
        "market_close": base_dt.replace(
            hour=MARKET_CLOSE_TIME.hour,
            minute=MARKET_CLOSE_TIME.minute
        ),
        "after_hours_close": base_dt.replace(
            hour=AFTER_HOURS_CLOSE.hour,
            minute=AFTER_HOURS_CLOSE.minute
        )
    }


def time_until_market_open(dt: Optional[datetime] = None) -> timedelta:
    """Get time until next market open."""
    if dt is None:
        dt = get_current_market_time()
    
    next_open = get_next_market_open(dt)
    return next_open - dt


def time_until_market_close(dt: Optional[datetime] = None) -> timedelta:
    """Get time until next market close."""
    if dt is None:
        dt = get_current_market_time()
    
    next_close = get_next_market_close(dt)
    return next_close - dt


def round_to_minute(dt: datetime) -> datetime:
    """Round datetime to nearest minute."""
    return dt.replace(second=0, microsecond=0)


def round_to_hour(dt: datetime) -> datetime:
    """Round datetime to nearest hour."""
    return dt.replace(minute=0, second=0, microsecond=0)


def get_timeframe_duration(timeframe: str) -> timedelta:
    """Get duration for common timeframes."""
    timeframe_map = {
        "1m": timedelta(minutes=1),
        "5m": timedelta(minutes=5),
        "15m": timedelta(minutes=15),
        "30m": timedelta(minutes=30),
        "1h": timedelta(hours=1),
        "1d": timedelta(days=1),
        "1w": timedelta(weeks=1),
    }
    
    return timeframe_map.get(timeframe.lower(), timedelta(minutes=1))


def get_period_start(dt: datetime, timeframe: str) -> datetime:
    """Get start of period for given timeframe."""
    if timeframe == "1d":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif timeframe == "1h":
        return dt.replace(minute=0, second=0, microsecond=0)
    elif timeframe == "15m":
        minute = (dt.minute // 15) * 15
        return dt.replace(minute=minute, second=0, microsecond=0)
    elif timeframe == "5m":
        minute = (dt.minute // 5) * 5
        return dt.replace(minute=minute, second=0, microsecond=0)
    elif timeframe == "1m":
        return dt.replace(second=0, microsecond=0)
    else:
        return dt