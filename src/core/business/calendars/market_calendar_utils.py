import pandas_market_calendars as mcal
import pandas as pd
from typing import Optional, Tuple, List
from datetime import date, timedelta, datetime

import calendar
# Export day_abbr as in the standard library (Monday=0)
day_abbr = list(calendar.day_abbr)

def get_market_calendar(exchange: str):
    """Get a pandas_market_calendars calendar for the given exchange code (e.g., 'LSE')."""
    return mcal.get_calendar(exchange)

def get_last_open_close(mkt_calendar, dt: pd.Timestamp) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """
    Given a pandas_market_calendars calendar and a datetime,
    return the last market open and close times before or at dt.
    """
    if not isinstance(dt, pd.Timestamp):
        dt = pd.Timestamp(dt)
    schedule = mkt_calendar.schedule.loc[:dt]
    if not schedule.empty:
        last_open = schedule['market_open'].iloc[-1]
        last_close = schedule['market_close'].iloc[-1]
        return last_open, last_close
    return None, None

def get_next_open_close(mkt_calendar, dt: pd.Timestamp) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """
    Given a pandas_market_calendars calendar and a datetime,
    return the next market open and close times after dt.
    """
    if not isinstance(dt, pd.Timestamp):
        dt = pd.Timestamp(dt)
    schedule = mkt_calendar.schedule.loc[dt:]
    if not schedule.empty:
        next_open = schedule['market_open'].iloc[0]
        next_close = schedule['market_close'].iloc[0]
        return next_open, next_close
    return None, None

def is_trading_day(trading_date: date, exchange: str = 'NASDAQ') -> bool:
    """
    Check if a given date is a trading day for the specified exchange.

    Args:
        trading_date: Date to check
        exchange: Exchange code (default: NASDAQ)

    Returns:
        True if trading day, False otherwise
    """
    try:
        calendar = get_market_calendar(exchange)

        # Convert to pandas Timestamp
        if isinstance(trading_date, date):
            pd_date = pd.Timestamp(trading_date)
        else:
            pd_date = pd.Timestamp(trading_date)

        # Get valid trading days for this date
        valid_days = calendar.valid_days(
            start_date=pd_date,
            end_date=pd_date
        )

        return len(valid_days) > 0

    except Exception:
        # Fallback to weekday check if calendar fails
        return trading_date.weekday() < 5  # Monday=0, Friday=4

def get_previous_trading_day(current_date: date, exchange: str = 'NASDAQ') -> Optional[date]:
    """
    Get the previous trading day before the given date.

    Args:
        current_date: Current date
        exchange: Exchange code (default: NASDAQ)

    Returns:
        Previous trading day as date, or None if not found
    """
    try:
        calendar = get_market_calendar(exchange)

        # Convert to pandas Timestamp
        if isinstance(current_date, date):
            pd_date = pd.Timestamp(current_date)
        else:
            pd_date = pd.Timestamp(current_date)

        # Get valid trading days ending before current date
        end_date = pd_date - pd.Timedelta(days=1)
        start_date = end_date - pd.Timedelta(days=10)  # Look back 10 days max

        valid_days = calendar.valid_days(
            start_date=start_date,
            end_date=end_date
        )

        if len(valid_days) > 0:
            return valid_days[-1].date()

        return None

    except Exception:
        # Fallback logic - go back until we find a weekday
        check_date = current_date - timedelta(days=1)
        for _ in range(7):  # Max 7 days back
            if check_date.weekday() < 5:  # Monday=0, Friday=4
                return check_date
            check_date -= timedelta(days=1)

        return None

def is_market_open(dt: datetime, exchange: str = 'NASDAQ') -> bool:
    """
    Check if the market is currently open at the given datetime.

    Args:
        dt: Datetime to check (should be timezone-aware)
        exchange: Exchange code (default: NASDAQ)

    Returns:
        True if market is open, False otherwise
    """
    try:
        calendar = get_market_calendar(exchange)

        # Convert to pandas Timestamp if needed
        if not isinstance(dt, pd.Timestamp):
            pd_dt = pd.Timestamp(dt)
        else:
            pd_dt = dt

        # Get market schedule for the date
        schedule = calendar.schedule(start_date=pd_dt.date(), end_date=pd_dt.date())

        if schedule.empty:
            return False

        # Check if datetime is between market open and close
        market_open = schedule['market_open'].iloc[0]
        market_close = schedule['market_close'].iloc[0]

        return market_open <= pd_dt <= market_close

    except Exception:
        # Fallback: assume market is open during weekdays 9:30 AM - 4:00 PM ET
        if dt.weekday() >= 5:  # Weekend
            return False

        # Convert to ET if not already
        et_tz = pd.Timestamp.now().tz_localize('US/Eastern').tz
        if dt.tzinfo:
            et_time = dt.astimezone(et_tz)
        else:
            et_time = dt.replace(tzinfo=et_tz)

        market_open_time = et_time.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close_time = et_time.replace(hour=16, minute=0, second=0, microsecond=0)

        return market_open_time <= et_time <= market_close_time

def get_market_hours(trading_date: date, exchange: str = 'NASDAQ') -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Get market open and close times for a given trading date.

    Args:
        trading_date: Date to get market hours for
        exchange: Exchange code (default: NASDAQ)

    Returns:
        Tuple of (market_open_datetime, market_close_datetime), or (None, None) if not a trading day
    """
    try:
        calendar = get_market_calendar(exchange)

        # Convert to pandas Timestamp
        if isinstance(trading_date, date):
            pd_date = pd.Timestamp(trading_date)
        else:
            pd_date = pd.Timestamp(trading_date)

        # Get market schedule for the date
        schedule = calendar.schedule(start_date=pd_date.date(), end_date=pd_date.date())

        if schedule.empty:
            return None, None

        market_open = schedule['market_open'].iloc[0]
        market_close = schedule['market_close'].iloc[0]

        # Convert to Python datetime objects
        return market_open.to_pydatetime(), market_close.to_pydatetime()

    except Exception:
        # Fallback: assume standard market hours for weekdays
        if trading_date.weekday() >= 5:  # Weekend
            return None, None

        # Standard NYSE/NASDAQ hours: 9:30 AM - 4:00 PM ET
        import pytz
        et_tz = pytz.timezone('US/Eastern')
        market_open = et_tz.localize(datetime.combine(trading_date, datetime.min.time().replace(hour=9, minute=30)))
        market_close = et_tz.localize(datetime.combine(trading_date, datetime.min.time().replace(hour=16, minute=0)))

        return market_open, market_close

def get_trading_days(start_date: date, end_date: date, exchange: str = 'NASDAQ') -> List[date]:
    """
    Get all trading days between start_date and end_date (inclusive).

    Args:
        start_date: Start date
        end_date: End date
        exchange: Exchange code (default: NASDAQ)

    Returns:
        List of trading days as date objects
    """
    try:
        calendar = get_market_calendar(exchange)

        # Convert to pandas Timestamps
        if isinstance(start_date, date):
            pd_start = pd.Timestamp(start_date)
        else:
            pd_start = pd.Timestamp(start_date)

        if isinstance(end_date, date):
            pd_end = pd.Timestamp(end_date)
        else:
            pd_end = pd.Timestamp(end_date)

        # Get valid trading days
        valid_days = calendar.valid_days(start_date=pd_start, end_date=pd_end)

        # Convert to Python date objects
        return [day.date() for day in valid_days]

    except Exception:
        # Fallback: generate weekdays between dates
        trading_days = []
        current = start_date

        while current <= end_date:
            if current.weekday() < 5:  # Monday=0, Friday=4
                trading_days.append(current)
            current += timedelta(days=1)

        return trading_days
