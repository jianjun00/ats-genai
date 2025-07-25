import pandas_market_calendars as mcal
import pandas as pd
from typing import Optional, Tuple

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
