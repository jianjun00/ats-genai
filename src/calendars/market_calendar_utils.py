import pandas_market_calendars as mcal
import pandas as pd
from typing import Optional, Tuple
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
