import datetime
from typing import Iterator, List, Optional

try:
    import pandas as pd
    import pandas_market_calendars as mcal
except ImportError:
    pd = None
    mcal = None

class ExchangeCalendar:
    def __init__(self, exchange: str):
        """
        Initialize the ExchangeCalendar for a given exchange (e.g., 'NYSE', 'LSE').
        """
        if mcal is None:
            raise ImportError("pandas_market_calendars is required for ExchangeCalendar. Install via pip.")
        self.exchange = exchange.upper()
        try:
            self.calendar = mcal.get_calendar(self.exchange)
        except Exception as e:
            raise ValueError(f"Exchange '{exchange}' is not supported by pandas_market_calendars: {e}")

    def is_holiday(self, date: datetime.date) -> bool:
        sched = self.calendar.schedule(str(date), str(date))
        return sched.empty

    def next_trading_date(self, date: datetime.date) -> Optional[datetime.date]:
        # Query a wide range forward to ensure we get the next day
        lookahead = 30
        sched = self.calendar.schedule(str(date), str(date + datetime.timedelta(days=lookahead)))
        dates = sched.index.date
        future = [d for d in dates if d > date]
        return future[0] if future else None

    def prior_trading_date(self, date: datetime.date) -> Optional[datetime.date]:
        # Query a wide range backward to ensure we get the prior day, up to 90 days
        lookback = 90
        search_start = date - datetime.timedelta(days=lookback)
        search_end = date - datetime.timedelta(days=1)
        sched = self.calendar.schedule(str(search_start), str(search_end))
        dates = sched.index.date
        if len(dates) == 0:
            return None
        return dates[-1]

    def trading_days(self, start_date: datetime.date, end_date: datetime.date) -> Iterator[datetime.date]:
        sched = self.calendar.schedule(str(start_date), str(end_date))
        for d in sched.index.date:
            yield d

    def all_trading_days(self, start_date: datetime.date, end_date: datetime.date) -> List[datetime.date]:
        return list(self.trading_days(start_date, end_date))
