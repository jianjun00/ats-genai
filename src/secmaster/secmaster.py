from config.environment import get_environment, Environment
from db.dao.secmaster_dao import SecMasterDAO
from datetime import date
from typing import List, Optional, Dict

class SecMaster:
    """
    Security master for S&P 500 membership. Can be used in two modes:
    1. Snapshot mode: initialized with as_of_date, all methods use this date.
    2. Batch mode: use get_spy_membership_over_dates(dates) for multiple dates efficiently.
    """
    def __init__(self, env: Environment = None, as_of_date: date = None):
        self.env = env or get_environment()
        self.dao = SecMasterDAO(self.env)
        self.as_of_date = as_of_date
        self._events = None  # Will hold all membership events
        self._membership_cache = {}  # Optional: cache for date->membership
        self._last_close_price_cache = {}
        self._market_cap_cache = {}
        self._adv_cache = {}


    async def load_all_membership_events(self):
        if self._events is None:
            self._events = await self.dao.get_spy_membership_events()

    async def get_spy_membership(self) -> List[str]:
        """Returns SPY membership as of self.as_of_date (set at init), using universe_membership table."""
        if self.as_of_date is None:
            raise ValueError("as_of_date must be set at initialization for snapshot mode.")
        await self.load_all_membership_events()
        if self.as_of_date in self._membership_cache:
            return sorted(self._membership_cache[self.as_of_date])
        membership = set()
        for row in self._events:
            # Add symbol if start_date <= as_of_date and (end_date is null or end_date > as_of_date)
            if row['start_date'] <= self.as_of_date and (row['end_date'] is None or row['end_date'] > self.as_of_date):
                membership.add(row['symbol'])
        self._membership_cache[self.as_of_date] = set(membership)
        return sorted(membership)

    async def advance(self, to_date: date) -> List[str]:
        """
        Advance the membership from self.as_of_date to to_date, updating caches for last_close_price, market_cap, and ADV (30d) for all tickers.
        Returns the new membership as a sorted list.
        """
        if self.as_of_date is None:
            raise ValueError("as_of_date must be set at initialization for advance().")
        await self.load_all_membership_events()
        self.as_of_date = to_date
        # Recompute membership as of to_date using new logic
        membership = set()
        for row in self._events:
            if row['start_date'] <= to_date and (row['end_date'] is None or row['end_date'] > to_date):
                membership.add(row['symbol'])
        # Update caches for tickers in new membership
        tickers = list(membership)
        self._last_close_price_cache = await self.dao.batch_last_close_prices(to_date, tickers)
        self._market_cap_cache = await self.dao.batch_market_caps(to_date, tickers)
        adv_cache = {}
        for ticker in tickers:
            adv = await self.dao.get_average_dollar_volume(ticker, to_date, 30)
            adv_cache[(ticker, 30)] = adv
        self._adv_cache = adv_cache
        return sorted(membership)

    async def get_spy_membership_over_dates(self, dates: List[date]) -> dict:
        """Efficiently get membership for a list of dates (must be sorted), using universe_membership logic. Returns {date: set(tickers)}."""
        await self.load_all_membership_events()
        results = {}
        for d in sorted(dates):
            membership = set()
            for row in self._events:
                if row['start_date'] <= d and (row['end_date'] is None or row['end_date'] > d):
                    membership.add(row['symbol'])
            results[d] = set(membership)
            self._membership_cache[d] = set(membership)
        return results

    async def get_last_close_price(self, ticker: str) -> float:
        """
        Return the last close price for ticker as of self.as_of_date, using cache if available.
        """
        if ticker in self._last_close_price_cache:
            return self._last_close_price_cache[ticker]
        if self.as_of_date is None:
            raise ValueError("as_of_date must be set at initialization.")
        price = await self.dao.get_last_close_price(ticker, self.as_of_date)
        self._last_close_price_cache[ticker] = price
        return price

    async def get_average_dollar_volume(self, ticker: str, window: int = 30) -> float:
        """
        Return the average daily dollar volume (close * volume) over the past `window` days as of self.as_of_date, using cache if available.
        """
        key = (ticker, window)
        if key in self._adv_cache:
            return self._adv_cache[key]
        if self.as_of_date is None:
            raise ValueError("as_of_date must be set at initialization.")
        avg_dv = await self.dao.get_average_dollar_volume(ticker, self.as_of_date, window)
        self._adv_cache[key] = avg_dv
        return avg_dv

    async def get_market_cap(self, ticker: str) -> float:
        """
        Return the latest market cap for ticker as of self.as_of_date, using cache if available.
        """
        if ticker in self._market_cap_cache:
            return self._market_cap_cache[ticker]
        if self.as_of_date is None:
            raise ValueError("as_of_date must be set at initialization.")
        mc = await self.dao.get_market_cap(ticker, self.as_of_date)
        self._market_cap_cache[ticker] = mc
        return mc
