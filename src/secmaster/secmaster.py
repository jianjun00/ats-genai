import asyncpg
from datetime import date
from typing import List

class SecMaster:
    """
    Security master for S&P 500 membership. Can be used in two modes:
    1. Snapshot mode: initialized with as_of_date, all methods use this date.
    2. Batch mode: use get_spy_membership_over_dates(dates) for multiple dates efficiently.
    """
    def __init__(self, db_url: str, as_of_date: date = None):
        self.db_url = db_url
        self.as_of_date = as_of_date
        self._events = None  # Will hold all membership events
        self._membership_cache = {}  # Optional: cache for date->membership
        self._last_close_price_cache = {}
        self._market_cap_cache = {}
        self._adv_cache = {}


    async def load_all_membership_events(self):
        if self._events is None:
            pool = await asyncpg.create_pool(self.db_url)
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT added, removed, event_date FROM spy_membership_change ORDER BY event_date"
                )
            await pool.close()
            self._events = [dict(row) for row in rows]

    async def get_spy_membership(self) -> List[str]:
        """Returns SPY membership as of self.as_of_date (set at init)."""
        if self.as_of_date is None:
            raise ValueError("as_of_date must be set at initialization for snapshot mode.")
        await self.load_all_membership_events()
        if self.as_of_date in self._membership_cache:
            return sorted(self._membership_cache[self.as_of_date])
        membership = set()
        for row in self._events:
            # Process all events up to and including as_of_date
            if row['event_date'] > self.as_of_date:
                continue
            if row['added']:
                membership.add(row['added'])
            elif row['removed']:
                membership.discard(row['removed'])
        self._membership_cache[self.as_of_date] = set(membership)
        return sorted(membership)

    async def advance(self, to_date: date) -> List[str]:
        """
        Advance the membership from self.as_of_date to to_date, applying all events in (self.as_of_date, to_date].
        Updates self.as_of_date and caches for last_close_price, market_cap, and ADV (30d) for all tickers.
        Returns the new membership as a sorted list.
        """
        if self.as_of_date is None:
            raise ValueError("as_of_date must be set at initialization for advance().")
        await self.load_all_membership_events()
        # Start from current membership
        membership = set(await self.get_spy_membership())
        # Apply only events in (self.as_of_date, to_date]
        for row in self._events:
            if not (self.as_of_date < row['event_date'] <= to_date):
                continue
            if row['added']:
                membership.add(row['added'])
            elif row['removed']:
                membership.discard(row['removed'])
        self.as_of_date = to_date
        self._membership_cache[to_date] = set(membership)
        tickers = list(membership)
        # Batch update caches for all tickers
        pool = await asyncpg.create_pool(self.db_url)
        async with pool.acquire() as conn:
            # Batch fetch last close prices
            rows = await conn.fetch(
                """
                SELECT symbol, close FROM daily_prices
                WHERE date = $1 AND symbol = ANY($2)
                """, to_date, tickers
            )
            self._last_close_price_cache = {row['symbol']: row['close'] for row in rows}

            # Batch fetch market cap from daily_prices (latest as of date)
            rows = await conn.fetch(
                """
                SELECT symbol, market_cap FROM daily_prices
                WHERE date = $1 AND symbol = ANY($2)
                """, to_date, tickers
            )
            self._market_cap_cache = {row['symbol']: row['market_cap'] for row in rows}

            # Batch fetch ADV for each ticker (window fixed at 30)
            adv_cache = {}
            for ticker in tickers:
                adv = await conn.fetchval(
                    """
                    SELECT AVG(close * volume) FROM (
                        SELECT close, volume FROM daily_prices
                        WHERE symbol = $1 AND date <= $2
                        ORDER BY date DESC LIMIT 30
                    ) sub
                    """, ticker, to_date, 30
                )
                adv_cache[(ticker, 30)] = adv
            self._adv_cache = adv_cache
        await pool.close()
        return sorted(membership)

    async def get_spy_membership_over_dates(self, dates: List[date]) -> dict:
        """Efficiently get membership for a list of dates (must be sorted). Returns {date: set(tickers)}."""
        await self.load_all_membership_events()
        results = {}
        membership = set()
        event_idx = 0
        events = self._events
        for d in sorted(dates):
            # Apply all events up to and including this date
            while event_idx < len(events) and events[event_idx]['event_date'] <= d:
                row = events[event_idx]
                if row['added']:
                    membership.add(row['added'])
                elif row['removed']:
                    membership.discard(row['removed'])
                event_idx += 1
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
        pool = await asyncpg.create_pool(self.db_url)
        async with pool.acquire() as conn:
            price = await conn.fetchval(
                """
                SELECT close FROM daily_prices
                WHERE symbol = $1 AND date <= $2
                ORDER BY date DESC LIMIT 1
                """, ticker, self.as_of_date)
        await pool.close()
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
        pool = await asyncpg.create_pool(self.db_url)
        async with pool.acquire() as conn:
            avg_dv = await conn.fetchval(
                """
                SELECT AVG(close * volume) FROM (
                    SELECT close, volume FROM daily_prices
                    WHERE symbol = $1 AND date <= $2
                    ORDER BY date DESC LIMIT $3
                ) sub
                """, ticker, self.as_of_date, window)
        await pool.close()
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
        pool = await asyncpg.create_pool(self.db_url)
        async with pool.acquire() as conn:
            mc = await conn.fetchval(
                """
                SELECT market_cap FROM daily_prices
                WHERE symbol = $1 AND date <= $2
                ORDER BY date DESC LIMIT 1
                """, ticker, self.as_of_date)
        await pool.close()
        self._market_cap_cache[ticker] = mc
        return mc
