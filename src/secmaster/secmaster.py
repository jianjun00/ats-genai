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
        Updates self.as_of_date and returns the new membership as a sorted list.
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
