import asyncpg
from datetime import date
from typing import List

class SecMaster:
    def __init__(self, db_url: str):
        self.db_url = db_url

    async def get_spy_membership(self, as_of_date: date) -> List[str]:
        pool = await asyncpg.create_pool(self.db_url)
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT added, removed, event_date
                FROM spy_membership_change
                WHERE event_date <= $1
                ORDER BY COALESCE(added, removed), event_date
            """, as_of_date)
        await pool.close()

        last_event = {}
        for row in rows:
            ticker = row['added'] or row['removed']
            if row['added']:
                last_event[ticker] = ('add', row['event_date'])
            elif row['removed']:
                last_event[ticker] = ('remove', row['event_date'])
        members = [ticker for ticker, (event, _) in last_event.items() if event == 'add']
        return sorted(members)
