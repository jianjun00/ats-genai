# universe_db.py
# Utility functions for multi-universe membership using universe and universe_membership tables
import asyncpg
from datetime import date
from typing import List, Optional

class UniverseDB:
    def __init__(self, db_url: str):
        self.db_url = db_url

    async def get_universe_id(self, universe_name: str) -> Optional[int]:
        pool = await asyncpg.create_pool(self.db_url)
        async with pool.acquire() as conn:
            row = await conn.fetchrow('SELECT id FROM universe WHERE name = $1', universe_name)
        await pool.close()
        return row['id'] if row else None

    async def get_universe_members(self, universe_id: int, as_of: date) -> List[str]:
        pool = await asyncpg.create_pool(self.db_url)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                '''SELECT symbol FROM universe_membership 
                   WHERE universe_id = $1 AND start_at <= $2 AND (end_at IS NULL OR end_at > $2)''',
                universe_id, as_of)
        await pool.close()
        return [row['symbol'] for row in rows]

    async def add_universe(self, name: str, description: Optional[str] = None) -> int:
        pool = await asyncpg.create_pool(self.db_url)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                'INSERT INTO universe (name, description) VALUES ($1, $2) RETURNING id',
                name, description)
        await pool.close()
        return row['id']

    async def add_universe_membership(self, universe_id: int, symbol: str, start_at: date, end_at: Optional[date] = None, meta: Optional[dict] = None):
        pool = await asyncpg.create_pool(self.db_url)
        async with pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO universe_membership (universe_id, symbol, start_at, end_at, meta) VALUES ($1, $2, $3, $4, $5)',
                universe_id, symbol, start_at, end_at, meta)
        await pool.close()

    async def update_universe_membership_end(self, universe_id: int, symbol: str, end_at: date):
        pool = await asyncpg.create_pool(self.db_url)
        async with pool.acquire() as conn:
            await conn.execute(
                'UPDATE universe_membership SET end_at = $1 WHERE universe_id = $2 AND symbol = $3 AND end_at IS NULL',
                end_at, universe_id, symbol)
        await pool.close()
