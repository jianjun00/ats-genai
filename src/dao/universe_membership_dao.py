from config.environment import Environment
import asyncpg

from datetime import date

class UniverseMembershipDAO:
    async def get_membership_changes(self, universe_id: int, as_of: date):
        table = self.env.get_table_name('universe_membership_changes')
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    f"SELECT universe_id, symbol, action, effective_date, reason FROM {table} WHERE universe_id = $1 AND effective_date <= $2",
                    universe_id, as_of
                )
                return [dict(row) for row in rows]
        finally:
            await pool.close()

    async def update_membership_end(self, universe_id: int, symbol: str, end_at):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    UPDATE {self.table_name}
                    SET end_at = $3
                    WHERE universe_id = $1 AND symbol = $2
                """, universe_id, symbol, end_at)
        finally:
            await pool.close()
    async def add_membership_full(self, universe_id: int, symbol: str, start_at, end_at=None):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (universe_id, symbol, start_at, end_at)
                    VALUES ($1, $2, $3, $4)
                """, universe_id, symbol, start_at, end_at)
        finally:
            await pool.close()

    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('universe_membership')
        self.db_url = self.env.get_database_url()

    async def add_membership(self, universe_id: int, symbol: str, start_at: date, end_at: date = None) -> bool:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (universe_id, symbol, start_at, end_at)
                    VALUES ($1, $2, $3, $4)
                """, universe_id, symbol, start_at, end_at)
                return True
        finally:
            await pool.close()

    async def remove_membership(self, universe_id: int, symbol: str, start_at: date) -> bool:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                result = await conn.execute(f"DELETE FROM {self.table_name} WHERE universe_id = $1 AND symbol = $2 AND start_at = $3", universe_id, symbol, start_at)
                return 'DELETE' in str(result)
        finally:
            await pool.close()

    async def get_memberships_by_universe(self, universe_id: int):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE universe_id = $1", universe_id)
        finally:
            await pool.close()

    async def get_active_memberships(self, universe_id: int, as_of):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(
                    f"SELECT * FROM {self.table_name} WHERE universe_id = $1 AND start_at <= $2 AND (end_at IS NULL OR end_at > $2)",
                    universe_id, as_of)
        finally:
            await pool.close()

    async def get_memberships_by_instrument(self, instrument_id: int):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE instrument_id = $1", instrument_id)
        finally:
            await pool.close()
