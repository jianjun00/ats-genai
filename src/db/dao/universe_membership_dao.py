from config.environment import Environment
import asyncpg

class UniverseMembershipDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('universe_membership')
        self.db_url = self.env.get_database_url()

    async def add_membership(self, universe_id: int, instrument_id: int) -> int:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                result = await conn.fetchrow(f"""
                    INSERT INTO {self.table_name} (universe_id, instrument_id)
                    VALUES ($1, $2)
                    RETURNING id
                """, universe_id, instrument_id)
                return result['id'] if result else None
        finally:
            await pool.close()

    async def remove_membership(self, membership_id: int) -> bool:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                result = await conn.execute(f"DELETE FROM {self.table_name} WHERE id = $1", membership_id)
                return 'DELETE' in result
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
