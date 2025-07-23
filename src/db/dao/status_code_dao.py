from config.environment import Environment
import asyncpg

class StatusCodeDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('status_code')
        self.db_url = self.env.get_database_url()

    async def insert_status(self, code, description):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (code, description)
                    VALUES ($1, $2)
                """, code, description)
        finally:
            await pool.close()

    async def get_status(self, code):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE code = $1", code)
        finally:
            await pool.close()

    async def list_statuses(self):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name}")
        finally:
            await pool.close()
