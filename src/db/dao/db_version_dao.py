from config.environment import Environment
import asyncpg

class DBVersionDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('db_version')
        self.db_url = self.env.get_database_url()

    async def get_version(self):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name} ORDER BY version DESC")
        finally:
            await pool.close()

    async def insert_version(self, version: int, description: str, migration_file: str):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (version, description, migration_file, applied_at)
                    VALUES ($1, $2, $3, now())
                    ON CONFLICT (version) DO UPDATE SET
                        description = EXCLUDED.description,
                        migration_file = EXCLUDED.migration_file,
                        applied_at = now()
                """, version, description, migration_file)
        finally:
            await pool.close()
