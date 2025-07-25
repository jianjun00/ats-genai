from config.environment import Environment
import asyncpg

class InstrumentAliasesDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('instrument_aliases')
        self.db_url = self.env.get_database_url()

    async def add_alias(self, instrument_id: int, alias: str, source: str = None) -> int:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                result = await conn.fetchrow(f"""
                    INSERT INTO {self.table_name} (instrument_id, alias, source)
                    VALUES ($1, $2, $3)
                    RETURNING id
                """, instrument_id, alias, source)
                return result['id'] if result else None
        finally:
            await pool.close()

    async def get_aliases(self, instrument_id: int):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE instrument_id = $1", instrument_id)
        finally:
            await pool.close()
