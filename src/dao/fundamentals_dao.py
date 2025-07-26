from config.environment import Environment
import asyncpg

class FundamentalsDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('fundamentals')
        self.db_url = self.env.get_database_url()

    async def insert_fundamental(self, instrument_id, date, market_cap):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (instrument_id, date, market_cap)
                    VALUES ($1, $2, $3)
                """, instrument_id, date, market_cap)
        finally:
            await pool.close()

    async def get_fundamental(self, instrument_id, date):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE instrument_id = $1 AND date = $2", instrument_id, date)
        finally:
            await pool.close()

    async def list_fundamentals(self, instrument_id):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE instrument_id = $1", instrument_id)
        finally:
            await pool.close()
