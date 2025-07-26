from config.environment import Environment
import asyncpg

class DailyMarketCapDAO:
    async def list_market_caps_for_date(self, as_of_date, instrument_id=None):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                if instrument_id is not None:
                    return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE date = $1 AND instrument_id = $2", as_of_date, instrument_id)
                else:
                    return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE date = $1", as_of_date)
        finally:
            await pool.close()
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('daily_market_cap')
        self.db_url = self.env.get_database_url()

    async def insert_market_cap(self, date, instrument_id, market_cap):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (date, instrument_id, market_cap)
                    VALUES ($1, $2, $3)
                """, date, instrument_id, market_cap)
        finally:
            await pool.close()

    async def get_market_cap(self, date, instrument_id):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE date = $1 AND instrument_id = $2", date, instrument_id)
        finally:
            await pool.close()

    async def list_market_caps(self, instrument_id):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE instrument_id = $1", instrument_id)
        finally:
            await pool.close()
