from config.environment import Environment
import asyncpg

class DailyPricesDAO:
    def __init__(self, db_url=None, env=None):
        self.db_url = db_url or (env.get_database_url() if env else None)
        self.env = env
        print(f"[DAO DEBUG] DailyPricesDAO using db_url: {self.db_url}")
    async def list_prices_for_date(self, as_of_date):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE date = $1", as_of_date)
        finally:
            await pool.close()
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('daily_prices')
        self.db_url = self.env.get_database_url()

    async def list_prices_for_instruments_and_date(self, instrument_ids, as_of_date):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE date = $1 AND instrument_id = ANY($2)", as_of_date, instrument_ids)
        finally:
            await pool.close()

    async def get_price(self, date, instrument_id):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE date = $1 AND instrument_id = $2", date, instrument_id)
        finally:
            await pool.close()

    async def list_prices(self, instrument_id):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE instrument_id = $1", instrument_id)
        finally:
            await pool.close()
