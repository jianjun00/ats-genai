from config.environment import Environment
import asyncpg

class DailyPricesDAO:
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

    async def list_prices_for_symbols_and_date(self, symbols, as_of_date):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE date = $1 AND symbol = ANY($2)", as_of_date, symbols)
        finally:
            await pool.close()

    async def get_price(self, date, symbol):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE date = $1 AND symbol = $2", date, symbol)
        finally:
            await pool.close()

    async def list_prices(self, symbol):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE symbol = $1", symbol)
        finally:
            await pool.close()
