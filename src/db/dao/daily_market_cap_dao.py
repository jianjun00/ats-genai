from config.environment import Environment
import asyncpg

class DailyMarketCapDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('daily_market_cap')
        self.db_url = self.env.get_database_url()

    async def insert_market_cap(self, date, symbol, market_cap):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (date, symbol, market_cap)
                    VALUES ($1, $2, $3)
                """, date, symbol, market_cap)
        finally:
            await pool.close()

    async def get_market_cap(self, date, symbol):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE date = $1 AND symbol = $2", date, symbol)
        finally:
            await pool.close()

    async def list_market_caps(self, symbol):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE symbol = $1", symbol)
        finally:
            await pool.close()
