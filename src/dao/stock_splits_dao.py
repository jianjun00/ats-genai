from config.environment import Environment
import asyncpg

class StockSplitsDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('stock_splits')
        self.db_url = self.env.get_database_url()

    async def insert_split(self, symbol, ex_date, split_ratio, split_from, split_to, source=None):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (symbol, ex_date, split_ratio, split_from, split_to, source)
                    VALUES ($1, $2, $3, $4, $5, $6)
                """, symbol, ex_date, split_ratio, split_from, split_to, source)
        finally:
            await pool.close()

    async def get_split(self, symbol, ex_date):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE symbol = $1 AND ex_date = $2", symbol, ex_date)
        finally:
            await pool.close()

    async def list_splits(self, symbol):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE symbol = $1", symbol)
        finally:
            await pool.close()
