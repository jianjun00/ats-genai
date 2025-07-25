from config.environment import Environment
import asyncpg

class DailyPricesTiingoDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('daily_prices_tiingo')
        self.db_url = self.env.get_database_url()

    async def insert_price(self, date, symbol, open_, high, low, close, adj_close, volume, status_id=None):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (date, symbol, open, high, low, close, adjClose, volume, status_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (date, symbol) DO UPDATE SET
                        open=EXCLUDED.open,
                        high=EXCLUDED.high,
                        low=EXCLUDED.low,
                        close=EXCLUDED.close,
                        adjClose=EXCLUDED.adjClose,
                        volume=EXCLUDED.volume,
                        status_id=EXCLUDED.status_id
                """, date, symbol, open_, high, low, close, adj_close, volume, status_id)
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
