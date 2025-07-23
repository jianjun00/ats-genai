from config.environment import Environment
import asyncpg

class DailyPricesPolygonDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('daily_prices_polygon')
        self.db_url = self.env.get_database_url()

    async def insert_price(self, date, symbol, open_, high, low, close, volume, market_cap=None):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (date, symbol, open, high, low, close, volume, market_cap)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (date, symbol) DO UPDATE SET
                        open=EXCLUDED.open,
                        high=EXCLUDED.high,
                        low=EXCLUDED.low,
                        close=EXCLUDED.close,
                        volume=EXCLUDED.volume,
                        market_cap=EXCLUDED.market_cap
                """, date, symbol, open_, high, low, close, volume, market_cap)
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
