from config.environment import Environment
import asyncpg

class DailyPricesPolygonDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('daily_prices_polygon')
        self.db_url = self.env.get_database_url()

    async def insert_price(self, date, instrument_id, open_, high, low, close, volume, market_cap=None):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (date, instrument_id, open, high, low, close, volume, market_cap)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (date, instrument_id) DO UPDATE SET
                        open=EXCLUDED.open,
                        high=EXCLUDED.high,
                        low=EXCLUDED.low,
                        close=EXCLUDED.close,
                        volume=EXCLUDED.volume,
                        market_cap=EXCLUDED.market_cap
                """, date, instrument_id, open_, high, low, close, volume, market_cap)
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
