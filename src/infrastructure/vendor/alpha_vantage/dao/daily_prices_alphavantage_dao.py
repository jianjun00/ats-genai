from src.core.shared.utils.environment import Environment
import asyncpg

class DailyPricesAlphaVantageDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('daily_price_polygon_alphavantage')
        self.db_url = self.env.get_database_url()

    async def insert_price(self, date, instrument_id, open_price, high_price, low_price, close, adj_close, volume):
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (date, instrument_id, open_price, high_price, low_price, close, adj_close, volume)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (date, instrument_id) DO UPDATE SET
                        open_price=EXCLUDED.open_price,
                        high_price=EXCLUDED.high_price,
                        low_price=EXCLUDED.low_price,
                        close=EXCLUDED.close,
                        adj_close=EXCLUDED.adj_close,
                        volume=EXCLUDED.volume,
                        updated_at=NOW()
                """, date, instrument_id, open_price, high_price, low_price, close, adj_close, volume)
        finally:
            await pool.close()

    async def get_price(self, date, instrument_id):
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE date = $1 AND instrument_id = $2", date, instrument_id)
                return row
        finally:
            await pool.close()

    async def list_prices(self, instrument_id):
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(f"SELECT * FROM {self.table_name} WHERE instrument_id = $1 ORDER BY date", instrument_id)
                return rows
        finally:
            await pool.close()

    async def batch_insert_prices(self, prices):
        """
        Batch insert prices. Each item in prices should be a dict with keys:
        date, instrument_id, open_price, high_price, low_price, close, adj_close, volume
        """
        if not prices:
            return
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                await conn.executemany(f"""
                    INSERT INTO {self.table_name} (date, instrument_id, open_price, high_price, low_price, close, adj_close, volume)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (date, instrument_id) DO UPDATE SET
                        open_price=EXCLUDED.open_price,
                        high_price=EXCLUDED.high_price,
                        low_price=EXCLUDED.low_price,
                        close=EXCLUDED.close,
                        adj_close=EXCLUDED.adj_close,
                        volume=EXCLUDED.volume,
                        updated_at=NOW()
                """,
                [
                    (
                        p['date'],
                        p['instrument_id'],
                        p.get('open_price'),
                        p.get('high_price'),
                        p.get('low_price'),
                        p.get('close'),
                        p.get('adj_close'),
                        p.get('volume'),
                    ) for p in prices
                ])
        finally:
            await pool.close()

    async def get_latest_date_for_instrument(self, instrument_id):
        """Get the latest date for which we have data for this instrument."""
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(f"""
                    SELECT MAX(date) as latest_date
                    FROM {self.table_name}
                    WHERE instrument_id = $1
                """, instrument_id)
                return row['latest_date'] if row else None
        finally:
            await pool.close()