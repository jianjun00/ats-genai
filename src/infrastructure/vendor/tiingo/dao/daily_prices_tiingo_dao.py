from shared.utils.environment import Environment
import asyncpg
import logging

class DailyPricesTiingoDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('daily_price_tiingo')
        self.db_url = self.env.get_database_url()
        self.logger = logging.getLogger(__name__)

    async def insert_price(self, date, instrument_id, open_, high, low, close, adj_close, volume, status_id=None):
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (date, instrument_id, open, high, low, close, adjClose, volume, status_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (date, instrument_id) DO UPDATE SET
                        open=EXCLUDED.open,
                        high=EXCLUDED.high,
                        low=EXCLUDED.low,
                        close=EXCLUDED.close,
                        adjClose=EXCLUDED.adjClose,
                        volume=EXCLUDED.volume,
                        status_id=EXCLUDED.status_id
                """, date, instrument_id, open_, high, low, close, adj_close, volume, status_id)
        finally:
            await pool.close()

    async def get_price(self, date, instrument_id):
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE date = $1 AND instrument_id = $2", date, instrument_id)
                self.logger.debug(f"get_price result for instrument_id={instrument_id}, date={date}: {row}")
                return row
        finally:
            await pool.close()

    async def list_prices(self, instrument_id):
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(f"SELECT * FROM {self.table_name} WHERE instrument_id = $1", instrument_id)
                self.logger.debug(f"list_prices result for instrument_id={instrument_id}: {rows}")
                return rows
        finally:
            await pool.close()

    async def batch_insert_prices(self, prices):
        """
        Batch insert prices. Each item in prices should be a dict with keys:
        date, instrument_id, open, high, low, close, adjClose, volume, status_id
        """
        if not prices:
            return
        pool = await asyncpg.create_pool(self.db_url, min_size=1, max_size=1)
        try:
            async with pool.acquire() as conn:
                await conn.executemany(f"""
                    INSERT INTO {self.table_name} (date, instrument_id, open, high, low, close, adjClose, volume, status_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (date, instrument_id) DO UPDATE SET
                        open=EXCLUDED.open,
                        high=EXCLUDED.high,
                        low=EXCLUDED.low,
                        close=EXCLUDED.close,
                        adjClose=EXCLUDED.adjClose,
                        volume=EXCLUDED.volume,
                        status_id=EXCLUDED.status_id
                """,
                [
                    (
                        p['date'],
                        p['instrument_id'],
                        p.get('open'),
                        p.get('high'),
                        p.get('low'),
                        p.get('close'),
                        p.get('adjClose'),
                        p.get('volume'),
                        p.get('status_id'),
                    ) for p in prices
                ])
        finally:
            await pool.close()
