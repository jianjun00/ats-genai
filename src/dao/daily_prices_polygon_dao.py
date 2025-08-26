from config.environment import Environment
import asyncpg
import logging

class DailyPricesPolygonDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('daily_prices_polygon')
        self.db_url = self.env.get_database_url()
        self.logger = logging.getLogger(__name__)

    async def insert_price(self, date, instrument_id, open_, high, low, close, volume, market_cap=None):
        self.logger.debug(f"insert_price date type: {type(date)}, value: {date}")
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
        self.logger.debug(f"get_price date type: {type(date)}, value: {date}")
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE date = $1 AND instrument_id = $2", date, instrument_id)
                self.logger.debug(f"get_price result for instrument_id={instrument_id}, date={date}: {row}")
                return dict(row) if row else None
        finally:
            await pool.close()

    async def list_prices(self, instrument_id):
        self.logger.debug(f"list_prices instrument_id type: {type(instrument_id)}, value: {instrument_id}")
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(f"SELECT * FROM {self.table_name} WHERE instrument_id = $1", instrument_id)
                self.logger.debug(f"list_prices result for instrument_id={instrument_id}: {rows}")
                return [dict(row) for row in rows]
        finally:
            await pool.close()

    async def batch_insert_prices(self, prices):
        """
        Batch insert prices. Each item in prices should be a dict with keys:
        date, instrument_id, open, high, low, close, volume, market_cap
        """
        if not prices:
            return
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.executemany(f"""
                    INSERT INTO {self.table_name} (date, instrument_id, open, high, low, close, volume, market_cap)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (date, instrument_id) DO UPDATE SET
                        open=EXCLUDED.open,
                        high=EXCLUDED.high,
                        low=EXCLUDED.low,
                        close=EXCLUDED.close,
                        volume=EXCLUDED.volume,
                        market_cap=EXCLUDED.market_cap
                """,
                [
                    (
                        p['date'],
                        p['instrument_id'],
                        p.get('open'),
                        p.get('high'),
                        p.get('low'),
                        p.get('close'),
                        p.get('volume'),
                        p.get('market_cap'),
                    ) for p in prices
                ])
        finally:
            await pool.close()
