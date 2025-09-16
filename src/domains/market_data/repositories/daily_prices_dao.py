from shared.utils.environment import Environment
import asyncpg
import logging

class DailyPriceDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('daily_price_polygon')
        self.db_url = self.env.get_database_url()
        self.logger = logging.getLogger(__name__)
        self.logger.debug(f"DailyPriceDAO using db_url: {self.db_url}")
        self.logger.debug(f"DailyPriceDAO env_type: {getattr(self.env, 'env_type', None)}")
        self.logger.debug(f"DailyPriceDAO table_name: {self.table_name}")

    async def list_prices_for_date(self, as_of_date):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE date = $1", as_of_date)
        finally:
            await pool.close()

    async def list_prices_for_instruments_and_date(self, instrument_ids, as_of_date):
        self.logger.debug(f"list_prices_for_instruments_and_date called with instrument_ids={instrument_ids}, as_of_date={as_of_date}")
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(f"SELECT * FROM {self.table_name} WHERE date = $1 AND instrument_id = ANY($2)", as_of_date, instrument_ids)
                self.logger.debug(f"Query returned {len(rows)} rows for date={as_of_date}")
                if not rows:
                    # Also print all available dates for these instrument_ids
                    all_rows = await conn.fetch(f"SELECT date, instrument_id FROM {self.table_name} WHERE instrument_id = ANY($1) ORDER BY date", instrument_ids)
                    date_map = {}
                    for r in all_rows:
                        iid = r['instrument_id']
                        d = r['date']
                        date_map.setdefault(iid, []).append(str(d))
                    self.logger.debug(f"Available dates per instrument_id: {date_map}")
                return rows
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

    async def insert_price(self, date, instrument_id, open_, high, low, close, volume):
        self.logger.debug(f"insert_price called with: date={date}, instrument_id={instrument_id}, open={open_}, high={high}, low={low}, close={close}, volume={volume}")
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    f"""
                    INSERT INTO {self.table_name} (date, instrument_id, open, high, low, close, volume)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (date, instrument_id) DO NOTHING
                    """,
                    date, instrument_id, open_, high, low, close, volume
                )
        finally:
            await pool.close()