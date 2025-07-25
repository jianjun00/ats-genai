from config.environment import Environment
import asyncpg

class InstrumentPolygonDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('instrument_polygon')
        self.db_url = self.env.get_database_url()

    async def insert_instrument(self, symbol, name, exchange, type_, currency, figi, isin, cusip, composite_figi, active, list_date, delist_date, raw):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (symbol, name, exchange, type, currency, figi, isin, cusip, composite_figi, active, list_date, delist_date, raw)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                    ON CONFLICT (symbol) DO UPDATE SET
                        name=EXCLUDED.name,
                        exchange=EXCLUDED.exchange,
                        type=EXCLUDED.type,
                        currency=EXCLUDED.currency,
                        figi=EXCLUDED.figi,
                        isin=EXCLUDED.isin,
                        cusip=EXCLUDED.cusip,
                        composite_figi=EXCLUDED.composite_figi,
                        active=EXCLUDED.active,
                        list_date=EXCLUDED.list_date,
                        delist_date=EXCLUDED.delist_date,
                        raw=EXCLUDED.raw,
                        updated_at=now()
                """, symbol, name, exchange, type_, currency, figi, isin, cusip, composite_figi, active, list_date, delist_date, raw)
        finally:
            await pool.close()

    async def get_instrument(self, symbol):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE symbol = $1", symbol)
        finally:
            await pool.close()

    async def list_instruments(self):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name}")
        finally:
            await pool.close()
