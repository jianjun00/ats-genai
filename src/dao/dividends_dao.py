from config.environment import Environment
import asyncpg

class DividendsDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('dividends')
        self.db_url = self.env.get_database_url()

    async def insert_dividend(self, instrument_id, ex_date, pay_date, record_date, amount, currency='USD', dividend_type='regular', source=None):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (instrument_id, ex_date, pay_date, record_date, amount, currency, dividend_type, source)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """, instrument_id, ex_date, pay_date, record_date, amount, currency, dividend_type, source)
        finally:
            await pool.close()

    async def get_dividend(self, instrument_id, ex_date, dividend_type='regular'):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE instrument_id = $1 AND ex_date = $2 AND dividend_type = $3", instrument_id, ex_date, dividend_type)
        finally:
            await pool.close()

    async def list_dividends(self, instrument_id):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE instrument_id = $1", instrument_id)
        finally:
            await pool.close()
