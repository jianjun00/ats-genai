import asyncpg
from config.environment import Environment

class DividendTiingoDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('dividend_tiingo')
        self.db_url = self.env.get_database_url()

    async def insert_dividend(self, dividend):
        pool = await asyncpg.create_pool(self.db_url)
        async with pool.acquire() as conn:
            await conn.execute(f"""
                INSERT INTO {self.table_name} (
                    symbol, ex_dividend_date, cash_amount, declaration_date, payment_date, record_date, description, refid
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (symbol, ex_dividend_date, refid) DO NOTHING
            """,
            dividend['symbol'],
            dividend['ex_dividend_date'],
            dividend['cash_amount'],
            dividend.get('declaration_date'),
            dividend.get('payment_date'),
            dividend.get('record_date'),
            dividend.get('description'),
            dividend.get('refid'),
            )
        await pool.close()

    async def get_dividends_by_symbol(self, symbol):
        pool = await asyncpg.create_pool(self.db_url)
        async with pool.acquire() as conn:
            rows = await conn.fetch(f"SELECT * FROM {self.table_name} WHERE symbol = $1 ORDER BY ex_dividend_date", symbol)
        await pool.close()
        return [dict(row) for row in rows]

    async def get_all_dividends(self):
        pool = await asyncpg.create_pool(self.db_url)
        async with pool.acquire() as conn:
            rows = await conn.fetch(f"SELECT * FROM {self.table_name} ORDER BY ex_dividend_date")
        await pool.close()
        return [dict(row) for row in rows]
