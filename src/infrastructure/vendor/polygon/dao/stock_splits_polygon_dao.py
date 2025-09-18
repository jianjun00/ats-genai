import asyncpg
from src.core.platform.config.environment import Environment

class StockSplitsPolygonDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('stock_splits_polygon')
        self.db_url = self.env.get_database_url()

    async def insert_split(self, split):
        pool = await asyncpg.create_pool(self.db_url)
        async with pool.acquire() as conn:
            await conn.execute(f"""
                INSERT INTO {self.table_name} (
                    symbol, execution_date, split_from, split_to, cash_amount,
                    declaration_date, payment_date, record_date, description, refid
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (symbol, execution_date, refid) DO NOTHING
            """,
            split['symbol'],
            split['execution_date'],
            split['split_from'],
            split['split_to'],
            split.get('cash_amount'),
            split.get('declaration_date'),
            split.get('payment_date'),
            split.get('record_date'),
            split.get('description'),
            split.get('refid'),
            )
        await pool.close()

    async def get_splits_by_symbol(self, symbol):
        pool = await asyncpg.create_pool(self.db_url)
        async with pool.acquire() as conn:
            rows = await conn.fetch(f"SELECT * FROM {self.table_name} WHERE symbol = $1 ORDER BY execution_date", symbol)
        await pool.close()
        return [dict(row) for row in rows]

    async def get_all_splits(self):
        pool = await asyncpg.create_pool(self.db_url)
        async with pool.acquire() as conn:
            rows = await conn.fetch(f"SELECT * FROM {self.table_name} ORDER BY execution_date")
        await pool.close()
        return [dict(row) for row in rows]
