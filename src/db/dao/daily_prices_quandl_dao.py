import asyncpg
from config.environment import Environment

class DailyPricesQuandlDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table = env.get_table_name('daily_prices_quandl')
        self.db_url = env.get_database_url()

    async def insert_price(self, date, symbol, open_, high, low, close, volume):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    f"INSERT INTO {self.table} (date, symbol, open, high, low, close, volume) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7) "
                    "ON CONFLICT (date, symbol) DO NOTHING",
                    date, symbol, open_, high, low, close, volume
                )
        finally:
            await pool.close()

    async def batch_insert_prices(self, prices, symbol):
        if not prices:
            return
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.executemany(
                    f"INSERT INTO {self.table} (date, symbol, open, high, low, close, volume) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7) "
                    "ON CONFLICT (date, symbol) DO NOTHING",
                    [(
                        row['date'],
                        symbol,
                        row['open'], row['high'], row['low'], row['close'], row['volume']
                    ) for row in prices]
                )
        finally:
            await pool.close()

    async def list_prices(self, symbol):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    f"SELECT * FROM {self.table} WHERE symbol = $1 ORDER BY date",
                    symbol
                )
                return [dict(row) for row in rows]
        finally:
            await pool.close()
