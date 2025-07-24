from config.environment import Environment
import asyncpg

class InstrumentsDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('instruments')
        self.db_url = self.env.get_database_url()

    async def create_instrument(self, symbol: str, name: str = None, exchange: str = None, type_: str = None, currency: str = None) -> int:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                result = await conn.fetchrow(f"""
                    INSERT INTO {self.table_name} (symbol, name, exchange, type, currency)
                    VALUES ($1, $2, $3, $4, $5)
                    RETURNING instrument_id
                """, symbol, name, exchange, type_, currency)
                return result['instrument_id'] if result else None
        finally:
            await pool.close()

    async def get_instrument(self, instrument_id: int):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE instrument_id = $1", instrument_id)
        finally:
            await pool.close()

    async def list_instruments(self):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name}")
        finally:
            await pool.close()
