from core.platform.config.environment import Environment
import asyncpg
from typing import List, Dict

class InstrumentsDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('instruments')
        self.db_url = self.env.get_database_url()

    async def count_instruments(self) -> int:
        """Count the total number of instruments."""
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(f"SELECT COUNT(*) as count FROM {self.table_name}")
                return row['count'] if row else 0
        finally:
            await pool.close()

    async def create_instrument(self, symbol: str, name: str = None, exchange: str = None, type_: str = None, currency: str = None, list_date=None, delist_date=None) -> int:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                result = await conn.fetchrow(f"""
                    INSERT INTO {self.table_name} (symbol, name, exchange, type, currency, list_date, delist_date)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    RETURNING id
                """, symbol, name, exchange, type_, currency, list_date, delist_date)
                return result['id'] if result else None
        finally:
            await pool.close()

    async def get_instrument(self, instrument_id: int):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE id = $1", instrument_id)
        finally:
            await pool.close()

    async def list_instruments(self):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name}")
        finally:
            await pool.close()

    async def get_instrument_by_symbol(self, symbol: str):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE symbol = $1", symbol)
        finally:
            await pool.close()

    async def create_instruments_batch(self, instruments: list[dict], pool_min_size: int = 1, pool_max_size: int = 1) -> list[int]:
        """
        Batch insert instruments. Each dict must have keys: symbol, name, exchange, type_, currency, list_date, delist_date
        Returns list of inserted IDs (order matches input).
        """
        if not instruments:
            return []
        pool = await asyncpg.create_pool(self.db_url, min_size=pool_min_size, max_size=pool_max_size)
        try:
            async with pool.acquire() as conn:
                stmt = f"""
                    INSERT INTO {self.table_name} (symbol, name, exchange, type, currency, list_date, delist_date)
                    SELECT x.symbol, x.name, x.exchange, x.type, x.currency, x.list_date, x.delist_date
                    FROM UNNEST(
                        $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::date[], $7::date[]
                    ) AS x(symbol, name, exchange, type, currency, list_date, delist_date)
                    ON CONFLICT (symbol) DO NOTHING
                    RETURNING id
                """
                # Transpose dicts to columns
                symbols = [i['symbol'] for i in instruments]
                names = [i.get('name') for i in instruments]
                exchanges = [i.get('exchange') for i in instruments]
                types = [i.get('type_') for i in instruments]
                currencies = [i.get('currency') for i in instruments]
                list_dates = [i.get('list_date') for i in instruments]
                delist_dates = [i.get('delist_date') for i in instruments]
                rows = await conn.fetch(stmt, symbols, names, exchanges, types, currencies, list_dates, delist_dates)
                return [row['id'] for row in rows]
        finally:
            await pool.close()

    async def get_symbols_by_ids(self, instrument_ids: List[int]) -> Dict[int, str]:
        """
        Get symbols for a list of instrument IDs

        Args:
            instrument_ids: List of instrument IDs to look up

        Returns:
            Dictionary mapping instrument_id to symbol
        """
        if not instrument_ids:
            return {}

        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(f"SELECT id, symbol FROM {self.table_name} WHERE id = ANY($1)", instrument_ids)
                return {row['id']: row['symbol'] for row in rows}
        finally:
            await pool.close()
