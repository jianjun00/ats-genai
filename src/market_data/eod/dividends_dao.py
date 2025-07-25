from config.environment import Environment
import asyncpg
from typing import Optional

class DividendsDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('dividends')
        self.db_url = self.env.get_database_url()

    async def insert_dividend(self, ex_date, amount, symbol=None, instrument_id=None, pay_date=None, record_date=None, currency='USD', dividend_type='regular', source=None, vendor_id=None, at_date=None):
        """
        Insert a dividend. Prefer instrument_id; if only symbol is given, resolve instrument_id from instrument_xref.
        """
        if instrument_id is None and symbol is not None:
            instrument_id = await self.resolve_instrument_id(symbol, vendor_id, at_date or ex_date)
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (symbol, instrument_id, ex_date, pay_date, record_date, amount, currency, dividend_type, source)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """, symbol, instrument_id, ex_date, pay_date, record_date, amount, currency, dividend_type, source)
        finally:
            await pool.close()

    async def list_dividends(self, symbol: Optional[str] = None, instrument_id: Optional[int] = None):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                if instrument_id is not None:
                    rows = await conn.fetch(f"SELECT * FROM {self.table_name} WHERE instrument_id = $1 ORDER BY ex_date ASC", instrument_id)
                elif symbol is not None:
                    rows = await conn.fetch(f"SELECT * FROM {self.table_name} WHERE symbol = $1 ORDER BY ex_date ASC", symbol)
                else:
                    raise ValueError("Must provide symbol or instrument_id")
                return [dict(row) for row in rows]
        finally:
            await pool.close()

    async def resolve_instrument_id(self, symbol, vendor_id=None, at_date=None):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                q = f"SELECT instrument_id FROM instrument_xref WHERE symbol = $1"
                params = [symbol]
                if vendor_id is not None:
                    q += " AND vendor_id = $2"
                    params.append(vendor_id)
                if at_date is not None:
                    if vendor_id is not None:
                        q += " AND (start_at <= $3 AND (end_at IS NULL OR end_at >= $3))"
                        params.append(at_date)
                    else:
                        q += " AND (start_at <= $2 AND (end_at IS NULL OR end_at >= $2))"
                        params.append(at_date)
                q += " ORDER BY start_at DESC LIMIT 1"
                row = await conn.fetchrow(q, *params)
                if not row:
                    raise ValueError(f"No instrument_id found for symbol={symbol}, vendor_id={vendor_id}, at_date={at_date}")
                return row['instrument_id']
        finally:
            await pool.close()
