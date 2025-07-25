from config.environment import Environment
import asyncpg

class DailyPricesTiingoDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('daily_prices_tiingo')
        self.db_url = self.env.get_database_url()

    async def insert_price(self, date, symbol=None, instrument_id=None, open_=None, high=None, low=None, close=None, adj_close=None, volume=None, status_id=None, vendor_id=None, at_date=None):
        """
        Insert price row. Prefer instrument_id; if only symbol is given, resolve instrument_id from instrument_xref (using vendor_id and at_date if provided).
        """
        if instrument_id is None and symbol is not None:
            instrument_id = await self.resolve_instrument_id(symbol, vendor_id, at_date or date)
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (date, symbol, instrument_id, open, high, low, close, adjClose, volume, status_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                """, date, symbol, instrument_id, open_, high, low, close, adj_close, volume, status_id)
        finally:
            await pool.close()

    async def resolve_instrument_id(self, symbol, vendor_id=None, at_date=None):
        """
        Lookup instrument_id from instrument_xref using symbol (and vendor_id, at_date if provided).
        """
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

    async def get_price(self, date, symbol=None, instrument_id=None):
        """
        Get price row by date and instrument_id (preferred), or symbol (legacy).
        """
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                if instrument_id is not None:
                    return await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE date = $1 AND instrument_id = $2", date, instrument_id)
                elif symbol is not None:
                    return await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE date = $1 AND symbol = $2", date, symbol)
                else:
                    raise ValueError("Must provide symbol or instrument_id")
        finally:
            await pool.close()

    async def list_prices(self, symbol=None, instrument_id=None):
        """
        List all prices for an instrument_id (preferred) or symbol (legacy).
        """
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                if instrument_id is not None:
                    return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE instrument_id = $1", instrument_id)
                elif symbol is not None:
                    return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE symbol = $1", symbol)
                else:
                    raise ValueError("Must provide symbol or instrument_id")
        finally:
            await pool.close()
