import asyncpg
from config.environment import Environment

class DailyPricesQuandlDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table = env.get_table_name('daily_prices_quandl')
        self.db_url = env.get_database_url()

    async def insert_price(self, date, symbol=None, instrument_id=None, open_=None, high=None, low=None, close=None, volume=None, vendor_id=None, at_date=None):
        """
        Insert price row. Prefer instrument_id; if only symbol is given, resolve instrument_id from instrument_xref (using vendor_id and at_date if provided).
        """
        if instrument_id is None and symbol is not None:
            instrument_id = await self.resolve_instrument_id(symbol, vendor_id, at_date or date)
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    f"INSERT INTO {self.table} (date, symbol, instrument_id, open, high, low, close, volume) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) "
                    "ON CONFLICT (date, symbol) DO NOTHING",
                    date, symbol, instrument_id, open_, high, low, close, volume
                )
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

    async def batch_insert_prices(self, prices, symbol=None, instrument_id=None, vendor_id=None):
        if not prices:
            return
        if instrument_id is None and symbol is not None and prices:
            # Try to resolve instrument_id from first row date
            instrument_id = await self.resolve_instrument_id(symbol, vendor_id, prices[0]['date'])
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                await conn.executemany(
                    f"INSERT INTO {self.table} (date, symbol, instrument_id, open, high, low, close, volume) "
                    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8) "
                    "ON CONFLICT (date, symbol) DO NOTHING",
                    [(
                        row['date'],
                        symbol,
                        instrument_id,
                        row['open'], row['high'], row['low'], row['close'], row['volume']
                    ) for row in prices]
                )
        finally:
            await pool.close()

    async def list_prices(self, symbol=None, instrument_id=None):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                if instrument_id is not None:
                    rows = await conn.fetch(
                        f"SELECT * FROM {self.table} WHERE instrument_id = $1 ORDER BY date",
                        instrument_id
                    )
                elif symbol is not None:
                    rows = await conn.fetch(
                        f"SELECT * FROM {self.table} WHERE symbol = $1 ORDER BY date",
                        symbol
                    )
                else:
                    raise ValueError("Must provide symbol or instrument_id")
                return [dict(row) for row in rows]
        finally:
            await pool.close()
