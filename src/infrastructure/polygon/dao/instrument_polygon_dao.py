from core.platform.config_env.environment import Environment
import asyncpg

class InstrumentPolygonDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('instrument_polygon')
        self.db_url = self.env.get_database_url()

    async def count_instruments(self) -> int:
        """Count the total number of instruments in the polygon table."""
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(f"SELECT COUNT(*) as count FROM {self.table_name}")
                return row['count'] if row else 0
        finally:
            import asyncio
            try:
                await asyncio.wait_for(pool.close(), timeout=2.0)
            except asyncio.TimeoutError:
                print("[WARN] pool.close() timed out after 2 seconds")

    async def get_latest_update_timestamp(self):
        """Get the timestamp of the most recently updated instrument."""
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(f"SELECT MAX(updated_at) as latest FROM {self.table_name}")
                return row['latest'] if row else None
        finally:
            import asyncio
            try:
                await asyncio.wait_for(pool.close(), timeout=2.0)
            except asyncio.TimeoutError:
                print("[WARN] pool.close() timed out after 2 seconds")

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
            import asyncio
            try:
                await asyncio.wait_for(pool.close(), timeout=2.0)
            except asyncio.TimeoutError:
                print("[WARN] pool.close() timed out after 2 seconds")

    async def get_instrument_by_symbol(self, symbol):
        print(f"[DEBUG][get_instrument_by_symbol] Creating pool for db_url={self.db_url}")
        pool = await asyncpg.create_pool(self.db_url)
        try:
            print(f"[DEBUG][get_instrument_by_symbol] Acquiring connection for symbol={symbol}")
            async with pool.acquire() as conn:
                print(f"[DEBUG][get_instrument_by_symbol] Executing fetchrow for symbol={symbol}")
                result = await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE symbol = $1", symbol)
                print(f"[DEBUG][get_instrument_by_symbol] Fetchrow result: {result}")
                return result
        finally:
            print(f"[DEBUG][get_instrument_by_symbol] Closing pool")
            import asyncio
            try:
                await asyncio.wait_for(pool.close(), timeout=2.0)
            except asyncio.TimeoutError:
                print("[WARN] pool.close() timed out after 2 seconds")

    async def get_all_symbols(self):
        print(f"[DEBUG][get_all_symbols] Creating pool for db_url={self.db_url}")
        pool = await asyncpg.create_pool(self.db_url)
        try:
            print(f"[DEBUG][get_all_symbols] Acquiring connection")
            async with pool.acquire() as conn:
                print(f"[DEBUG][get_all_symbols] Executing fetch for all symbols from {self.table_name}")
                rows = await conn.fetch(f"SELECT symbol FROM {self.table_name}")
                symbols = [row['symbol'] for row in rows]
                print(f"[DEBUG][get_all_symbols] Symbols fetched: {symbols}")
                return symbols
        finally:
            print(f"[DEBUG][get_all_symbols] Pool closed")
            import asyncio
            try:
                await asyncio.wait_for(pool.close(), timeout=2.0)
            except asyncio.TimeoutError:
                print("[WARN] pool.close() timed out after 2 seconds")
