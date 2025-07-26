from config.environment import Environment
import asyncpg

from datetime import datetime

class UniverseMembershipDAO:
    def __init__(self, db_url=None, env=None):
        self.db_url = db_url or (env.get_database_url() if env else None)
        self.env = env
        print(f"[DAO DEBUG] UniverseMembershipDAO using db_url: {self.db_url}")
    async def get_membership_changes(self, universe_id: int, as_of: datetime):
        table = self.env.get_table_name('universe_membership_changes')
        sql = f"SELECT universe_id, instrument_id, symbol, action, effective_date, reason FROM {table} WHERE universe_id = $1 AND effective_date <= $2"
        print(f"[DEBUG] Querying membership_changes table: {table}")
        print(f"[DEBUG] SQL: {sql}")
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                table_info = await conn.fetch(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}'")
                print(f"[DEBUG] Columns for {table}: {table_info}")
                rows = await conn.fetch(sql, universe_id, as_of)
                return [dict(row) for row in rows]
        finally:
            await pool.close()

    async def update_membership_end(self, universe_id: int, symbol=None, instrument_id=None, end_at=None, vendor_id=None, at_date=None):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                if instrument_id is None and symbol is not None:
                    instrument_id = await self.resolve_instrument_id(symbol, vendor_id, at_date)
                if instrument_id is not None:
                    await conn.execute(f"""
                        UPDATE {self.table_name}
                        SET end_at = $3
                        WHERE universe_id = $1 AND instrument_id = $2 AND end_at IS NULL
                    """, universe_id, instrument_id, end_at)
                else:
                    await conn.execute(f"""
                        UPDATE {self.table_name}
                        SET end_at = $3
                        WHERE universe_id = $1 AND symbol = $2 AND end_at IS NULL
                    """, universe_id, symbol, end_at)
        finally:
            await pool.close()
    async def add_membership_full(self, universe_id: int, symbol=None, instrument_id=None, start_at=None, end_at=None, vendor_id=None):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                if instrument_id is None and symbol is not None:
                    instrument_id = await self.resolve_instrument_id(symbol, vendor_id, start_at)
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (universe_id, symbol, instrument_id, start_at, end_at)
                    VALUES ($1, $2, $3, $4, $5)
                """, universe_id, symbol, instrument_id, start_at, end_at)
        finally:
            await pool.close()

    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('universe_membership')
        self.db_url = self.env.get_database_url()

    async def resolve_instrument_id(self, symbol, vendor_id=None, at_date=None):
        """
        Lookup instrument_id from instrument_xref using symbol (and vendor_id, at_date if provided).
        """
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                # Use correct table and columns for instrument_xrefs
                table_name = self.env.get_table_name('instrument_xrefs')
                q = f"SELECT instrument_id FROM {table_name} WHERE symbol = $1"
                params = [symbol]
                if vendor_id is not None:
                    q += " AND vendor_id = $2"
                    params.append(vendor_id)
                if at_date is not None:
                    # at_date must be a datetime.date object for asyncpg
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

    async def add_membership(self, universe_id: int, symbol=None, instrument_id=None, start_at=None, end_at=None, vendor_id=None) -> bool:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                if instrument_id is None and symbol is not None:
                    instrument_id = await self.resolve_instrument_id(symbol, vendor_id, start_at)
                await conn.execute(f"""
                    INSERT INTO {self.table_name} (universe_id, symbol, instrument_id, start_at, end_at)
                    VALUES ($1, $2, $3, $4, $5)
                """, universe_id, symbol, instrument_id, start_at, end_at)
                return True
        finally:
            await pool.close()

    async def remove_membership(self, universe_id: int, symbol: str, start_at: datetime) -> bool:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                result = await conn.execute(f"DELETE FROM {self.table_name} WHERE universe_id = $1 AND symbol = $2 AND start_at = $3", universe_id, symbol, start_at)
                return 'DELETE' in str(result)
        finally:
            await pool.close()

    async def get_memberships_by_universe(self, universe_id: int):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE universe_id = $1", universe_id)
        finally:
            await pool.close()

    async def get_active_memberships(self, universe_id: int, as_of):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(
                    f"SELECT * FROM {self.table_name} WHERE universe_id = $1 AND start_at <= $2 AND (end_at IS NULL OR end_at > $2)",
                    universe_id, as_of)
        finally:
            await pool.close()

    async def get_memberships_by_instrument(self, instrument_id: int):
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                return await conn.fetch(f"SELECT * FROM {self.table_name} WHERE instrument_id = $1", instrument_id)
        finally:
            await pool.close()
