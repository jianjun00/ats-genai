from config.environment import Environment
import asyncpg
from typing import Optional, List, Dict, Any

from .vendors_dao import VendorsDAO

class InstrumentXrefsDAO:
    async def get_symbol_by_instrument_id_vendor_name(self, instrument_id: int, vendor_name: str = "ticker") -> Optional[str]:
        """
        Lookup symbol from instrument_xrefs using instrument_id and vendor_id (looked up by vendor_name).
        """
        from .vendors_dao import VendorsDAO
        vendors_dao = VendorsDAO(self.env)
        vendor_row = await vendors_dao.get_vendor_by_name(vendor_name)
        print(f"[DEBUG][get_symbol_by_instrument_id_vendor_name] instrument_id={instrument_id}, vendor_name={vendor_name}, vendor_row={vendor_row}")
        if not vendor_row:
            return None
        vendor_id = vendor_row['id']
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                print(f"[DEBUG][get_symbol_by_instrument_id_vendor_name] Querying for instrument_id={instrument_id}, vendor_id={vendor_id} in {self.table_name}")
                row = await conn.fetchrow(
                    f"SELECT symbol FROM {self.table_name} WHERE instrument_id = $1 AND vendor_id = $2",
                    instrument_id, vendor_id
                )
                print(f"[DEBUG][get_symbol_by_instrument_id_vendor_name] Fetched row: {row}")
                return row['symbol'] if row else None
        finally:
            await pool.close()

    async def get_all_symbols(self):
        # Find vendor_id for name='ticker'
        vendors_dao = VendorsDAO(self.env)
        vendor_row = await vendors_dao.get_vendor_by_name('ticker')
        if not vendor_row:
            return []
        ticker_vendor_id = vendor_row['id']
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(f"SELECT DISTINCT symbol FROM {self.table_name} WHERE vendor_id = $1", ticker_vendor_id)
                symbols = [row['symbol'] for row in rows]
                return symbols
        finally:
            await pool.close()

    async def resolve_instrument_id_by_symbol(self, symbol, at_date=None):
        print(f"[DEBUG][resolve_instrument_id_by_symbol][ARGS] symbol={symbol}, at_date={at_date}")
        """
        Lookup instrument_id from instrument_xrefs using symbol and vendor_id for 'ticker'.
        """
        from .vendors_dao import VendorsDAO
        vendors_dao = VendorsDAO(self.env)
        vendor_row = await vendors_dao.get_vendor_by_name("ticker")
        if not vendor_row:
            print(f"[DEBUG][resolve_instrument_id_by_symbol] vendor 'ticker' not found!")
            return None
        vendor_id = vendor_row['id']
        print(f"[DEBUG][resolve_instrument_id_by_symbol] Using vendor_id={vendor_id} for symbol={symbol}")
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                table_name = self.table_name
                q = f"SELECT instrument_id FROM {table_name} WHERE symbol = $1 AND vendor_id = $2"
                params = [symbol, vendor_id]
                if at_date is not None:
                    # at_date must be a datetime.date object for asyncpg
                    q += " AND (start_at <= $3 AND (end_at IS NULL OR end_at >= $3))"
                    params.append(at_date)
                print(f"[DEBUG][resolve_instrument_id_by_symbol] Executing: {q} with params {params}")
                row = await conn.fetchrow(q, *params)
                print(f"[DEBUG][resolve_instrument_id_by_symbol] Fetched row: {row}")
                return row['instrument_id'] if row else None
        finally:
            await pool.close()

    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('instrument_xrefs')
        self.db_url = self.env.get_database_url()

    async def create_xref(self, instrument_id: int, vendor_id: int, symbol: str, start_at: str, type: Optional[str] = None, end_at: Optional[str] = None) -> int:
        if start_at is None:
            raise ValueError("start_at is required and cannot be None")
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                result = await conn.fetchrow(f"""
                    INSERT INTO {self.table_name} (instrument_id, vendor_id, symbol, type, start_at, end_at)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    RETURNING id
                """, instrument_id, vendor_id, symbol, type, start_at, end_at)
                return result['id'] if result else None
        finally:
            await pool.close()

    async def get_xref(self, xref_id: int) -> Optional[Dict[str, Any]]:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE id = $1", xref_id)
                if row:
                    d = dict(row)
                    if hasattr(d.get('start_at'), 'date'):
                        d['start_at'] = d['start_at'].date()
                    if d.get('end_at') is not None and hasattr(d['end_at'], 'date'):
                        d['end_at'] = d['end_at'].date()
                    return d
                return None
        finally:
            await pool.close()

    async def list_xrefs_for_instrument(self, instrument_id: int) -> List[Dict[str, Any]]:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(f"SELECT * FROM {self.table_name} WHERE instrument_id = $1", instrument_id)
                result = []
                for row in rows:
                    d = dict(row)
                    if hasattr(d.get('start_at'), 'date'):
                        d['start_at'] = d['start_at'].date()
                    if d.get('end_at') is not None and hasattr(d['end_at'], 'date'):
                        d['end_at'] = d['end_at'].date()
                    result.append(d)
                return result
        finally:
            await pool.close()

    async def list_xrefs_for_vendor(self, vendor_id: int) -> List[Dict[str, Any]]:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(f"SELECT * FROM {self.table_name} WHERE vendor_id = $1", vendor_id)
                result = []
                for row in rows:
                    d = dict(row)
                    if hasattr(d.get('start_at'), 'date'):
                        d['start_at'] = d['start_at'].date()
                    if d.get('end_at') is not None and hasattr(d['end_at'], 'date'):
                        d['end_at'] = d['end_at'].date()
                    result.append(d)
                return result
        finally:
            await pool.close()

    async def find_xref(self, vendor_id: int, symbol: str) -> Optional[Dict[str, Any]]:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE vendor_id = $1 AND symbol = $2", vendor_id, symbol)
                if row:
                    d = dict(row)
                    if hasattr(d.get('start_at'), 'date'):
                        d['start_at'] = d['start_at'].date()
                    if d.get('end_at') is not None and hasattr(d['end_at'], 'date'):
                        d['end_at'] = d['end_at'].date()
                    return d
                return None
        finally:
            await pool.close()

    async def create_xrefs_batch(self, xrefs: list[dict], pool_min_size: int = 1, pool_max_size: int = 1) -> list[int]:
        """
        Batch insert xrefs. Each dict must have keys: instrument_id, vendor_id, symbol, start_at, type, end_at
        Returns list of inserted IDs (order matches input).
        """
        if not xrefs:
            return []
        pool = await asyncpg.create_pool(self.db_url, min_size=pool_min_size, max_size=pool_max_size)
        try:
            async with pool.acquire() as conn:
                stmt = f"""
                    INSERT INTO {self.table_name} (instrument_id, vendor_id, symbol, type, start_at, end_at)
                    SELECT x.instrument_id, x.vendor_id, x.symbol, x.type, x.start_at, x.end_at
                    FROM UNNEST(
                        $1::int[], $2::int[], $3::text[], $4::text[], $5::date[], $6::date[]
                    ) AS x(instrument_id, vendor_id, symbol, type, start_at, end_at)
                    ON CONFLICT (instrument_id, vendor_id, start_at) DO NOTHING
                    RETURNING id
                """
                instrument_ids = [x['instrument_id'] for x in xrefs]
                vendor_ids = [x['vendor_id'] for x in xrefs]
                symbols = [x['symbol'] for x in xrefs]
                types = [x.get('type') for x in xrefs]
                start_ats = [x['start_at'] for x in xrefs]
                end_ats = [x.get('end_at') for x in xrefs]
                rows = await conn.fetch(stmt, instrument_ids, vendor_ids, symbols, types, start_ats, end_ats)
                return [row['id'] for row in rows]
        finally:
            await pool.close()
