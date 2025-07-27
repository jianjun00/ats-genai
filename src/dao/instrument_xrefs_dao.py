from config.environment import Environment
import asyncpg
from typing import Optional, List, Dict, Any

class InstrumentXrefsDAO:
    async def resolve_instrument_id(self, symbol, vendor_id=None, at_date=None):
        """
        Lookup instrument_id from instrument_xrefs using symbol (and vendor_id, at_date if provided).
        """
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                table_name = self.table_name
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
                row = await conn.fetchrow(q, *params)
                if row:
                    return row['instrument_id']
                return None
        finally:
            await pool.close()

    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('instrument_xrefs')
        self.db_url = self.env.get_database_url()

    async def create_xref(self, instrument_id: int, vendor_id: int, symbol: str, type: Optional[str] = None, start_at: Optional[str] = None, end_at: Optional[str] = None) -> int:
        from datetime import date
        if start_at is None:
            start_at = date.today().isoformat()
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
