from config.environment import Environment
import asyncpg
from typing import Optional, List, Dict, Any

class InstrumentXrefsDAO:
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
                return dict(row) if row else None
        finally:
            await pool.close()

    async def list_xrefs_for_instrument(self, instrument_id: int) -> List[Dict[str, Any]]:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(f"SELECT * FROM {self.table_name} WHERE instrument_id = $1", instrument_id)
                return [dict(row) for row in rows]
        finally:
            await pool.close()

    async def list_xrefs_for_vendor(self, vendor_id: int) -> List[Dict[str, Any]]:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(f"SELECT * FROM {self.table_name} WHERE vendor_id = $1", vendor_id)
                return [dict(row) for row in rows]
        finally:
            await pool.close()

    async def find_xref(self, vendor_id: int, symbol: str) -> Optional[Dict[str, Any]]:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(f"SELECT * FROM {self.table_name} WHERE vendor_id = $1 AND symbol = $2", vendor_id, symbol)
                return dict(row) if row else None
        finally:
            await pool.close()
