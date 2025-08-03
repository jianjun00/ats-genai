# universe_db.py
# Utility functions for multi-universe membership using universe and universe_membership tables
from config.environment import Environment
from dao.universe_dao import UniverseDAO
from dao.universe_membership_dao import UniverseMembershipDAO
from datetime import date
from typing import List, Optional

class UniverseDB:
    async def get_membership_changes(self, universe_id: int, as_of: date):
        return await self.universe_membership_dao.get_membership_changes(universe_id, as_of)

    def __init__(self, env: Environment = None):
        print(f"[DEBUG] UniverseDB.__init__ received env of type {type(env)}: {env}")
        self.env = env or Environment()
        self.universe_dao = UniverseDAO(self.env)
        self.universe_membership_dao = UniverseMembershipDAO(self.env)

    async def get_universe_id(self, universe_name: str) -> Optional[int]:
        universe = await self.universe_dao.get_universe_by_name(universe_name)
        return universe['id'] if universe else None

    async def get_universe_members(self, universe_id: int, as_of: date) -> List[str]:
        memberships = await self.universe_membership_dao.get_active_memberships(universe_id, as_of)
        instrument_ids = [row['instrument_id'] for row in memberships if row.get('instrument_id') is not None]
        if not instrument_ids:
            return []
        # Query all symbols for instrument_ids in one go
        instruments_dao = __import__('dao.instruments_dao', fromlist=['InstrumentsDAO']).InstrumentsDAO(self.env)
        pool = await __import__('asyncpg').create_pool(self.env.get_database_url())
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch(f"SELECT id, symbol FROM {instruments_dao.table_name} WHERE id = ANY($1)", instrument_ids)
                id_to_symbol = {row['id']: row['symbol'] for row in rows}
            return [id_to_symbol.get(instrument_id) for instrument_id in instrument_ids]
        finally:
            await pool.close()


    async def add_universe(self, name: str, description: Optional[str] = None) -> int:
        return await self.universe_dao.create_universe(name, description)

    async def add_universe_membership(self, universe_id: int, symbol: str, start_at: date, end_at: Optional[date] = None):
        await self.universe_membership_dao.add_membership_full(universe_id=universe_id, symbol=symbol, start_at=start_at, end_at=end_at)

    async def update_universe_membership_end(self, universe_id: int, symbol: str, end_at: date):
        # Map symbol to instrument_id before calling DAO
        from dao.instrument_xrefs_dao import InstrumentXrefsDAO
        xrefs_dao = InstrumentXrefsDAO(self.env)
        instrument_id = await xrefs_dao.resolve_instrument_id(symbol)
        await self.universe_membership_dao.update_membership_end(universe_id=universe_id, instrument_id=instrument_id, end_at=end_at)
