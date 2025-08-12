# universe_db.py
# Utility functions for multi-universe membership using universe and universe_membership tables
from config.environment import Environment
from dao.universe_dao import UniverseDAO
from dao.universe_membership_dao import UniverseMembershipDAO
from dao.instruments_dao import InstrumentsDAO
from datetime import date
from typing import List, Optional, Dict

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
        """
        Get the list of symbols for a universe as of a specific date.
        
        Args:
            universe_id: ID of the universe
            as_of: Date to check membership for
            
        Returns:
            List of symbols that are members of the universe on the given date
        """
        memberships = await self.universe_membership_dao.get_active_memberships(universe_id, as_of)
        instrument_ids = [row['instrument_id'] for row in memberships if row.get('instrument_id') is not None]
        
        if not instrument_ids:
            return []
            
        # Resolve instrument IDs to symbols
        instruments_dao = InstrumentsDAO(self.env)
        id_to_symbol = await instruments_dao.get_symbols_by_ids(instrument_ids)
        
        # Return symbols in the same order as the instrument IDs
        return [id_to_symbol.get(iid) for iid in instrument_ids if id_to_symbol.get(iid) is not None]


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
