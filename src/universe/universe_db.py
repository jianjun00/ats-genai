# universe_db.py
# Utility functions for multi-universe membership using universe and universe_membership tables
from config.environment import get_environment, Environment
from db.dao.universe_dao import UniverseDAO
from db.dao.universe_membership_dao import UniverseMembershipDAO
from datetime import date
from typing import List, Optional

class UniverseDB:
    def __init__(self, env: Environment = None):
        self.env = env or get_environment()
        self.universe_dao = UniverseDAO(self.env)
        self.universe_membership_dao = UniverseMembershipDAO(self.env)

    async def get_universe_id(self, universe_name: str) -> Optional[int]:
        universe = await self.universe_dao.get_universe_by_name(universe_name)
        return universe['id'] if universe else None

    async def get_universe_members(self, universe_id: int, as_of: date) -> List[str]:
        memberships = await self.universe_membership_dao.get_active_memberships(universe_id, as_of)
        return [row['symbol'] for row in memberships]

    async def add_universe(self, name: str, description: Optional[str] = None) -> int:
        return await self.universe_dao.create_universe(name, description)

    async def add_universe_membership(self, universe_id: int, symbol: str, start_at: date, end_at: Optional[date] = None):
        await self.universe_membership_dao.add_membership_full(universe_id, symbol, start_at, end_at)

    async def update_universe_membership_end(self, universe_id: int, symbol: str, end_at: date):
        await self.universe_membership_dao.update_membership_end(universe_id, symbol, end_at)
