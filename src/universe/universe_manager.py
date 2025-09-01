import asyncpg
import gin
from typing import List
from datetime import date
from config.environment import Environment
from .universe_db import UniverseDB
from dataclasses import dataclass

@dataclass
class UniverseMembershipChange:
    universe_id: int
    symbol: str
    action: str
    effective_date: str
    reason: str

import logging

@gin.configurable
class UniverseManager:
    """
    Manages universe membership operations, including updates and queries.
    """
    def __init__(self, env: Environment, universe_id: int):
        self.env = env or Environment()
        self.logger = logging.getLogger(__name__)        
        self.universe_db = UniverseDB(self.env)
        self.universe_id = universe_id

    async def update_universe_membership(self, membership_changes: List[UniverseMembershipChange]) -> None:
        """
        Apply membership changes to the universe.
        Args:
            membership_changes: List of UniverseMembershipChange to apply
        """
        if not membership_changes:
            return
        self.logger.info(f"Applying {len(membership_changes)} membership changes")
        pool = await asyncpg.create_pool(self.env.get_database_url())
        try:
            async with pool.acquire() as conn:
                for change in membership_changes:
                    await self._apply_single_membership_change(conn, change)
        finally:
            await pool.close()

    async def _apply_single_membership_change(self, conn, change: UniverseMembershipChange) -> None:
        table_name = self.env.get_table_name('universe_membership_changes')
        query = f"""
        INSERT INTO {table_name} (universe_id, symbol, action, effective_date, reason, created_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (universe_id, symbol, action, effective_date) DO UPDATE SET
            reason = EXCLUDED.reason,
            updated_at = NOW()
        """
        await conn.execute(
            query,
            change.universe_id,
            change.symbol,
            change.action,
            change.effective_date,
            change.reason,
            date.today()
        )

    async def get_members(self, universe_id: int, as_of_date: date) -> List[int]:
        """
        Get the list of instrument IDs for a universe as of a specific date.
        """
        return await self.universe_db.get_universe_members(universe_id, as_of_date)

    async def update_for_sod(self, runner, current_time) -> None:
        """
        Start-of-day hook: set instrument_ids to universe_membership valid at as_of_date.
        """
        as_of_date = current_time.date()
        self.logger.info(f"UniverseManager.update_for_sod called for universe_id={self.universe_id} at {as_of_date}")
        ids = await self.get_members(self.universe_id, as_of_date)
        # Ensure all IDs are integers
        for iid in ids:
            assert isinstance(iid, int), f"instrument_id must be int, got {type(iid)}: {iid}"
        self.instrument_ids = ids
        self.logger.info(f"UniverseManager.instrument_ids set to {self.instrument_ids}")

    async def update_for_eod(self, runner, current_time) -> None:
        """
        Update membership for end of day (EOD) processing. This could include rolling membership, applying changes, etc.
        """
        # Example: apply all membership changes up to as_of_date
        as_of_date = current_time.date()
        raw_changes = await self.universe_db.get_membership_changes(self.universe_id, as_of_date)
        # Convert dicts to UniverseMembershipChange objects
        membership_changes = [UniverseMembershipChange(**change) for change in raw_changes]
        await self.update_universe_membership(membership_changes)

