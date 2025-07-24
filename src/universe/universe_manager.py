import asyncpg
from typing import List, Optional
from datetime import date
from config.environment import get_environment, Environment
from .universe_db import UniverseDB
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class UniverseMembershipChange:
    symbol: str
    action: str
    effective_date: str
    reason: str
    metadata: Optional[Dict[str, Any]] = None

import logging

class UniverseManager:
    async def update_for_eod(self, universe_id: int, as_of_date: date) -> None:
        """
        End-of-day hook for UniverseManager. Implement EOD membership or state updates if needed.
        """
        self.logger.info(f"UniverseManager.update_for_eod called for universe_id={universe_id} at {as_of_date}")
        # Add EOD logic if needed

    """
    Manages universe membership operations, including updates and queries.
    """
    def __init__(self, env: Optional[Environment] = None):
        self.env = env or get_environment()
        self.logger = logging.getLogger(__name__)
        self.universe_db = UniverseDB(self.env)

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
        INSERT INTO {table_name} (symbol, action, effective_date, reason, created_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (symbol, effective_date) DO UPDATE SET
            action = EXCLUDED.action,
            reason = EXCLUDED.reason,
            updated_at = NOW()
        """
        await conn.execute(
            query,
            change.symbol,
            change.action.value,
            change.effective_date,
            change.reason,
            date.today()
        )

    async def get_members(self, universe_id: int, as_of_date: date) -> List[str]:
        """
        Get the list of member symbols for a universe as of a specific date.
        """
        return await self.universe_db.get_universe_members(universe_id, as_of_date)

    async def update_for_sod(self, universe_id: int, as_of_date: date) -> None:
        """
        Start-of-day hook: set instrument_ids to universe_membership valid at as_of_date.
        """
        self.logger.info(f"UniverseManager.update_for_sod called for universe_id={universe_id} at {as_of_date}")
        self.instrument_ids = await self.get_members(universe_id, as_of_date)
        self.logger.info(f"UniverseManager.instrument_ids set to {self.instrument_ids}")

    async def update_for_eod(self, universe_id: int, as_of_date: date) -> None:
        """
        Update membership for end of day (EOD) processing. This could include rolling membership, applying changes, etc.
        """
        # Example: apply all membership changes up to as_of_date
        membership_changes = await self.universe_db.get_membership_changes(universe_id, as_of_date)
        await self.update_universe_membership(membership_changes)
