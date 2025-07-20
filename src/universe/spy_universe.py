# moved from project root
import asyncio
from datetime import datetime, date
from typing import List, Optional
from universe_db import UniverseDB

class SPYUniverse:
    def __init__(self, db_url: str, universe_name: str = 'S&P 500'):
        self.db_url = db_url
        self.universe_name = universe_name
        self._universe_id = None
        self._universe_db = UniverseDB(db_url)

    async def get_universe(self, as_of: Optional[date] = None) -> List[str]:
        if as_of is None:
            as_of = datetime.utcnow().date()
        if self._universe_id is None:
            self._universe_id = await self._universe_db.get_universe_id(self.universe_name)
            if self._universe_id is None:
                raise ValueError(f"Universe '{self.universe_name}' not found in DB.")
        return await self._universe_db.get_universe_members(self._universe_id, as_of)

