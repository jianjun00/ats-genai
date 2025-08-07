import asyncpg
from typing import List, Optional, Dict, Any
from state.universe_state import UniverseStateInterval
from config.environment import Environment
from datetime import datetime

class UniverseStateIntervalDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.db_url = env.get_database_url()

    async def create(self, universe_id: int, duration: str, start_date_time, end_date_time) -> int:
        """Insert a new UniverseStateInterval record. Returns the new id."""
        conn = await asyncpg.connect(self.db_url)
        try:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self.env.get_table_name('universe_state_interval')} (
                    universe_id, duration, start_date_time, end_date_time
                ) VALUES ($1, $2, $3, $4)
                RETURNING id
                """,
                universe_id, duration, start_date_time, end_date_time
            )
            return row['id']
        finally:
            await conn.close()

    async def get(self, id: int) -> Optional[dict]:
        """Fetch a UniverseStateInterval by id."""
        conn = await asyncpg.connect(self.db_url)
        try:
            row = await conn.fetchrow(
                f"SELECT * FROM {self.env.get_table_name('universe_state_interval')} WHERE id = $1",
                id
            )
            return dict(row) if row else None
        finally:
            await conn.close()

    async def list(self, universe_id: int = None) -> list:
        """List UniverseStateIntervals, optionally filter by universe_id."""
        conn = await asyncpg.connect(self.db_url)
        try:
            if universe_id is not None:
                rows = await conn.fetch(
                    f"SELECT * FROM {self.env.get_table_name('universe_state_interval')} WHERE universe_id = $1",
                    universe_id
                )
            else:
                rows = await conn.fetch(
                    f"SELECT * FROM {self.env.get_table_name('universe_state_interval')}"
                )
            return [dict(row) for row in rows]
        finally:
            await conn.close()

    async def delete(self, id: int) -> bool:
        """Delete a UniverseStateInterval by id."""
        conn = await asyncpg.connect(self.db_url)
        try:
            result = await conn.execute(
                f"DELETE FROM {self.env.get_table_name('universe_state_interval')} WHERE id = $1",
                id
            )
            return result.startswith("DELETE")
        finally:
            await conn.close()
