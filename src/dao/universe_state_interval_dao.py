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

    async def list(self, universe_id: int = None, start_date_time: str = None, end_date_time: str = None) -> list:
        """List UniverseStateIntervals, optionally filter by universe_id, start_date_time, and end_date_time."""
        conn = await asyncpg.connect(self.db_url)
        try:
            query = f"SELECT * FROM {self.env.get_table_name('universe_state_interval')}"
            filters = []
            params = []
            if universe_id is not None:
                filters.append("universe_id = $%d" % (len(params) + 1))
                params.append(universe_id)
            if start_date_time is not None:
                filters.append("start_date_time >= $%d" % (len(params) + 1))
                params.append(start_date_time)
            if end_date_time is not None:
                filters.append("end_date_time <= $%d" % (len(params) + 1))
                params.append(end_date_time)
            if filters:
                query += " WHERE " + " AND ".join(filters)
            rows = await conn.fetch(query, *params)
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
