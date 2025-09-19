import asyncpg
from typing import Optional, List
from core.platform.config.environment import Environment

class FactorIntervalDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.db_url = env.get_database_url()

    async def create(self, universe_state_interval_id: int, factor_name: str, factor_value: float) -> int:
        """Insert a new FactorInterval. Returns new id."""
        conn = await asyncpg.connect(self.db_url)
        try:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self.env.get_table_name('factor_interval')} (
                    universe_state_interval_id, factor_name, factor_value
                ) VALUES ($1, $2, $3)
                RETURNING id
                """,
                universe_state_interval_id, factor_name, factor_value
            )
            return row['id']
        finally:
            await conn.close()

    async def get(self, id: int) -> Optional[dict]:
        conn = await asyncpg.connect(self.db_url)
        try:
            row = await conn.fetchrow(
                f"SELECT * FROM {self.env.get_table_name('factor_interval')} WHERE id = $1",
                id
            )
            return dict(row) if row else None
        finally:
            await conn.close()

    async def list(self, universe_state_interval_id: int = None) -> List[dict]:
        conn = await asyncpg.connect(self.db_url)
        try:
            if universe_state_interval_id is not None:
                rows = await conn.fetch(
                    f"SELECT * FROM {self.env.get_table_name('factor_interval')} WHERE universe_state_interval_id = $1",
                    universe_state_interval_id
                )
            else:
                rows = await conn.fetch(
                    f"SELECT * FROM {self.env.get_table_name('factor_interval')}"
                )
            return [dict(row) for row in rows]
        finally:
            await conn.close()

    async def delete(self, id: int) -> bool:
        conn = await asyncpg.connect(self.db_url)
        try:
            result = await conn.execute(
                f"DELETE FROM {self.env.get_table_name('factor_interval')} WHERE id = $1",
                id
            )
            return result.startswith("DELETE")
        finally:
            await conn.close()
