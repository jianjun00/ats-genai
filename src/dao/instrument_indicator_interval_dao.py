import asyncpg
from typing import Optional, List
from config.environment import Environment

class InstrumentIndicatorIntervalDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.db_url = env.get_database_url()

    async def create(self, instrument_interval_id: int, indicator_name: str, indicator_value: float, indicator_status: str = None) -> int:
        """Insert a new InstrumentIndicatorInterval. Returns new id."""
        conn = await asyncpg.connect(self.db_url)
        try:
            row = await conn.fetchrow(
                f"""
                INSERT INTO {self.env.get_table_name('instrument_indicator_interval')} (
                    instrument_interval_id, indicator_name, indicator_value, indicator_status
                ) VALUES ($1, $2, $3, $4)
                RETURNING id
                """,
                instrument_interval_id, indicator_name, indicator_value, indicator_status
            )
            return row['id']
        finally:
            await conn.close()

    async def get(self, id: int) -> Optional[dict]:
        conn = await asyncpg.connect(self.db_url)
        try:
            row = await conn.fetchrow(
                f"SELECT * FROM {self.env.get_table_name('instrument_indicator_interval')} WHERE id = $1",
                id
            )
            return dict(row) if row else None
        finally:
            await conn.close()

    async def list(self, instrument_interval_id: int = None) -> List[dict]:
        conn = await asyncpg.connect(self.db_url)
        try:
            if instrument_interval_id is not None:
                rows = await conn.fetch(
                    f"SELECT * FROM {self.env.get_table_name('instrument_indicator_interval')} WHERE instrument_interval_id = $1",
                    instrument_interval_id
                )
            else:
                rows = await conn.fetch(
                    f"SELECT * FROM {self.env.get_table_name('instrument_indicator_interval')}"
                )
            return [dict(row) for row in rows]
        finally:
            await conn.close()

    async def delete(self, id: int) -> bool:
        conn = await asyncpg.connect(self.db_url)
        try:
            result = await conn.execute(
                f"DELETE FROM {self.env.get_table_name('instrument_indicator_interval')} WHERE id = $1",
                id
            )
            return result.startswith("DELETE")
        finally:
            await conn.close()
