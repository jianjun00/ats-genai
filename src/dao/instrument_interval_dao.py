import asyncpg
from typing import Optional, List
from config.environment import Environment

class InstrumentIntervalDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.db_url = env.get_database_url()

    async def create(self, universe_state_interval_id: int, instrument_id: int, open: float, high: float, low: float, close: float, traded_volume: float, traded_dollar: float, status: str, market_cap: float, start_date_time=None, end_date_time=None) -> int:
        """Insert a new InstrumentInterval. Returns new id."""
        conn = await asyncpg.connect(self.db_url)
        try:
            # Include start_date_time and end_date_time if provided
            if start_date_time is not None and end_date_time is not None:
                row = await conn.fetchrow(
                    f"""
                    INSERT INTO {self.env.get_table_name('instrument_interval')} (
                        universe_state_interval_id, instrument_id, open, high, low, close, traded_volume, traded_dollar, status, market_cap,
                        start_date_time, end_date_time
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    RETURNING id
                    """,
                    universe_state_interval_id, instrument_id, open, high, low, close, traded_volume, traded_dollar, status, market_cap,
                    start_date_time, end_date_time
                )
            else:
                row = await conn.fetchrow(
                    f"""
                    INSERT INTO {self.env.get_table_name('instrument_interval')} (
                        universe_state_interval_id, instrument_id, open, high, low, close, traded_volume, traded_dollar, status, market_cap
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    RETURNING id
                    """,
                    universe_state_interval_id, instrument_id, open, high, low, close, traded_volume, traded_dollar, status, market_cap
                )
            return row['id']
        finally:
            await conn.close()

    async def get(self, id: int) -> Optional[dict]:
        conn = await asyncpg.connect(self.db_url)
        try:
            row = await conn.fetchrow(
                f"SELECT * FROM {self.env.get_table_name('instrument_interval')} WHERE id = $1",
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
                    f"SELECT * FROM {self.env.get_table_name('instrument_interval')} WHERE universe_state_interval_id = $1",
                    universe_state_interval_id
                )
            else:
                rows = await conn.fetch(
                    f"SELECT * FROM {self.env.get_table_name('instrument_interval')}"
                )
            return [dict(row) for row in rows]
        finally:
            await conn.close()

    async def delete(self, id: int) -> bool:
        conn = await asyncpg.connect(self.db_url)
        try:
            result = await conn.execute(
                f"DELETE FROM {self.env.get_table_name('instrument_interval')} WHERE id = $1",
                id
            )
            return result.startswith("DELETE")
        finally:
            await conn.close()
