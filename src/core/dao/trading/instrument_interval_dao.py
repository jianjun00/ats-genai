import asyncpg
from typing import Optional, List
from core.platform.config_env.environment import Environment

class InstrumentIntervalDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.db_url = env.get_database_url()

    async def create(self, universe_state_interval_id: int, instrument_id: int, open: float, high: float, low: float, close: float, traded_volume: float, traded_dollar: float, status: str, market_cap: float, start_date_time=None, end_date_time=None, interval_duration: str = "60m", run_id: str = None) -> int:
        """Insert a new InstrumentInterval. Returns new id."""
        
        # 🚨 UUID SYSTEM: Use UUID from Environment if available, otherwise use provided run_id
        effective_run_id = run_id
        if hasattr(self.env, 'get_run_uuid') and self.env.get_run_uuid() is not None:
            effective_run_id = self.env.get_run_uuid()
            print(f"[UUID DEBUG] InstrumentIntervalDAO using Environment UUID: {effective_run_id}")
        elif effective_run_id is None:
            print(f"[UUID DEBUG] InstrumentIntervalDAO: No UUID available in Environment or parameters")
        
        # FAIL FAST: Require timezone-naive datetime objects for PostgreSQL consistency
        if start_date_time is not None and hasattr(start_date_time, 'tzinfo') and start_date_time.tzinfo is not None:
            raise ValueError(f"start_date_time must be timezone-naive for PostgreSQL storage, got timezone-aware: {start_date_time}")
        if end_date_time is not None and hasattr(end_date_time, 'tzinfo') and end_date_time.tzinfo is not None:
            raise ValueError(f"end_date_time must be timezone-naive for PostgreSQL storage, got timezone-aware: {end_date_time}")
        
        conn = await asyncpg.connect(self.db_url)
        try:
            # Include interval_start and interval_end if provided
            if start_date_time is not None and end_date_time is not None:
                if effective_run_id is not None:
                    row = await conn.fetchrow(
                        f"""
                        INSERT INTO {self.env.get_table_name('instrument_interval')} (
                            universe_state_interval_id, instrument_id, open, high, low, close, traded_volume, traded_dollar, status, market_cap,
                            interval_start, interval_end, interval_duration, run_id
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                        RETURNING id
                        """,
                        universe_state_interval_id, instrument_id, open, high, low, close, traded_volume, traded_dollar, status, market_cap,
                        start_date_time, end_date_time, interval_duration, effective_run_id
                    )
                else:
                    row = await conn.fetchrow(
                        f"""
                        INSERT INTO {self.env.get_table_name('instrument_interval')} (
                            universe_state_interval_id, instrument_id, open, high, low, close, traded_volume, traded_dollar, status, market_cap,
                            interval_start, interval_end, interval_duration
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                        RETURNING id
                        """,
                        universe_state_interval_id, instrument_id, open, high, low, close, traded_volume, traded_dollar, status, market_cap,
                        start_date_time, end_date_time, interval_duration
                    )
            else:
                if effective_run_id is not None:
                    row = await conn.fetchrow(
                        f"""
                        INSERT INTO {self.env.get_table_name('instrument_interval')} (
                            universe_state_interval_id, instrument_id, open, high, low, close, traded_volume, traded_dollar, status, market_cap, interval_duration, run_id
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                        RETURNING id
                        """,
                        universe_state_interval_id, instrument_id, open, high, low, close, traded_volume, traded_dollar, status, market_cap, interval_duration, effective_run_id
                    )
                else:
                    row = await conn.fetchrow(
                        f"""
                        INSERT INTO {self.env.get_table_name('instrument_interval')} (
                            universe_state_interval_id, instrument_id, open, high, low, close, traded_volume, traded_dollar, status, market_cap, interval_duration
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                        RETURNING id
                        """,
                        universe_state_interval_id, instrument_id, open, high, low, close, traded_volume, traded_dollar, status, market_cap, interval_duration
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
