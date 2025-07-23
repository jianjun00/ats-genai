from config.environment import Environment
import asyncpg
from typing import Optional, List

class EventsDAO:
    def __init__(self, env: Environment):
        self.env = env
        self.table_name = self.env.get_table_name('events')
        self.db_url = self.env.get_database_url()

    async def get_events(self, symbol: Optional[str]=None, event_type: Optional[str]=None, start: Optional[str]=None, end: Optional[str]=None) -> List[dict]:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                query = f"SELECT * FROM {self.table_name} WHERE TRUE"
                params = []
                if symbol:
                    query += f" AND symbol = ${{len(params) + 1}}"
                    params.append(symbol)
                if event_type:
                    query += f" AND event_type = ${{len(params) + 1}}"
                    params.append(event_type)
                if start:
                    query += f" AND event_time >= ${{len(params) + 1}}"
                    params.append(start)
                if end:
                    query += f" AND event_time <= ${{len(params) + 1}}"
                    params.append(end)
                query += " ORDER BY event_time DESC LIMIT 1000"
                rows = await conn.fetch(query, *params)
                return [dict(row) for row in rows]
        finally:
            await pool.close()

    async def insert_event(self, event_type, symbol, event_time, reported_time, source, data) -> dict:
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"""INSERT INTO {self.table_name} (event_type, symbol, event_time, reported_time, source, data)
                       VALUES ($1, $2, $3, $4, $5, $6)
                       RETURNING *""",
                    event_type, symbol, event_time, reported_time, source, data
                )
                return dict(row)
        finally:
            await pool.close()
