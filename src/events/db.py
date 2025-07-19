import asyncpg
import os
from .schemas import EventIn
from typing import Optional

TSDB_URL = os.getenv('TSDB_URL', 'postgresql://localhost:5432/trading_db')

async def get_events(symbol: Optional[str]=None, event_type: Optional[str]=None, start: Optional[str]=None, end: Optional[str]=None):
    pool = await asyncpg.create_pool(TSDB_URL, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        query = "SELECT * FROM events WHERE TRUE"
        params = []
        if symbol:
            query += f" AND symbol = ${len(params) + 1}"
            params.append(symbol)
        if event_type:
            query += f" AND event_type = ${len(params) + 1}"
            params.append(event_type)
        if start:
            query += f" AND event_time >= ${len(params) + 1}"
            params.append(start)
        if end:
            query += f" AND event_time <= ${len(params) + 1}"
            params.append(end)
        query += " ORDER BY event_time DESC LIMIT 1000"
        rows = await conn.fetch(query, *params)
    await pool.close()
    return [dict(row) for row in rows]

async def insert_event(event: EventIn):
    pool = await asyncpg.create_pool(TSDB_URL, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """INSERT INTO events (event_type, symbol, event_time, reported_time, source, data)
               VALUES ($1, $2, $3, $4, $5, $6)
               RETURNING *""",
            event.event_type, event.symbol, event.event_time, event.reported_time, event.source, event.data
        )
    await pool.close()
    return dict(row)
