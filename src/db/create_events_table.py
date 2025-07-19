import asyncpg
import asyncio
import os

TSDB_URL = os.getenv('TSDB_URL', 'postgresql://localhost:5432/trading_db')

CREATE_EVENTS_SQL = """
CREATE TABLE IF NOT EXISTS events (
    id SERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    symbol TEXT,
    event_time TIMESTAMPTZ NOT NULL,
    reported_time TIMESTAMPTZ,
    source TEXT,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_events_symbol_time ON events(symbol, event_time);
CREATE INDEX IF NOT EXISTS idx_events_type_time ON events(event_type, event_time);
"""

async def main():
    pool = await asyncpg.create_pool(TSDB_URL, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_EVENTS_SQL)
    await pool.close()
    print("events table created.")

if __name__ == '__main__':
    asyncio.run(main())
