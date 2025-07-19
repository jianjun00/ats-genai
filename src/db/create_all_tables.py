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

CREATE_DAILY_PRICES_SQL = """
CREATE TABLE IF NOT EXISTS daily_prices (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    PRIMARY KEY (date, symbol)
);
"""

CREATE_HYPERTABLE_SQL = """
SELECT create_hypertable('daily_prices', 'date', if_not_exists => TRUE);
"""

CREATE_SPY_MEMBERSHIP_SQL = """
CREATE TABLE IF NOT EXISTS spy_membership (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    effective_date DATE NOT NULL,
    removal_date DATE
);
CREATE INDEX IF NOT EXISTS idx_spy_membership_symbol ON spy_membership(symbol);
CREATE INDEX IF NOT EXISTS idx_spy_membership_effective ON spy_membership(effective_date);
"""

CREATE_SPY_MEMBERSHIP_CHANGE_SQL = """
CREATE TABLE IF NOT EXISTS spy_membership_change (
    id SERIAL PRIMARY KEY,
    change_date DATE NOT NULL,
    added TEXT,
    removed TEXT
);
"""

async def main():
    pool = await asyncpg.create_pool(TSDB_URL, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_EVENTS_SQL)
        await conn.execute(CREATE_DAILY_PRICES_SQL)
        try:
            await conn.execute(CREATE_HYPERTABLE_SQL)
        except Exception as e:
            print(f"Hypertable creation skipped or failed: {e}")
        await conn.execute(CREATE_SPY_MEMBERSHIP_SQL)
        await conn.execute(CREATE_SPY_MEMBERSHIP_CHANGE_SQL)
    await pool.close()
    print("All tables created (and hypertable if TimescaleDB is enabled).")

if __name__ == '__main__':
    asyncio.run(main())
