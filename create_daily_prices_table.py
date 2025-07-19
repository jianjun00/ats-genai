import asyncpg
import asyncio
import os

TSDB_URL = os.getenv('TSDB_URL', 'postgresql://localhost:5432/trading_db')

CREATE_TABLE_SQL = """
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

async def main():
    pool = await asyncpg.create_pool(TSDB_URL, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_TABLE_SQL)
        try:
            await conn.execute(CREATE_HYPERTABLE_SQL)
        except Exception as e:
            print(f"Hypertable creation skipped or failed: {e}")
    await pool.close()
    print("daily_prices table created (and hypertable if TimescaleDB is enabled).")

if __name__ == '__main__':
    asyncio.run(main())
