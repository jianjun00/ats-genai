import asyncpg
import asyncio
import os

TSDB_URL = os.getenv('TSDB_URL', 'postgresql://localhost:5432/trading_db')

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

async def main():
    pool = await asyncpg.create_pool(TSDB_URL, min_size=1, max_size=2)
    async with pool.acquire() as conn:
        await conn.execute(CREATE_SPY_MEMBERSHIP_SQL)
    await pool.close()
    print("spy_membership table created.")

if __name__ == '__main__':
    asyncio.run(main())
