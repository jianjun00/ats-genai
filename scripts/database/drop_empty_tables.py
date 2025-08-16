import os
import asyncpg
import asyncio

DB_URL = os.environ.get('DATABASE_URL') or os.environ.get('INTG_DATABASE_URL') or 'postgresql://localhost/intg_trading_db'

async def drop_empty_tables():
    conn = await asyncpg.connect(DB_URL)
    tables = await conn.fetch("""
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public'
    """)
    for table in tables:
        tablename = table['tablename']
        count = await conn.fetchval(f'SELECT COUNT(*) FROM "{tablename}"')
        if count == 0:
            print(f'Dropping empty table: {tablename}')
            await conn.execute(f'DROP TABLE IF EXISTS "{tablename}" CASCADE')
    await conn.close()

if __name__ == "__main__":
    asyncio.run(drop_empty_tables())
