import pytest
import asyncpg
from src.config.environment import get_environment

@pytest.fixture(autouse=True, scope="function")
async def auto_backup_restore_all_intg_tables(event_loop):
    """
    Automatically backup and restore all intg_ tables before and after each test in intg_tests.
    """
    env = get_environment()
    db_url = env.get_database_url()
    pool = await asyncpg.create_pool(db_url)
    async with pool.acquire() as conn:
        tables = [r['tablename'] for r in await conn.fetch("""
            SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'intg_%'""")]
        backups = {}
        for table in tables:
            backups[table] = await conn.fetch(f'SELECT * FROM {table}')
    try:
        yield
    finally:
        async with pool.acquire() as conn:
            for table in tables:
                await conn.execute(f'TRUNCATE {table} CASCADE')
                rows = backups.get(table, [])
                if rows:
                    columns = rows[0].keys()
                    col_str = ','.join(columns)
                    values_template = ','.join([f'${i+1}' for i in range(len(columns))])
                    insert_sql = f'INSERT INTO {table} ({col_str}) VALUES ({values_template})'
                    for row in rows:
                        await conn.execute(insert_sql, *row.values())
    await pool.close()
