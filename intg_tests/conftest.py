import pytest
import asyncpg
import asyncio
from src.config.environment import get_environment

@pytest.fixture
def event_loop():
    # Needed to allow async fixtures in pytest
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def backup_and_restore_tables():
    """
    Usage:
        await backup_and_restore_tables(pool, ["intg_daily_prices", "intg_stock_splits"])
    This will backup the tables before the test and restore them after.
    """
    backups = {}
    async def _backup_and_restore(pool, table_names):
        env = get_environment()
        async with pool.acquire() as conn:
            # Backup
            for table in table_names:
                rows = await conn.fetch(f'SELECT * FROM {table}')
                backups[table] = rows
        try:
            yield
        finally:
            async with pool.acquire() as conn:
                for table in table_names:
                    await conn.execute(f'TRUNCATE {table} CASCADE')
                    rows = backups.get(table, [])
                    if rows:
                        columns = rows[0].keys()
                        col_str = ','.join(columns)
                        values_template = ','.join([f'${i+1}' for i in range(len(columns))])
                        insert_sql = f'INSERT INTO {table} ({col_str}) VALUES ({values_template})'
                        for row in rows:
                            await conn.execute(insert_sql, *row.values())
    return _backup_and_restore
