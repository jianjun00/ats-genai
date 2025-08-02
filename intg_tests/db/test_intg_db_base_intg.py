import asyncpg
from config.environment import get_environment, set_environment, EnvironmentType

set_environment(EnvironmentType.INTEGRATION)
env = get_environment()
TSDB_URL = env.get_database_url()

def get_test_db_url():
    """
    Returns the integration test database URL for use in intg_tests.
    """
    return TSDB_URL

class AsyncPGTestDBBase:
    """
    Base class for integration tests using asyncpg and the integration test database.
    Provides setup/teardown and utility methods for DB tests.
    """
    @classmethod
    async def clear_table(cls, table_name):
        pool = await asyncpg.create_pool(TSDB_URL)
        async with pool.acquire() as conn:
            await conn.execute(f"DELETE FROM {env.get_table_name(table_name)}")
        await pool.close()

    @classmethod
    async def fetch_all(cls, table_name):
        pool = await asyncpg.create_pool(TSDB_URL)
        async with pool.acquire() as conn:
            rows = await conn.fetch(f"SELECT * FROM {env.get_table_name(table_name)}")
        await pool.close()
        return rows
