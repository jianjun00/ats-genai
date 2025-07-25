import pytest
import asyncpg
from src.db.test_db_manager import TestDatabaseManager

@pytest.fixture
async def unit_test_db_clean():
    """Fixture for a completely empty unit test database (no tables)."""
    db_manager = TestDatabaseManager("unit")
    test_db_url = await db_manager.setup_test_database()
    # Drop all tables to ensure DB is empty
    pool = await asyncpg.create_pool(test_db_url)
    try:
        async with pool.acquire() as conn:
            # Drop all user tables
            tables = await conn.fetch("""
                SELECT tablename FROM pg_tables WHERE schemaname = 'public'
            """)
            for row in tables:
                await conn.execute(f'DROP TABLE IF EXISTS "{row["tablename"]}" CASCADE')
    finally:
        await pool.close()
    yield test_db_url
    await db_manager.teardown_test_database()
