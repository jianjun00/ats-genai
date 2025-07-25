import pytest
import asyncpg
from src.db.test_db_manager import TestDatabaseManager
from db.migration_manager import MigrationManager

@pytest.fixture
async def unit_test_db_clean():
    """Fixture for a completely empty unit test database (no tables)."""
    db_manager = TestDatabaseManager("unit")
    test_db_url = await db_manager.setup_test_database()
    migration_manager = MigrationManager(test_db_url)
    await migration_manager.migrate_to_latest()
    # Drop all tables to ensure DB is empty
    pool = await asyncpg.create_pool(test_db_url)
    print(f"test_db_url: {test_db_url}")
    try:
        async with pool.acquire() as conn:
            # Drop all user tables
            tables = await conn.fetch("""
                SELECT tablename FROM pg_tables WHERE schemaname = 'public'
            """)
            for row in tables:
                print(f"dropping {row['tablename']}")
                await conn.execute(f'DROP TABLE IF EXISTS "{row["tablename"]}" CASCADE')
    finally:
        await pool.close()
    yield test_db_url
    await db_manager.teardown_test_database()
    # Clean up backup/dump files created for this test DB
    import os
    import glob
    from pathlib import Path
    from datetime import datetime
    
    # Get the test DB name (e.g., test_trading_db_abc123)
    db_name = test_db_url.split('/')[-1]
    backup_dir = Path(__file__).parent / "migrations" / "backups"
    if backup_dir.exists():
        # Find all dump files matching this test DB
        pattern = f"{db_name}_*.dump"
        for dump_file in backup_dir.glob(pattern):
            try:
                dump_file.unlink()
                print(f"[CLEANUP] Deleted backup dump: {dump_file}")
            except Exception as e:
                print(f"[CLEANUP] Failed to delete {dump_file}: {e}")
