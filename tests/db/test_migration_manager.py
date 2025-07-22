"""
Unit tests for the migration manager.
Each test gets its own isolated database.
"""

import pytest
import asyncio
from src.db.migration_manager import MigrationManager
from src.db.test_db_manager import unit_test_db


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_migration_manager_initial_version(unit_test_db):
    """Test that migration manager can track initial version."""
    manager = MigrationManager(unit_test_db)
    
    # Initial version should be -1 (no migrations applied)
    version = await manager.get_current_version()
    assert version == -1
    
    # After applying initial migration, version should be 0
    await manager.migrate_to_latest()
    version = await manager.get_current_version()
    assert version >= 0


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_migration_validation(unit_test_db):
    """Test migration validation functionality."""
    manager = MigrationManager(unit_test_db)
    
    # Apply migrations
    await manager.migrate_to_latest()
    
    # Validate migrations
    is_valid = await manager.validate_migrations()
    assert is_valid is True


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_table_prefix_application(unit_test_db):
    """Test that table prefixes are correctly applied in migrations."""
    manager = MigrationManager(unit_test_db)
    
    # Check that table prefix is applied
    assert manager.table_prefix == "test_"
    
    # Apply migrations
    await manager.migrate_to_latest()
    
    # Verify that tables have correct prefix
    import asyncpg
    pool = await asyncpg.create_pool(unit_test_db)
    try:
        async with pool.acquire() as conn:
            # Check if prefixed table exists
            result = await conn.fetchval("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = 'test_db_version'
            """)
            assert result == 1
    finally:
        await pool.close()
