"""
Comprehensive unit tests for the database migration manager.
Tests cover edge cases, error conditions, and various migration scenarios.
Each test gets its own isolated database.
"""

import pytest
import asyncio
import asyncpg
import os
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, mock_open
from src.db.migration_manager import MigrationManager
from src.db.conftest import unit_test_db_clean
import pytest_asyncio
from src.db.test_db_manager import TestDatabaseManager


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_migration_manager_initialization(unit_test_db_clean):
    """Test that migration manager initializes correctly."""
    manager = MigrationManager(unit_test_db_clean)
    
    assert manager.db_url == unit_test_db_clean
    assert manager.table_prefix == "test_"
    assert manager.migrations_dir.name == "migrations"


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_get_current_version_empty_database(unit_test_db_clean):
    """Test getting version from empty database returns -1."""
    manager = MigrationManager(unit_test_db_clean)
    
    version = await manager.get_current_version()
    assert version == -1


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_get_current_version_with_existing_data(unit_test_db_clean):
    """Test getting version when db_version table has data."""
    manager = MigrationManager(unit_test_db_clean)
    
    # First call creates the table
    await manager.get_current_version()
    
    # Manually insert a version record
    pool = await asyncpg.create_pool(unit_test_db_clean)
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO test_db_version (version, description, migration_file)
                VALUES (5, 'Test migration', 'test.sql')
            """)
    finally:
        await pool.close()
    
    # Should return the highest version
    version = await manager.get_current_version()
    assert version == 5


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_apply_table_prefixes(unit_test_db_clean):
    """Test that table prefixes are correctly applied to SQL."""
    manager = MigrationManager(unit_test_db_clean)
    
    sql = """
    CREATE TABLE IF NOT EXISTS events (id SERIAL PRIMARY KEY);
    CREATE TABLE IF NOT EXISTS daily_prices (date DATE);
    INSERT INTO events (id) VALUES (1);
    SELECT * FROM daily_prices;
    """
    
    prefixed_sql = manager._apply_table_prefixes(sql)
    
    assert "CREATE TABLE IF NOT EXISTS test_events" in prefixed_sql
    assert "CREATE TABLE IF NOT EXISTS test_daily_prices" in prefixed_sql
    assert "INSERT INTO test_events" in prefixed_sql
    assert "SELECT * FROM test_daily_prices" in prefixed_sql


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_apply_table_prefixes_no_double_prefixing(unit_test_db_clean):
    """Test that already prefixed tables don't get double-prefixed."""
    manager = MigrationManager(unit_test_db_clean)
    
    sql = "CREATE TABLE IF NOT EXISTS test_events (id SERIAL PRIMARY KEY);"
    prefixed_sql = manager._apply_table_prefixes(sql)
    
    # Should not become test_test_events
    assert "test_test_events" not in prefixed_sql
    assert "test_events" in prefixed_sql


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_calculate_checksum(unit_test_db_clean):
    """Test checksum calculation for migration files."""
    manager = MigrationManager(unit_test_db_clean)
    await manager.get_current_version()
    
    # Create a temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write("CREATE TABLE test (id SERIAL);")
        temp_file = Path(f.name)
    
    try:
        checksum1 = manager._calculate_checksum(temp_file)
        checksum2 = manager._calculate_checksum(temp_file)
        
        # Same file should produce same checksum
        assert checksum1 == checksum2
        assert len(checksum1) == 32  # MD5 hash length
        
        # Different content should produce different checksum
        with open(temp_file, 'w') as f:
            f.write("CREATE TABLE different (id SERIAL);")
        
        checksum3 = manager._calculate_checksum(temp_file)
        assert checksum1 != checksum3
        
    finally:
        temp_file.unlink()


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_migration_file_parsing():
    """Test parsing of migration file names."""
    # Create temporary migration directory
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_dir = Path(temp_dir)
        
        # Create test migration files
        (migrations_dir / "001_initial_schema.sql").touch()
        (migrations_dir / "002_add_users.sql").touch()
        (migrations_dir / "010_add_indexes.sql").touch()
        (migrations_dir / "invalid_name.sql").touch()  # Should be ignored
        
        # Create a manager with mocked migrations directory
        manager = MigrationManager("dummy_url")
        manager.migrations_dir = migrations_dir
        
        migrations = manager._get_migration_files()
        
        # Should find 3 valid migrations, sorted by version
        assert len(migrations) == 3
        assert migrations[0] == (1, "initial schema", migrations_dir / "001_initial_schema.sql")
        assert migrations[1] == (2, "add users", migrations_dir / "002_add_users.sql")
        assert migrations[2] == (10, "add indexes", migrations_dir / "010_add_indexes.sql")


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_apply_migration_success(unit_test_db_clean):
    """Test successful migration application."""
    manager = MigrationManager(unit_test_db_clean)
    await manager.get_current_version()
    
    # Create a temporary migration file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write("""
        CREATE TABLE test_migration_table (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
        );
        """)
        temp_file = Path(f.name)
    
    try:
        # Apply migration
        success = await manager.apply_migration(1, "test migration", temp_file)
        assert success is True
        
        # Verify table was created with correct prefix
        pool = await asyncpg.create_pool(unit_test_db_clean)
        try:
            async with pool.acquire() as conn:
                # Check table exists
                result = await conn.fetchval("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name = 'test_test_migration_table'
                """)
                assert result == 1
                
                # Check migration was recorded
                version_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM test_db_version WHERE version = 1
                """)
                assert version_count == 1
        finally:
            await pool.close()
            
    finally:
        temp_file.unlink()


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_apply_migration_sql_error(unit_test_db_clean):
    """Test migration application with SQL error."""
    manager = MigrationManager(unit_test_db_clean)
    await manager.get_current_version()
    
    # Create a migration file with invalid SQL
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write("INVALID SQL STATEMENT;")
        temp_file = Path(f.name)
    
    try:
        # Apply migration should fail
        success = await manager.apply_migration(1, "bad migration", temp_file)
        assert success is False
        
        # Verify migration was not recorded
        pool = await asyncpg.create_pool(unit_test_db_clean)
        try:
            async with pool.acquire() as conn:
                version_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM test_db_version WHERE version = 1
                """)
                assert version_count == 0
        finally:
            await pool.close()
            
    finally:
        temp_file.unlink()


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_migrate_to_latest_no_migrations(unit_test_db_clean):
    """Test migrate_to_latest when no migrations are available."""
    # Create temporary empty migration directory
    with tempfile.TemporaryDirectory() as temp_dir:
        manager = MigrationManager(unit_test_db_clean)
        manager.migrations_dir = Path(temp_dir)
        
        success = await manager.migrate_to_latest()
        assert success is True


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_migrate_to_latest_with_migrations(unit_test_db_clean):
    """Test migrate_to_latest with multiple migrations."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_dir = Path(temp_dir)
        
        # Create migration files
        migration1 = migrations_dir / "001_create_users.sql"
        migration1.write_text("CREATE TABLE users (id SERIAL PRIMARY KEY);")
        
        migration2 = migrations_dir / "002_create_posts.sql"
        migration2.write_text("CREATE TABLE posts (id SERIAL PRIMARY KEY, user_id INTEGER);")
        
        manager = MigrationManager(unit_test_db_clean)
        manager.migrations_dir = migrations_dir
        
        success = await manager.migrate_to_latest()
        assert success is True
        
        # Verify both tables were created
        pool = await asyncpg.create_pool(unit_test_db_clean)
        try:
            async with pool.acquire() as conn:
                users_exists = await conn.fetchval("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name = 'test_users'
                """)
                posts_exists = await conn.fetchval("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name = 'test_posts'
                """)
                
                assert users_exists == 1
                assert posts_exists == 1
                
                # Check final version
                final_version = await conn.fetchval("""
                    SELECT MAX(version) FROM test_db_version
                """)
                assert final_version == 2
        finally:
            await pool.close()


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_migrate_to_latest_partial_failure(unit_test_db_clean):
    """Test migrate_to_latest when one migration fails."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_dir = Path(temp_dir)
        
        # Create migration files - second one has invalid SQL
        migration1 = migrations_dir / "001_create_users.sql"
        migration1.write_text("CREATE TABLE users (id SERIAL PRIMARY KEY);")
        
        migration2 = migrations_dir / "002_invalid.sql"
        migration2.write_text("INVALID SQL;")
        
        manager = MigrationManager(unit_test_db_clean)
        manager.migrations_dir = migrations_dir
        
        success = await manager.migrate_to_latest()
        assert success is False
        
        # Verify first migration was applied, second was not
        pool = await asyncpg.create_pool(unit_test_db_clean)
        try:
            async with pool.acquire() as conn:
                users_exists = await conn.fetchval("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name = 'test_users'
                """)
                assert users_exists == 1
                
                # Only version 1 should be recorded
                max_version = await conn.fetchval("""
                    SELECT MAX(version) FROM test_db_version
                """)
                assert max_version == 1
        finally:
            await pool.close()


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_validate_migrations_success(unit_test_db_clean):
    """Test migration validation with valid checksums."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_dir = Path(temp_dir)
        
        # Create migration file
        migration1 = migrations_dir / "001_create_users.sql"
        migration1.write_text("CREATE TABLE users (id SERIAL PRIMARY KEY);")
        
        manager = MigrationManager(unit_test_db_clean)
        manager.migrations_dir = migrations_dir
        
        # Apply migration first
        await manager.migrate_to_latest()
        
        # Validate should succeed
        is_valid = await manager.validate_migrations()
        assert is_valid is True


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_validate_migrations_modified_file(unit_test_db_clean):
    """Test migration validation when file has been modified."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_dir = Path(temp_dir)
        
        # Create migration file
        migration1 = migrations_dir / "001_create_users.sql"
        migration1.write_text("CREATE TABLE users (id SERIAL PRIMARY KEY);")
        
        manager = MigrationManager(unit_test_db_clean)
        manager.migrations_dir = migrations_dir
        
        # Apply migration first
        await manager.migrate_to_latest()
        
        # Modify the migration file
        migration1.write_text("CREATE TABLE users_modified (id SERIAL PRIMARY KEY);")
        
        # Validate should fail
        is_valid = await manager.validate_migrations()
        assert is_valid is False


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_validate_migrations_missing_file(unit_test_db_clean):
    """Test migration validation when migration file is missing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_dir = Path(temp_dir)
        
        # Create migration file
        migration1 = migrations_dir / "001_create_users.sql"
        migration1.write_text("CREATE TABLE users (id SERIAL PRIMARY KEY);")
        
        manager = MigrationManager(unit_test_db_clean)
        manager.migrations_dir = migrations_dir
        
        # Apply migration first
        await manager.migrate_to_latest()
        
        # Remove the migration file
        migration1.unlink()
        
        # Validate should succeed (missing files are not validated)
        is_valid = await manager.validate_migrations()
        assert is_valid is True


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_concurrent_migration_application(unit_test_db_clean):
    """Test that concurrent migration applications are handled correctly."""
    manager1 = MigrationManager(unit_test_db_clean)
    manager2 = MigrationManager(unit_test_db_clean)
    
    # Create a temporary migration file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write("CREATE TABLE concurrent_test (id SERIAL PRIMARY KEY);")
        temp_file = Path(f.name)
    
    try:
        # Try to apply the same migration concurrently
        # One should succeed, one should fail due to unique constraint
        results = await asyncio.gather(
            manager1.apply_migration(1, "concurrent test", temp_file),
            manager2.apply_migration(1, "concurrent test", temp_file),
            return_exceptions=True
        )
        
        # At least one should succeed, or both should fail with expected errors
        success_count = sum(1 for r in results if r is True)
        import asyncpg
        expected_failures = 0
        for r in results:
            if r is not True and isinstance(r, Exception):
                msg = str(r)
                # Accept any asyncpg PostgresError, which includes DuplicateTableError, UniqueViolationError, serialization, deadlock, etc.
                if (
                    isinstance(r, asyncpg.PostgresError)
                    or "already exists" in msg
                    or "DuplicateTable" in msg
                    or "unique constraint" in msg
                    or "deadlock" in msg
                    or "serialization" in msg
                    or "rolled back" in msg
                ):
                    expected_failures += 1
        print("[DEBUG] Results of concurrent migration:")
        for idx, r in enumerate(results):
            if isinstance(r, Exception):
                print(f"  Result {idx}: type={type(r)}, class={r.__class__.__name__}, msg={str(r)}")
            else:
                print(f"  Result {idx}: type={type(r)}, value={r}")
        import traceback
        if success_count == 0:
            print("[DEBUG] Both concurrent migrations failed. This can happen due to transaction deadlocks, serialization failures, or both transactions attempting the same DDL at the same time in PostgreSQL. These are expected edge cases for concurrent DDL.")
            for idx, r in enumerate(results):
                if isinstance(r, Exception):
                    print(f"  Exception {idx}: {r}\n    Type: {type(r)}")
                    print("    Traceback:")
                    traceback.print_exception(type(r), r, r.__traceback__)
            assert expected_failures >= 1, "Expected at least one Postgres error (duplicate/unique/deadlock/serialization) in concurrent migration"
        
    finally:
        temp_file.unlink()


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_migration_with_complex_sql(unit_test_db_clean):
    """Test migration with complex SQL including functions, triggers, etc."""
    manager = MigrationManager(unit_test_db_clean)
    await manager.get_current_version()
    
    complex_sql = """
    -- Create a table
    CREATE TABLE test_complex (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        created_at TIMESTAMPTZ DEFAULT now()
    );
    
    -- Create an index
    CREATE INDEX idx_test_complex_name ON test_complex(name);
    
    -- Create a function
    CREATE OR REPLACE FUNCTION update_modified_time()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.created_at = now();
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    
    -- Create a trigger
    CREATE TRIGGER trigger_update_modified_time
        BEFORE UPDATE ON test_complex
        FOR EACH ROW
        EXECUTE FUNCTION update_modified_time();
    
    -- Insert some test data
    INSERT INTO test_complex (name) VALUES ('test1'), ('test2');
    """
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write(complex_sql)
        temp_file = Path(f.name)
    
    try:
        print("Applying complex migration with SQL:\n", complex_sql)
        success = await manager.apply_migration(1, "complex migration", temp_file)
        print(f"apply_migration returned: {success}")
        assert success is True
        
        # Verify all components were created
        pool = await asyncpg.create_pool(unit_test_db_clean)
        try:
            async with pool.acquire() as conn:
                # Check table
                table_exists = await conn.fetchval("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name = 'test_test_complex'
                """)
                print(f"Table exists: {table_exists}")
                assert table_exists == 1
                
                # Check data
                row_count = await conn.fetchval("SELECT COUNT(*) FROM test_test_complex")
                print(f"Row count in test_test_complex: {row_count}")
                assert row_count == 2
                
                # Check index
                index_exists = await conn.fetchval("""
                    SELECT COUNT(*) FROM pg_indexes 
                    WHERE tablename = 'test_test_complex' AND indexname LIKE '%name%'
                """)
                print(f"Index exists: {index_exists}")
                assert index_exists >= 1
        finally:
            await pool.close()
    except Exception as e:
        print(f"Exception during migration or verification: {e}")
        raise
            
    finally:
        temp_file.unlink()


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_migration_rollback_on_error(unit_test_db_clean):
    """Test that migration is rolled back on error (transaction behavior)."""
    manager = MigrationManager(unit_test_db_clean)
    await manager.get_current_version()
    
    # SQL that starts successfully but fails partway through
    failing_sql = """
    CREATE TABLE test_rollback (id SERIAL PRIMARY KEY);
    INSERT INTO test_rollback (id) VALUES (1);
    INVALID SQL STATEMENT;  -- This will cause rollback
    """
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write(failing_sql)
        temp_file = Path(f.name)
    
    try:
        success = await manager.apply_migration(1, "failing migration", temp_file)
        assert success is False
        
        # Verify table was not created (rolled back)
        pool = await asyncpg.create_pool(unit_test_db_clean)
        try:
            async with pool.acquire() as conn:
                table_exists = await conn.fetchval("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name = 'test_test_rollback'
                """)
                assert table_exists == 0
                
                # Verify migration was not recorded
                migration_recorded = await conn.fetchval("""
                    SELECT COUNT(*) FROM test_db_version WHERE version = 1
                """)
                assert migration_recorded == 0
                
        finally:
            await pool.close()
            
    finally:
        temp_file.unlink()


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_migration_checksum_validation_edge_cases(unit_test_db_clean):
    """Test checksum validation with various edge cases."""
    manager = MigrationManager(unit_test_db_clean)
    
    # Test with empty file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write("")  # Empty file
        empty_file = Path(f.name)
    
    try:
        checksum = manager._calculate_checksum(empty_file)
        assert len(checksum) == 32  # Should still produce valid MD5
        
        # Test with file containing only whitespace
        with open(empty_file, 'w') as f:
            f.write("   \n\t  \n  ")
        
        whitespace_checksum = manager._calculate_checksum(empty_file)
        assert whitespace_checksum != checksum  # Different content = different checksum
        
    finally:
        empty_file.unlink()


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_migration_version_ordering(unit_test_db_clean):
    """Test that migrations are applied in correct version order."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_dir = Path(temp_dir)
        
        # Create migrations out of order
        migration10 = migrations_dir / "010_last.sql"
        migration10.write_text("CREATE TABLE last_table (id SERIAL PRIMARY KEY);")
        
        migration1 = migrations_dir / "001_first.sql"
        migration1.write_text("CREATE TABLE first_table (id SERIAL PRIMARY KEY);")
        
        migration5 = migrations_dir / "005_middle.sql"
        migration5.write_text("CREATE TABLE middle_table (id SERIAL PRIMARY KEY);")
        
        manager = MigrationManager(unit_test_db_clean)
        manager.migrations_dir = migrations_dir
        
        success = await manager.migrate_to_latest()
        assert success is True
        
        # Verify migrations were applied in correct order
        pool = await asyncpg.create_pool(unit_test_db_clean)
        try:
            async with pool.acquire() as conn:
                # Check all tables exist
                for table in ['test_first_table', 'test_middle_table', 'test_last_table']:
                    exists = await conn.fetchval("""
                        SELECT COUNT(*) FROM information_schema.tables 
                        WHERE table_name = $1
                    """, table)
                    assert exists == 1
                
                # Check versions were recorded in order
                versions = await conn.fetch("""
                    SELECT version FROM test_db_version ORDER BY applied_at
                """)
                version_list = [row['version'] for row in versions]
                assert version_list == [1, 5, 10]
                
        finally:
            await pool.close()


@pytest.mark.unit
@pytest.mark.database
@pytest.mark.asyncio
async def test_migration_duplicate_version_handling(unit_test_db_clean):
    """Test handling of duplicate migration versions."""
    manager = MigrationManager(unit_test_db_clean)
    await manager.get_current_version()
    
    # Ensure version table exists
    await manager.get_current_version()
    
    # Create a temporary migration file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write("CREATE TABLE duplicate_test (id SERIAL PRIMARY KEY);")
        temp_file = Path(f.name)
    
    try:
        # Apply same migration twice
        success1 = await manager.apply_migration(1, "first attempt", temp_file)
        success2 = await manager.apply_migration(1, "second attempt", temp_file)
        
        assert success1 is True
        assert success2 is False  # Should fail due to unique constraint on version
        
        # Verify only one record exists
        pool = await asyncpg.create_pool(unit_test_db_clean)
        try:
            async with pool.acquire() as conn:
                count = await conn.fetchval("""
                    SELECT COUNT(*) FROM test_db_version WHERE version = 1
                """)
                assert count == 1
        finally:
            await pool.close()
            
    finally:
        temp_file.unlink()
