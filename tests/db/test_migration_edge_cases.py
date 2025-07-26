"""
Comprehensive unit tests for database migration edge cases.
Tests cover error conditions, validation, and various migration scenarios.
Uses proper async fixture handling.
"""

import pytest
import asyncio
import asyncpg
import os
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch
from src.db.migration_manager import MigrationManager
from src.db.test_db_manager import TestDatabaseManager
from config.environment import set_environment, EnvironmentType
import pytest_asyncio

@pytest_asyncio.fixture
def pristine_test_db(request):
    """
    Fixture factory for a pristine (no migrations) test database per test function.
    Usage:
        @pytest.mark.asyncio
        async def test_something(pristine_test_db):
            db_url = await pristine_test_db()
            ...
    """
    async def factory():
        test_name = request.node.name if hasattr(request, 'node') else None
        db_manager = TestDatabaseManager("unit", test_name=test_name, run_migrations=False)
        db_url = await db_manager.setup_test_database()
        # Teardown logic must be handled by the test (or addfinalizer if needed)
        return db_url
    return factory


@pytest_asyncio.fixture
async def isolated_test_db(request):
    """
    Fixture for isolated test database per test function.
    Args:
        request: pytest fixture request object (automatically provided)
    Usage:
        @pytest.mark.asyncio
        async def test_something(isolated_test_db):
            ...
    """
    set_environment(EnvironmentType.TEST)
    test_name = request.node.name if hasattr(request, 'node') else None
    db_manager = TestDatabaseManager("unit", test_name=test_name, run_migrations=False)
    test_db_url = await db_manager.setup_test_database()
    yield test_db_url
    await db_manager.teardown_test_database()


@pytest_asyncio.fixture
async def isolated_test_db_migrate(request):
    """
    Fixture for isolated test database per test function.
    Args:
        request: pytest fixture request object (automatically provided)
    Usage:
        @pytest.mark.asyncio
        async def test_something(isolated_test_db):
            ...
    """
    set_environment(EnvironmentType.TEST)
    test_name = request.node.name if hasattr(request, 'node') else None
    db_manager = TestDatabaseManager("unit", test_name=test_name, run_migrations=True)
    test_db_url = await db_manager.setup_test_database()
    yield test_db_url
    await db_manager.teardown_test_database()

@pytest.mark.asyncio
async def test_migration_manager_basic_functionality(isolated_test_db):
    """Test basic migration manager functionality."""
    manager = MigrationManager(isolated_test_db)
    
    # Test initialization
    assert manager.db_url == isolated_test_db
    assert manager.table_prefix == "test_"
    assert manager.migrations_dir.name == "migrations"
    
    # Test getting current version from empty database
    version = await manager.get_current_version()
    assert version == -1


@pytest.mark.asyncio
async def test_table_prefix_application(isolated_test_db):
    """Test that table prefixes are correctly applied to SQL."""
    manager = MigrationManager(isolated_test_db)
    
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


@pytest.mark.asyncio
async def test_table_prefix_no_double_prefixing(isolated_test_db):
    """Test that already prefixed tables don't get double-prefixed."""
    manager = MigrationManager(isolated_test_db)
    
    sql = "CREATE TABLE IF NOT EXISTS test_events (id SERIAL PRIMARY KEY);"
    prefixed_sql = manager._apply_table_prefixes(sql)
    
    # Should not become test_test_events
    assert "test_test_events" not in prefixed_sql
    assert "test_events" in prefixed_sql


@pytest.mark.asyncio
async def test_checksum_calculation(isolated_test_db):
    """Test checksum calculation for migration files."""
    manager = MigrationManager(isolated_test_db)
    
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


@pytest.mark.asyncio
async def test_apply_migration_success(pristine_test_db):
    """Test successful migration application on pristine DB."""
    db_url = await pristine_test_db()
    manager = MigrationManager(db_url)
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
        pool = await asyncpg.create_pool(db_url)
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


@pytest.mark.asyncio
async def test_apply_migration_sql_error(pristine_test_db):
    """Test migration application with SQL error on pristine DB."""
    db_url = await pristine_test_db()
    manager = MigrationManager(db_url)
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
        pool = await asyncpg.create_pool(db_url)
        try:
            async with pool.acquire() as conn:
                version_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM test_db_version WHERE version = 1
                """)
                all_versions = await conn.fetch("SELECT * FROM test_db_version")
                print(f"[DEBUG] test_db_version contents: {all_versions}")
                assert version_count == 0
        finally:
            await pool.close()
            
    finally:
        temp_file.unlink()


@pytest.mark.asyncio
async def test_migration_rollback_on_error(isolated_test_db):
    """Test that migration is rolled back on error (transaction behavior)."""
    manager = MigrationManager(isolated_test_db)
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
        pool = await asyncpg.create_pool(isolated_test_db)
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


@pytest.mark.asyncio
async def test_migrate_to_latest_with_multiple_migrations(isolated_test_db):
    """Test migrate_to_latest with multiple migrations."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_dir = Path(temp_dir)
        
        # Create migration files
        migration1 = migrations_dir / "001_create_users.sql"
        migration1.write_text("CREATE TABLE users (id SERIAL PRIMARY KEY);")
        
        migration2 = migrations_dir / "002_create_posts.sql"
        migration2.write_text("CREATE TABLE posts (id SERIAL PRIMARY KEY, user_id INTEGER);")
        
        manager = MigrationManager(isolated_test_db)
        manager.migrations_dir = migrations_dir
        
        success = await manager.migrate_to_latest()
        assert success is True
        
        # Verify both tables were created
        pool = await asyncpg.create_pool(isolated_test_db)
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


@pytest.mark.asyncio
async def test_migrate_to_latest_partial_failure(isolated_test_db):
    """Test migrate_to_latest when one migration fails."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_dir = Path(temp_dir)
        
        # Create migration files - second one has invalid SQL
        migration1 = migrations_dir / "001_create_users.sql"
        migration1.write_text("CREATE TABLE users (id SERIAL PRIMARY KEY);")
        
        migration2 = migrations_dir / "002_invalid.sql"
        migration2.write_text("INVALID SQL;")
        
        manager = MigrationManager(isolated_test_db)
        manager.migrations_dir = migrations_dir
        
        success = await manager.migrate_to_latest()
        assert success is False
        
        # Verify first migration was applied, second was not
        pool = await asyncpg.create_pool(isolated_test_db)
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


@pytest.mark.asyncio
async def test_validation_with_modified_file(isolated_test_db):
    """Test migration validation when file has been modified."""
    with tempfile.TemporaryDirectory() as temp_dir:
        migrations_dir = Path(temp_dir)
        
        # Create migration file
        migration1 = migrations_dir / "001_create_users.sql"
        migration1.write_text("CREATE TABLE users (id SERIAL PRIMARY KEY);")
        
        manager = MigrationManager(isolated_test_db)
        manager.migrations_dir = migrations_dir
        
        # Apply migration first
        await manager.migrate_to_latest()
        
        # Modify the migration file
        migration1.write_text("CREATE TABLE users_modified (id SERIAL PRIMARY KEY);")
        
        # Validate should fail
        is_valid = await manager.validate_migrations()
        assert is_valid is False


@pytest.mark.asyncio
async def test_migration_version_ordering(isolated_test_db):
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
        
        manager = MigrationManager(isolated_test_db)
        manager.migrations_dir = migrations_dir
        
        success = await manager.migrate_to_latest()
        assert success is True
        
        # Verify migrations were applied in correct order
        pool = await asyncpg.create_pool(isolated_test_db)
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


@pytest.mark.asyncio
async def test_duplicate_version_handling(pristine_test_db):
    """Test handling of duplicate migration versions on pristine DB."""
    db_url = await pristine_test_db()
    manager = MigrationManager(db_url)
    await manager.get_current_version()
    
    # Create a temporary migration file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write("CREATE TABLE duplicate_test (id SERIAL PRIMARY KEY);")
        temp_file = Path(f.name)
    
    try:
        # Apply same migration twice
        success1 = await manager.apply_migration(1, "first attempt", temp_file)
        success2 = await manager.apply_migration(1, "second attempt", temp_file)
        
        assert success1 is True, "First migration application should succeed"
        assert success2 is False, "Second migration application should fail due to duplicate version"
        
        # Verify only one record exists
        pool = await asyncpg.create_pool(db_url)
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


@pytest.mark.asyncio
async def test_complex_sql_migration(pristine_test_db):
    """Test migration with complex SQL including functions, triggers, etc. on pristine DB."""
    db_url = await pristine_test_db()
    manager = MigrationManager(db_url)
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
    
    -- Insert some test data
    INSERT INTO test_complex (name) VALUES ('test1'), ('test2');
    """
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
        f.write(complex_sql)
        temp_file = Path(f.name)
    
    try:
        success = await manager.apply_migration(1, "complex migration", temp_file)
        assert success is True
        
        # Verify all components were created
        pool = await asyncpg.create_pool(db_url)
        try:
            async with pool.acquire() as conn:
                # Check table
                table_exists = await conn.fetchval("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name = 'test_test_complex'
                """)
                assert table_exists == 1
                
                # Check data
                row_count = await conn.fetchval("SELECT COUNT(*) FROM test_test_complex")
                assert row_count == 2
                
                # Check index
                index_exists = await conn.fetchval("""
                    SELECT COUNT(*) FROM pg_indexes 
                    WHERE tablename = 'test_test_complex' AND indexname LIKE '%name%'
                """)
                assert index_exists >= 1
                
        finally:
            await pool.close()
            
    finally:
        temp_file.unlink()


@pytest.mark.asyncio
async def test_empty_migration_directory(isolated_test_db):
    """Test migrate_to_latest when no migrations are available."""
    # Create temporary empty migration directory
    with tempfile.TemporaryDirectory() as temp_dir:
        manager = MigrationManager(isolated_test_db)
        manager.migrations_dir = Path(temp_dir)
        
        success = await manager.migrate_to_latest()
        assert success is True


@pytest.mark.asyncio
async def test_checksum_edge_cases(isolated_test_db):
    """Test checksum validation with various edge cases."""
    manager = MigrationManager(isolated_test_db)
    
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
