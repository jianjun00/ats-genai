"""
Test Database Management for Unit and Integration Tests.

This module provides:
- Isolated test database setup/teardown for unit tests
- Shared integration test database management
- Test data fixtures and cleanup utilities
- Environment-aware test table management
"""

import asyncio
import asyncpg
import os
import pytest
import pytest_asyncio
import uuid
import hashlib
from typing import Dict, List, Optional, Any
from contextlib import asynccontextmanager
from src.config.environment import Environment, EnvironmentType
from src.db.migration_manager import MigrationManager
import logging
logger = logging.getLogger(__name__)
from dateutil import parser as date_parser
import datetime
import gin
from config.database import Database

@gin.configurable
class TestDatabaseManager:
    """Manages test databases for unit and integration tests."""

    
    def __init__(self, test_type: str = "unit", run_migrations: bool = True, database_obj: Database = None):
        print(f"[GIN DEBUG] TestDatabaseManager __init__: test_type={test_type}, database_obj={database_obj}")
        """
        Initialize test database manager.
        
        Args:
            test_type: "unit" or "integration"
            run_migrations: If False, do not apply migrations after DB creation (default: True)
            database_obj: Optional, a Database object to use as the test database (used for DB name override)
        """
        # Use the appropriate environment based on test type
        logger.debug(f"TestDatabaseManager: test_type={test_type}, run_migrations={run_migrations}, database_obj={database_obj}")
        self.test_type = test_type
        if database_obj:
            # Override DB name in URL
            url = database_obj.get_database_url()
            import re
            # Replace last path segment with database_name
            self.db_url = re.sub(r"(?<=/)[^/]+$", database_obj.database, url)
            print(f"[GIN DEBUG] TestDatabaseManager using database_name override: {database_obj.database} → db_url={self.db_url}")
        else:
            self.db_url = None  # Will be set below
        if test_type == "integration":
            if self.db_url is None:
                # Default integration DB URL
                from intg_tests.db.test_intg_db_base_intg import get_test_db_url
                self.db_url = get_test_db_url()
            self.env = Environment(EnvironmentType.INTEGRATION, db_url=self.db_url)
        else:
            if self.db_url is None:
                raise ValueError("TestDatabaseManager requires database_obj for unit tests to supply db_url.")
            self.env = Environment(env_type=EnvironmentType.TEST, db_url=self.db_url)
        # Extract table prefix from environment
        sample_table = self.env.get_table_name("sample")
        self.table_prefix = sample_table.replace("sample", "")
        self.run_migrations = run_migrations
        # test_db_suffix logic moved to fixture 

    
    async def setup_test_database(self) -> str:
        """
        Set up test database with latest schema (unless run_migrations is False).
        
        Returns:
            Database URL for the test database
        """
        if self.test_type == "unit":
            # Create isolated database for unit test
            db_config = self.env.get_database_config()
            test_db_name = db_config['database']
            print(f"[DEBUG] Creating test DB: {test_db_name}")
            await self._create_test_database(test_db_name)
            test_db_url = self.db_url
            print(f"[DEBUG] Using test DB URL: {test_db_url}, test_db_name: {test_db_name}")
            # Run migrations after DB creation
            if self.run_migrations:
                print(f"[DEBUG] Running migrations on test DB: {test_db_name}")
                migration_manager = MigrationManager(self.db_url)
                await migration_manager.migrate_to_latest()
                # --- DEBUG: Check table visibility with asyncpg right after migration ---
                import asyncpg
                try:
                    pool = await asyncpg.create_pool(self.db_url)
                    async with pool.acquire() as conn:
                        tables = await conn.fetch("""
                            SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename
                        """)
                        print(f"[DEBUG] (asyncpg) Tables in DB after migration: {[t['tablename'] for t in tables]}")
                    await pool.close()
                except Exception as e:
                    print(f"[DEBUG] (asyncpg) Could not list tables after migration: {e}")
                # --- END DEBUG ---
        else:
            # Use shared integration database
            test_db_url = self.db_url
        
        print(f"[DEBUG] TestDatabaseManager.run_migrations = {self.run_migrations}")
        return test_db_url
    
    async def teardown_test_database(self):
        """Clean up test database after test completion."""
        if self.test_type == "unit":
            # Drop the entire test database
            db_config = self.env.get_database_config()
            base_db_name = db_config['database']
            test_db_name = f"{base_db_name}"
            print(f"[DEBUG] Dropping test DB: {test_db_name}")
            await self._drop_test_database(test_db_name)
        else:
            # For integration tests, clean up test data but keep schema
            await self.cleanup_test_data()
    
    async def _create_test_database(self, db_name: str):
        """Create a new test database."""
        # Connect to postgres database to create new database
        db_config = self.env.get_database_config()
        base_db_name = db_config['database']
        postgres_url = self.db_url.replace(f"/{base_db_name}", "/postgres")
        print(f"[DEBUG] Connecting to postgres DB: {postgres_url}")
        pool = await asyncpg.create_pool(postgres_url)
        try:
            async with pool.acquire() as conn:
                # Drop DB if it exists (for clean test isolation)
                exists = await conn.fetchval(
                    "SELECT 1 FROM pg_database WHERE datname = $1", db_name
                )
                if exists:
                    # Debug: List tables before drop
                    print(f"[DEBUG] Database '{db_name}' exists. Checking tables before drop...")
                    try:
                        temp_db_url = self.db_url.replace(f"/{base_db_name}", f"/{db_name}")
                        temp_pool = await asyncpg.create_pool(temp_db_url)
                        async with temp_pool.acquire() as temp_conn:
                            tables = await temp_conn.fetch("""
                                SELECT tablename FROM pg_tables WHERE schemaname = 'public'
                            """)
                            print(f"[DEBUG] Tables in '{db_name}' before drop: {[t['tablename'] for t in tables]}")
                    except Exception as e:
                        print(f"[DEBUG] Could not list tables in '{db_name}': {e}")
                    # Terminate connections and drop
                    await conn.execute(f'''SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{db_name}' AND pid <> pg_backend_pid()''')
                    await conn.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
                await conn.execute(f'CREATE DATABASE "{db_name}"')
                # Debug: List tables after creation (should be empty)
                try:
                    temp_db_url = self.db_url.replace(f"/{base_db_name}", f"/{db_name}")
                    temp_pool = await asyncpg.create_pool(temp_db_url)
                    async with temp_pool.acquire() as temp_conn:
                        tables = await temp_conn.fetch("""
                            SELECT tablename FROM pg_tables WHERE schemaname = 'public'
                        """)
                        print(f"[DEBUG] Tables in '{db_name}' after creation: {[t['tablename'] for t in tables]}")
                except Exception as e:
                    print(f"[DEBUG] Could not list tables in '{db_name}' after creation: {e}")
        finally:
            await pool.close()
    
    async def _drop_test_database(self, db_name: str):
        """Drop a test database."""
        import asyncpg
        db_config = self.env.get_database_config()
        base_db_name = db_config['database']
        postgres_url = self.db_url.replace(f"/{base_db_name}", "/postgres")
        print(f"[DEBUG][DROP_DB] About to drop test DB: {db_name}")
        try:
            pool = await asyncpg.create_pool(postgres_url)
            try:
                async with pool.acquire() as conn:
                    # Terminate active connections to the database
                    await conn.execute("""
                        SELECT pg_terminate_backend(pid)
                        FROM pg_stat_activity
                        WHERE datname = $1 AND pid <> pg_backend_pid()
                    """, db_name)
                    # Drop the database
                    await conn.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
            finally:
                await pool.close()
        except asyncpg.exceptions.InvalidCatalogNameError as e:
            # Ignore if database does not exist (including pool creation)
            print(f"[DEBUG] Database '{db_name}' does not exist or could not connect, ignoring drop: {e}")
    
    async def cleanup_test_data(self):
        """Clean up test data from integration test database."""
        logger.debug(f"Cleaning up test data from {self.db_url}")
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    # Define cleanup order (respecting foreign key constraints)
                    cleanup_tables = [
                        'universe_membership',
                        'instrument_aliases',
                        'instrument_metadata',
                        'daily_prices',
                        'daily_prices_tiingo',
                        'daily_prices_polygon',
                        'daily_market_cap',
                        'fundamentals',
                        'events',
                        'instruments',
                        'instrument_polygon',
                        'universe',
                        'vendors'
                    ]
                    
                    for table in cleanup_tables:
                        # Only clean up test data, not reference data
                        if table in ['status_code']:  # Skip reference tables
                            continue
                        
                        full_table_name = f"{self.table_prefix}{table}"
                        logger.debug(f"Deleted test data from {full_table_name}")
                        await conn.execute(f"DELETE FROM {full_table_name}")
                        # Reset sequences for tables with serial primary keys
                        if table in ['vendors', 'instruments', 'universe', 'events']:
                            await conn.execute(f"""
                                SELECT setval(pg_get_serial_sequence('{full_table_name}', 'id'), 1, false)
                            """)
        finally:
            await pool.close()
    
    async def backup_tables(self, tables: List[str]):
        """Backup tables by copying their contents to *_backup tables."""
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                for table in tables:
                    full_table_name = f"{self.table_prefix}{table}"
                    backup_table_name = f"{full_table_name}_backup"
                    # Drop backup table if exists, then create
                    await conn.execute(f"DROP TABLE IF EXISTS {backup_table_name}")
                    await conn.execute(f"CREATE TABLE {backup_table_name} AS TABLE {full_table_name}")
        finally:
            await pool.close()

    async def restore_tables(self, tables: List[str]):
        """Restore tables from their *_backup tables and drop the backups."""
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                for table in tables:
                    full_table_name = f"{self.table_prefix}{table}"
                    backup_table_name = f"{full_table_name}_backup"
                    # Truncate original table and restore from backup
                    await conn.execute(f"TRUNCATE {full_table_name} RESTART IDENTITY CASCADE")
                    await conn.execute(f"INSERT INTO {full_table_name} SELECT * FROM {backup_table_name}")
                    await conn.execute(f"DROP TABLE IF EXISTS {backup_table_name}")
        finally:
            await pool.close()

    async def load_test_fixtures(self, fixtures: Dict[str, List[Dict[str, Any]]]):
        """
        Load test data fixtures into the database.
        Converts string date/datetime fields to Python objects for asyncpg compatibility.
        Args:
            fixtures: Dictionary mapping table names to lists of row data
        """
        date_fields = {"date", "start_at", "end_at", "created_at", "updated_at"}
        tables = list(fixtures.keys())
        await self.backup_tables(tables)
        pool = await asyncpg.create_pool(self.db_url)
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    for table_name, rows in fixtures.items():
                        if not rows:
                            continue
                        full_table_name = f"{self.table_prefix}{table_name}"
                        columns = list(rows[0].keys())
                        placeholders = ', '.join(f'${i+1}' for i in range(len(columns)))
                        column_names = ', '.join(columns)
                        insert_sql = f"""
                            INSERT INTO {full_table_name} ({column_names})
                            VALUES ({placeholders})
                        """
                        for row in rows:
                            values = []
                            for col in columns:
                                val = row[col]
                                if col in date_fields and isinstance(val, str):
                                    try:
                                        dt = date_parser.parse(val)
                                        if dt.time() == datetime.time(0, 0):
                                            val = dt.date()
                                        else:
                                            val = dt
                                    except Exception:
                                        pass
                                values.append(val)
                            # Debug logging
                            logger.debug(f"[DEBUG] Inserting row into table: {full_table_name}")
                            logger.debug(f"[DEBUG] insert_sql: {insert_sql}")
                            logger.debug(f"[DEBUG] columns: {columns}")
                            logger.debug(f"[DEBUG] values: {values}")
                            logger.debug(f"[DEBUG] value types: {[type(v) for v in values]}")
                            try:
                                await conn.execute(insert_sql, *values)
                            except Exception as e:
                                logger.error(f"[ERROR] Exception inserting into {full_table_name}: {e}")
                                logger.error(f"[ERROR] insert_sql: {insert_sql}")
                                logger.error(f"[ERROR] values: {values}")
                                logger.error(f"[ERROR] value types: {[type(v) for v in values]}")
                                import traceback
                                logger.error(traceback.format_exc())
                                raise
        finally:
            await pool.close()


class IntegrationTestSession:
    """Manages shared state for integration test sessions."""
    
    _instance = None
    _db_manager = None
    _test_db_url = None
    _session_id = None
    
    @classmethod
    async def get_instance(cls):
        """Get or create the singleton integration test session."""
        if cls._instance is None:
            cls._instance = cls()
            cls._session_id = uuid.uuid4().hex[:8]
            cls._db_manager = TestDatabaseManager("integration")
            cls._test_db_url = await cls._db_manager.setup_test_database()
        return cls._instance
    
    @classmethod
    async def cleanup(cls):
        """Clean up the integration test session."""
        if cls._db_manager:
            await cls._db_manager.teardown_test_database()
            cls._instance = None
            cls._db_manager = None
            cls._test_db_url = None
            cls._session_id = None
    
    @property
    def db_url(self) -> str:
        """Get the test database URL."""
        return self._test_db_url
    
    @property
    def session_id(self) -> str:
        """Get the session ID for this test run."""
        return self._session_id


@pytest_asyncio.fixture(scope="session")
async def integration_test_db():
    """Fixture for integration tests - provides shared database for session."""
    session = await IntegrationTestSession.get_instance()
    
    yield session.db_url
    
    # Cleanup happens at session end


@pytest_asyncio.fixture
async def clean_integration_db(integration_test_db):
    """Fixture that cleans integration database before each test."""
    session = await IntegrationTestSession.get_instance()
    await session._db_manager.cleanup_test_data()
    
    yield integration_test_db


@asynccontextmanager
async def test_database_context(test_type: str = "unit"):
    """
    Context manager for test database setup/teardown.
    
    Usage:
        async with test_database_context("unit") as db_url:
            # Use db_url for testing
    """
    db_manager = TestDatabaseManager(test_type)
    test_db_url = await db_manager.setup_test_database()
    
    try:
        yield test_db_url
    finally:
        await db_manager.teardown_test_database()


# Example test fixtures for common test data

import pytest_asyncio
import pytest
import asyncpg
from pathlib import Path
from src.db.migration_manager import MigrationManager
from src.config.environment import Environment, EnvironmentType

@pytest_asyncio.fixture
async def unit_test_db(request):
    """
    Fixture for a completely empty unit test database (no tables, no migrations applied).
    Ensures environment is TEST. Used for DAO and non-migration tests.
    Generates a unique test DB name per test and passes it to TestDatabaseManager.
    """
    import gin
    import uuid
    from config.database import Database
    test_file = str(request.fspath) if hasattr(request, 'fspath') else "nofile"
    test_file_base = os.path.splitext(os.path.basename(test_file))[0]
    test_file_base = ''.join(c for c in test_file_base if c.isalnum())
    test_name = request.node.name if hasattr(request, 'node') else None
    if test_name:
        hash_part = hashlib.sha1(test_name.encode('utf-8')).hexdigest()[:8]
        truncated = ''.join(c for c in test_name if c.isalnum())[:8]
        db_name = f"test_db_{test_file_base}_{truncated}_{hash_part}"
    else:
        db_name = f"test_db_{test_file_base}_{uuid.uuid4().hex[:8]}"
 
    # Construct Database object for this test
    db_obj = Database(
        host="localhost",
        port=5432,
        user="postgres",
        password="password",
        database=db_name,
        base_database=db_name,
        pool_min_size=1,
        pool_max_size=10,
        command_timeout=60
    )
    db_manager = gin.get_configurable(TestDatabaseManager)("unit", database_obj=db_obj)
    test_db_url = await db_manager.setup_test_database()

    migration_manager = MigrationManager(test_db_url)
    print(f"[DEBUG] Applying migrations to {test_db_url}")    # Apply migrations to ensure latest schema for both unit and integration tests
    await migration_manager.migrate_to_latest()

    yield test_db_url
    await db_manager.teardown_test_database()
    # Clean up backup/dump files created for this test DB
    backup_dir = Path(__file__).parent / "../db/migrations/backups"
    if backup_dir.exists():
        pattern = f"{db_name}_*.dump"
        for dump_file in backup_dir.glob(pattern):
            try:
                dump_file.unlink()
            except Exception:
                pass

@pytest_asyncio.fixture
async def unit_test_db_clean(request):
    """
    Fixture for a completely empty unit test database (no tables).
    Ensures environment is TEST. Used for DB migration and isolation tests.
    Generates a unique test DB name per test and passes it to TestDatabaseManager.
    """
    import gin
    import uuid
    from config.database import Database
    test_file = str(request.fspath) if hasattr(request, 'fspath') else "nofile"
    test_file_base = os.path.splitext(os.path.basename(test_file))[0]
    test_file_base = ''.join(c for c in test_file_base if c.isalnum())
    test_name = request.node.name if hasattr(request, 'node') else None
    if test_name:
        hash_part = hashlib.sha1(test_name.encode('utf-8')).hexdigest()[:8]
        truncated = ''.join(c for c in test_name if c.isalnum())[:8]
        db_name = f"test_db_{test_file_base}_{truncated}_{hash_part}"
    else:
        db_name = f"test_db_{test_file_base}_{uuid.uuid4().hex[:8]}"
    # Construct Database object for this test
    db_obj = Database(
        host="localhost",
        port=5432,
        user="postgres",
        password="password",
        database=db_name,
        base_database=db_name,
        pool_min_size=1,
        pool_max_size=10,
        command_timeout=60
    )
    db_manager = gin.get_configurable(TestDatabaseManager)("unit", database_obj=db_obj)
    test_db_url = await db_manager.setup_test_database()

    # Drop all tables for a clean DB
    import asyncpg
    conn = await asyncpg.connect(test_db_url)
    tables = await conn.fetch("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema='public' AND table_type='BASE TABLE';
    """)
    for row in tables:
        await conn.execute(f'DROP TABLE IF EXISTS "{row["table_name"]}" CASCADE;')
    await conn.close()

    yield test_db_url
    await db_manager.teardown_test_database()
    # Clean up backup/dump files created for this test DB
    backup_dir = Path(__file__).parent / "../db/migrations/backups"
    if backup_dir.exists():
        pattern = f"{db_name}_*.dump"
        for dump_file in backup_dir.glob(pattern):
            try:
                dump_file.unlink()
            except Exception:
                pass

@pytest.fixture(scope="session")
def test_env():
    """Fixture to provide a test Environment instance with universe_id set."""
    env = Environment(EnvironmentType.TEST)
    return env

SAMPLE_FIXTURES = {
    "vendors": [
        {
            "name": "test_vendor",
            "description": "Test data vendor",
            "website": "https://test.com",
            "api_key_env_var": "TEST_API_KEY"
        }
    ],
    "instruments": [
        {
            "symbol": "TEST",
            "name": "Test Stock",
            "exchange": "NYSE",
            "type": "stock",
            "currency": "USD",
            "active": True
        }
    ],
    "daily_prices": [
        {
            "date": "2024-01-01",
            "symbol": "TEST",
            "open": 100.0,
            "high": 105.0,
            "low": 99.0,
            "close": 103.0,
            "volume": 1000000
        }
    ]
}
